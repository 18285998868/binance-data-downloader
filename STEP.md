# 代码执行步骤

## 模块一：启动与参数

1. `crates/binance-data-cli/src/main.rs` 初始化 tracing：`RUST_LOG` 缺省为 `info`。
2. `cli::Cli` 解析参数：`-c/--config` 必填，`--verify` 可选。
3. 组装 `RunOptions`，调用 `binance_data_core::run_with_options`。
4. `run_with_options` 创建 `CancelToken`、安装 Ctrl-C 处理器，再以 `Endpoints::live()` 进入 `run_pipeline`。
5. `run_pipeline` 顺序执行五步：`config::load` → `prepare::run` → `start_dates::resolve` → `download::run` → `report::finish`。

```bash
cargo run --release -- -c config.toml
cargo run --release -- -c config.toml --verify
```

## 模块二：配置加载

1. 异步读取 TOML 文件。
2. 反序列化为 `RawConfig`；`#[serde(deny_unknown_fields)]` 拒绝未知键。
3. `Config::try_from(RawConfig)` 做语义校验与归一化：
   - `market` / `symbol` / `dataset` 必填；
   - `symbol` 转大写、排序、去重，字符集限制为 `[A-Za-z0-9_]`；
   - `[[dataset]]` 展开成 `(type, period, interval)` 的 `DownloadJob`；
   - 重复 job、非法 `concurrency`、非法 `not_found_pct_threshold`、`start > end` 均直接报错。
4. 默认值：`output_dir = "."`、`concurrency = 64`、`end = 昨天（UTC）`；未来的 `end` 截到昨天。
5. 业务代码只接触字段私有的 `Config`。

```bash
cp config/00.example.toml config.toml
cargo test -p binance-data-core config
```

## 模块三：运行准备

1. `RunLock::acquire` 在 `{output_dir}/.cache/run.lock` 上取 OS 独占锁；阻塞 I/O 走 `spawn_blocking`，已有实例运行时立即失败。
2. `Cache::new(output_dir)` 把缓存根设为 `{output_dir}/.cache`，按域懒加载 JSON 文档。
3. `client::build` 建共享 HTTP client：连接超时 10s、读超时 60s、整请求超时 300s、连接池空闲 30s。
4. `scan_local_files` 遍历输出目录：
   - 每 16K 条目分批，避免把整棵树读进内存；
   - 为已存在的 `.zip` 记录相对路径的 128 位哈希；
   - 清理本工具产生且超过 5 分钟的 `{name}.zip.{pid}.tmp`。
5. 用 `RLIMIT_NOFILE` 计算并发上限：`(soft - 64) / 2`，夹在 `1..=1024`；低于配置值时告警。
6. 计算 `end = cfg.resolved_end(Utc::now().date_naive())`，复制 `symbols` / `jobs` 到 `PreparedRun`，记录批启动日志。

```bash
ls -la ../data/binance-smoke/.cache
```

## 模块四：起始日解析

1. 每个 symbol 以最多 8 个并发执行两条独立请求，`tokio::join!` 保证互不阻塞：
   - 市场 API：`klines?symbol=...&interval=1m&limit=1&startTime=0`，取首根 1m K 线日期；缓存域 `start_date`，键 `{market}:{symbol}`；
   - 归档清单：对每个 job 请求 S3 `max-keys=1`，取该前缀下最早的 key；缓存域 `archive_start`。
2. 两个缓存 TTL 均为 30 天；损坏条目按 miss 处理并重新获取。
3. 按 job 合并两个来源：
   - 普通数据集取 `max(api_date, archive_date)`；
   - `indexPriceKlines` 优先归档日期（指数可能早于合约）。
4. 实际起点为 `max(config.start, symbol_start)`；未配置 `config.start` 时直接使用 symbol 起点。
5. 失败降级：
   - API 返回 `-1121` 未知 symbol：致命，计入失败；
   - 其他 API 错误：归档清单覆盖全部 job 则继续；否则有 `config.start` 时把缺失 job 标记为 degraded 并从该日续跑；两者都没有则跳过该 symbol 并计入失败。
6. Ctrl-C 会停止新的查询，并丢弃在途请求。

```bash
jq . ../data/binance-smoke/.cache/start_date.json
jq . ../data/binance-smoke/.cache/archive_start.json
```

## 模块五：下载循环

1. `download::run` 按 `symbol × job` 串行；每个 pair 开始前检查取消标志。
2. 起始日解析失败的 symbol 跳过其全部 pair，但仍计入最终失败。
3. 单 pair（`download/pair.rs`）：
   - 起点按 period 对齐：daily 用当天，monthly 向下取整到月初；
   - 终点只取完整档期：monthly 不请求未完成的当月；
   - `start > end` 记录告警并跳过。
4. 任务构建（`download/tasks.rs` 与 `download/tasks/urls.rs`）：
   - URL 与落盘相对路径都直接使用归档路径 `data/{market}/{period}/{kind}/{symbol}/...`；
   - 生成器是迭代器，长区间不会先物化整张表；
   - 用本地文件哈希集过滤已存在的 zip。
5. 单 pair 内通过 `buffer_unordered(concurrency)` 有界并发；并发值已在 prepare 阶段按文件描述符上限裁剪。
6. 单文件（`download/fetch/stream.rs`）：
   - 写入 `{dest}.{pid}.tmp`，失败或取消由 `TempFileGuard` 立即删除；
   - 在 120s 重试窗口内重试瞬时错误，并遵守 `Retry-After`；404 永久失败且不重试；
   - 校验 zip magic 与 `Content-Length`；`--verify` 额外打开 zip 中央目录；
   - `sync_all` 后原子 `rename` 到目标路径，再 fsync 父目录。
7. 404 计入 `not_found`，不算失败；取消计入 `cancelled`，且不重置熔断计数。
8. 连续 5 个 pair 失败触发熔断，提前结束批处理并提示重跑续传。

```bash
find ../data/binance-smoke -name '*.zip' | head
```

## 模块六：报告与退出码

1. `report::finish` 汇总 pair 统计与起始日错误。
2. 404 只告警；degraded 只告警并列出 `symbol/job@start`。
3. 满足任一条件即判失败：
   - 有 symbol 起始日解析失败；
   - 有 pair 失败；
   - 404 占比达到 `not_found_pct_threshold`（整批被取消且未完成时不应用该阈值）。
4. 失败返回 `AppError::Data(FailureReport)`；起始日与 pair 两个来源各自最多保留 20 条明细。
5. 退出码：
   - `0`：成功；或用户取消且没有其他失败（已下载文件保持有效，可重跑续传）；
   - `1`：配置、I/O、网络或数据失败；
   - `2`：命令行参数错误（clap 默认行为）。

```bash
RUST_LOG=info ./target/release/binance-data-downloader -c config.toml; echo "exit=$?"
```

## 模块七：独立校验器

1. `binance-data-verify --dir <输出目录> [--jobs N]` 只校验已有文件，不下载、不改文件。
2. `verify_tree` 要求 `--dir` 是目录；`--jobs 0` 使用可用 CPU 数，其他值夹在 `1..=available*4`。
3. rayon 线程池内用 `walkdir` 遍历（不跟随符号链接），只处理 `.zip`（扩展名不区分大小写）。
4. 每个文件调用 `verify_zip`：只读中央目录，要求可解析且至少有一个条目。
5. 失败项排序后输出到 stderr，存在失败退出 `1`；目录不存在直接报错；没有 zip 时打印提示并以 `0` 退出。

```bash
./target/release/binance-data-verify --dir ../data/binance-smoke
./target/release/binance-data-verify --dir ../data/binance-smoke --jobs 8
```

## 模块八：验收

1. 跑静态门禁、测试与文档。
2. 用固定 7 天窗口的冒烟配置验证完整链路，输出写到仓库外的隔离目录。
3. 重复执行，验证幂等与缓存命中。
4. 独立校验产物。

```bash
./scripts/check.sh
RUST_LOG=info cargo run --release -- -c config/00.smoke.toml
RUST_LOG=info cargo run --release -- -c config/00.smoke.toml
./target/release/binance-data-verify --dir ../data/binance-smoke
```

## 模块九：代码地图与延伸阅读

1. 五步 pipeline 与模块树：见 [`docs/architecture.md`](./docs/architecture.md)。
2. 起始日、重试、原子落盘、退出码细节：见 [`docs/reliability.md`](./docs/reliability.md)。
3. 配置字段与校验规则：见 [`docs/config.md`](./docs/config.md)。
4. 落盘路径与月档语义：见 [`docs/datasets.md`](./docs/datasets.md)。
5. 开发流程与质量门禁：见 [`docs/development.md`](./docs/development.md)。

```bash
rg -n 'run_pipeline|resolve_start_dates|run_downloads|fn finish' crates
```

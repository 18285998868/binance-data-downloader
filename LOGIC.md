# binance-data-downloader 处理流程

下载 Binance USDT-M 永续合约日度 K 线归档(`https://data.binance.vision/data/futures/um/daily/klines/...`),每文件为一个 `symbol × timeframe × 日期` 的 zip。

## 入口

```text
main.rs
  └─ cli::parse()              # 解析 --config/-c <path>
     └─ app::run(&config)      # 主流程
```

`--config` 为必填项;核心逻辑全部在 `app::run` 编排。

## 主流程 `app::run`

```text
1. AppConfig::load(path)     # TOML 解析 + 校验(deny_unknown_fields)
2. 构建 reqwest::Client       # UA / 30s timeout / pool_max_idle_per_host=256
3. scan_local_files(data_dir) # 一次性扫描本地 .zip, 顺带清理残留 .tmp
4. 计算 end = cfg.end 或昨天   # cfg.end 缺省 → 本地昨天
5. for symbol (串行)
     for timeframe (串行)
       run_one(...)           # 一个 (symbol, tf) 配对
6. 若有配对失败 → 聚合报错(AppError::Data), 最多保留 20 条明细
```

**并发模型**:配对(层)串行,配对内并发(symbol × timeframe 双循环 × 配对内 `buffer_unordered(concurrency)`)。总同时进行的下载数严格等于 `concurrency`,不做全局信号量。

## 配对下载 `run_one`

```text
resolve_start(...)          # 解析实际起始日期;解析失败则该配对报错
if start > end → 跳过该配对
tasks = build_download_tasks(symbol, tf, start, end, local, output_dir)
if tasks 非空:
    stats = run_downloads::run_downloads(client, tasks, cfg.concurrency)
    # 有界并发 + 增量统计
else:
    # 无待下载文件(全部已缓存或范围为空)
```

### `resolve_start`

```text
sym_start = get_first_kline_date(symbol, tf)   # 交易所首根 K 线日期(带缓存)
实际 start = max(配置 start, sym_start)         # 配置 start 缺省则直接用 sym_start
```

- 避免向交易所请求交易所首根 K 线之前不存在的日期文件。
- 查询走缓存:`.cache/start_date/start_date.redb`,key 为 `symbol:tf`,命中则不请求 `fapi.binance.com`。

## 任务构建 `build_download_tasks`

```

```text
build_urls(symbol, tf, start, end)   # 逐日生成 DownloadUrl
    ├─ rel_path = futures/um/daily/klines/{pair}/{tf}/{pair}-{tf}-{date}.zip
    └─ url      = https://data.binance.vision/data/{rel_path}
then:
    过滤 local(已存在的 rel_path)           # 跳过已下载
    → 映射为 DownloadTask{ url, dest=output_dir/rel_path, pair, tf }
```

每对任务量为日期跨度(天)数,O(日数) 内存,可接受。

## 有界并发下载 `run_downloads::run_downloads`

```text
run_downloads_inner(client, tasks, concurrency, download_fn)
  tasks ─流式─> buffer_unordered(concurrency) ─增量统计─> DownloadStats{total, ok, not_found, failed}
```

- 最多 `concurrency` 个 future 同时轮询(缺省 16,`concurrency.max(1)` 兜底)。
- **增量计数**:`while let Some(result) = stream.next()` 边完成边累加,不 `collect` 全量结果。
- 每完成 100 个打一条进度:`done/total · ok/not_found/failed`。
- **失败隔离**:单个文件失败仅计数,继续处理其余任务;最终 `failed > 0` 才返回 `AppError::DownloadFailed("{failed}/{total} failed")`。
- **404 不视为失败**:计入 `not_found`(预期出现在首/尾边界日期)。
- 失败明细日志最多打印 20 条,超出部分汇总计数。

## 单文件下载 `download_to_file(steps/06_run_downloads/stream.rs)`

```text
GET url ──> 2xx? ──> 写入 <dest>.zip.tmp(流式逐 chunk) ──> flush ──> rename → <dest>.zip
     |        非 2xx
     |          ├─ 404        → 永久失败(NotFound), 不重试
     |          └─ 其他状态码 → 临时失败(Transient), 重试
     |        网络/读流错误    → 临时失败, 重试
```

**原子性**:先写 `.zip.tmp` 再 rename,避免半截文件;被中断留下的 `.tmp` 由 `scan_local_files` 下次运行清理。

## 重试与退避

- `backoff::default_backoff()`:`ExponentialBackoff`, `max_elapsed_time = 30s`,其余默认。
- 临时失败(网络错误、HTTP 5xx)套重试;永久失败(404、JSON/IO 错误)直接返回不重试。

## 本地文件扫描 `scan_local_files`

- `spawn_blocking` 中 `walkdir` 遍历 `output_dir`(CPU/IO 密集,不阻塞异步运行时)。
- 收集 `.zip`(以相对路径为 key)供 `build_download_tasks` 过滤。
- 顺带删除 `.tmp` 残留,并记 warn。

## 错误分级

| 层级 | 类型 | 说明 |
|---|---|---|
| 内部库逻辑 | `AppError`(thiserror) | HTTP/IO/JSON/配置/API/日期/下载/Join 各类变体 |
| 应用层 | `anyhow` | `main.rs` 给 `app::run` 加 `.context(...)` 上下文 |
| panic | 无(生产路径) | 客户端构建等已改为 `?` 传播;`unwrap` 仅存在于测试 |

**终止语义**:
- 配置非法(空 symbol/timeframe、`concurrency=0`、未知字段、日期格式错)→ 启动即失败。
- 某配对解析首根 K 线失败 → 该配对报错,继续其他配对。
- 某配对内文件下载失败(非 404)→ 该配对报错(`failed/total failed`),文件级失败不中止同批其余任务。
- 批处理结束:有任一配对失败 → 整体返回 `AppError::Data("{n} pair(s) failed: ...")`。

## 关键配置项与默认值

见 `config/config.example.toml`。字段:`symbol`(必填)、`timeframe`(必填,须显式全列)、`start`/`end`(可选 `YYYY-MM-DD`)、`output_dir`(缺省 `data`)、`concurrency`(缺省 `16`)。

## 模块职责速查

源码按流水线步骤归档于 `src/steps/NN_动词_名词.rs`,磁盘名带数字前缀,`lib.rs` 用
`#[path]` 将模块名映射为可读的步骤名(`cli`/`config`/`scan_local_files`/`get_start_date`/
`build_download_tasks`/`run_downloads`/`app`),对外 API(`bdd::cli`、`bdd::app`)不变。

```text
src/main.rs                     入口, anyhow context
src/lib.rs                      #[path] 模块声明 + DownloadTask 定义 + re-export
src/error.rs                    支撑: AppError(thiserror)
src/cache.rs                    支撑: redb 通用缓存(read/write/TTL)
src/backoff.rs                  支撑: 默认指数退避(max 30s)

steps/01_cli.rs                 cli:  clap: --config/-c (必填)
steps/02_config.rs              config: AppConfig 加载 + validate
steps/03_scan_local_files.rs    scan_local_files: 扫描已有 .zip + 清理 .tmp
steps/04_get_start_date.rs      get_start_date: 交易所首根 K 线日期(API + redb 缓存)
steps/05_build_download_tasks.rs build_download_tasks: 逐日生成 URL → 过滤本地已有 → DownloadTask
steps/06_run_downloads/mod.rs   run_downloads: buffer_unordered 有界并发 + 增量统计 + 进度/失败上限
steps/06_run_downloads/stream.rs run_downloads: 单文件流式下载: .tmp 原子写 + 重试退避
steps/07_app.rs                 app: 编排(配置 → client → 扫描 → 串行配对 → run_one)
                                        + resolve_start + 批统计
```
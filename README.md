# binance-data-downloader

## 项目目标

> 从 [Binance Public Data](https://data.binance.vision/?prefix=data/futures/um/daily/klines/) 下载永续合约 K 线数据文件（保持原始交易所文件格式）
>
> 网址：https://data.binance.vision
> - futures/
>   - cm/
>   - um/
>     - daily/
>       - aggTrades/
>       - bookDepth/
>       - bookTicker/
>       - indexPriceKlines/
>       - klines/
>         - {symbol}/
>           - {timeframe}/
>             - {symbol}-{timeframe}-{yyyy-mm-dd}.zip
>       - markPriceKlines/
>       - metrics/
>       - premiumIndexKlines/
>       - trades/
>       - monthly/
>   - option/
>   - spot/

## 技术选型

- rust，edition = "2024"，MSRV 1.88

## 快速开始

```bash
cargo build --release
RUST_LOG=info ./target/release/binance-data-downloader -c config.test.toml
```

- 日志走 `tracing`，由 `RUST_LOG` 控制（缺省 `info`）。
- 已存在的 `.zip` 会跳过，因此**重复执行是安全的**，可用于增量补全。
- 运行中 `Ctrl-C` 可随时中断，重跑即从中断处续传（详见 [中断语义](#中断与并发)）。

## 冒烟配置

```toml
symbol = ["BTCUSDT"] # 必填，字符集 [A-Z0-9_]（自动转大写）
timeframe = ["1d"] # ["1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d"]
# start = "2020-01-01" # 可选
# end   = "2025-01-01" # 可选
# output_dir = "data" # 可选
# concurrency = 64 # 可选
```

完整字段说明见 `config.example.toml`。

## 架构设计

```
main.rs          入口：初始化 tracing、解析 -c/--config、调用 bdd::run()
lib.rs           外观导出（run / Cli / AppError，其余模块私有）
cli.rs           clap 参数
config.rs        TOML 直解 + 校验 + 归一化（Timeframe / AppConfig / DownloadTask）
error.rs         thiserror 错误类型 AppError
backoff.rs       重试策略与 Retry-After 钳制
cache.rs         redb 通用 KV 缓存（值带 8 字节时间戳）
cancel.rs        取消信号（watch::Sender），pair 循环与在途下载共享
lock.rs          输出目录单实例锁（O_EXCL + PID 存活性判定）
scan.rs          本地文件扫描（分批流式）与孤儿 tmp 清理
start_date.rs    币种首根 K 线日期获取与缓存（fapi + redb）
app/
  mod.rs         流水线编排
  urls.rs        下载 URL 生成
  tasks.rs       下载任务构建（剔除本地已有）
  download/
    mod.rs       有界并发扇出与统计
    stream.rs    单文件下载、重试、内容校验、原子落盘
tests/
  public_api.rs  公开接口的集成测试
```

### 可测试性

`ARCHIVE_BASE` 与 `FAPI_BASE` 是常量，但两者都以**参数形式**向下传递（`build_urls` /
`build_download_tasks` / `get_first_kline_date`），汇聚到 `app::Endpoints`。

生产路径固定用 `Endpoints::live()`；`#[cfg(test)]` 的 `run_with_endpoints()` 把两个 base
指向同一个 `wiremock` 服务，因此**编排逻辑全部可测，且不新增任何用户配置项**。
`cancel` token 同样由 `run_inner` 接收，所以取消分支也能从测试触发。

据此覆盖的编排分支：完整下载、404 容忍、永久下载失败、起始日降级（D4）、
起始日不可解析、未知币种、取消中断（D3）、并发锁拒绝（D5）、
`resolve_start_dates` 的成功/降级/失败三态归类。

> 设计取舍见「起始日按 `1m` 解析」一节——把 base 提为参数**不等于**按周期分别解析起始日，
> D1 的取舍保持不变。

## 完整配置

```toml
symbol = ["BTCUSDT"] # 必填，非空，字符集 [A-Z0-9_]（USDⓈ-M 季度合约形如 BTCUSDT_250926）；自动转大写并排序去重
timeframe = ["1d"] # 必填，非空，Timeframe 枚举 12 值 ["1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d"]，自动排序去重
# start = "2020-01-01" # 可选，缺省=币种首日；与 end 双写且 start>end 直接报错
# end = "2025-01-01" # 可选，缺省昨天；未来值截到昨天
# output_dir = "data" # 可选，缺省 "data"；数据根目录，程序自动追加 binance/data/um/daily/klines/...，不要手写进去
# concurrency = 64 # 可选，缺省 64；取值 0 < c ≤ 1024；单个 pair 内的最大并发下载数
```

- 未知键拒收（`deny_unknown_fields`，拼错即报错）。
- 所有配置错误都带配置文件路径（由 `AppError::Config.path` 承载，`Display` 只渲染一次）。
- `concurrency` 只作用于**单个 pair 内部**；pair 与 pair 之间始终串行。

## 完整执行逻辑

1. **加载并校验配置** — 读文件 → `toml` 解析 → 校验（`symbol`/`timeframe` 非空、`symbol` 字符集 `[A-Z0-9_]`、`concurrency` 满足 `0 < c ≤ 1024`、双 `Some` 且 `start>end` 报错）。
2. **取输出目录单实例锁**（`lock.rs`）— 已有活跃运行则直接失败退出（见 [中断与并发](#中断与并发)）。
3. **建全局 `reqwest::Client`** — UA / 连接超时 10s / 读超时 60s / 连接池每 host 256。
4. **扫描本地文件**（`scan.rs`）— 一次遍历，得到相对路径的 128-bit 哈希集合；同遍清理本工具样式的 `.tmp`（`{name}.zip.{pid}.tmp`）。
5. **解析 `end`** — `min(config.end, 昨天)`，缺省昨天（archive 按 UTC 分区，所以用 UTC 今天）。
6. **并发起始日期** — `buffer_unordered(8)`，每币一次 fapi 请求，结果按 `{symbol}:1m` 缓存 30 天。
7. **双层串行循环 `symbol × timeframe`**，每个 pair：
   1. `cfg.start > end` → 跳过；
   2. 起始日 = `max(cfg.start, 币种首日)`；
   3. 解析后起始日 `> end` → 跳过（币上市晚于区间）；
   4. 逐日组 URL 并剔除本地已有，生成 tasks；
   5. 非空则 `buffer_unordered(concurrency)` 并发下载，打印 pair 级统计。
8. **汇总** — 零失败 `Ok`；否则 `Err(Data(...))`，分别列出 symbol 级与 pair 级失败数及明细（≤ 20 条）。

## 关键设计决策

### 起始日按 `1m` 解析，适用于全部周期（已知取舍）

起始日只按 `Timeframe::M1` 查一次（fapi 的 klines 接口对各周期回答一致），
循环内对所有周期复用。代价是**高周期归档的起始日可能晚于首根分钟线**，
多出来的那些天必然 404。

实测（BTCUSDT）：

| 事实 | 值 |
| --- | --- |
| fapi 首根 `1m` K 线 | 2019-09-08 |
| 首个存在的 `1d` 归档 | 2019-12-31 |
| 2019-09-08 的 `1d` 归档 | HTTP 404 |

一次实测运行（本地已有 2019-12-31 ～ 2026-10-16 的 `1d` 数据）生成了 115 个任务，
其中 114 个是上述必然失败的日期，1 个是当天尚未发布的归档 —— 全部 404，退出码 0。

这是**有意接受**的：按周期分别解析会把前置阶段的请求量从 N 次放大到 N×12，
而多出来的 404 不影响数据正确性（见下节 404 语义），下游 `kline-reader` 另有 gap 检查兜底。

### 404 语义：容忍，不改变退出码

- 404 单次即止，不重试，计入 `not_found` 而**非**失败。
- pair 级与批量级各打一条 WARN，带数量与占比。
- 即使整批 100% 404，退出码仍为 0。

理由：币种不是在区间内每天都上市（起始日之前必然 404），当天的归档尚未发布也必然 404。
这两类是常态，不应与"网络故障"混为一谈。**若需要 CI 判定，请检查 WARN 日志中的 `pct`。**

### 起始日获取失败的降级（不计入失败）

| 情况 | 行为 |
| --- | --- |
| fapi 瞬态失败（限速 / 抖动），且配置给了 `start` | 降级用 `config.start`，逐币 WARN，汇总再打一条带币种列表的 WARN；**不计入失败，退出码 0** |
| 币种未知（HTTP 400 且 body `code=-1121`） | 硬失败，该币种整组跳过，计入失败，退出码 1 |
| fapi 失败且未配置 `start` | 无可退让，计入失败 |

理由：降级是**可恢复**路径——程序确实继续下载了，只是最早几天可能缺失。
把它算作失败会让"带告警地完成"和"真的坏了"无法区分，因此降级只告警，失败才 `exit != 0`。

### 中断与并发

**Ctrl-C**：一个取消信号同时被两处观察——

- pair 循环顶部：不再调度新的 pair；
- 每个在途下载：`tokio::select!` 竞速取消信号，立刻放弃请求。

因此中断是**即时**的，不必等当前 pair（可达数千文件）跑完。实测 SIGINT 后 0 秒退出。
取消不算失败：退出码 0，已落盘的文件有效，重跑即续传。
被放弃的下载会留下 `{name}.zip.{pid}.tmp`，下次运行的扫描会清理
（PID 已不存在且超过 5 分钟存活窗口）。

**并发运行：当前禁止。** 同一 `output_dir` 上第二个进程会以退出码 1 失败，并打印
持有锁的 PID。原因是它们会争抢同一个 `redb` 缓存文件（`Database::create` 会阻塞等文件锁），
以及同一批目标文件。锁以 `O_EXCL` 创建，只有在持有者**可证明已消失**（Linux 上读 `/proc/<pid>`；
否则退化为 mtime 超过 5 分钟）时才会被回收。
多运行并发支持在后续版本提供。

## 可靠性机制

| 风险 | 处理 |
| --- | --- |
| `config.start` 早于币种上市日 → 大量 404 | 按币取一次首根日期（fapi + redb 缓存 30 天，位于 `{output_dir}/.cache/`），起始取 `max(币种首日, config.start)` |
| `config.start` 晚于结束日 → 空任务 | 配置节双 `Some` 且 `start>end` 直接报错；运行时 `cfg.start>end` 与 `resolved>end` 两次跳过 |
| `config.end` 超过今天 → 数据未发布 → 404 | `AppConfig::resolved_end` 一次取 `min(config.end, 昨天)`，下游只消费 |
| 本地已有文件 → 重复下载 | 循环前扫描一次，建 task 时剔除，重复执行安全 |
| 网络故障 | 指数退避重试，单文件总预算 120s |
| 404 | 单次即止，记 `not_found`，不重试、不算失败 |
| `Retry-After` 特别大 | 钳制到剩余重试窗口，窗口耗尽即硬停 |
| 服务端 200 返回 HTML 错误页 | 校验 zip 魔数 `PK\x03\x04`（跨 chunk 累积校验），不符即重试 |
| 响应体被截断 | 与 `Content-Length` 校验，不符即重试 |
| 系统性故障持续 | 连续 5 个 pair 失败即熔断中止本轮，重跑续传 |
| `concurrency` 手滑写成极大值 | 校验 `0 < concurrency ≤ 1024` |
| 配置写重复或写错 | `symbol/timeframe` 排序去重；未知键拒收；错误带文件路径 |
| 缓存值损坏 | 按未命中处理（WARN 并重取），不中断下载 |
| 缓存读取失败 | 按未命中处理（WARN），不中断下载 |
| 同目录并发运行 | 单实例锁，直接失败退出 |
| 上次运行崩溃残留 `.tmp` | 扫描时按 PID + mtime 判定，回收死进程留下的 |

## 开发

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

- `clippy` 启用了 `correctness/suspicious/style/complexity/perf` + `pedantic`，
  并额外打开 `missing_docs`。
- 内部 API 一律 `pub(crate)`，对外只暴露 `run` / `Cli` / `AppError`。
- 测试覆盖：配置校验边界、URL/任务生成、缓存 TTL 与并发、扫描分批与 tmp 清理、
  zip 魔数跨 chunk（自建 chunked HTTP 服务）、取消信号、重试与 `Retry-After` 钳制、
  fapi 响应的各种畸形形态，以及整套编排分支（见「可测试性」）。

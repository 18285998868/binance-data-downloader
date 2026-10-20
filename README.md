# binance-data-downloader

## 项目目标

> 从 [Binance Public Data](https://data.binance.vision/?prefix=data/futures/um/daily/klines/) 下载永续合约 K 线数据文件（保持原始交易所文件格式）
>
> 网址：<https://data.binance.vision>
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

- rust，edition = "2024"，**永远用最新 stable**（`rust-toolchain.toml` 固定 `channel = "stable"`）
  - 刻意**不声明** `rust-version`：`rust-version` 是"最低支持版本"的下限声明，与"永远用最新
    stable"直接冲突（它会拦截比声明更新的 API，且需要随每次 stable 升级手工跟进）。
    详见 `Cargo.toml` 内的注释与 `creats/vector-ta/Cargo.toml` 的同款说明。
  - 进到本目录直接 `cargo build` 即可，工具链自动选最新 stable，无需手工指定 `+stable`。

## 快速开始

```bash
cargo build --release
RUST_LOG=info ./target/release/binance-data-downloader -c config.test.toml

# 可选：额外校验每个归档的 zip 结构（多一遍读盘）
RUST_LOG=info ./target/release/binance-data-downloader -c config.test.toml --verify
```

> 工具链由 `rust-toolchain.toml` 固定为最新 stable（见「技术选型」），无需手工指定版本。

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

```text
main.rs          入口：初始化 tracing、解析 -c/--config/--verify、调用 bdd::run_with_options()
lib.rs           外观导出（run / RunOptions / Cli / AppError / FailureReport，其余模块私有）
cli.rs           clap 参数
config.rs        TOML 直解 + 校验 + 归一化（Timeframe / AppConfig / DownloadTask）
error.rs         thiserror 错误类型 AppError + 结构化 FailureReport
backoff.rs       重试策略与 Retry-After 钳制
cache.rs         redb 通用 KV 缓存（值带 8 字节时间戳，支持批量写）
cancel.rs        取消信号（watch::Sender），预取阶段、pair 循环与在途下载共享
limits.rs        进程限制（RLIMIT_NOFILE）→ 实际可承受的并发
lock.rs          输出目录单实例锁（O_EXCL + PID 存活性判定，异步 tokio::fs）
scan.rs          本地文件扫描（分批流式、ahash 路径哈希）与孤儿 tmp 清理
start_date.rs    币种首根 K 线日期获取与缓存（fapi + redb）
archive.rs       归档 S3 清单：某 symbol×周期的真实首日（roxmltree 解析 XML）
app/
  mod.rs         流水线编排
  start_dates.rs 起始日解析：两个来源并发取、逐周期精确、组合与降级策略
  urls.rs        下载 URL 生成（迭代器，按需分配）
  tasks.rs       下载任务构建（剔除本地已有）
  download/
    mod.rs       有界并发扇出与统计（失败明细进 FailureReport）
    stream.rs    单文件下载、重试、内容校验、原子落盘、可选 zip 结构校验
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
`resolve_start_dates` 的成功/降级/失败三态归类、
配置起始日晚于结束日时跳过 pair、币种上市日晚于结束日时跳过 pair、
连续 pair 失败触发熔断中止、运行前即已取消时不调度任何 pair、
运行中取消后不再调度下一个 pair、404 占比达到/未达到阈值。

起始日解析本身在 `app/start_dates.rs` 里，策略可单独单测：
归档清单救场（API 失败但清单有数据）、逐周期各自的精确首日、
两者都无且无 `config.start` 时该币种失败。

> 设计取舍见「起始日取自归档自己的清单」一节。

## 完整配置

```toml
symbol = ["BTCUSDT"] # 必填，非空，字符集 [A-Z0-9_]（USDⓈ-M 季度合约形如 BTCUSDT_250926）；原样保留用于报错回显，运行时使用转大写并排序去重后的视图
timeframe = ["1d"] # 必填，非空，Timeframe 枚举 12 值 ["1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d"]，自动排序去重
# start = "2020-01-01" # 可选，缺省=币种首日；与 end 双写且 start>end 直接报错
# end = "2025-01-01" # 可选，缺省昨天；未来值截到昨天
# output_dir = "data" # 可选，缺省 "data"；数据根目录，程序自动追加 binance/data/um/daily/klines/...，不要手写进去
# concurrency = 64 # 可选，缺省 64；取值 0 < c ≤ 1024；单个 pair 内的最大并发下载数（实际值还会被进程的 fd 上限收紧）
# not_found_pct_threshold = 50.0 # 可选，缺省无（404 永不改变退出码）；取值 0 < t ≤ 100，本轮 404 占比达到 t 即判失败
```

- 未知键拒收（`deny_unknown_fields`，拼错即报错）。
- 所有配置错误都带配置文件路径（由 `AppError::Config.path` 承载，`Display` 只渲染一次）。
- `concurrency` 只作用于**单个 pair 内部**；pair 与 pair 之间始终串行。
- `concurrency` 是**请求值**而非承诺：每个在途下载占 2 个 fd（socket + 临时文件），
  实际并发会被 `RLIMIT_NOFILE` 收紧（见「中断与并发」），收紧时打一条 WARN。

## 完整执行逻辑

1. **加载并校验配置** — 读文件 → `toml` 解析 → 校验（`symbol`/`timeframe` 非空、`symbol` 字符集 `[A-Z0-9_]`、`concurrency` 满足 `0 < c ≤ 1024`、`not_found_pct_threshold` 满足 `0 < t ≤ 100`、双 `Some` 且 `start>end` 报错）。
2. **取输出目录单实例锁**（`lock.rs`）— 已有活跃运行则直接失败退出（见 [中断与并发](#中断与并发)）。
3. **建全局 `reqwest::Client`** — UA / 连接超时 10s / 读超时 60s / **整体请求超时 300s** / 连接池每 host 256。
4. **扫描本地文件**（`scan.rs`）— 一次遍历，得到相对路径的 128-bit 哈希集合（ahash，固定种子）；同遍清理本工具样式的 `.tmp`（`{name}.zip.{pid}.tmp`）。
5. **解析 `end`** — `min(config.end, 昨天)`，缺省昨天（archive 按 UTC 分区，所以用 UTC 今天）。
6. **并发起始日期** — `buffer_unordered(8)`，每币**并发**发两类请求：一次 fapi（证明币种存在）+ 每周期一次归档清单（取该周期的真实首日）；两个来源互不依赖，结果缓存 30 天。
7. **双层串行循环 `symbol × timeframe`**，每个 pair：
   1. `cfg.start > end` → 跳过；
   2. 起始日 = `max(cfg.start, 币种首日)`；
   3. 解析后起始日 `> end` → 跳过（币上市晚于区间）；
   4. 逐日组 URL 并剔除本地已有，生成 tasks；
   5. 非空则 `buffer_unordered(concurrency)` 并发下载，打印 pair 级统计。
8. **汇总** — 零失败 `Ok`；否则 `Err(Data(FailureReport))`，分别列出 symbol 级与 pair 级失败数及明细（≤ 20 条）。

## 关键设计决策

### 起始日取自归档自己的清单，而不是交易所 API

起始日有两个来源，取**较晚**的那个：

| 来源 | 回答的问题 | 请求量 |
| --- | --- | --- |
| fapi `klines`（`1m`） | 币种**什么时候开始交易** | 每币 1 次 |
| 归档 S3 清单 | 归档**什么时候开始发布** | 每币每周期 1 次 |

之所以要第二个来源：fapi 报告的是"首根 K 线"，归档发布却更晚。最老的币种两者相差
**114 天**（BTCUSDT：fapi 2019-09-08，归档 2019-12-31），若只按 fapi 日期起跑，这 114 天
必然全部 404。

实测（2026-10-20，对 Binance 公开接口直接探测）：

| 事实 | 值 |
| --- | --- |
| fapi 首根 `1m` / `5m` / `1h` / `4h` / `1d` K 线 | **全部**为 2019-09-08 |
| 归档首日（`1m` / `5m` / `1h` / `4h` / `1d`） | **全部**为 2019-12-31 |
| 2019-09-08 的 `1m` 归档 | HTTP 404 |
| 2019-09-08 的 `1d` 归档 | HTTP 404 |

三点结论：

1. **按周期分别查 fapi 没有收益。** fapi 对所有周期返回同一个日期，多发 N×11 次请求拿到的
   答案一模一样 —— 所以 fapi 侧"只按 `1m` 解析"不是近似，而是**唯一正确**的做法。
2. **归档首日与周期无关。** 全部周期都从同一天开始发布（BTCUSDT/ETHUSDT/SOLUSDT/
   1000PEPEUSDT/DOGEUSDT 均如此）。404 的根因不是"高周期更晚"，而是
   **fapi 首根 K 线早于归档首日** 这段归档发布缺口。
3. **归档清单是唯一能回答"归档什么时候有数据"的地方。** S3 清单按 key 排序，
   `max-keys=1` 返回的就是**最早**的文件（单次请求 ~650 字节 XML）：
   `https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&max-keys=1&prefix=data/futures/um/daily/klines/{symbol}/{tf}/`

实现要点：

- **两个来源并发取，互不依赖。** fapi 被限速（429）或抖动时，只要归档清单答了，该币种
  依然能解析出**精确**起始日 —— 归档里有文件本身就证明了币种存在。反之清单为空
  （归档还没发布这个币）才退回 fapi 日期，行为与改造前完全一致。
- **逐周期各自的精确首日。** 每个 `(symbol, timeframe)` 用自己的清单首日，不用一个
  共享近似值：若某币的 `1d` 归档比 `1m` 晚，共用早的日期会让 `1d` 多出一批必然 404
  的请求 —— 而 `not_found_pct_threshold` 会把这种**由实现选择造成**的 404 读成一次
  失败。
- 清单请求失败（网络/解析）→ WARN 后退回 fapi 日期，**不**让运行失败。
- 清单结果按 `{symbol}:{timeframe}` 缓存 30 天，**一个币的多个周期合并成一次事务写入**
  （redb 每次提交都是一次 fsync）；"为空"**不缓存**（归档每天都在发布）。
- fapi 仍然保留：它是"币种是否存在"的判据（`-1121` 未知币种依然是硬失败）。

实测效果：BTCUSDT `1d`、`start=2019-01-01`、`end=2020-01-05` 的一次真实运行，
`not_found` 从 **114 → 0**，落盘 6 个文件，退出码 0。

一次实测运行（本地已有 2019-12-31 ～ 2026-10-16 的 `1d` 数据）曾生成 115 个任务，
其中 114 个是必然失败的日期 —— 全部 404，退出码 0。**改造后这 114 个 404 已被消除**
（起始日自动校正到归档首日）。

### 404 语义：默认容忍，可用阈值改为失败

- 404 单次即止，不重试，计入 `not_found` 而**非**失败。
- pair 级与批量级各打一条 WARN，带数量与占比。
- 默认（未配 `not_found_pct_threshold`）：即使整批 100% 404，退出码仍为 0。
- 配了 `not_found_pct_threshold = t`：本轮 404 占比**达到** `t` 即判失败，退出码 1，
  错误信息形如 `not-found share 50.0% reached the configured threshold of 50.0%`。

理由：币种不是在区间内每天都上市（起始日之前必然 404），当天的归档尚未发布也必然 404。
这两类是常态，不应与"网络故障"混为一谈。但**"几乎全 404"往往意味着区间或币种配错了**，
所以给了阈值开关，让 CI 可以按需把"覆盖不全"升级为失败，而不必去 grep WARN 日志。

### 起始日获取失败的降级（不计入失败）

| 情况 | 行为 |
| --- | --- |
| fapi 瞬态失败（限速 / 抖动），但归档清单答了 | 用清单的精确首日，**不降级、不计入失败** |
| fapi 瞬态失败，且配置给了 `start` | 降级用 `config.start`，逐币 WARN，汇总再打一条带币种列表的 WARN；**不计入失败，退出码 0** |
| 币种未知（HTTP 400 且 body `code=-1121`） | 硬失败，该币种整组跳过，计入失败，退出码 1 |
| fapi 失败、清单也为空、且未配置 `start` | 无可退让，计入失败 |
| 归档清单请求失败 / 解析失败 | WARN 后退回 fapi 日期，**不**计入失败（行为与改造前一致） |
| 归档清单为空（币种刚上市，归档还没发布） | 退回 fapi 日期，**不**计入失败 |

理由：降级是**可恢复**路径——程序确实继续下载了，只是最早几天可能缺失。
把它算作失败会让"带告警地完成"和"真的坏了"无法区分，因此降级只告警，失败才 `exit != 0`。

### 中断与并发

**Ctrl-C**：一个取消信号同时被三处观察——

- 起始日预取阶段：不再发起新的查询；
- **pair 循环**（`symbol × timeframe` 的内层）：不再调度新的 pair ——
  一个 symbol 可能有十几个周期，每个都是数千个文件，所以检查放在**周期**粒度，
  而不是只有 symbol 粒度；
- 每个在途下载：`tokio::select!` 竞速取消信号，立刻放弃请求。

因此中断是**即时**的，不必等当前 pair（可达数千文件）跑完。实测 SIGINT 后 0 秒退出。
取消不算失败：退出码 0，已落盘的文件有效，重跑即续传。
被放弃的下载会留下 `{name}.zip.{pid}.tmp`，下次运行的扫描会清理
（PID 已不存在且超过 5 分钟存活窗口）。

> **已知边界**：起始日的**预取阶段（第 6 步）只阻止"再发起新请求"**，已经在途的请求仍会
> 等到返回。Ctrl-C 不会立刻掐断 fapi / 归档清单的单个请求（它们的超时是 10s 连接 + 60s 读），
> 但不会再启动新的查询。币种数不多时影响可忽略。

**并发上限由进程决定。** 每个在途下载占 **2 个 fd**（socket + 临时文件），
而 `config.concurrency` 允许配到 1024 —— 超过进程的 `RLIMIT_NOFILE` 就会 `EMFILE`，
表现为大批下载失败（实测 `ulimit -n 32` + `concurrency 64` → 71 个文件里 35 个失败，
`Os { code: 24, kind: TooManyOpenFiles }`）。因此实际并发取
`min(config.concurrency, (RLIMIT_NOFILE - 64) / 2)`，被收紧时打一条 WARN。

**并发运行：当前禁止。** 同一 `output_dir` 上第二个进程会以退出码 1 失败，并打印
持有锁的 PID。原因是它们会争抢同一个 `redb` 缓存文件（`Database::create` 会阻塞等文件锁），
以及同一批目标文件。锁以 `O_EXCL` 创建，只有在持有者**可证明已消失**（Linux 上读 `/proc/<pid>`；
否则退化为 mtime 超过 5 分钟）时才会被回收。
多运行并发支持在后续版本提供。

## 可靠性机制

| 风险 | 处理 |
| --- | --- |
| `config.start` 早于币种上市日 → 大量 404 | 起始日取 `max(该周期的归档首日, fapi 首根日, config.start)`；归档首日来自归档自己的 S3 清单，404 被彻底消除 |
| 归档清单不可用 | WARN 后退回 fapi 日期，行为与改造前一致，不中断下载 |
| fapi 限速导致起始日查不到 | 两个来源并发取：清单答了就用清单的精确首日，fapi 失败不再连累该币种 |
| `config.start` 晚于结束日 → 空任务 | 配置节双 `Some` 且 `start>end` 直接报错；运行时 `cfg.start>end` 与 `resolved>end` 两次跳过 |
| `config.end` 超过今天 → 数据未发布 → 404 | `AppConfig::resolved_end` 一次取 `min(config.end, 昨天)`，下游只消费 |
| 本地已有文件 → 重复下载 | 循环前扫描一次，建 task 时剔除，重复执行安全 |
| 网络故障 | 指数退避重试，单文件总预算 120s |
| 服务端慢速滴水（每 59 秒 1 字节） | 整体请求超时 300s（`read_timeout` 只管单次读，管不住这种情况） |
| 404 | 单次即止，记 `not_found`，不重试、不算失败（可配 `not_found_pct_threshold` 改为失败） |
| `Retry-After` 特别大 | 钳制到剩余重试窗口，窗口耗尽即硬停 |
| 服务端 200 返回 HTML 错误页 | 校验 zip 魔数 `PK\x03\x04`（跨 chunk 累积校验），不符即重试 |
| 响应体被截断 | 与 `Content-Length` 校验，不符即重试 |
| zip 结构损坏（魔数正确但中央目录坏） | `--verify` 打开归档读中央目录，失败即重试；默认关闭（多一遍读盘） |
| 系统性故障持续 | 连续 5 个 pair 失败即熔断中止本轮，重跑续传 |
| `concurrency` 手滑写成极大值 | 校验 `0 < concurrency ≤ 1024`；实际并发再被 `RLIMIT_NOFILE` 收紧 |
| 并发超过进程 fd 上限 → `EMFILE` | 实际并发 = `min(配置值, (RLIMIT_NOFILE - 64) / 2)`，每个在途下载占 2 个 fd；收紧时打 WARN |
| 配置写重复或写错 | `symbol/timeframe` 排序去重；未知键拒收；错误带文件路径 |
| 缓存值损坏 | 按未命中处理（WARN 并重取），不中断下载 |
| 缓存读取失败 | 按未命中处理（WARN），不中断下载 |
| 同目录并发运行 | 单实例锁，直接失败退出 |
| 两个进程同时回收同一把陈旧锁 | 删除时容忍 `ENOENT`，最终由 `O_EXCL` 创建裁决胜者，不会误报 I/O 错误 |
| 上次运行崩溃残留 `.tmp` | 扫描时按 PID + mtime 判定，回收死进程留下的 |
| 错误日志缺上下文 | 错误以 `Debug` 渲染，`AppError` 的 `#[source]` 链（reqwest / io / json）完整可见 |
| 批量失败只报数量不报原因 | 失败明细（前 N 条，含 URL 与原因）进 `FailureReport.details`，最终错误行直接可读 |
| 日期游标越过 `NaiveDate::MAX` | 游标用 `checked_add_days` 推进，越界即停，不 panic |

## 开发

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

- `clippy` 启用了 `correctness/suspicious/style/complexity/perf` + `pedantic`，
  并额外打开 `missing_docs`。
- 内部 API 一律 `pub(crate)`，对外只暴露 `run` / `run_with_options` / `RunOptions` /
  `Cli` / `AppError` / `FailureReport`。
- 工具链由 `rust-toolchain.toml` 固定为最新 stable；`Cargo.toml` 刻意不声明 `rust-version`
  （理由见「技术选型」与 `Cargo.toml` 内的注释）。
- `Cargo.toml` 里的空 `[workspace]` 表让本 crate 自成 workspace root —— 与
  `creats/` 下的其他 crate 惯例一致（否则会被上级 workspace 捕获而无法构建）。
- 依赖：`rlimit`（读 `RLIMIT_NOFILE`）、`ahash`（路径哈希，固定种子、无 `runtime-rng`）、
  `roxmltree`（归档清单 XML）、`zip`（`--verify` 读中央目录，`default-features = false`）。
- 公开 API 带有可执行 doc 示例（doctest）：`run` / `Cli` / `AppError` 各一段，
  由 `cargo test` 的 doc-test 阶段运行（`Cli` 的示例用 `long_about` 隔离，避免泄漏进 `--help`）。
- crate 根文档即本 README（`#![doc = include_str!("../README.md")]`），二者不会漂移；
  因此 README 里的代码块必须标注语言（未标注的会被 rustdoc 当成 Rust 编译）。
- 测试覆盖：配置校验边界、URL/任务生成、缓存 TTL 与并发、缓存批量写、扫描分批与
  tmp 清理、路径哈希的稳定性与区分度、进程 fd 上限与并发收紧、
  zip 魔数跨 chunk（自建 chunked HTTP 服务）、zip 结构校验、取消信号、重试与
  `Retry-After` 钳制、fapi 响应的各种畸形形态、归档清单解析，以及整套编排分支
  （见「可测试性」）。
- 编排分支还额外覆盖：配置起始日晚于结束日、币种上市日晚于结束日时跳过该 pair、
  连续失败触发熔断、运行前即已取消时不调度任何 pair、运行中取消后不再调度下一个
  pair、404 占比达到/未达到阈值、归档清单救场（fapi 失败）、逐周期各自的精确首日。

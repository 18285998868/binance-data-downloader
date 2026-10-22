# binance-data-downloader

从 [Binance Public Data](https://data.binance.vision) 下载 **spot / USDⓈ-M / COIN-M**
的 11 种归档数据集，并保持原始 zip 文件格式与网页路径。

## 数据集支持矩阵

`market = "spot" | "futures/um" | "futures/cm"`，`periods = ["daily", "monthly"]`。

| `type` | spot | futures/um | futures/cm | 支持的 period |
| --- | --- | --- | --- | --- |
| `aggTrades` | ✅ | ✅ | ✅ | daily / monthly |
| `trades` | ✅ | ✅ | ✅ | daily / monthly |
| `klines` | ✅ | ✅ | ✅ | daily / monthly |
| `bookDepth` | ❌ | ✅ | ✅ | daily |
| `bookTicker` | ❌ | ✅ | ✅ | daily / monthly |
| `fundingRate` | ❌ | ✅ | ✅ | monthly |
| `liquidationSnapshot` | ❌ | ❌ | ✅ | daily |
| `metrics` | ❌ | ✅ | ✅ | daily |
| `indexPriceKlines` | ❌ | ✅ | ✅ | daily / monthly |
| `markPriceKlines` | ❌ | ✅ | ✅ | daily / monthly |
| `premiumIndexKlines` | ❌ | ✅ | ✅ | daily / monthly |

K 线类数据集必须提供 `intervals`，其他数据集禁止提供：

- spot daily：`1s` … `1d`
- spot monthly：`1s` … `1mo`
- um/cm daily：`1m` … `1d`（`3d`、`1w`、`1mo` 只有月档完整覆盖，日档不请求）
- um/cm monthly：`1m` … `1mo`

## 快速开始

```bash
cp config.example.toml config.toml
# 按需修改 market / symbol / dataset / output_dir / start / end
cargo build --release
RUST_LOG=info ./target/release/binance-data-downloader -c config.toml

# 可选：额外校验本次新下载文件的 zip 结构
RUST_LOG=info ./target/release/binance-data-downloader -c config.toml --verify

# 独立校验已有输出目录中的全部 .zip（不下载、不改文件）
./target/release/binance-data-verify --dir /data/binance
./target/release/binance-data-verify --dir /data/binance --jobs 8
```

- 已存在的 `.zip` 按路径跳过，重复执行安全，可用于增量补全。
- `Ctrl-C` 立即停止调度与在途下载；重跑从已完成文件处续跑。
- 日志由 `RUST_LOG` 控制（缺省 `info`）。

## 目录布局：严格镜像网页路径

`output_dir` 是归档根目录（即网页 `data/` 的父目录），本地路径不再有第二套规则：

```toml
market = "futures/um"
output_dir = "/data/binance"
```

```text
/data/binance/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-02.zip
/data/binance/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip
/data/binance/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01-02.zip
/data/binance/data/futures/cm/daily/liquidationSnapshot/BTCUSD_PERP/BTCUSD_PERP-liquidationSnapshot-2024-01-02.zip
/data/binance/data/futures/cm/daily/indexPriceKlines/BTCUSD/1m/BTCUSD-1m-2020-06-30.zip
```

规则汇总：

- 完整路径 = `output_dir` + `data/{spot|futures/um|futures/cm}/{daily|monthly}/{type}/{SYMBOL}[/{interval}]/{file}`。
- K 线类文件名：`SYMBOL-INTERVAL-YYYY-MM[-DD].zip`。
- 非 K 线类文件名：`SYMBOL-TYPE-YYYY-MM[-DD].zip`。
- `indexPriceKlines` 是唯一例外：归档按**标的指数**存放，而不是合约。配置
  `BTCUSD_PERP` 时，网页与本地路径都使用 `BTCUSD`；配置 `BTCUSDT_250926` 时使用
  `BTCUSDT`。文件内容与路径仍与网页完全一致，用户无需手动改名。
- 因此同一标的的永续与季度合约不能同时配置 `indexPriceKlines`：它们会落到同一路径，
  配置校验会直接拒绝，避免并发写同一个文件。
- 默认 `output_dir = "."`，即当前目录下的 `data/...`。
- **旧版本路径不兼容**：旧数据在 `data/um/...` 或 `binance/data/um/...`，新版本只认
  `data/futures/um/...`。迁移示例：

```bash
mkdir -p /data/binance/data/futures
mv /data/binance/data/um /data/binance/data/futures/um
# cm 同理
```

## 配置

完整示例见 `config.example.toml`。最小配置：

```toml
market = "futures/um"
symbol = ["BTCUSDT"]

[[dataset]]
type = "klines"
periods = ["daily", "monthly"]
intervals = ["1m", "1d"]
```

字段说明：

```toml
market = "futures/um" # 必填：spot | futures/um | futures/cm（也接受 um / cm 简写）
symbol = ["BTCUSDT"]  # 必填：非空，字符集 [A-Za-z0-9_]；运行时转大写、排序、去重
# start = "2020-01-01" # 可选：缺省=每个 job 自己的归档首日
# end   = "2025-01-01" # 可选：缺省昨天；未来值截到昨天
# output_dir = "/data/binance" # 可选：data/ 的父目录；缺省 "."
# concurrency = 64 # 可选：0 < c ≤ 1024；单个 pair 内最大并发
# not_found_pct_threshold = 50.0 # 可选：0 < t ≤ 100；404 占比达到 t 判失败

[[dataset]]
type = "klines"          # 必填：11 种之一
periods = ["daily"]      # 必填：非空，daily / monthly
intervals = ["1m"]       # K 线类必填；非 K 线类不得出现
```

校验规则：

- 未知键拒收（`deny_unknown_fields`，拼错即报错）。
- `market`、`symbol`、`dataset` 必填；`dataset` 不允许为空。
- 每种 `type × market × period` 必须真实存在；`intervals` 必须在该 market/period 下存在。
- 重复展开出的同一个 `(type, period, interval)` job 直接报错。
- `start > end` 直接报错；`concurrency`、`not_found_pct_threshold` 越界直接报错。
- 配置错误都带配置文件路径。

## 月档语义

- 只下载**完整月份**：`end = 2024-01-15` 时，1 月不会下载，最后一档是 2023-12。
- `start = 2024-01-15` 时，会下载完整的 2024-01；起始日按“月”对齐，而不是丢弃该月。
- `fundingRate` 只有月档；`bookDepth` / `metrics` / `liquidationSnapshot` 只有日档。

## 执行逻辑

1. 加载并校验配置，展开 `[[dataset]]` 为 concrete jobs。
2. 取 `output_dir` 单实例锁（OS advisory 文件锁）。
3. 建全局 `reqwest::Client`：UA、连接 10s、单次读 60s、整体 300s、每 host 256 连接。
4. 扫描本地 `.zip`，建相对路径哈希集合；顺带回收超过 5 分钟的本工具 `.tmp`。
5. 解析 `end = min(config.end, 昨天 UTC)`。
6. **并发解析起始日**：每币一次市场 klines API（`1m`，证明币种存在）+ 每个 job 一次
   S3 归档清单；两个来源并发，结果缓存 30 天。
7. **双层串行循环 `symbol × job`**：
   1. 日档按天、月档按完整月份计算实际区间；
   2. 起始日 = `max(config.start, 该 job 的归档首日 / API 首日)`；`indexPriceKlines`
      直接用归档首日（指数历史可能早于合约上市），再与 `config.start` 取较晚者；
   3. 起始晚于结束 → 跳过；
   4. 逐文件生成 URL、剔除本地已有；
   5. 非空则按 `concurrency` 并发下载，输出 pair 统计。
8. 汇总：零失败返回 `Ok`；否则返回 `Err(Data(FailureReport))`，明细最多 20 条。

## 关键设计

### 起始日：API 与归档清单取较晚者

`indexPriceKlines` 例外：它按标的指数存放，指数可能早于合约上市（BTCUSD 指数
2020-06-30 就有归档，而 `BTCUSD_PERP` 合约 API 首根 K 线是 2020-08-11）。该数据集直接
采用归档首日，避免丢掉合约上市前的指数历史。

| 来源 | 回答的问题 | 请求量 |
| --- | --- | --- |
| 市场 klines API（`1m`） | 币种**什么时候开始交易** | 每币 1 次 |
| 归档 S3 清单 | 归档**什么时候开始发布** | 每 job 1 次 |

API 的首根 K 线通常早于归档首日（BTCUSDT：2019-09-08 vs 2019-12-31），只按 API 起跑会
产生大量必然 404。归档清单按 key 排序，`max-keys=1` 即最早文件：

```text
https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&max-keys=1&prefix=data/futures/um/daily/klines/BTCUSDT/1m/
```

- 两个来源并发，互不依赖；API 失败但清单有数据时仍可精确解析。
- **每个 job 各自的首日**：不同数据集 / 周期可以有不同的归档起点。
- API 未知币种（`-1121`）仍是硬失败，避免拼写错误静默成功。
- 清单失败/为空 → 退回 API 日期（有 `config.start` 时也可降级），只 WARN，不失败。
- 缓存 key 含 `market + job + symbol`，避免 spot/um/cm 同名币互相污染。
- 空清单不缓存（归档每天发布）。

### 404 语义

- 404 单次即止，不重试，计入 `not_found`，默认不改变退出码。
- 配 `not_found_pct_threshold = t` 后，本轮 404 占比达到 `t` 即判失败。
- 起始日解析已尽量消除“归档尚未发布”的 404；若比例异常，通常意味着区间或配置有误。

### 中断与并发

- 一个取消 token 同时被三处观察：起始日预取、`symbol × job` 循环、每个在途下载。
- 取消不算失败，退出码 0；已落盘文件有效。
- 每个在途下载占 2 个 fd（socket + 临时文件），实际并发还会被
  `min(config.concurrency, (RLIMIT_NOFILE - 64) / 2)` 收紧，收紧时打 WARN。
- 同一 `output_dir` 同时只允许一个进程；第二个进程直接失败退出。

### 可靠性

| 风险 | 处理 |
| --- | --- |
| `config.start` 早于归档首日 → 404 | 起始日取 `max(API 首日, 归档首日, config.start)`（index 用归档首日） |
| 归档清单不可用 | WARN 后退回 API 日期，不中断 |
| API 限速 | 清单答了就用清单；两者都失败且无 `start` 才跳过该币 |
| 月档未发布完整 | 只请求完整月份 |
| 本地已有文件 | 扫描一次并剔除；重复执行安全 |
| 服务端返回 HTML / 截断 | 校验 zip 魔数与 `Content-Length`，不符重试 |
| zip 结构损坏 | `--verify` 校验中央目录；旧文件用 `binance-data-verify` |
| 系统性故障 | 连续 5 个 pair 失败即熔断，重跑续传 |
| 慢速滴水响应 | 整体请求超时 300s |
| 并发超过 fd 上限 | 按 `RLIMIT_NOFILE` 收紧并发 |
| 崩溃残留 `.tmp` | 下次扫描按 mtime 超过 5 分钟回收 |
| 落盘中断 | 临时文件 + `fsync` + `rename` 原子替换 |
| 日期游标溢出 | `checked_add_*`，越界即停，不 panic |

## 架构

```text
main.rs          入口：tracing、-c/--config/--verify、调用 bdd::run_with_options()
bin/verify.rs    独立校验器 binance-data-verify
lib.rs           公开导出 run / RunOptions / Cli / verify_zip / AppError / FailureReport
cli.rs           clap 参数
dataset.rs       Market / Period / Interval / DatasetKind / DownloadJob（路径与文件命名）
config.rs        TOML 解析、校验、jobs() 展开
app/
  mod.rs         流水线编排（market、job 循环）
  start_dates.rs 起始日解析与降级
  urls.rs        URL / 相对路径生成（迭代器）
  tasks.rs       任务构建（剔除本地已有）
  download/      有界并发、重试、原子落盘、可选 zip 校验
archive.rs       S3 清单解析（按 job）
start_date.rs    市场 klines API 首日
scan.rs          本地文件扫描与 tmp 清理
cache.rs         redb KV 缓存
backoff.rs       重试 / Retry-After / 120s 窗口
limits.rs        RLIMIT_NOFILE → 实际并发
lock.rs          输出目录单实例锁
verify.rs        zip 中央目录校验
tests/public_api.rs 公开接口集成测试
```

## 开发

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

- 工具链由 `rust-toolchain.toml` 固定为最新 stable；`Cargo.toml` 刻意不声明
  `rust-version`，理由见文件内注释。
- 内部 API 一律 `pub(crate)`；公开面仅 `run` / `run_with_options` / `RunOptions` /
  `Cli` / `verify_zip` / `AppError` / `FailureReport`。
- crate 根文档即本 README（`#![doc = include_str!("../README.md")]`）。
- 测试覆盖：11 种数据集能力矩阵、路径/文件名、日/月迭代、配置校验、起始日三态、
  URL/任务生成、缓存、扫描、重试分类、原子落盘、取消与整套编排分支。

# binance-data-downloader

从 Binance Public Data 下载 USDT-M 永续合约日度 K 线归档(zip),流式写入本地,按
`symbol × timeframe × 日期` 一个文件一个 zip。

## 文件结构

```
config/
  config.toml              # 实际运行配置(gitignore, 不提交)
  config.example.toml      # 配置模板(字段说明)
data/                      # 下载产物: futures/um/daily/klines/{symbol}/{tf}/{symbol}-{tf}-{date}.zip (gitignore)
LOGIC.md                   # 处理流程与技术细节
docs/dev.md                # 历史开发笔记
src/
  main.rs                  # 入口: tracing → bdd::cli::parse → bdd::app::run
  lib.rs                   # #[path] 模块声明 + DownloadTask 定义
  error.rs                 # 支撑: AppError (thiserror)
  cache.rs                 # 支撑: redb 缓存 (read/write/TTL)
  backoff.rs               # 支撑: 默认指数退避 (max 30s)
  steps/                   # 按流水线步骤归档 (磁盘文件名带数字前缀, 模块名 = 可读步骤名)
    01_cli.rs                  cli:  解析 --config/-c
    02_config.rs               config: 加载/校验 TOML 配置
    03_scan_local_files.rs     scan_local_files: 扫描本地 .zip + 清理 .tmp
    04_get_start_date.rs       get_start_date: 交易所首根 K 线日期 (API + redb 缓存)
    05_build_download_tasks.rs build_download_tasks: 生成 URL → 过滤已下载 → DownloadTask
    06_run_downloads/
      mod.rs                   run_downloads: buffer_unordered 有界并发 + 增量统计
      stream.rs                run_downloads: 单文件流式下载 .tmp → rename 原子写
    07_app.rs                  app: 顶层编排 (run + run_one + resolve_start + 批统计)
```

## 配置

`config/config.toml`(示例见 `config/config.example.toml`):

```toml
symbol = ["BTCUSDT"]       # 必填, 至少一个交易对
timeframe = ["1d"]         # 必填, 至少一个周期
# start = "2020-01-01"     # 可选, 省略则取交易所首根 K 线日期(不会早于首根)
# end   = "2025-01-01"     # 可选, 省略则取昨天
output_dir = "data"        # 可选, 输出目录, 默认 "data"
concurrency = 16           # 可选, 配对内最大并发下载数, 默认 16
```

未知字段、空 `symbol`/`timeframe`、`concurrency = 0`、非法日期都会在启动时报错退出。

## 用法

```bash
cargo run --release -- -c config/config.toml
# 或直接运行二进制:
target/release/binance-data-downloader -c config/config.toml
```

并发模型:配对(层)串行,配对内 `buffer_unordered(concurrency)` 有界并发;增量统计,
每完成 100 个文件打一条进度;文件级失败不中断同批其余任务,404 计为 `not_found` 不计失败。

## 测试

```bash
cargo test
```

单元测试 + mock 测试覆盖配置校验、URL 生成、任务构建、并发调度、单文件下载(404/5xx)、
本地扫描、缓存、退避等。
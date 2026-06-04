# binance-data-downloader

从 Binance Public Data 下载历史 K 线数据，解压 CSV 并转换为 Parquet 格式。

## 文件结构

```
config/
  config.toml          # 批量下载配置
  run_once.toml        # 单 (symbol, timeframe) 配置
src/
  main.rs              # 入口
  lib.rs               # 顶层 run() -> run_batch::run()
  error.rs             # AppError 定义
  run_batch/
    mod.rs
    config.rs          # BatchConfig
    load.rs            # 加载 config.toml (调用 common::config::load_toml)
    run_batch.rs       # 展开 symbol×timeframe, 依次调用 run_once
  run_once/
    mod.rs
    config.rs          # RunOnceConfig
    get_start.rs       # 获取合约首根K线日期 (moka 缓存)
    run_once.rs        # 单对下载编排: 构建URL→扫描本地→过滤→分块下载→处理→写入Parquet
  task/
    mod.rs             # DownloadTask 定义
    urls.rs            # 构建下载 URL
    local_file.rs      # 扫描本地已下载文件
  downloader/          # 下载→解压→CSV→类型转换→清洗→Parquet 流水线
  clean.rs             # .tmp 文件清理
tests/
  integration.rs       # 集成测试
```

## 批量下载 (run_batch)

```toml
# config/config.toml
symbol = ["BTCUSDT", "ETHUSDT"]   # 必须
timeframe = ["1m", "5m", "1h"]    # 必须
# max_concurrent = 1000           # 文件级并发, 默认 1000
# start = "2020-01-01"            # 可选, 默认从交易所获取
# end   = "2025-01-01"            # 可选, 默认昨天
# dir   = "data"                  # 可选, 默认 ./data
```

## 单对下载 (run_once)

```toml
# config/run_once.toml
symbol = "BTCUSDT"         # 必须
timeframe = "1d"           # 必须
# start = "2020-01-01"     # 可选, 默认从交易所获取
# end   = "2025-01-01"     # 可选, 默认昨天
# dir   = "data"           # 可选, 默认 ./data
```

## 用法

```bash
cargo run -p data-downloader
```

## 执行流程

```
run()
  └─ run_batch::run()
       ├─ 加载 config.toml
       ├─ 构建 HTTP client
       ├─ 遍历 (symbol × timeframe)
       │    顺序执行, 每个 pair 内部文件级并发
       │    └─ run_once::run_once()
       │         ├─ get_start::get_first_kline_date() (moka 缓存)
       │         ├─ scan_local_files()
       │         ├─ 按 30 天分块
       │         └─ 下载→解压→CSV→类型转换→清洗→写入 Parquet
       │               文件级并发 buffer_unordered(max_concurrent)
       └─ 完成
```

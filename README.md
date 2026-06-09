# binance-data-downloader

从 Binance Public Data 下载历史 K 线数据，解压 CSV 并转换为 Parquet 格式。

## 文件结构

```
config/
  config.toml              # 批量下载配置
src/
  main.rs                  # 入口: 初始化 tracing → run()
  lib.rs                   # 模块导出 + re-export run()
  error.rs                 # AppError 定义 (thiserror)
  config.rs                # AppConfig (config crate)
  runner.rs                # run(): 加载配置, 展开 symbol × timeframe, 并发调度
  run_one.rs               # run_one(): 单对下载编排 (resolve_start → scan → build → download)
  task/
    mod.rs                 # DownloadTask 结构体
    builder.rs             # build_download_tasks(): URL → 过滤本地已有 → DownloadTask
    urls.rs                # build_urls(): 按日期生成 Binance 下载 URL
    get_local_files.rs     # scan_local_files(): 扫描 data_dir 已有 zip/parquet
    get_start.rs           # get_first_kline_date(): 交易所首根 K 线日期 (redb 缓存)
    get_symbols.rs         # resolve_symbols(): 配置列表 or 从 exchangeInfo 拉取
    download/
      mod.rs               # run_downloads(): 并发下载 + 聚合结果; csv_to_dataframe_sync()
      download_bytes.rs    # download_bytes_with_retry(): HTTP GET + backoff 重试
      extract_zip.rs       # extract_csv_from_bytes(): 从 zip 提取 csv
      csv_table_title.rs   # detect_header() / default_header(): 表头检测与生成
      csv_set_col_types.rs # convert_types(): open_time/close_time/count → Int64, 其余 → Float64
      df_to_parquet.rs     # write_parquet_sync(): 原子写入 (tmp → rename)
tests/
  common/mod.rs            # 测试公共函数
  download.rs              # 网络集成测试: 下载 ZIP, 404, 批量
  integration.rs           # 集成测试: 本地扫描 → URL → 跳过
  pipeline.rs              # 端到端: download → parquet 验证; 跳过已有文件
```

## 配置

```toml
# config/config.toml
# symbol = ["BTCUSDT"]    # 可选, 默认: 从交易所自动获取全量 TRADING 符号
# timeframe = ["1d"]      # 可选, 默认: 全部 12 个周期
# start = "2020-01-01"    # 可选, 默认: 交易所首根 K 线日期
# end = "2025-01-01"      # 可选, 默认: 昨天
# dir = "data"            # 可选, 默认: data
# max_concurrent = 32     # 可选, 默认: 32, pair 级别和文件级别共用, 上限 512
```

## 用法

```bash
cargo run -p binance-data-downloader
cargo build -p binance-data-downloader --release
target/release/binance-data-downloader --config creats/binance-data-downloader/config/config.toml
```

## 测试

```bash
# 单元测试 + mock 测试
cargo test -p binance-data-downloader

# 含网络测试
cargo test -p binance-data-downloader -- --include-ignored
```

## 执行流程

```
run()
  ├─ 加载 config.toml
  ├─ cleanup_tmp()  (清理 .tmp 残留文件, 仅执行一次)
  ├─ 构建 HTTP client (LazyLock 全局单例)
  ├─ 解析 symbol / timeframe
  ├─ 笛卡尔积 → pairs[]
  │
  └─ buffer_unordered(max_concurrent)  (pair 级别并发)
       └─ run_one()
            ├─ resolve_start(): 交易所首根日期 (redb 缓存)
            ├─ scan_local_files(): 扫描已有 zip/parquet → 过滤
            ├─ build_download_tasks(): 按天生成 URL → 跳过已存在
            │
            └─ run_downloads()  (文件级别并发)
                 └─ download_bytes_with_retry()  (backoff 重试)
                      ├─ 404 → Permanent (不重试)
                      ├─ 5xx/网络错误 → Transient (重试)
                      └─ 空 body → Permanent (不重试)
                 └─ process_zip()  (spawn_blocking)
                      ├─ extract_csv_from_bytes()
                      ├─ csv_to_dataframe_sync()  (自动检测表头)
                      ├─ convert_types()  (Int64 / Float64)
                      └─ write_parquet_sync()  (tmp → rename 原子写)
```

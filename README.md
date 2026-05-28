# Binance Data Downloader

从 Binance Public Data 下载历史 K 线数据，解压 CSV 并转换为 Parquet 格式。

# 配置

```toml
# market_type = ["futures"]              # ["spot", "futures"]
# max_concurrent = 1000                   # 最大并发数
# data_dir = "./data"                    # 数据存储目录
# symbols = ["BTCUSDT", "ETHUSDT"]     # 留空则自动获取所有 TRADING 交易对
# timeframes = ["1m", "5m", "15m"]     # 默认 12 个周期
# proxy_url = "http://127.0.0.1:7890"
# start_date = "2020-01-01"            # 默认从交易所获取
# end_date = "2025-01-01"              # 默认昨天
# log_level = "info"                   # 默认 "debug"
```

# 执行流程

```
binance-data-downloader/
  Cargo.toml
  download.toml
  src/
    main.rs                              # 入口: 加载配置 → downloader()
    lib.rs                               # downloader() 编排: 分批构建任务 → 下载 → 转换
    error.rs                             # AppError (thiserror)
    clean.rs                             # 清理中断残留的 .tmp 文件
    config/
      mod.rs                             # 导出 Config, MarketType
      def.rs                             # Config 结构体 + 默认值 + load()
      market_type.rs                     # MarketType 枚举 (Spot/Futures)
    task/
      mod.rs                             # DownloadTask + build_tasks()
      store.rs                           # DiskCache (sled) + async wrapper
      symbols.rs                         # 交易对解析 (exchangeInfo API + backoff)
      start_date.rs                      # 起始日期解析 (klines API + backoff)
      urls.rs                            # URL 生成 (symbol×type×tf×date 笛卡尔积)
      local_file.rs                      # 扫描本地已下载文件
    downloader/
      mod.rs                             # run_downloads() + pipeline_inner()
      http.rs                            # HTTP GET + backoff 重试
      extract_zip_from_bytes.rs          # 从 ZIP 字节提取 CSV
      csv_to_dataframe/
        mod.rs                           # CSV → DataFrame
        table_title.rs                   # 列名检测
        set_column_types.rs              # 类型转换 (Int64/Float64)
      data_clean/
        mod.rs                           # re-export data_clean::clean_dataframe
      dataframe_to_parquet.rs            # 原子写入 Parquet (.tmp → rename)

```

# 用法

```bash
cargo run -p data-downloader
```

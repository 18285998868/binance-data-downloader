# Binance Data Downloader

从 [Binance Public Data](https://data.binance.vision) 下载历史 K 线数据，
提取 CSV 并转换为 Parquet 格式。

## 配置

```toml
# Binance 数据下载器配置
# symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]   # 交易对 (默认=全部)
# timeframes = ["1m", "5m", "1h"]                # 周期 (默认=全部)
market_type = ["futures"]                        # 默认:["futures"], 可选:["futures","spot"]
max_concurrent = 1000                            # 最大并发 (默认=100)
data_dir = "/data/nutilustrader/data"            # 数据目录 (默认=$CARGO_MANIFEST_DIR/data)
proxy_url = "http://127.0.0.1:1080"              # HTTP 代理 (可选)
# log_level = "debug"                            # 日志级别 (可选,也可通过 LOG_LEVEL 设置)
# start_date = "2023-01-01"                      # 起始日期 (可选)
# end_date = "2024-12-31"                        # 截止日期 (可选)
```

## 执行流程

```
main.rs                                         # 入口 → lib::run()
└── lib.rs                                      # 下载器主逻辑
    ├── config/default_symbols.rs               # 若没有 syms: 按 market_type 并行请求 exchangeInfo, 缓存 1 周
    ├── config/default_timeframes.rs            # 若没有 tfs: 使用 ALL_TIMEFRAMES
    ├── config/default_start_date.rs            # 若没有 start_date: 逐 (sym,mt) 从 Binance API 查最早日期, 缓存 1 周
    │
    ├── 笛卡儿积: sym × tf × market_type × date_range
    │   └── urls() 惰性迭代器 (零中间 Vec 分配)
    │       ├── 连接 URL
    │       ├── 去重 (vs 本地 parquet/zip)
    │       └── 构建 DownloadTask
    │
    └── 并发执行 download::run_downloads
        └── pipeline_inner (全内存管道)
            ├── 1. HTTP GET → Vec<u8>              # ZIP 字节直入内存, 不写盘
            ├── 2. extract_csv_from_bytes           # 从内存 ZIP 解压 CSV
            ├── 3. csv_to_dataframe                 # Polars 解析 CSV
            ├── 4. convert_types                    # 逐列 cast (零 df.clone)
            ├── 5. clean_dataframe                  # 排序 → 去重 → OHLC 校验 → 去空值
            └── 6. write_parquet (原子 .tmp + rename)

    首次启动: 2 HTTP 请求 (exchangeInfo + start_dates), 之后 1 周内走缓存
    每个下载: TCP 1 次, 零额外磁盘 I/O
```

## 性能优化

| 优化点 | 之前 | 之后 |
|---|---|---|
| ZIP 文件处理 | 写 .tmp → rename .zip → 读 .zip → 删 .zip (2 次磁盘 I/O) | 全内存, 零 ZIP 磁盘 I/O |
| `convert_types` | `df.clone()` + 逐列 `with_column` (O(rows×cols) 复制) | 逐列 cast 后 `DataFrame::new` (O(cols) 复制) |
| `task.clone()` on retry | 每次重试克隆完整 `DownloadTask` (String + PathBuf) | 闭包捕获引用, 无需克隆 |
| 笛卡尔积内存 | 完整 `Vec<DownloadUrl>` 分配后过滤 | `urls()` 惰性迭代器, 链式 filter+map+collect |
| market_type 请求 | 串行请求 spot / futures exchangeInfo | `try_join_all` 并行请求 |
| 缓存过期 | 永不过期 | 1 周 TTL (mtime 检测) |

## 文件结构

```
src/
├── main.rs                  # CLI 入口
├── lib.rs                   # 主 pipeline
├── error.rs                 # AppError
├── config/
│   ├── mod.rs               # Config, MarketType, load_config
│   ├── cache.rs             # JsonFileCache (1W 过期)
│   ├── retry.rs             # 指数退避重试
│   ├── default_symbols.rs   # 默认交易对 (并行 + 缓存 1W)
│   ├── default_timeframes.rs# 默认周期
│   └── default_start_date.rs# 默认起始日期 (逐 sym 缓存 1W)
    └── download/
        ├── mod.rs               # run_downloads + pipeline_inner
        ├── fetch.rs             # HTTP 下载 (内存) + ZIP 解压 (内存)
        └── convert/
            ├── mod.rs
            ├── reader.rs        # CSV → DataFrame + 类型转换
            ├── clean.rs         # 数据清洗 (排序/去重/OHLC校验/去空值)
            └── writer.rs        # 原子写 Parquet
```

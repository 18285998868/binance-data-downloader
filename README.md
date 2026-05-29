# 介绍

从 Binance Public Data 下载历史 K 线数据，解压 CSV 并转换为 Parquet 格式。

# 配置

```toml
proxy_url = "socks5h://127.0.0.1:7890"
# market_type = ["futures"]
# max_concurrent = 1000
# data_dir = "./data"
# symbols = ["BTCUSDT", "ETHUSDT"]
# timeframes = ["1m", "5m", "15m"]
# start_date = "2020-01-01"
# end_date = "2025-01-01"
# log_level = "info"
```

# 执行流程

```mermaid
flowchart LR
  A[读取 download.toml] --> B[解析 Config]
  B --> C[构建 reqwest Client]
  C --> D{有 proxy_url?}
  D -->|是| E[添加代理]
  D -->|否| F[返回 Client]
```

```mermaid
flowchart LR
  A[开始] --> B{配置指定了 symbols?}
  B -->|是| C[直接使用]
  B -->|否| D[调用 exchangeInfo API]
  D --> E[过滤 TRADING 状态]
  E --> F[sled 缓存 7d]
  F --> G[对每个 symbol<br>查询首根 K 线时间]
  G --> H[缓存 start_date]
  H --> I[返回 symbols + start_dates]
```

```mermaid
flowchart LR
  A[分批 symbols / 每批 20] --> B[按 30 天分块]
  B --> C[遍历 symbol×type×tf×date]
  C --> D[生成 URL]
  D --> E{本地已下载?}
  E -->|是| F[跳过]
  E -->|否| G[构建 DownloadTask<br>url + dest + pair + tf]
  G --> H[返回任务列表]
```

```mermaid
flowchart LR
  A[DownloadTask 列表] --> B[futures buffer_unordered<br>max_concurrent]
  B --> C[HTTP GET]
  C --> D{状态码?}
  D -->|200| E[返回字节]
  D -->|404| F[Permanent 结束]
  D -->|其他/网络错误| G[ExponentialBackoff 重试]
  G --> C
```

```mermaid
flowchart LR
  A[ZIP 字节] --> B[打开 ZipArchive]
  B --> C[遍历 entries]
  C --> D{是目录?}
  D -->|是| E[跳过]
  D -->|否| F{扩展名 .csv?}
  F -->|否| G[继续]
  F -->|是| H[读取 CSV 字节]
  H --> I[返回 CSV]
```

```mermaid
flowchart LR
  A[CSV 字节] --> B{有列名行?}
  B -->|是| C[直接解析]
  B -->|否| D[添加默认列名]
  C --> E[Polars DataFrame]
  D --> E
  E --> F[列类型转换<br>Int64 / Float64]
  F --> G[数据清洗<br>common::data_clean]
  G --> H[返回 DataFrame]
```

```mermaid
flowchart LR
  A[DataFrame] --> B[创建父目录]
  B --> C[写入 .parquet.tmp]
  C --> D[ParquetWriter 序列化]
  D --> E[rename 原子替换]
  E --> F[清理残留 .tmp]
```

# 用法

```bash
cargo run -p data-downloader
```

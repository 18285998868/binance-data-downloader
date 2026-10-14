# binance-data-downloader

## 项目目标

> 从 [Binance Public Data](https://data.binance.vision/?prefix=data/futures/um/daily/klines/) 下载 永续合约 K 线数据文件(保持原始交易所文件格式)

## 技术选型

- rust
- edition = "2024"

## 冒烟配置

```toml
symbol = ["BTCUSDT"] # 必填
timeframe = ["1d"] #  ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
# start = "2020-01-01" # 可选
# end   = "2025-01-01" # 可选
output_dir = "data" # 可选
```

## 逻辑编排

```
main.rs         入口, 初始化tracing, 解析参数 --config/-c 并将其传递给 app::(&cli.config)
app/            逻辑处理, 根据配置构建下载 urls -> 执行下载
                01加载配置
                for symbol in config.symbol
                  for timeframe in config.timeframe
                    构建从 config.start 到 config.end 的下载链接列表 urls,格式: data/futures/um/daily/klines/{symbol}/{timeframe}/{symbol}-{timeframe}-{YYYY-MM-DD}.zip
                    从下载链接列表构建下载 tasks : 将 urls 的每一个加上前缀 https://data.binance.vision/ 构建真正的下载链接列表
                    执行下载
                下载完成时,打印汇总信息
                

```

## 架构设计

```
main.rs
lib.rs
cli.rs
config.rs
app/
  mod.rs app 执行逻辑代码实现
  01_build_urls.rs
  02_build_download_tasks.rs
  03_download.rs
  
```

## 问题:

- 当给的 config.start 超过币种最早开始时间时,会造成网络404,
  - 解决: 获取币种开始时间,并且缓存到 config.output_dir ,下载链接列表 urls 的开始时间是 max(币种开始时间,config.start)
  - 文件: `src/utils/get_start_date.rs`, `src/utils/cathe.rs`
- 当给的 config.end 超过今天时,数据尚未更新(含今天),会造成网络404,
  - 解决: urls的结束时间为 min(config.end, 昨天)
- 当本地已经有下载的数据文件时,会导致重复下载
  - 解决: 在循环开始前扫描本地已有文件, 在构建urls 时剔除本地已有文件实现数据更新而不重复下载
  - 文件: `src/utils/scan_local_files.rs`
- 当遇到网络故障时
  - 解决: 增加重试机制,确保数据下载成功
  - 文件: `src/utils/backoff.rs`

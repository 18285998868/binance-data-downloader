# 逻辑:
读取配置
如果没有symbol 则获取所有 symbol,过滤掉含-的symbol
本地文件扫描 一次扫描,后续消费
for symbol 串行
  for timeframe 串行
    获取 symbol x tf 的开始时间
    构建urls
    根据本地文件 ,过滤已经下载的urls
    http并发下载urls,流式写入zip,不再做数据处理
```toml
symbol = ["BTCUSDT"] # 是否必须: 否,默认自动获取交易所全量TRADING
# timeframe = ["1d"]  # 可选,默认见上表
# start = "2020-01-01" # 可选,默认从交易所获取首根K线
# end = "2025-01-01"   # 可选,默认昨天
# dir = "data"         # 数据存储目录,可选,默认cwd/data
```

```
src/
  main.rs             # 入口
  cli.rs              # --config/-c
  lib.rs              # 模块导出
  error.rs            # AppError (thiserror)
  config.rs           # AppConfig, AppConfig::load()
  app.rs              # 编排: 加载配置 → 构建 client → scan_local_files
                      #       resolve_symbols → 串行 symbol → timeframe → run_one
  cache.rs            # redb 本地缓存 (read/write, TTL)
  backoff.rs          # default_backoff() ExponentialBackoff
  exchange_info.rs    # Binance Futures exchangeInfo API + 缓存
  get_local_files.rs  # scan_local_files: 扫描 .zip + 清理 .tmp
  get_symbols.rs      # resolve_symbols: 自动获取全量 symbol (过滤含 `-` 的)
  get_start_date.rs   # get_first_kline_date: API + 缓存
  urls.rs             # build_urls: 生成下载 URL
  builder.rs          # build_download_tasks: URL → 过滤 → DownloadTask
  download/
    mod.rs            # run_downloads + aggregate_results
    stream.rs         # download_to_file: stream HTTP → .zip.tmp → rename .zip
```

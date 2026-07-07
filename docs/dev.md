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
  main.rs           # 只做入口
  cli.rs            #  --config/-c
  lib.rs            # 只做导出
  error.rs          # 库:thiserror
  app.rs            # 编排逻辑:
                    # 加载配置->构建共享client->scan_local_files
                    # if symbol=None,symbol=resolve_symbols()
                    # 串行遍历 symbol → timeframe → run_one()
                    # 构建开始时间resolve_start()
                    # scan_local_files(已提升到runner,一次扫描传入)
                    # build_download_tasks() (过滤local已有)
                    # run_downloads()
  config.rs         # 库:config-rs AppConfig 配置,AppConfig::load()
  get_local_files.rs # 扫描本地文件 + 清理.tmp
  get_symbols.rs    # exchange_info 全量缓存(redb),加载过滤
  get_start_date.rs # 获取第一根k线时间
  urls.rs           # 构建下载urls
  builder.rs        # 构建tasks
  download.rs
```

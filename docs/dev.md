逻辑编排:

|参数|默认|
|---|---|
|symbol|交易所获取|
|timeframe|默认["1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d"]
|start|交易所获取,(symbol,timeframe,start)|
|end|昨天|

```
展开symbol x timeframe 笛卡尔积->for each -> 获取开始时间(symbol,timeframe,start)->构建urls->FuturesUnordered并发下载(无人工限制,由reqwest连接池管理)

并发下载:
下载->解压到内存->csv处理->原子写入parquet
```

目录扫描::src/task/get_local_files.rs::遍历目录一次完成: *.tmp删除 + .zip/.parquet归一化.zip(key用于跳过已缓存)

TODO:
库使用:
- 配   置:  config-rs (支持 --config=<path>)
- 重试机制:  backoff
- 缓   存:  redb (common/cache.rs)
- 错   误:  thiserror + anyhow
- 并   发:  FuturesUnordered (无buffer_unordered/semaphore)
- 
代码要求:
- 零拷贝设计 (Cow\<str\> in scan, Arc\<HashSet\> 共享local文件列表)
- 内存安全
```toml
symbol = ["BTCUSDT"] # 是否必须: 否,默认自动获取交易所全量TRADING
# timeframe = ["1d"]  # 可选,默认见上表
# start = "2020-01-01" # 可选,默认从交易所获取首根K线
# end = "2025-01-01"   # 可选,默认昨天
# dir = "data"         # 数据存储目录,可选,默认cwd/data
```

```
src/
  main.rs # 只做入口,支持 --config=<path>
  lib.rs # 只做导出
  config.rs # 库:config-rs AppConfig 配置,AppConfig::load()
  error.rs # 库:thiserror
  runner.rs # 编排逻辑:
            # 加载配置->构建共享client->scan_local_files(Arc<HashSet>)
            # if symbol=None,symbol=resolve_symbols()
            # 笛卡尔积展开 symbol × timeframe,FuturesUnordered并行执行 run_one()
  run_one.rs # 构建开始时间resolve_start()
             # scan_local_files(已提升到runner,一次扫描传入)
             # build_download_tasks() (过滤local已有)
             # run_downloads()
  task/
    mod.rs
    get_symbols.rs # exchange_info 全量缓存(redb),加载过滤
    get_start.rs # 获取第一根k线时间
    urls.rs # 构建下载urls
    get_local_files.rs # 扫描本地文件 + 清理.tmp
    builder.rs # 构建tasks
    download/
      mod.rs # FuturesUnordered并发下载->解压->csv->parquet
      download_bytes.rs
      extract_zip.rs
      csv_table_title.rs
      csv_set_col_types.rs
      df_to_parquet.rs
```
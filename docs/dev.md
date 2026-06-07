TODO:
库使用:
- 配   置:  config-rs
- 重试机制:  backoff
- 缓   存:  moka
- 错   误:  thiserror
- 
代码要求:
- 零拷贝设计
- 内存安全
```toml
symbol = ["BTCUSDT"] # 必须
# timeframe = ["1d"]   #可选
# start = "2020-01-01" # 可选,默认从交易所获取
# end = "2025-01-01"#可选, 默认昨天
# dir = "data" # 数据存储目录,可选,默认cwd/data
# max_concurrent = 1000 # 最大并发数,默认1000
```

```
src/
  main.rs # 只做入口
  lib.rs # 只做导出
  config.rs # 库:config-rs AppConfig 配置,AppConfig::load()
  error.rs # 库:thiserror
  runner.rs # 编排逻辑: 
            # if symbol=None,symbol=resolve_symbols()
            # 笛卡尔积展开 symbol × timeframe,并行执行 run_one()
  run_one.rs # 构建开始时间resolve_start()
             # cleanup_tmp()
             # scan_local_files()
             # build_download_tasks()
             # run_downloads()
  task/
    mod.rs 
    get_symbols.rs # exchange_info 全量缓存,加载过滤
    get_start.rs # 获取第一根k线时间,moka缓存
    urls.rs # 构建下载urls
    get_local_files.rs # 扫描本地文件
    builder.rs # 构建tasks
    download/
      mod.rs # 并行下载->解压->csv->parquet
      download_bytes.rs
      extract_zip.rs
      csv_table_title.rs
      csv_set_col_types.rs
      df_to_parquet.rs
```

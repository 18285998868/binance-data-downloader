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
```bash
creats/binance-data-downloader/
    config/
        config.toml 
                        # TOML文件格式:
                        # 每个字段行后跟着注释
                        # 注释格式: # 必须, 默认值:   ,可选参数:
        
    docs/
    src/
        main.rs # 入口,调用lib.rs
        lib.rs #  调用run_batch/run_batch.rs
        error.rs # thiserror
        config/
            mod.rs 
            config.rs # 使用 impl Default
            load.rs # 调用creats/common/src/config/load_toml.rs 加载 env!("CARGO_MANIFEST_DIR")/config/config.toml
            get_exchangeInfo.rs  # 获取 binance exchangeInfo,moka缓存
            get_symbols.rs  # 从 get_exchangeInfo.rs中筛选出symbol
            get_start.rs    # moka缓存
                            # 入参:symbol,timeframe 获取binance 合约 第一跟k线 https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={timframe}&startTime=0&limit=1,转换开始时间,
        task/
            mod.rs
            urls.rs # 构建下载列表:urls
                    # for symbol
                        # for timeframe
                            # https://data.binance.vision/data/futures/um/daily/klines/{symbol}/{timframe}/{symbol}-{timframe}-{start}.zip
                            # ...
                            # https://data.binance.vision/data/futures/um/daily/klines/{symbol}/{timframe}/{symbol}-{timframe}-{end}.zip

            get_local_files.rs  # 扫描本地文件:get_local_files,walkdir
                                # {dir}/futures/um/daily/klines/{symbol}/{timframe}/*,清理下载中断时的tmp文件:clean_tmp()
                                # 后缀名归一化为.zip
            dl_urls.rs  # 生成下载urls: dl_urls=urls-local_files
            builder.rs  # 构建tasks: dl_urls -> tasks
            download/
                mod.rs  # downloader() # 下载流程编排:并发执行下载 -> download_bytes -> extract_zip -> csv_table_title -> csv_set_col_types -> df_to_parquet
                download_bytes.rs  # 
                extract_zip.rs
                csv_table_title.rs
                csv_set_col_types.rs
                df_to_parquet.rs
        tests/ #集成测试 

```
```toml
symbol = ["BTCUSDT"] # 必须
timeframe = ["1d"]   # 必须
# start = "2020-01-01" # 可选,默认从交易所获取
# end = "2025-01-01"#可选, 默认昨天
# dir = "data" # 数据存储目录,可选,默认cwd/data
# max_concurrent = 1000 # 最大并发数,默认1000
```
逻辑:
```
symbol,timeframe,start,end,dir,max_concurrent
# 构建urls列表:
# 过滤本地文件:
# 构建下载列表dl_urls
# 构建tasks
# 并行下载 max_concurrent
    # 下载-解压到内存-内存加载csv-处理表头-处理列数据类型-数据清洗-原子写入parquet
```

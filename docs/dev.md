TODO:

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
            get_symbols.rs # moka缓存
            get_start.rs # # moka缓存
            retry.rs # backoff
        task/
            mod.rs
            urls.rs
            get_local_files.rs
            dl_urls.rs
            download/
                mod.rs
                downloader.rs # 单个文件下载流程编排
                http.rs
                ...
        tests/ #集成测试 

```
run_once/
```toml
symbol = ["BTCUSDT"] # 必须
timeframe = ["1d"]   # 必须
# start = "2020-01-01" # 可选,默认从交易所获取
# end = "2025-01-01"#可选, 默认昨天
# dir = "data" # 数据存储目录,可选,默认cwd/data
# max_concurrent = 1000 # 最大并发数,默认1000
```
symbol,timeframe,start,end

get_start.rs # 获取binance 合约symbol第一跟k线 https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={timframe}&startTime=0&limit=1,转换开始时间
start=get_start.rs

构建下载列表:urls
https://data.binance.vision/data/futures/um/daily/klines/{symbol}/{timframe}/{symbol}-{timframe}-{start}.zip
...
https://data.binance.vision/data/futures/um/daily/klines/{symbol}/{timframe}/{symbol}-{timframe}-{end}.zip

扫描本地文件:local_files
{dir}/futures/um/daily/klines/{symbol}/{timframe}/*
后缀名归一化为.zip

生成下载urls: dl_urls
dl_urls=urls-local_files

下载-解压到内存-内存加载csv-处理表头-处理列数据类型-数据清洗-原子写入parquet

run_batch/
配置加载
```toml
# symbol = ["BTCUSDT"] # 可选 ,默认从交易所获取
# timeframe = ["1d"]   # 可选,默认全部
# start = "2020-01-01" # 可选,默认从交易所获取
# end = "2025-01-01"#可选, 默认昨天
# dir = "data" # 数据存储目录,可选,默认cwd/data
# max_concurrent = 1000 # 最大并发数,默认1000
```
展开symbol,timeframe
for symbol
 for timeframe
  构建配置 symbol,timeframe,start,end
  run_once::run_once()

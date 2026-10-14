# binance-data-downloader

## 项目目标

> 从 [Binance Public Data](https://data.binance.vision/?prefix=data/futures/um/daily/klines/) 下载 永续合约 K 线数据文件(保持原始交易所文件格式)
> 网站数据布局:

- 网址: https://data.binance.vision
  - futures/
    - cm/
    - um/
      - daily/
        - aggTrades/
        - bookDepth/
        - bookTicker/
        - indexPriceKlines/
        - klines/
          - {symbol}/
            - {timeframe}/
              - {symbol}-{timeframe}-{yyyy-mm-dd}.zip
        - markPriceKlines/
        - metrics/
        - premiumIndexKlines/
        - trades/
      - monthly/
  - option/
  - spot/

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
main.rs    只做入口
lib.rs     只做模块导出
cli.rs    clap
config.rs config-rs
error.rs  thiserror
app/
  mod.rs app 执行逻辑代码实现
  01_build_urls.rs
  02_build_download_tasks.rs
  03_download.rs

```

## 实现

### main.rs

- 初始化 tracing 日志
- 传递参数并且调用 app::run()

### app

#### 加载配置&效验

解析层（load:37-42 + 结构体属性）：

1. 文件可读，失败带路径。
2. TOML 可解析 + 日期格式 YYYY-MM-DD 合法，失败带路径。
3. deny_unknown_fields：未知键拒收。
   语义层（validate:52-88）：
4. symbol 非空。
5. timeframe 非空。
6. timeframe 白名单 12 值（VALID_TIMEFRAMES）。
7. concurrency≠0。
8. 双 Some 且 start>end 报错；另附归一化：双列表 sort+dedup。

#### 建全局 reqwest Client（UA/30s 超时/池 256）

#### end 处理

end=cfg.resolved_end(今天)（min(end,昨天)/缺省昨天）

#### 并行获取 symbol x 1m 的开始时间

#### 双层串行循环 symbol × timeframe

逐 symbol 调 run_one；失败只记数+收明细（上限 20），不中断后续 symbol。

##### run_one

1. 143-154：cfg.start>end 直接 Ok 跳过（end 已是钳制后的，cfg.start 显式未来才触发）。
2. 156：resolve_start 纯 max(cfg.start, sym_start)（sym_start 循环前按币取好传进来）。
3. 158-167：resolved>end 再跳过（币上市晚于区间）。
4. 177-184：build_download_tasks 逐日组 URL 并剔本地已有。
5. 186-212：有任务则 download::run_downloads 并发下并打统计，空则 debug 跳过；client 参数只剩下载用。

#### 打印汇总

## 问题:

- 当给的 config.start 早于币种最早开始时间时,会造成网络404,
  - 解决: 按币取一次币种开始时间(`symbol x 1m`,经 fapi 首根 + redb 缓存 30 天,位置 `~/.cache/`),下载起始取 max(币种开始时间,config.start)
  - 位置: 循环前 `buffer_unordered(8)` 并行取全币种,循环内 `run_one` 只做纯 max,不再联网
  - 文件: `src/utils/get_start_date.rs`, `src/utils/cache.rs`
- 当给的 config.start 晚于结束时间时,会建空任务,
  - 解决: 配置节双 Some 直接报错;运行时 `cfg.start>end` 跳过省 API,`resolved>end` 再跳过
- 当给的 config.end 超过今天时,数据尚未更新(含今天),会造成网络404,
  - 解决: `AppConfig::resolved_end` 一次性取 min(config.end, 昨天),下游只消费不再算时间
- 当本地已经有下载的数据文件时,会导致重复下载
  - 解决: 在循环开始前扫描本地已有文件一次(同遍清 `.tmp`), 在构建tasks 时剔除本地已有文件实现数据更新而不重复下载
  - 文件: `src/utils/scan_local_files.rs`
- 当遇到网络故障时
  - 解决: 增加重试机制,确保数据下载成功;404 单次即止记容忍,不重试
  - 文件: `src/utils/backoff.rs`
- 当配置写重复或写错时,会误下或空跑,
  - 解决: `symbol/timeframe` 排序去重;报错带文件路径;未知键拒收(`deny_unknown_fields`)

## 完整架构

```
main.rs    只做入口
lib.rs     只做模块导出
cli.rs    clap
config.rs toml 直解 + validate
error.rs  thiserror
app/
  mod.rs app 执行逻辑代码实现
  01_build_urls.rs
  02_build_download_tasks.rs
  03_download/ (mod.rs + stream.rs)
utils/
  mod.rs 聚合入口
  get_start_date.rs
  scan_local_files.rs
  cache.rs (redb)
  backoff.rs
```

## 完整配置

```toml
symbol = ["BTCUSDT"] # 必填，非空，自动排序去重
timeframe = ["1d"] # 必填，非空，12 值白名单 ["1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d"]，自动排序去重
# start = "2020-01-01" # 可选，缺省=币种首日；与 end 双写且 start>end 直接报错
# end = "2025-01-01" # 可选，缺省昨天；未来值截到昨天
# output_dir = "data" # 可选，缺省 "data"；数据根目录，程序自动追加 binance/data/um/daily/klines/...，不要手写进去
# concurrency = 64 # 可选，缺省 64；必须≠0；单个 pair 内最大并发下载数
```

- 未知键拒收（拼错即报错）；读写/解析失败带文件路径。
- 完整示例见 `config.example.toml`，大批量见 `config.top100.toml`，增量补全见 `config.update.toml`。

## 完整执行逻辑

- main.rs
  - 入口
  - 初始化日志 tracing（`RUST_LOG`，缺省 info）
  - 解析 `--config/-c` 参数给 `app::run()` 并调用它
- cli.rs
  - clap 4 derive 解析，`--config/-c` 必填，返回 `PathBuf`
- config.rs
  - `load`：读文件（失败带路径）→ `toml::from_str`（失败带路径）→ `validate`
  - 缺省：容器级 `Default`（空列表/`data`/16/`None`）
  - 校验：`symbol`/`timeframe` 非空，`timeframe` 12 值白名单，`concurrency≠0`，双 `Some` 且 `start>end` 报错，未知键拒收
  - 归一化：双列表 `sort+dedup`
  - `resolved_end(today)`：`min(end,昨天)`/缺省昨天，调用方算一次
  - `DownloadTask{url, dest, pair, timeframe}` 定义在此
- error.rs
  - `AppError`（thiserror）：Config/Api/Data/Date/Cache/Http/Io/Json/DownloadFailed/NotFound/Join
- app/mod.rs
  - 建全局 `reqwest Client`（UA/30s 超时/池 256）
  - 调 `utils::scan_local_files` 全量扫本地一次（同遍清 `.tmp`）
  - `end = cfg.resolved_end(今天)`，打启动日志
  - 循环前 `buffer_unordered(8)` 并行取全币种 `1m` 首根，收 `HashMap`；坏币记失败跳过整组
  - 双层串行 `symbol×timeframe` 逐 pair 调 `run_one`，失败记数收明细（≤20 条）不中断
  - 汇总：零失败 `Ok`，否则 `Err(Data("N pair(s) failed"))`
  - `run_one`：`cfg.start>end` 跳过 → 纯 `max(cfg.start,sym_start)` → `resolved>end` 跳过 → 组 tasks → 非空并发下打统计，空则跳过
- app/01_build_urls.rs
  - `DownloadUrl{url, rel_path, pair, timeframe}`，逐日拼 `binance/data/um/daily/klines/{币}/{周期}/{币}-{周期}-{日期}.zip`，前缀 `https://data.binance.vision/data/`
- app/02_build_download_tasks.rs
  - 调 `build_urls`，过滤本地已有（相对路径），`dest = output_dir.join(rel)`，得 `Vec<DownloadTask>`
- app/03_download/mod.rs
  - `buffer_unordered(concurrency)` 有界并发，每 100 个打进度，统计 `ok/not_found/failed`，`failed>0` 返 `Err`
- app/03_download/stream.rs
  - 单文件：GET → 404 永久 `NotFound` 不重试 → 429/5xx/网络瞬态指数退避（30s 上限）→ 其余 4xx 永久失败
  - 建父目录 → 流式写 `.zip.tmp` → `flush` → `rename` 原子落盘
- utils/mod.rs
  - 聚合入口：`backoff/cache/get_start_date/scan_local_files`
- utils/scan_local_files.rs
  - `spawn_blocking + walkdir` 遍历，得相对 `.zip` 集合；目录不存在返空；同遍删 `.tmp`
- utils/get_start_date.rs
  - `redb` 命中（TTL 30 天）直返，否则 `fapi klines?limit=1&startTime=0` 取首根写缓存；429/5xx 重试，其余 4xx 永久失败
- utils/cache.rs
  - `redb` 通用 KV（`~/.cache/{domain}/`），值带 8 字节时间戳，读时判 TTL，坏库只 warn 不中断
- utils/backoff.rs
  - 指数退避，`max_elapsed_time = 30s`

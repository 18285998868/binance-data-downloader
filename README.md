# binance-data-downloader 项目开发

## 需求分析

- 项目目标: 从 [Binance Public Data](https://data.binance.vision/?prefix=data/futures/um/daily/klines/) 下载 永续合约日度 K 线归档(zip)

## 技术选型

- 语言:rust
- edition = "2024"

## 冒烟配置

```toml
symbol = ["BTCUSDT"] # 必填
timeframe = ["1d"] # 可选 ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
# start = "2020-01-01" # 可选
# end   = "2025-01-01" # 可选
output_dir = "data" # 可选
```

## 逻辑编排

```
启动 -> 初始化 tracing; 解析 --config/-c
v
加载配置 -> toml 反序列化 + validate(symbol/timeframe 非空, concurrency≠0)
v
准备 -> 建 reqwest Client; 扫描本地已有 .zip 并清理 .tmp; end = config.end 或昨天
v
构建url() -> resolve_start = max(config.start, 首根K线日期), start>end 跳过; 逐日生成 futures/um/daily/klines/{pair}/{tf}/…zip
v
构建下载任务 -> 过滤本地已有文件 → Vec<DownloadTask>
v
执行下载 -> buffer_unordered(concurrency) 有界并发; .zip.tmp 流式落盘 → rename 原子生效; 404 永久放弃/网络·5xx 指数重试≤30s; 统计 ok/not_found/failed
v
汇总 -> 聚合失败 pair 明细(≤20 条); failed>0 返回 Err 否则 Ok
```

## 架构设计

```
# 支撑件(cli/config/error/cache/backoff)在 crate 根, 不编号; 执行代码在 app/, 按"调用执行流程"编号: main → cli → config → 01_scan → 02_start_date → 03_build_tasks → 04_run_downloads
main.rs                        # 入口: 初始化 tracing(fmt + env-filter, 缺省 info) → bdd::cli::parse() → bdd::app::run(&cli.config); 错误 anyhow 逐层加 context
lib.rs                         # 只做导出: #[path] 挂载 app/NN_* 步骤模块 + 支撑模块; pub use app::run / error::AppError
cli.rs                         # 支撑件: 命令行解析 clap 4(derive), 解析 --config/-c(必填), 返回 PathBuf
config.rs                      # 支撑件: 配置解析 toml + serde(deny_unknown_fields): symbol[]/timeframe[]/start/end(output_dir 默认 "data", concurrency 默认 16); validate 校验; 定义 DownloadTask{url, dest, pair, timeframe}
error.rs                       # 支撑件: AppError(thiserror, Config/Http/Io/Api/NotFound/DownloadFailed/Date/Data), #[from] 实现 ? 自动转换
cache.rs                       # 支撑件: redb 通用 KV 缓存 read/write(domain,key,ttl), 值带 8 字节时间戳前缀实现 TTL; 进程级静态 DB 池
backoff.rs                     # 支撑件: default_backoff() 指数退避, max_elapsed_time = 30s
app/                           # 执行代码
app/mod.rs                     # 整体编排 run(config_path) (伪代码):
                               #   1 AppConfig::load + validate (symbol/timeframe 非空, concurrency≠0, deny_unknown_fields)
                               #   2 建 reqwest Client(UA=binance-data-downloader/0.1, timeout=30s, pool_max_idle_per_host=256)
                               #   3 scan_local_files(output_dir) 一次性扫描本地 .zip 相对路径(spawn_blocking) + 清理 .tmp
                               #   4 end = config.end 或 Local::now() - 1天
                               #   5 for symbol in symbols                    # 品种层(串行)
                               #   6   for timeframe in timeframes           # 时间帧层(串行)
                               #   7     run_one: cfg.start > end → 直接跳过(不请求首根); 否则 resolve_start = max(start, get_first_kline_date)  # redb 缓存 → klines?limit=1&startTime=0
                               #   8       resolved start > end → warn 跳过
                               #   9       build_download_tasks(逐日 URL + 过滤本地已有)
                               #  10       run_downloads(client, tasks, concurrency) → 统计 ok/not_found/failed
                               #  11 失败 pair 聚合(≤20 明细); failed>0 → Err 否则 Ok
app/01_scan_local_files.rs     # 扫描本地: spawn_blocking + walkdir 遍历 output_dir → HashSet<相对 .zip 路径>; 目录不存在返回空; walk 错误 warn 不中断; 同遍删除 .tmp
app/02_get_start_date.rs       # 起始日期: get_first_kline_date(client,symbol,tf) — redb 缓存命中直返; 否则请求 fapi klines limit=1&startTime=0 取首根写入缓存; HTTP 状态码检查: 429/5xx 指数重试, 其余 4xx 永久失败
app/03_build_download_tasks.rs # 构建任务: build_urls 逐日生成 data.binance.vision URL + 相对路径(futures/um/daily/klines/{pair}/{tf}/{pair}-{tf}-{date}.zip) → 过滤本地已有 → Vec<DownloadTask>
app/04_run_downloads/          # 运行下载
  mod.rs                       # 运行下载: futures buffer_unordered(concurrency) 有界并发(至少 1); 增量统计 total/ok/not_found/failed; 进度每 100 个; 失败明细 ≤20
  stream.rs                    # 单文件下载: GET → dest 父目录建链 → .zip.tmp 流式写 → flush → rename 原子落盘; 404 容忍(不重试); 网络错误/429/5xx 指数重试, 其余 4xx 永久失败
```

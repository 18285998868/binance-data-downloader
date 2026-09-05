# binance-data-downloader 项目开发

## 需求分析

- 项目目标: 从 [Binance Public Data](https://data.binance.vision/?prefix=data/futures/um/daily/klines/) 下载 永续合约日度 K 线归档(zip)

## 技术选型

- 语言:rust
- edition = "2024"

## 架构设计

| 文件架构(保持顺序)                 | 功能简介         | 库依赖                                    | 具体逻辑                                                                                                        |
| ---------------------------------- | ---------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `main.rs`                          | 只做入口调用     | tokio、tracing-subscriber、anyhow         | 初始化 tracing(fmt + env-filter,缺省 info)→ `bdd::cli::parse()` → `bdd::app::run(config)`;错误以 anyhow 逐层加 context |
| `lib.rs`                           | 只做模块导入导出 | std(PathBuf)                              | `#[path]` 挂载 `app/` 各步骤 + 支撑模块;定义 `DownloadTask`;`pub use app::run` / `error::AppError`              |
| `error.rs`                         | 错误处理         | thiserror 2                               | `AppError` 枚举(Config/Http/Io/Api/NotFound/DownloadFailed/Data…),`#[from]` 实现 `?` 自动转换                   |
| `cache.rs`                         | 缓存(redb)       | redb 4                                    | `read/write(domain,key,ttl)` 通用 KV 缓存,值带 8 字节时间戳前缀实现 TTL;静态进程级 DB 池                        |
| `backoff.rs`                       | 退避策略         | backoff 0.4(tokio)                        | `default_backoff()`:指数退避,`max_elapsed_time = 30s`                                                           |
| `app/`                             | 步骤目录         | —                                         | 流水线按 `NN_` 编号归档;磁盘名带数字前缀,模块名经 `#[path]` 取可读步骤名                                        |
| `app/mod.rs`                       | 整体流程编配     | chrono、reqwest、anyhow                   | load 配置 → 建 client → 扫描本地 → 计算 end → 串行 `run_one`(resolve_start→build→run_downloads)→ 聚合配对失败  |
| `app/01_cli.rs`                    | 命令行解析       | clap 4(derive)                            | 解析 `--config/-c`(必填),返回 `PathBuf`                                                                         |
| `app/02_config.rs`                 | 配置解析         | toml 1.1 + serde(`deny_unknown_fields`)   | `AppConfig::load` 读 TOML;校验 symbol/timeframe 非空、`concurrency≠0`、日期格式;缺省值兜底                      |
| `app/03_scan_local_files.rs`       | 扫描本地文件     | walkdir 2                                 | `spawn_blocking` 遍历 output_dir,收集既有 `.zip` 相对路径集合,顺带清理 `.tmp`                                   |
| `app/04_get_start_date.rs`         | 获取起始日期     | reqwest(json)、redb、chrono、backoff      | `get_first_kline_date`:redb 命中直接返回;否则请求 klines 首根写入缓存;`resolve_start = max(config.start, 首根日期)` |
| `app/05_build_download_tasks.rs`   | 构建下载任务     | chrono                                    | `build_urls` 逐日生成 URL+相对路径;`build_download_tasks` 过滤本地已有 → `Vec<DownloadTask>`                    |
| `app/06_run_downloads/mod.rs`      | 运行下载任务     | futures(buffer_unordered)、tokio、reqwest | 流式有界并发(concurrency);增量统计 ok/not_found/failed;每 100 个进度;失败明细≤20;`failed>0` 返回错误            |
| `app/06_run_downloads/stream.rs`   | 下载任务流       | reqwest、tokio、backoff                   | 单文件 GET → `.zip.tmp` 流式写 → flush → rename 原子落盘;404 永久不重试,网络/5xx 指数重试(30s 封顶)             |

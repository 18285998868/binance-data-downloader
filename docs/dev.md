# binance-data-downloader 项目开发

## 需求分析

- 项目目标: 从 [Binance Public Data](https://data.binance.vision/?prefix=data/futures/um/daily/klines/) 下载 永续合约日度 K 线归档(zip)

## 技术选型

- 语言:rust
- edition = "2024"

## 架构设计

| 文件架构(保持顺序)                 | 功能简介         | 库依赖                                    |
| ---------------------------------- | ---------------- | ----------------------------------------- |
| `main.rs`                          | 只做入口调用     | tokio、tracing-subscriber、anyhow         |
| `lib.rs`                           | 只做模块导入导出 | std(PathBuf)                              |
| `error.rs`                         | 错误处理         | thiserror 2                               |
| `cache.rs`                         | 缓存(redb)       | redb 4                                    |
| `backoff.rs`                       | 退避策略         | backoff 0.4(tokio)                        |
| `app/`                             | 步骤目录         | —                                         |
| `app/mod.rs`                       | 整体流程编配     | chrono、reqwest、anyhow                   |
| `app/01_cli.rs`                    | 命令行解析       | clap 4(derive)                            |
| `app/02_config.rs`                 | 配置解析         | toml 1.1 + serde(`deny_unknown_fields`)   |
| `app/03_scan_local_files.rs`       | 扫描本地文件     | walkdir 2                                 |
| `app/04_get_start_date.rs`         | 获取起始日期     | reqwest(json)、redb、chrono、backoff      |
| `app/05_build_download_tasks.rs`   | 构建下载任务     | chrono                                    |
| `app/06_run_downloads/mod.rs`      | 运行下载任务     | futures(buffer_unordered)、tokio、reqwest |
| `app/06_run_downloads/stream.rs`   | 下载任务流       | reqwest、tokio、backoff                   |

## 具体逻辑

```text
入口 -> main.rs -> 调用 lib 导出的 app::run

流程编排 -> app/mod.rs -> 加载配置 → 建 client → 扫描本地 → 计算 end → 串行 run_one → 聚合失败
              -> app/01_cli.rs                -> 解析 --config/-c(必填),返回路径
              -> app/02_config.rs             -> 读 TOML,校验 symbol/timeframe/concurrency/日期
              -> app/03_scan_local_files.rs   -> walkdir 收集 .zip 相对路径,顺带清理 .tmp
              -> app/04_get_start_date.rs     -> redb 命中/API 查询首根 K 线,resolve_start 钳制
              -> app/05_build_download_tasks.rs -> 逐日生成 URL,过滤本地已有 → DownloadTask
              -> app/06_run_downloads/mod.rs  -> buffer_unordered(concurrency) 增量统计/进度/失败上限
              -> app/06_run_downloads/stream.rs -> GET 流式写 .zip.tmp → flush → rename;404 永久/5xx 重试
```

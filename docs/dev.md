# binance-data-downloader 项目开发

## 需求分析
- **项目目标**: 从 Binance Public Data(`data.binance.vision`)下载 USDT-M 永续合约日度 K 线归档(zip),按 `symbol × timeframe × 日期` 一文件一个 zip 落到本地,供策略回测与指标研究使用。
- **功能需求**:
  - 命令行 `-c/--config <path>` 指定 TOML 配置(必填)。
  - 配置声明 `symbol`(必填)、`timeframe`(必填)、可选 `start`/`end`、`output_dir`(缺省 `data`)、`concurrency`(缺省 16)。
  - 增量幂等:已存在的本地文件自动跳过;`start` 缺省钳制到交易所首根 K 线日期(redb 缓存)。
  - 边界 404(交易所无该日期数据)计入 `not_found` 不视为失败;文件级失败不中断同批;最终有失败才报错。
  - 有界并发下载 + 流式磁盘写入 + 原子落盘。
- **用户故事**:
  - 作为策略研究员,我设置 `symbol=["BTCUSDT"], timeframe=["1d"]` 即可补全历史日线到昨天。
  - 作为运维,我重跑同一命令时,不应重复下载已存在的文件(断点续传)。
  - 作为使用者,我希望个别文件失败不拖垮整批,并能在日志中看到进度与失败明细。

## 技术选型
- **语言/运行时**: Rust 2024 edition;异步采用 `tokio`(full features)。
- **CLI**: `clap` 4(derive),单一 `--config/-c` 参数。
- **HTTP**: `reqwest` 0.13(`rustls` TLS + `socks` 代理支持,关闭默认 native-tls),配共享连接池。
- **并发**: `futures::stream::buffer_unordered(concurrency)` 有界并发,增量统计不 collect 全量。
- **配置**: `toml` 1.1 + `serde`;`deny_unknown_fields` 未知字段启动即报错。
- **错误处理**: `thiserror`(库内 `AppError`)+ `anyhow`(主流程 context)。
- **日志**: `tracing` + `tracing-subscriber`(env-filter)。
- **缓存**: `redb` 4(KV 落盘,存首根 K 线日期;TTL)。
- **重试**: `backoff` 0.4(tokio),默认指数退避,max 30s。
- **扫描**: `walkdir`(收集已有 zip / 清理 .tmp)。
- **测试**: `wiremock`(mock HTTP)+ `tempfile`。

## 架构设计
- **总体结构**: 单包 crate,`main.rs` 只做入口调用,核心逻辑全部在 `lib`(crate 名 `bdd`)。
  ```
  main.rs: cli::parse() → app::run(config)
  ```
  `lib.rs` 用 `#[path]` 声明步骤模块;磁盘文件名带数字前缀,模块名 = 可读步骤名。
- **系统架构(流水线)**:
  ```
  01_cli → 02_config → 03_scan_local_files → 04_get_start_date → 05_build_download_tasks → 06_run_downloads → 07_app(编排)
  ```
  支撑层:`error.rs`(AppError)、`cache.rs`(redb 缓存)、`backoff.rs`(退避)。
- **并发模型**: 配对(symbol × timeframe)层串行;配对内 `buffer_unordered` 有界并发,总量恒等于 `concurrency`。
- **API 设计**: 对外暴露 `bdd::cli::parse()` 与 `bdd::app::run()`;步骤间数据流:`Config → HashSet<rel_path> → NaiveDate → Vec<DownloadTask> → DownloadStats`。
- **数据设计**: 下载产物 `{output_dir}/futures/um/daily/klines/{pair}/{tf}/{pair}-{tf}-{date}.zip`;缓存库 `~/.cache/start_date/start_date.redb`,key `symbol:tf`。

## 开发实现
- **01_parse_cli.rs (`cli`)**: clap 解析 `--config/-c`(必填),返回路径。
- **02_load_config.rs (`config`)**: `AppConfig::load` 读 TOML(`deny_unknown_fields`),校验 symbol/timeframe 非空、`concurrency≠0`、日期格式;缺省值兜底。
- **03_scan_local_files.rs (`scan_local_files`)**: `spawn_blocking` + `walkdir` 一次性扫描本地 `.zip` 相对路径集合,顺带清理 `.tmp` 残留。
- **04_get_start_date.rs (`get_start_date`)**: `get_first_kline_date` 查 `symbol:tf` 首根 K 线日期;redb 命中则不请求 `fapi.binance.com`,否则 API 查询并回写缓存;`resolve_start = max(配置 start, 首根日期)`。
- **05_build_download_tasks.rs (`build_download_tasks`)**: `build_urls` 逐日生成 URL+相对路径,`build_download_tasks` 过滤本地已存在 → `Vec<DownloadTask>`。
- **06_run_downloads/ (`run_downloads`)**: `mod.rs` 流式 `buffer_unordered(concurrency)`,`while let` 增量统计 ok/not_found/failed,每 100 个打进度,失败明细最多 20 条,`failed>0` 才返回错误;`stream.rs` 单文件 `GET → .zip.tmp 流式写 → flush → rename` 原子落盘,非 2xx 判 404(永久)或 Transient 重试。
- **07_run.rs (`app`)**: 顶层编排:加载配置 → 建 client → 扫描 → 计算 end(配置或昨天)→ 串行遍历配对 `run_one` → 聚合配对失败(最多 20 条明细)。
- **关键约定**: `.tmp` 文件可被中断残留,下次扫描自动清理;404 计数为 `not_found` 并视为预期;`concurrency.max(1)` 兜底。

## 测试验证
- **单元测试 + mock**: 覆盖配置校验、URL/任务构建、首根 K 线日期解析、本地扫描、并发调度与增量统计、单文件下载(404/5xx/成功,wiremock)、缓存与退避。
- **验证命令**:
  ```bash
  cargo check -p binance-data-downloader --all-targets
  cargo clippy -p binance-data-downloader --all-targets   # 0 警告
  cargo test -p binance-data-downloader                   # 45 通过
  cargo run -p binance-data-downloader --release -- -c config/config.toml  # 冒烟
  ```
- **冒烟结果**: 单配对(BTCUSDT/1d)完整跑通,`start > end` 场景、边界 404(`not_found`)行为符合预期。

## 部署上线
- **构建**: `cargo build --release`(profile: fat LTO、codegen-units=1、strip symbols)。
- **部署形态**: 本地/服务器直接跑二进制 + `config.toml`;无外部服务依赖(数据源为公开归档)。
- **运行**: `RUST_LOG=info binance-data-downloader -c config/config.toml`;网络代理走 socks5(`reqwest` socks 支持)。
- **配置监控**: tracing 日志含批次进度(`done/total · ok/not_found/failed`)与失败明细,可接日志平台;`failed > 0` 时进程以非零退出码返回(供上层告警)。

## 维护迭代
- **已知边界**: 边缘日期 404 为预期;某配对解析首根 K 线失败仅该配对报错,不拖垮整批。
- **可能的后续方向**:
  - kline 归档细粒度(other intervals)、现货数据源、`start/end` 校验前置到配置层。
  - 全局并发信号量替代"配对串行"约束,提高多配对吞吐。
  - 失败明细超过 20 条时的落盘持久化。
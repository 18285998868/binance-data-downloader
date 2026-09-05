# 开发笔记:每个步骤做了什么

源码按流水线步骤归档于 `src/steps/NN_*.rs`,模块名经 `lib.rs` 的 `#[path]` 映射为可读步骤名(`cli`/`config`/`scan_local_files`/`get_start_date`/`build_download_tasks`/`run_downloads`/`app`)。步骤之间通过函数调用串联,`07_app.rs` 是顶层编排。

## 01_cli.rs (`cli`)

clap 解析命令行,唯一参数 `--config/-c`(必填),返回配置路径。

## 02_config.rs (`config`)

`AppConfig::load`:读 TOML(`deny_unknown_fields`),校验(symbol/timeframe 非空、concurrency≠0、日期格式),产出 `AppConfig`。

## 03_scan_local_files.rs (`scan_local_files`)

`scan_local_files`:`spawn_blocking` 里 walkdir 遍历 `output_dir`,收集已有 `.zip` 的相对路径形成 `HashSet`,顺手清理残留 `.tmp`。

## 04_get_start_date.rs (`get_start_date`)

`get_first_kline_date`:查 `symbol:tf` 的交易所首根 K 线日期;先命中 redb 缓存,否则请求 `fapi.binance.com/fapi/v1/klines?limit=1&startTime=0`,解析后写回缓存。用于把起始日期钳制到不早于首根 K 线。

## 05_build_download_tasks.rs (`build_download_tasks`)

`build_urls`:按日期生成下载 URL + 相对路径(`data.binance.vision/data/futures/um/daily/klines/...`);`build_download_tasks`:过滤掉本地已存在的,映射为 `DownloadTask{url, dest, pair, timeframe}`。

## 06_run_downloads/ (`run_downloads`)

- `mod.rs`:`run_downloads`,把任务流入 `buffer_unordered(concurrency)` 有界并发下载,`while let` 增量统计 `DownloadStats{ok, not_found, failed}`,每完成 100 个打一条进度,失败明细最多记 20 条,`failed > 0` 才返回错误。
- `stream.rs`:`download_to_file`,GET → 非 2xx 判 404(永久失败)或重试 → 流式写入 `.zip.tmp` → flush → rename 原子落盘;网络错误 / 5xx 套 `backoff` 重试。

## 07_app.rs (`app`)

顶层编排 `run`:load 配置 → 建 reqwest client → 扫描本地 → 计算 end(配置或昨天)→ 串行遍历 `symbol × timeframe` 调 `run_one`(`resolve_start` 钳制起始日期 → 建任务 → `run_downloads`)→ 聚合配对失败,最多留 20 条明细。
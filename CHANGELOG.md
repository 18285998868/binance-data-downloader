# 变更记录

本文件遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 变更

- crate 重命名：`bdd` → `binance-data-core`，`bdd-cli` → `binance-data-cli`；
  目录同步为 `crates/binance-data-core` 与 `crates/binance-data-cli`。
- 起始日缓存由 `redb` 改为每 domain 一个 JSON 文档，写入保持
  `临时文件 + fsync + rename`；移除 `redb` 依赖。
- 独立校验器的遍历逻辑下沉到 `binance-data-cli` 库，二进制只解析参数并映射退出码。
- `binance-data-verify --jobs` 增加上限：`0` 仍表示可用 CPU 数，显式值夹紧到
  `available × 4`。
- 端到端 pipeline 测试拆分为 `downloads` / `start_dates` / `cancel` / `prepare`。
- 输出目录锁的阻塞文件 I/O 移入 `spawn_blocking`。
- 错误明细上限 `MAX_BATCH_ERRORS` 集中到 `limits.rs`。
- User-Agent 改为从 `CARGO_PKG_NAME` / `CARGO_PKG_VERSION` 生成。
- README 拆分为 `docs/` 下的主题文档，并补充 ADR。
- 质量门禁移除空转的 `cargo bench --no-run`（仓库没有基准）。
- `config/00.smoke.toml` 的 `output_dir` 改为相对路径 `../data/binance`。
- 移除 `deny.toml` 中未使用的 `Zlib` 许可证。
- 两个 crate 标记 `publish = false`，移除错误的 `authors` 元数据。
- 旧版 `python/download.ipynb` 移至 `docs/legacy/`。

## [0.1.0] - 2026-11-13

### 新增

- 11 种 Binance 归档数据集的下载，spot / USDⓈ-M / COIN-M。
- 按网页路径原样落盘的目录布局。
- 起始日解析：市场 API 与 S3 归档清单并发合并，缓存 30 天。
- 有界并发、取消 token、404 阈值、连续失败熔断。
- 流式下载、内容校验、`fsync` + `rename` 原子落盘、残留 tmp 回收。
- 独立校验器 `binance-data-verify` 与下载期 `--verify`。
- 本地与 CI 共用的质量门禁 `scripts/check.sh`。

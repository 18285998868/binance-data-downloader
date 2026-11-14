# binance-data-downloader

从 [Binance Public Data](https://data.binance.vision) 下载 **spot / USDⓈ-M / COIN-M**
的 11 种归档数据集，保持原始 zip 格式，并按网页路径原样落盘。

## 快速开始

```bash
cp config/00.example.toml config.toml
# 按需修改 market / symbol / dataset / output_dir / start / end
cargo build --release
RUST_LOG=info ./target/release/binance-data-downloader -c config.toml

# 可选：校验本次新下载文件的 zip 结构
RUST_LOG=info ./target/release/binance-data-downloader -c config.toml --verify

# 独立校验已有输出目录中的全部 .zip（不下载、不改文件）
./target/release/binance-data-verify --dir path/to/data/binance
./target/release/binance-data-verify --dir path/to/data/binance --jobs 8
```

- 已存在的 `.zip` 按相对路径跳过，重复执行安全，可用于增量补全。
- 需要代理时用 `HTTPS_PROXY` / `NO_PROXY` 控制，见[可靠性](docs/reliability.md#模块九代理)。
- `Ctrl-C` 立即停止调度与在途下载；重跑从已完成文件处续跑，取消不算失败。
- 日志由 `RUST_LOG` 控制（缺省 `info`）。
- 成功与被取消的退出码为 `0`，任何失败为 `1`。

## 文档

| 文档 | 内容 |
| --- | --- |
| [数据集与路径](docs/datasets.md) | 11 种数据集矩阵、K 线周期、落盘布局、月档语义 |
| [配置](docs/config.md) | 全部字段、默认值、校验规则、冒烟配置 |
| [架构](docs/architecture.md) | 五步 pipeline、模块树、公开接口 |
| [可靠性](docs/reliability.md) | 起始日解析、404、取消、并发、重试、原子落盘、退出码 |
| [开发](docs/development.md) | 工具链、质量门禁、测试布局、打包限制 |
| [架构决策](docs/adr/) | 不声明 MSRV、JSON 缓存、串行 pair 循环、内部 crate |
| [变更记录](CHANGELOG.md) | 版本历史 |

## 要求

- 最新 stable Rust（`rust-toolchain.toml` 自动切换；刻意不声明 `rust-version`，
  见 [ADR-0001](docs/adr/0001-no-rust-version.md)）。
- Linux 或 macOS：并发上限读取 `RLIMIT_NOFILE`，Windows 不在支持范围。

## 开发

```bash
./scripts/check.sh        # 本地门禁
./scripts/check.sh --ci   # CI：nextest 使用 ci profile
```

详见 [开发文档](docs/development.md)。

## 许可证

MIT，见仓库根目录的 `LICENSE` 文件。

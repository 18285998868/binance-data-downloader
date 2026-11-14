# 开发

## 模块一：工具链

1. `rust-toolchain.toml` 固定 `stable`，并安装 `rustfmt`、`clippy`。
2. 项目不声明 `rust-version`，见 [ADR-0001](./adr/0001-no-rust-version.md)。
3. 依赖版本用 `Cargo.lock` 锁定；门禁统一带 `--locked`。

```bash
rustc --version
cargo --version
```

## 模块二：质量门禁

1. 本地与 CI 共用 `scripts/check.sh`。
2. 提交前必须全绿；任何警告按失败处理。
3. `cargo-nextest`、`cargo-deny`、`cargo-audit` 未安装时脚本打印 `skip`，不会伪装成通过。

```bash
./scripts/check.sh        # 本地
./scripts/check.sh --ci   # CI：nextest 使用 ci profile
```

门禁顺序：fmt → check → clippy（全特性 / 无默认特性）→ nextest → doctest → doc →
cargo-deny → cargo-audit。

说明：

- 仓库没有基准，门禁不再执行空转的 `cargo bench --no-run`；新增 `benches/` 后应把该步骤加回。
- nextest 不运行 doctest，因此 doctest 单独执行。
- 无 MSRV job，原因见 ADR-0001。

## 模块三：lint 与格式

1. lint 级别集中在根 `Cargo.toml` 的 `[workspace.lints]`。
2. `unsafe_code = "forbid"`；生产路径禁止 `unwrap/expect/panic/todo`，测试豁免由 `clippy.toml` 控制。
3. `rustfmt.toml`、`clippy.toml` 只放工具专属配置。
4. 依赖审计配置在 `deny.toml`；未使用的许可证条目会被 `cargo-deny` 报告。

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## 模块四：测试布局

1. 单元测试与实现同文件，放 `#[cfg(test)] mod tests`。
2. 端到端 pipeline 测试放在 `crates/binance-data-core/src/pipeline_tests/`，按行为分
   `downloads`、`start_dates`、`cancel`、`prepare`，共享 mock 在 `harness`。
3. 这是有意偏离 `tests/` 惯例：`run_with_endpoints` 与 `config::for_tests` 是 crate 私有接缝，
   移进 `tests/` 会迫使它们变成公开 API。
4. CLI 集成测试放 `crates/binance-data-cli/tests/`，通过 `CARGO_BIN_EXE_*` 运行真实二进制。
5. 公共 API 契约测试放 `crates/binance-data-core/tests/public_api.rs`。

```bash
cargo nextest run --workspace
cargo test --workspace --doc
```

## 模块五：打包边界

1. 两个 crate 都是内部 crate，`publish = false`，不发布到 crates.io。
2. `binance-data-core` 的 crate 文档通过 `include_str!` 引用仓库根 `README.md`。
3. 该引用位于 crate 目录之外，因此 `cargo package` 的 verify 步骤会失败；
   这是已知且有意的限制，不是发布流程的一部分。
4. 若未来需要打包，把 README 复制进 crate 并加防漂移测试，或改用独立的 crate 文档。

```bash
cargo package -p binance-data-core --allow-dirty --no-verify
```

## 模块六：文档维护

1. README 只保留入口与链接；细节按主题放在 `docs/`。
2. 文档路径相对当前文件（同级 `./a`、子目录 `./a/b`、上级 `../a`）。
3. 架构决策写在 `docs/adr/`，一个决策一个文件。
4. 行为或公开接口变化同步更新 `CHANGELOG.md`。
5. 移动文件后用 `rg` 检查旧路径，并修正脚本、注释与 `.gitignore`。

```bash
rg -n 'crates/core|crates/cli|bdd' .
```

## 模块七：历史文件

`docs/legacy/download.ipynb` 是旧版 Python 下载脚本，描述的是已废弃的
`data/um/...` 布局，仅作参考，不参与构建与门禁。

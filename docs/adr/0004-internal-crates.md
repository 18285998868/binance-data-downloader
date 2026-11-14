# ADR-0004：两个 crate 均为内部 crate

## 状态

已接受（2026-11）。

## 背景

`binance-data-core` 与 `binance-data-cli` 只服务于本仓库的两个二进制，没有外部使用者。
直接发布会带来额外承诺：semver、文档、元数据、打包验证。

同时，核心 crate 的文档通过 `include_str!("../../../README.md")` 引用仓库根 README，
该文件在 crate 目录之外。

## 决策

1. 工作区 `[workspace.package]` 设 `publish = false`，成员继承。
2. 不承诺 semver；公开面只保留嵌入方可能需要的类型。
3. 接受 `cargo package` 的 verify 失败：内部 crate 不进入打包/发布流程。
4. 若未来要发布，需先把 README 移入 crate 或改用独立 crate 文档，并补防漂移测试。

## 后果

- `cargo publish` 在本地直接失败，不会误传。
- 打包不是门禁的一部分，文档已注明限制。
- 公开 API 仍按库的标准写文档与测试，便于将来改变发布决策。

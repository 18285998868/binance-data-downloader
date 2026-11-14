# ADR-0001：不声明 `rust-version`

## 状态

已接受（2026-11）。

## 背景

项目目标始终是“用最新 stable”。`rust-version` 是**最低支持版本**的下限声明：

1. 它允许 cargo 用更老的工具链构建，鼓励版本漂移。
2. `clippy::incompatible_msrv` 会拦截任何比声明更新的 API，恰好挡住“用上最新 Rust”。
3. 它需要随每次 stable 升级手工跟进，迟早成为过期信息。

## 决策

1. 不声明 `rust-version`。
2. 下限由 `rust-toolchain.toml` 隐式表达：只会用最新 stable。
3. CI 不设 MSRV job。

## 后果

- 贡献者必须使用最新 stable。
- 依赖树自身的 rustc 下限不是本项目的承诺，而是使用最新 stable 时自然满足的事实。

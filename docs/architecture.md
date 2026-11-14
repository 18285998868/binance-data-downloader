# 架构

## 模块一：五步 pipeline

1. `config`：加载 TOML、校验、展开 `[[dataset]]` 为 concrete jobs。
2. `prepare`：取输出目录单实例锁、建 HTTP client、扫描本地文件、裁剪并发。
3. `start_dates`：并发解析每个 `(symbol, job)` 的起始日，结果缓存 30 天。
4. `download`：`symbol × job` 串行循环，pair 内按 `concurrency` 并发下载。
5. `report`：汇总统计，决定 `Ok` 或 `Err(Data(FailureReport))`。

```bash
cargo nextest run -p binance-data-core
```

## 模块二：crate 布局

```text
crates/binance-data-core/src/  下载引擎（包名 binance-data-core）
  lib.rs                公开导出 run / RunOptions / verify_zip / AppError / FailureReport
  app.rs                五步流程：config -> prepare -> start_dates -> download -> report
  config/
    mod.rs              TOML 加载、Config 门面
    raw.rs              反序列化结构与默认值
    validate.rs         语义校验、symbol/job 派生
    error.rs            配置错误（路径只渲染一次）
  prepare/
    mod.rs              本次运行的资源准备
    lock.rs             输出目录单实例锁（阻塞 I/O 走 spawn_blocking）
    client.rs           HTTP client
    signal.rs           Ctrl-C 处理
  start_dates/
    mod.rs              并发调度、结果与降级
    market.rs           市场 klines API 首日
    archive.rs          S3 清单解析（按 job）
    merge.rs            两个来源合并为每个 job 的起始日
    error.rs            起始日错误
  download/
    mod.rs              market/job 循环、404 阈值、熔断
    pair.rs             单个 symbol/job pair
    tasks.rs            任务构建（剔除本地已有）
      urls.rs           URL / 相对路径生成（迭代器）
    fetch.rs            有界并发调度
      stream.rs         重试、流式落盘、原子 rename
      validate.rs       zip magic / Content-Length / 可选 zip 校验
    error.rs            单文件下载错误
  report.rs             成功/失败判定与 FailureReport
  dataset.rs            Market / Period / Interval / DatasetKind / DownloadJob
  scan.rs               本地文件扫描与 tmp 清理
  cache.rs              JSON 文件 KV 缓存（CacheError）
  backoff.rs            通用重试 / Retry-After / 120s 窗口
  limits.rs             RLIMIT_NOFILE → 实际并发、错误明细上限
  endpoints.rs          生产/测试端点
  verify.rs             zip 中央目录校验
  pipeline_tests/       端到端测试：downloads / start_dates / cancel / prepare
crates/binance-data-cli/src/   CLI 包 binance-data-cli
  lib.rs                可供测试的库入口
  verify.rs             目录遍历 + 并发校验，返回 Summary
  main.rs               下载器入口：tracing、-c/--config/--verify
  cli.rs                clap 参数
  bin/verify/main.rs    校验器入口：参数、结果渲染、退出码
crates/binance-data-cli/tests/
  cli.rs                真实二进制的 CLI 集成测试
  verify.rs             校验器：空目录 / 合法 / 损坏 / 缺失
crates/binance-data-core/tests/public_api.rs  公开接口集成测试
```

## 模块三：依赖方向

1. `binance-data-cli` 依赖 `binance-data-core`，反向不成立。
2. 核心 crate 不读环境变量、不初始化 tracing subscriber、不调用 `process::exit`。
3. 二进制只做参数解析、日志初始化、调用库、映射退出码。

```bash
cargo tree -p binance-data-cli
```

## 模块四：公开接口

1. 内部 API 一律 `pub(crate)`。
2. 公开面仅 `run`、`run_with_options`、`RunOptions`、`verify_zip`、`AppError`、
   `FailureReport`。
3. 端到端测试需要的接缝（`run_with_endpoints`、`config::for_tests`）保持 crate 私有，
   测试放在 `src/pipeline_tests/`，不为此扩大公开 API。

## 模块五：设计决策

- 不声明 `rust-version`：[ADR-0001](./adr/0001-no-rust-version.md)。
- 缓存用 JSON 文件而非嵌入式数据库：[ADR-0002](./adr/0002-json-cache.md)。
- `symbol × job` 串行循环：[ADR-0003](./adr/0003-serial-pair-loop.md)。
- 两个 crate 均为内部 crate：[ADR-0004](./adr/0004-internal-crates.md)。

## 模块六：文档策略

1. `binance-data-core` 的 crate 文档即仓库根 `README.md`，避免两份说明漂移。
2. 细节按主题拆到 `docs/`，README 只保留入口和链接。
3. 文档内路径全部相对当前文件；移动文件后用 `rg` 检查旧路径。

```bash
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps
```

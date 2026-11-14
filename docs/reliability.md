# 可靠性

## 模块一：起始日解析

1. 市场 klines API（`1m`）回答“币种什么时候开始交易”，每币一次。
2. S3 归档清单（`max-keys=1`）回答“归档什么时候开始发布”，每 job 一次。
3. 两个来源并发，互不依赖；归档日期通常更晚，因此作为主约束。
4. 起始日 = `max(config.start, API 首日, 归档首日)`。
5. `indexPriceKlines` 例外：直接用归档首日，再与 `config.start` 取较晚者，因为指数可能早于合约上市。
6. 每个 job 保留自己的首日；不同数据集/周期可以有不同的归档起点。
7. 缓存 key 含 `market + job + symbol`，跨市场不互相污染；TTL 30 天；空清单不缓存。

```text
https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&max-keys=1&prefix=data/futures/um/daily/klines/BTCUSDT/1m/
```

## 模块二：失败分级

1. API 返回 `-1121 unknown symbol`：硬失败，避免拼写错误静默成功。
2. 临时性 API 失败且配置了 `start`：降级到配置日期，只 WARN。
3. 临时性 API 失败且没有 `start`、归档也没有数据：该币跳过并计入报告。
4. 归档清单不可用：退回 API 日期。
5. 单个文件 404：计入 `not_found`，默认不改变退出码。

## 模块三：404 阈值

1. 404 单次即止，不重试。
2. 配置 `not_found_pct_threshold = t` 后，本轮 404 占比达到 `t` 即判失败。
3. 取消导致样本不完整时不应用阈值，避免 Ctrl-C 把正常续传变成失败。

```toml
not_found_pct_threshold = 50.0
```

## 模块四：取消与并发

1. 一个取消 token 同时被三处观察：起始日预取、`symbol × job` 循环、每个在途下载。
2. 取消不算失败，退出码 `0`；已落盘文件有效。
3. 每个在途下载占 2 个 fd（socket + 临时文件），实际并发为
   `min(config.concurrency, (RLIMIT_NOFILE - 64) / 2, 1024)`，收紧时打 WARN。
4. 同一 `output_dir` 同时只允许一个进程；第二个进程立即失败。
5. 锁文件 `.cache/run.lock` 由 OS advisory lock 管理，进程退出自动释放。

## 模块五：重试与落盘

1. 可重试：连接错误、5xx、408、429、zip 校验失败、截断响应。
2. 不重试：404、其余 4xx、未知币种、认证/参数错误。
3. 429/503 的 `Retry-After` 优先于指数退避；总重试窗口 120 秒。
4. 先写 `{dest}.{pid}.tmp`，校验魔数与 `Content-Length`，`fsync` 后 `rename` 原子替换。
5. 崩溃残留的 `.tmp` 在下次扫描时按 mtime 超过 5 分钟回收。

## 模块六：系统性故障

1. 连续 5 个 pair 失败即熔断，停止整批，避免网络中断时逐个耗尽重试窗口。
2. 重跑时已存在的文件按路径跳过，从断点续传。
3. 错误明细最多保留 20 条，报告不会因一次全量失败而不可读。

## 模块七：风险对照

| 风险 | 处理 |
| --- | --- |
| `config.start` 早于归档首日 → 404 | 起始日取 `max(API 首日, 归档首日, config.start)`（index 用归档首日） |
| 归档清单不可用 | WARN 后退回 API 日期，不中断 |
| API 限速 | 清单答了就用清单；两者都失败且无 `start` 才跳过该币 |
| 月档未发布完整 | 只请求完整月份 |
| 本地已有文件 | 扫描一次并剔除；重复执行安全 |
| 服务端返回 HTML / 截断 | 校验 zip 魔数与 `Content-Length`，不符重试 |
| zip 结构损坏 | `--verify` 校验中央目录；旧文件用 `binance-data-verify` |
| 系统性故障 | 连续 5 个 pair 失败即熔断，重跑续传 |
| 慢速滴水响应 | 整体请求超时 300s |
| 并发超过 fd 上限 | 按 `RLIMIT_NOFILE` 收紧并发 |
| 崩溃残留 `.tmp` | 下次扫描按 mtime 超过 5 分钟回收 |
| 落盘中断 | 临时文件 + `fsync` + `rename` 原子替换 |
| 日期游标溢出 | `checked_add_*`，越界即停，不 panic |

## 模块八：退出码

1. `0`：全部成功，或被 Ctrl-C 取消。
2. `1`：配置错误、起始日解析致命失败、下载失败、404 占比超阈值。
3. 错误输出到 stderr；成功摘要输出到 stdout。

```bash
RUST_LOG=info ./target/release/binance-data-downloader -c config.toml
echo $?
```

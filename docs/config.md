# 配置

## 模块一：加载顺序

1. 命令行 `-c/--config` 指定唯一配置文件。
2. 文件按 TOML 反序列化：原始结构只描述文件，不接受未知键。
3. 语义校验通过后才生成业务侧 `Config`，失败时错误带配置文件路径。
4. 环境变量不参与配置合并；日志级别用 `RUST_LOG` 单独控制。

## 模块二：最小配置

```toml
market = "futures/um"
symbol = ["BTCUSDT"]

[[dataset]]
type = "klines"
periods = ["daily", "monthly"]
intervals = ["1m", "1d"]
```

## 模块三：全部字段

```toml
market = "futures/um" # 必填：spot | futures/um | futures/cm（也接受 um / cm 简写）
symbol = ["BTCUSDT"]  # 必填：非空，字符集 [A-Za-z0-9_]；运行时转大写、排序、去重
# start = "2020-01-01" # 可选：缺省=每个 job 自己的归档首日
# end   = "2025-01-01" # 可选：缺省昨天；未来值截到昨天
# output_dir = "path/to/data/binance" # 可选：data/ 的父目录；缺省 "."
# concurrency = 64 # 可选：0 < c ≤ 1024；单个 pair 内最大并发
# not_found_pct_threshold = 50.0 # 可选：0 < t ≤ 100；404 占比达到 t 判失败

[[dataset]]
type = "klines"          # 必填：11 种之一
periods = ["daily"]      # 必填：非空，daily / monthly
intervals = ["1m"]       # K 线类必填；非 K 线类不得出现
```

完整带注释示例见 [`config/00.example.toml`](../config/00.example.toml)。

## 模块四：校验规则

1. 未知键拒收（`deny_unknown_fields`，拼错即报错）。
2. `market`、`symbol`、`dataset` 必填；`dataset` 不允许为空。
3. 每种 `type × market × period` 必须真实存在；`intervals` 必须在该 market/period 下存在。
4. 重复展开出的同一个 `(type, period, interval)` job 直接报错。
5. `start > end` 直接报错；`concurrency`、`not_found_pct_threshold` 越界直接报错。
6. `indexPriceKlines` 落到相同指数路径的重复配置直接报错。

## 模块五：日期与月档

1. `end` 缺省为昨天（UTC）；未来值截到昨天。
2. `start` 缺省时逐 job 解析归档首日，见[可靠性](./reliability.md)。
3. 月档只请求完整月份；`start` 落在月中时从该月整月开始。

## 模块六：运行

1. 复制示例配置。
2. 修改字段。
3. 用 release 二进制运行。

```bash
cp config/00.example.toml config.toml
cargo build --release
RUST_LOG=info ./target/release/binance-data-downloader -c config.toml
```

仓库内的 [`config/00.smoke.toml`](../config/00.smoke.toml) 固定 7 天窗口，并把
`output_dir` 指向仓库外的 `../data/binance-smoke`，用于在仓库根目录做一次真实冒烟：

```bash
RUST_LOG=info cargo run --release -- -c config/00.smoke.toml
```

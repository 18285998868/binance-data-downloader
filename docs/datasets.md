# 数据集与路径

## 模块一：支持矩阵

1. `market` 取 `spot`、`futures/um`、`futures/cm`。
2. `periods` 取 `daily`、`monthly`。
3. 每种 `type` 在目标市场下必须真实存在，否则配置校验直接报错。

| `type` | spot | futures/um | futures/cm | 支持的 period |
| --- | --- | --- | --- | --- |
| `aggTrades` | ✅ | ✅ | ✅ | daily / monthly |
| `trades` | ✅ | ✅ | ✅ | daily / monthly |
| `klines` | ✅ | ✅ | ✅ | daily / monthly |
| `bookDepth` | ❌ | ✅ | ✅ | daily |
| `bookTicker` | ❌ | ✅ | ✅ | daily / monthly |
| `fundingRate` | ❌ | ✅ | ✅ | monthly |
| `liquidationSnapshot` | ❌ | ❌ | ✅ | daily |
| `metrics` | ❌ | ✅ | ✅ | daily |
| `indexPriceKlines` | ❌ | ✅ | ✅ | daily / monthly |
| `markPriceKlines` | ❌ | ✅ | ✅ | daily / monthly |
| `premiumIndexKlines` | ❌ | ✅ | ✅ | daily / monthly |

## 模块二：K 线周期

1. K 线类数据集必须提供 `intervals`，其他数据集禁止提供。
2. 周期必须在该 market/period 下存在。

- spot daily：`1s` … `1d`
- spot monthly：`1s` … `1mo`
- um/cm daily：`1m` … `1d`（`3d`、`1w`、`1mo` 只有月档完整覆盖，日档不请求）
- um/cm monthly：`1m` … `1mo`

## 模块三：落盘布局

1. `output_dir` 是归档根目录，即网页 `data/` 的父目录。
2. 本地路径严格镜像网页路径，没有第二套规则。
3. 旧版本路径（`data/um/...`）不兼容，需要手工迁移。

```toml
market = "futures/um"
output_dir = "path/to/data/binance"
```

```text
path/to/data/binance/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-02.zip
path/to/data/binance/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip
path/to/data/binance/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01-02.zip
path/to/data/binance/data/futures/cm/daily/liquidationSnapshot/BTCUSD_PERP/BTCUSD_PERP-liquidationSnapshot-2024-01-02.zip
path/to/data/binance/data/futures/cm/daily/indexPriceKlines/BTCUSD/1m/BTCUSD-1m-2020-06-30.zip
```

规则：

- 完整路径 = `output_dir` + `data/{spot|futures/um|futures/cm}/{daily|monthly}/{type}/{SYMBOL}[/{interval}]/{file}`。
- K 线类文件名：`SYMBOL-INTERVAL-YYYY-MM[-DD].zip`。
- 非 K 线类文件名：`SYMBOL-TYPE-YYYY-MM[-DD].zip`。
- 默认 `output_dir = "."`，即当前目录下的 `data/...`。

## 模块四：indexPriceKlines 例外

1. 该数据集按**标的指数**存放，而不是合约。
2. 配置 `BTCUSD_PERP` 时，网页与本地路径都使用 `BTCUSD`。
3. 配置 `BTCUSDT_250926` 时使用 `BTCUSDT`。
4. 同一标的的永续与季度合约不能同时配置该数据集：它们会落到同一路径，配置校验会拒绝。

## 模块五：月档语义

1. 只下载**完整月份**：`end = 2024-01-15` 时，1 月不会下载，最后一档是 2023-12。
2. `start = 2024-01-15` 时仍会下载完整的 2024-01：起始日按月对齐，而不是丢弃该月。
3. `fundingRate` 只有月档；`bookDepth` / `metrics` / `liquidationSnapshot` 只有日档。

## 模块六：迁移旧数据

1. 在归档根目录下创建 `data/futures`。
2. 把旧目录 `data/um` 移到 `data/futures/um`。
3. `cm` 同理。

```bash
mkdir -p path/to/data/binance/data/futures
mv path/to/data/binance/data/um path/to/data/binance/data/futures/um
```

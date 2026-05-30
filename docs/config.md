# 配置模块

```
src/config/
├── mod.rs          # 模块声明, 重导出
├── default.rs      # Config 结构体, 加载/解析/默认值
├── get_proxy.rs    # 代理地址读取 (环境变量)
├── market_type.rs  # MarketType 枚举 + 关联 URL/路径方法
├── start_date.rs   # 每个交易对的起始日期解析 (内存缓存)
└── symbols.rs      # 交易对列表解析 (内存缓存, 自动或手动)
```

## Config 结构体

```rust
pub struct Config {
    pub symbols:       Option<Vec<String>>,
    pub timeframes:    Option<Vec<String>>,
    pub market_type:   Vec<MarketType>,
    pub max_concurrent: usize,
    pub data_dir:      PathBuf,
    pub start_date:    Option<String>,
    pub end_date:      Option<String>,
}
```

## 加载流程

```mermaid
flowchart TD
    A[Config::load] --> B[env!CARGO_MANIFEST_DIR]
    B --> C["join config/config.toml"]
    C --> D[std::fs::read_to_string]
    D --> E[toml::from_str]
    E --> F[返回 Config]
```

## 交易对解析 `symbols.rs`

```mermaid
flowchart LR
    A[Config::resolve_symbols] --> B{config.symbols 有值?}
    B -- 是 --> C[直接返回]
    B -- 否 --> D{market_type 为空?}
    D -- 是 --> E[返回空]
    D -- 否 --> F[遍历 market_type]
    F --> G{内存缓存命中?}
    G -- 是 --> H[返回缓存]
    G -- 否 --> I[fetch_trading_symbols]
    I --> J[GET /exchangeInfo]
    J --> K[过滤 TRADING]
    K --> L[写入进程级缓存]
    L --> M[合并/去重/排序]
```

## 起始日期解析 `start_date.rs`

```mermaid
flowchart LR
    A[resolve_start_dates] --> B[遍历 symbol × market_type]
    B --> C{内存缓存命中?}
    C -- 是 --> D[直接返回]
    C -- 否 --> E[misses 列表]
    E --> F[并发 10 路拉取]
    F --> G[GET /klines?startTime=0]
    G --> H[指数退避重试]
    H --> I[写入进程级缓存]
    I --> J[返回 StartDateMap]
```

## 代理优先级

环境变量链路: `HTTPS_PROXY` → `https_proxy` → `HTTP_PROXY` → `http_proxy` → `ALL_PROXY` → `all_proxy`。通过 `dotenvy` 从 `.env` 载入。

```mermaid
flowchart LR
    A[HTTPS_PROXY] --> B{有值?}
    B -- 是 --> C[返回]
    B -- 否 --> D[HTTP_PROXY]
    D --> E{有值?}
    E -- 是 --> C
    E -- 否 --> F[ALL_PROXY]
    F --> C
```

## 默认值

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `symbols` | `None` | 自动获取 |
| `timeframes` | 1m/3m/5m/…/1d | 12个标准周期 |
| `market_type` | `[Futures]` | 永续合约 |
| `max_concurrent` | `1000` | 并发数 |
| `data_dir` | `data` | 数据目录 |
| `start_date` | `None` | 从交易所获取 |
| `end_date` | 当天 | — |

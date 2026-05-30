# Task 模块

下载任务的构建与调度。

## 模块结构

```
src/task/
├── mod.rs          # DownloadTask, build_tasks()
├── local_file.rs   # 扫描本地已有文件
└── urls.rs         # 生成下载 URL
```

## 数据流

```mermaid
flowchart LR
    subgraph CONFIG [config/start_date.rs]
        D[resolve_start_dates]
        D --> D1{内存缓存命中?}
        D1 -- 是 --> D2[直接返回]
        D1 -- 否 --> D3[Binance klines API startTime=0]
        D3 --> D4[指数退避重试]
        D4 --> D5[写入进程级内存缓存]
    end

    subgraph INPUT [输入]
        A1[Config]
        A2[symbols]
    end

    INPUT --> C[build_tasks]

    subgraph SCAN [local_file.rs]
        B[scan_local_files] --> B1[walkdir 递归扫描 data_dir]
        B1 --> B2[收集 .zip / .parquet 路径]
        B2 --> B3[归一化为 .zip 后缀, 存入 HashSet]
    end

    subgraph URLS [urls.rs]
        E[build_urls]
        E --> E1[market_type × symbol × timeframe × date 笛卡尔积]
        E1 --> E2["data.binance.vision/data/{rel}"]
    end

    SCAN --> C
    CONFIG --> C
    C --> URLS
    URLS --> F[DownloadTask[]]
```

## 模块职责

### `mod.rs`

- `DownloadTask` — 单个下载任务, 包含 URL、目标路径、交易对、周期
- `build_tasks()` — 组装 `urls::build_urls` 输出, 过滤本地已存在文件, 生成 `DownloadTask` 列表

### `local_file.rs`

- `scan_local_files()` — 遍历 `data_dir`, 收集所有 `.zip` 和 `.parquet` 文件路径, 归一化为 `.zip` 后缀后返回, 与 URL 的 `rel_path` 直接匹配
- 使用 `spawn_blocking` 避免阻塞 async 运行时

### `urls.rs`

- `build_urls()` — 生成 `symbol × market_type × timeframe × date` 笛卡尔积的下载 URL
- URL 格式: `https://data.binance.vision/data/{rel_path_prefix}/{symbol}/{timeframe}/{symbol}-{timeframe}-{date}.zip`
- 每个 symbol 的起始日期受 `config.start_date` / 缓存 start_date / 1年前 三层兜底
- 结束日期受 `config.end_date` 和 `chunk_end` 取小

## 事件流

1. `build_tasks()` 接收已解析的 `Config` 和 `Vec<String>` symbols
2. 计算常量 `config_sd`/`config_ed`/`one_year_ago` (避免 build_urls 重复解析)
3. 调用 `urls::build_urls()` 生成待下载 URL 列表
4. 用 `local` 集合 `.zip`-归一化路径直接过滤已存在文件
5. 组装为 `Vec<DownloadTask>` 返回

## 默认值

| 参数 | 默认值 | 说明 |
|------|--------|------|
| 起始日期 | `config.start_date` → 缓存 → 1年前 | 优先级递减 |
| 结束日期 | `chunk_end` 与 `config.end_date` 取小 | — |
| 缓存 | 进程级内存缓存 | 随进程生命周期 |
| 并发数 | 10 | start_date 拉取 |

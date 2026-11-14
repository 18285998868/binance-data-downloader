# ADR-0002：缓存用 JSON 文件，而非嵌入式数据库

## 状态

已接受（2026-11），取代早期的 `redb` 实现。

## 背景

缓存只保存起始日：每 `(market, job, symbol)` 一个 `YYYY-MM-DD`，量级在几百条以内，
TTL 30 天。

`redb` 为此引入了：一个数据库文件、写事务、后台线程、额外的传递依赖和一套
schema 升级路径。对于这点数据，它提供的并发写能力并不需要——输出目录锁已经保证
同一棵树只有一个进程。

## 决策

1. 每个 domain 一个 JSON 文档：`{output_dir}/.cache/{domain}.json`。
2. 文档带 `version` 字段；版本不符是 `CacheError::Invalid`，不是静默 miss。
3. 写入走临时文件 + `fsync` + `rename`，父目录也 `fsync`。
4. 读写在 `spawn_blocking` 中执行，async 包装不阻塞执行器。
5. domain 名限制为 ASCII 字母、数字、`_`、`-`，避免路径逃逸。

## 后果

- 文档可人工检查，不需要专用工具。
- 依赖树少一个 crate。
- 同一棵树的并发写依赖已有的目录锁；多进程同时写不在支持范围。
- 损坏文档会明确报错，调用方决定是否当作 miss 重新获取。

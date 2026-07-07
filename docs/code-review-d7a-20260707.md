# D7A 代码评审报告

日期：2026-07-07

## 1. ⚠️ 严重：时区变更 `UTC → Asia/Shanghai` — 数据兼容性问题

`MybatisRechargeOrderPersistenceAdapter.java` 中，`persistedAt()` 从 `ZoneOffset.UTC` 改为 `ZoneId.of("Asia/Shanghai")`。

如果该 repo 之前已有写入过数据库的旧记录，则历史数据的 `create_time`、`edit_time`、`next_retry_time` 是 UTC 时间，而新记录的这些字段是 Asia/Shanghai 时间（比 UTC 大 8 小时）。如果补偿扫描任务用 `WHERE next_retry_time <= NOW()` 来筛选需处理的行，新老记录会处于不同时区基准，导致扫描行为不一致。

**建议**：确认当前环境是否已存在旧数据。若存在，需要数据迁移脚本将旧记录的 datetime 字段统一 +8 小时后再上线此改动；或者在添加一个配置开关，允许通过 Nacos 切换时区策略以灰度切换。

---

## 2. ⚠️ 严重：`warn → error` 日志级别升级 — 线上告警风暴

多个适配器的异常路径把 `log.warn` 改成了 `log.error`：

- `RedisAcceptRechargeOrderAdapter.accept()` — Lua 执行失败
- `RedisSessionAcceptReliabilityAdapter.accept()` — Lua 失败、会话打开失败
- `RedisSessionAcceptReliabilityAdapter.waitForReplication()` — session 丢失、WAIT 失败
- `RedisSessionAcceptReliabilityAdapter.cancelUnconfirmedAccept()` — 取消失败
- `RedisSessionAcceptReliabilityAdapter.markPersistQueued()` / `markDbPersisted()` / `markPersistFailed()` — 状态推进失败

这些路径原本是 `warn` 级别，在 H5B→D5B 失败重试链路中是**可恢复的预期事件**（Redis 抖动、主从切换），业务自身有补偿机制兜底。升级为 `error` 后，如果监控系统基于 `ERROR` 级别触发告警，每次 Redis 瞬时故障都会产生大量告警。

**建议**：若日志级别升级是为了体现 H5B fail-closed 语义，至少对 `REDIS_WRITE_DISABLED`、`REDIS_SESSION_UNAVAILABLE` 这类有补偿兜底的路径保持 `warn`；将 `error` 留给 MySQL 事务失败等无法自动恢复的路径。

---

## 3. ⚠️ 中等：`markDbPersisted` 超时后未取消 `CompletableFuture` — Redis 脏写入

`PersistRechargeOrderConsumerService.markDbPersisted()` 中，当 `.get()` 超时后返回 `REDIS_MARK_TIMEOUT`，consumer ack 当前消息，但底层 Reactor 链路的 Redis Lua 调用**仍在执行**。如果超时后的 Redis 调用最终成功执行，它会在 consumer 不知情的情况下写入 `persisted_version` 或 `DB_PERSISTED`。后续补偿重投消息再次调用同一 Lua 时，由于 `persisted_version` 已被推进，可能遇到意外的状态。

**建议**：超时后对 `CompletableFuture` 调用 `future.cancel(true)`，或在 Lua 中加入幂等参数（如 message_id）以确保重复调用不会引入副作用。

---

## 4. ⚠️ 中等：`markPersistFailed` 超时后同样未取消 Future

`PersistRechargeOrderConsumerService.markPersistFailed()` 有相同问题。虽然 `markPersistFailed` 的返回值被忽略（void 语义），但未取消的 Future 会持有 Redis 连接/会话引用，直到 Reactor 超时或响应返回才释放。如果该路径频繁触发，可能累积未释放的连接。

---

## 5. ℹ️ 设计确认：Lua `MARKED` 路径返回后 `persist_status` 非 `DB_PERSISTED`

`mark_db_persisted.lua` 中，当 `currentOrderVersion > nextPersistedVersion` 时返回 `MARKED`。

Consumer 把 `MARKED` 当作成功，返回 `PersistRechargeOrderResult.persisted()`、ack 消息。但 Redis 中该订单的 `persist_status` 仍为 `PENDING_MQ` / `PERSIST_QUEUED` 等旧状态，且未被移出 pending ZSET。这是**设计意图**（版本未追平，保留补偿入口），但前提是补偿任务确实会重新扫描 pending ZSET 并重新投递。

**需要确认**：补偿任务是否会陷入「重复投递 → 相同 `MARKED` → ack → 再投递」的紧循环？如果有冷却/退避机制 (如 `next_retry_time`)，则此路径正确。

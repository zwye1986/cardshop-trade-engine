# D5-SNAPSHOT 代码审查报告

审查时间：2026-07-06

## 1. `isHttpUrl` 中客户端非法 URL 打 ERROR 日志 → 告警误报

`V1CreateRechargeOrderUseCaseService.java` `isHttpUrl()` 方法中，`IllegalArgumentException` catch 块从静默改为 `log.error`。这是参数校验路径，上游传非法 `notify_url` 不是系统故障。ERROR 级别会触发线上告警，对应的是客户端错误。

**建议**：改回 WARN 或 INFO。

## 2. 异常登记失败日志 WARN → ERROR → 可能产生噪音告警

三处从 `log.warn` 升为 `log.error`：

- `recordWaitCancelFailedAnomalyAsync` `exceptionally` 回调
- `recordWaitCancelFailedAnomalyAsync` `catch(RuntimeException)`
- `recordWaitCancelFailedAnomaly` `catch(RuntimeException)`

异常登记是 fire-and-forget 旁路，登记失败不改变接口响应。`exceptionally` 回调中如果是 `DuplicateKeyException`（幂等冲突），完全是预期行为，不应打 ERROR。

**建议**：至少将 `exceptionally` 回调中的日志降回 WARN。

## 3. Redis 会话打开失败日志 WARN → ERROR 范围

以下三处在 `RedisSessionAcceptReliabilityAdapter` 中从 WARN 升为 ERROR：

- `accept()` 中 `openSession()` 失败
- `cancelUnconfirmedAccept()` 中 `openSession()` 失败
- `markPersistQueued()` / `markDbPersisted()` / `markPersistFailed()` 中 `openSession()` 失败

这些场景会返回降级结果而非抛异常，ERROR 级别本身合理（确实是系统故障）。但如果连接池短暂耗尽也会触发 ERROR 风暴。

**建议**：确认告警系统有去重/聚合能力。

## 4. `closeRedisWriteForUnknownAccept` 同步执行阻塞响应线程

`toWaitFailureResult` 中：

```java
closeRedisWriteForUnknownAccept(...);  // 同步调用
recordWaitCancelFailedAnomalyAsync(...);  // fire-and-forget
return CompletableFuture.completedFuture(processingResult(...));
```

`closeRedisWriteForUnknownAccept` 在当前 `InMemoryRedisHaGuard` 实现中是纯内存 `AtomicBoolean` 操作，很快。但如果未来替换为需要网络 I/O 的实现，会直接拉长 cancel 失败路径的 HTTP 响应。

**建议**：加注释标注"HA Guard closeWrite 必须极快，否则应异步化"。

## 5. HA Guard 关闭后无自动恢复机制 → 单次故障导致全量拒绝

`InMemoryRedisHaGuard.closeWrite()` 一旦调用，所有后续下单请求都在 `acceptAfterDependencyGate` 被拒绝，返回 `SYSTEM_BUSY`。本变更集中没有 re-open 逻辑（无 Sentinel 事件监听、无超时自动恢复、无健康检查重开）。

这意味着一个订单的 WAIT_CANCEL_FAILED 会导致整个节点 Redis 写入永久关闭，直到人工介入或重启。D5B 的 anomaly 自动核查任务（`AcceptAnomalyAutoCheckService`）也不包含 HA Guard 恢复逻辑。

**建议**：至少在 D7/D8 阶段加入超时自动恢复或健康检查驱动的 re-open 机制。

## 6. 异常自动核查：snapshot 为空永远转人工

`AcceptAnomalyAutoCheckService.checkBatch` 中 `snapshot.isEmpty()` 永远返回 `manualReason` 非 null → 转 `MANUAL_REQUIRED`。`resolvedNotAccepted` 计数器在 D5B 阶段始终为 0。

这不影响正确性（保守策略合理），但如果监控面板依赖这些计数器判断核查进度，需注意 D5B 不会有"已确认未受理"的结论。

**建议**：在监控/运营文档中注明 D5B 核查不会产生 `RESOLVED_NOT_ACCEPTED` 结论。

## 7. MQ mandatory return 走了不必要的重试

`RabbitPersistRechargeOrderMessagePublisher.publishAttempt` 中 mandatory return 返回 `failed` 后，走 `retryOrFail` 重试逻辑。但如果 mandatory return 的根因是 exchange/routing key 配置错误（持久性问题），重试不会成功，只会耗尽重试次数后触发熔断。

**建议**：在 `retryOrFail` 中对 `MANDATORY_RETURN` 做特殊处理——直接失败不重试，因为 routing 错误不是瞬态故障。

## 审查结论

本轮改动功能完整、设计合理。以上 7 点中，第 1 点和第 2 点建议在合入前修复（涉及告警噪音），第 5 点建议在后续版本规划中处理，其余为低风险建议。

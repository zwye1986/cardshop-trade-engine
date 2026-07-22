# Outbox 履约：拆 Job、并行与公平调度设计

> 日期：2026-07-22  
> 状态：已批准（C + P1 + F3 + 双年龄监控 C）  
> 关联：pro143 压测 `PERF-A39-C100-R100-T5-5M-B15-*` 实证——落库 3 万但终态成功仅数百，根因是履约 Outbox 产能不足且 DISPATCH/QUERY 混扫抢名额。

## 1. 问题

1. 单一 `rechargeOutboxPublishJob` 按 `created_at ASC` 混扫 `SUPPLIER_DISPATCH` / `SUPPLIER_QUERY` / `INVALID_ORDER`。积压时 QUERY（含 mock PROCESSING 退避）占用同一 `limit`，稀释派单吞吐。
2. Worker 串行 claim + 同步 HTTP，有效派单约 2 QPS，远低于受理 100 QPS。
3. 同账号频控失败记入 `failed` 并回写 FREQUENCY_WAIT，空转占名额（压测清库后当前为 0，但仍需结构性规避）。
4. 缺少「创建年龄 / 到期年龄」双指标，难以观测积压老化。

## 2. 目标与非目标

**目标**

- DISPATCH 与 QUERY 调度隔离；旧 handler 兼容。
- Job 内可配置并行打供应商；同 `org+account` 串行。
- 避频控空转；在可跑集合中仍按 `created_at` 优先最老事件。
- 暴露 `age_since_created` 与 `age_since_due` 监控。

**非目标**

- Outbox → MQ 异步履约重构。
- 改供应商协议或 mock `readyAt`。
- 本轮不强制多实例扩容运维（仍兼容现有 SHARDING_BROADCAST）。

## 3. 方案摘要（已定）

| 项 | 选择 |
| --- | --- |
| Job 拆分 | **C**：新增 `rechargeSupplierDispatchJob`、`rechargeSupplierQueryJob`；`rechargeOutboxPublishJob` 保留，**默认仅 DISPATCH**；param `eventTypes=DISPATCH\|QUERY\|ALL` 可覆盖 |
| 并行 | **P1**：`trade.engine.fulfillment.outbox-parallelism` 默认 **8**（1～32）；XXL `parallelism=` 可覆盖 |
| 公平调度 | **F3**：账号去重 + Redis 频控预检；不 claim、不计 failed，计 `deferred` |
| 年龄 | **C**：监控双年龄；挑选仍按 `created_at ASC` |

QUERY Job 处理：`SUPPLIER_QUERY` + `INVALID_ORDER`。

## 4. 处理流水线

```text
scan(eventTypes, overfetch=limit*K)
  -> order by created_at ASC, id ASC   // 最老优先
  -> fair filter (DISPATCH only):
       - same (org,account) keep earliest only
       - if Redis frequency lock owned by other orderId -> deferred
  -> take up to `limit`
  -> serial CAS claim
  -> thread pool execute (parallelism N; per-account mutex)
  -> existing markSuccess / markFailed / backoff
```

- Overfetch 系数 `outbox-scan-overfetch-factor` 默认 **3**，扫描上限 `min(limit*K, 1000)`（与现有 mapper safeLimit 对齐）。
- QUERY/INVALID 不做账号去重与频控预检。
- Claim 失败仍计 `skipped`；公平/频控跳过计 `deferred`。

## 5. API / 端口变更

### 5.1 `RechargeOutboxPort`

```java
List<RechargeOutboxEvent> selectDueForShard(
    int shardTotal, int shardIndex, int limit, List<OutboxEventType> eventTypes);
```

保留无 `eventTypes` 的重载：行为改为**仅 DISPATCH**（与旧 Job 新默认一致），避免误混扫。

### 5.2 `RechargeOutboxWorkerService`

```java
RechargeOutboxBatchResult processDue(
    int shardTotal, int shardIndex, int limit, String workerId,
    List<OutboxEventType> eventTypes, int parallelism);
```

### 5.3 `RechargeOutboxBatchResult`

增加 `deferred` 字段；日志与 XXL 完成行输出 `deferred=`。

### 5.4 XXL Handlers

| Handler | 默认 eventTypes | 说明 |
| --- | --- | --- |
| `rechargeSupplierDispatchJob` | DISPATCH | 新 |
| `rechargeSupplierQueryJob` | QUERY + INVALID_ORDER | 新 |
| `rechargeOutboxPublishJob` | DISPATCH | 兼容；`eventTypes=ALL` 可恢复旧混扫 |

Param 扩展：`limit`、`workerId`、`batchId`、`parallelism`、`eventTypes`。

### 5.5 配置（`trade.engine.fulfillment`）

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `outbox-parallelism` | 8 | 1～32 |
| `outbox-scan-overfetch-factor` | 3 | ≥1 |

既有 `outbox-claim-lease-seconds`、`frequency-*` 不变。

## 6. 监控

在派单 outbox 观测快照上明确双年龄（按 `event_type` + `status`）：

- `trade_engine_dispatch_outbox_max_age_since_created_seconds`（`now - created_at`）
- `trade_engine_dispatch_outbox_max_age_since_due_seconds`（`now - next_retry_time`，仅 due 行；未到期可为 0 或不输出）

兼容：保留或映射既有 `trade_engine_dispatch_outbox_max_age_seconds` / `trade_engine_outbox_max_age_seconds` 为 **since_created**，避免看板突然断数。

## 7. 同账号并行语义

- 公平过滤后，同账号本批最多 1 条 DISPATCH。
- 线程池内仍按 `orgCode + '\0' + account` 加锁，防止 QUERY 与 DISPATCH、或跨批边界并发踩频控。
- 频控 `acquire` 失败仍走现有 FREQUENCY_WAIT + markFailed（预检已大幅降低此路径）。

## 8. 文档同步

- `docs/交行直充业务系统详细设计.md`「9.1」「10.1」
- `docs/交行直充业务系统高可用方案设计.md` 分片补偿 / outbox 相关节
- `docs/部署手册.md` XXL 任务表：新增两 Job，更新旧 Job 默认语义

## 9. 测试

- Worker：eventTypes 过滤；overfetch + 账号去重；频控预检 deferred；并行同账号串行；batchResult.deferred。
- XXL handlers：三 handler 参数解析与委托。
- Observability：双年龄字段/指标名。

## 10. 成功标准

- 积压场景下 DISPATCH due 下降速率明显高于改造前（同 limit/cron）。
- `direct recharge frequency wait` / FREQUENCY_WAIT 占比下降。
- 旧 `rechargeOutboxPublishJob` 默认不再混扫 QUERY。
- Prometheus 可同时看到 created/due 最大年龄。

## 11. 部署注意（pro143）

1. 发布应用后，在 XXL 调度中心**新建并启用**两个新 Job（建议 cron 与 limit 可独立于旧任务）。
2. 可将旧 `rechargeOutboxPublishJob` 暂停，或保留作 DISPATCH 备份。
3. 观察 `deferred`、双年龄与终态 `order_status=12` 增长率。

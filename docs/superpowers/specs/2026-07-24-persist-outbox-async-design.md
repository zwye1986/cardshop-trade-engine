# Persist 事务边界收缩与 Outbox 异步落库设计

## 目标与边界

**目标**：在 `timer-enabled=true` 常开的前提下，消除 persist 批事务内 `recharge_dispatch_outbox` / `recharge_balance_delta_outbox` 写入与 timer claim 之间的锁争用，使批事务均值从秒级降到百毫秒级，单商户压测落库吞吐 **≥100/s**（争取追平入口 ~150–200 QPS）。

**背景证据（pro143，2026-07-23 ~ 2026-07-24）**：

- timer OFF 基线：落库 ~40–50/s，批事务 ~3.5–4.5s；`dispatch_outbox` phase ~8–14s，`recharge_order_ext` phase ~2–3s。
- timer OFF + consumers=8 对照：`dispatch_outbox` phase ~0.09s，`recharge_order_ext` phase ~0.07s，落库 ~22/s（单队列低并发）→ ~40–50/s（consumers=8）。
- timer ON + `outbox-parallelism=32`：落库 ~7/s，批事务 ~15s。
- timer ON + `outbox-parallelism=4`：落库 ~25/s，批事务仍秒级，ready 持续堆积。
- timer ON + `dispatchInitialDelay=5s`：仅错开起步，稳态争用不变。

**根因（已验证）**：

`MybatisBatchRechargeOrderPersistenceAdapter.persistBatch`（`cardshop-trade-engine-infrastructure/.../MybatisBatchRechargeOrderPersistenceAdapter.java:139-205`）在 **一个 `@Transactional` 事务内** 写 5 张表：`mc_order → recharge_order_ext → mc_depart_record → recharge_dispatch_outbox → recharge_balance_delta_outbox`。

timer 热路径 `RechargeOutboxWorkerService.processEvent`（`cardshop-trade-engine-application/.../RechargeOutboxWorkerService.java:278-315`）对 `recharge_dispatch_outbox` 执行 `claimProcessingSql`（`RechargeDispatchOutboxMapper.java:341-362`）——按 `uk_dispatch_outbox_event_id(event_id)` 单行 UPDATE，自动提交短事务（`MybatisRechargeDispatchOutboxAdapter.claim:96-100` 无 `@Transactional` 包裹）。

REPEATABLE READ 下，claim UPDATE 持有唯一键行 X 锁及相邻 gap lock；persist 批事务的 `INSERT ... ODKU` 要在相邻区间插入新 `event_id`，拿不到插入意向锁，**整个 persist 批事务卡在 `dispatch_outbox` phase**。由于 outbox 写与 `mc_order`/`ext`/`depart` 在同一事务，批事务不提交意味着同事务的 `mc_order`/`ext` 行锁也不释放，连带阻塞其他 consumer 的批写与入口查询。`balance_delta_outbox` 同理（`RechargeBalanceDeltaOutboxMapper.java:73-75` 幂等 ODKU + 余额刷账 worker 扫描争用）。

降 `outbox-parallelism` 只减少同时持锁的 claim 数量，不消除"outbox 写在 persist 事务内"这一结构性耦合；调 delay 不改变稳态重叠。timer OFF 把 phase 从 8–14s 降到 0.09s（~100×）证明：该 phase 耗时几乎全部来自 outbox 写-vs-claim 争用，而非表本身或 persist 自身并发。

**本期做**：

1. persist 批事务收缩为只写 **事实表**：`mc_order` + `recharge_order_ext` + `mc_depart_record`（预扣流水）。
2. `recharge_dispatch_outbox` 与 `recharge_balance_delta_outbox` 改为 **批事务提交后、同进程异步幂等写入**，由独立 `RechargePersistOutboxAsyncWriter` 承担，使用独立连接池与受控并发。
3. timer 调度时机调整为"outbox 异步写成功后"再 schedule，避免 claim miss 抖动；schedule 失败仍由低频 recovery scan 兜底。

**本期不做**：

- 不改 outbox 表 schema、唯一键、`event_id` 规则、`shard_key` 语义。
- 不改 timer / worker / XXL-Job 的 scan/claim/状态机逻辑。
- 不引入新的 outbox 持久化表或新 Redis 结构（复用现有 Redis pending 补投作为崩溃恢复锚点）。
- 不改 persist MQ 8 分片（`2026-07-23-persist-shard-8-design.md` 保持独立，本期可与之并行或先行；关系见末节）。

## 事务边界与崩溃恢复

### 新的事务边界

persist 批消费路径调整为两段：

**第一段（事实事务，`@Transactional`）**：`mc_order` + `recharge_order_ext` + `mc_depart_record`，原子提交。这是订单事实的可靠沉淀点。

**第二段（outbox 异步写，独立短事务）**：`recharge_dispatch_outbox` + `recharge_balance_delta_outbox`，幂等 ODKU（`insertDispatchOutboxBatchIfAbsent` / `insertBalanceDeltaBatchIfAbsent`，`ON DUPLICATE KEY UPDATE event_id=event_id`）。第二段在第一段提交后、同 consumer 线程内同步调用 `RechargePersistOutboxAsyncWriter.writeBatch(...)`，但 writer 内部用独立 `DataSource`/连接池，不复用 persist consumer 的 Hikari。

**入参集合修正（2026-07-24 评审发现）**：writer 入参必须是 **本批全部 selected aggregates**，而不是「与第一段相同集合」。原因：consumer 的二次确认跳过（`alreadyPersistedProbes`，按 `recharge_order_ext.persist_status=DB_PERSISTED AND persisted_version>=orderVersion` 判定）在第一段提交后即视为已落库；若崩溃/失败发生在「事实已提交、outbox 未写」窗口，MQ 重投时该订单会被二次确认跳过，writer 若只收第一段集合将永久缺失 outbox 行，派单永不发生。因此二次确认跳过只作用于第一段事实事务（降本优化），**不得作用于第二段 outbox 写**；重投场景下 writer 对已落库订单重放 ODKU 是无害 no-op。

两段之间不共享事务，因此第二段的 outbox 锁争用 **不会回传到第一段**：即使 outbox 写因 timer claim 暂时阻塞，`mc_order`/`ext`/`depart` 已提交、行锁已释放，其他 consumer 与入口查询不受影响。

### 消费端到端时序（修订）

```
persistBatchFactsOnly(factsCandidates)      // 事实事务，写 mc_order/ext/depart，提交；二次确认已落库的可跳过
  -> outboxAsyncWriter.writeBatch(allSelected)  // 独立短事务，幂等写 dispatch_outbox + balance_delta_outbox；
                                             // 入参为本批全部 selected aggregates，不随二次确认跳过
  -> scheduleDispatchAfterMysqlCommit(allSelected)  // outbox 可见后再 schedule timer；同 eventId 替换幂等
  -> markDbPersisted(...)              // Redis 推进，清理 pending
  -> listener ack
```

不变量：**ack 必在 outbox 写成功之后**，因此「Redis pending 已清的订单必有 outbox 行」。writer 失败时不 ack、不 markDbPersisted，整批走现有 retry/pending 路径。

`outboxAsyncWriter.writeBatch` 抛异常时 **不 ack 当前 MQ 消息**：进入 listener 现有 retry/pending 路径，由 `markPersistFailed` 保留 Redis pending，等待 MQ retry 或 `RechargePersistMqRepublishService` 重投。

### 崩溃恢复证明

复用现有 Redis pending + MQ 重投 + ODKU 幂等，不新增恢复机制。穷举崩溃窗口：

| 崩溃窗口 | 已落库 | outbox 行 | Redis pending | 恢复路径 |
| --- | --- | --- | --- | --- |
| 事实事务未提交 | 无 | 无 | 在 | MQ retry，重投后重新走两段 |
| 事实事务提交后、outbox 写前 | mc_order/ext/depart | 无 | 在（未 markDbPersisted） | MQ retry / pending 补投 → 重消费：二次确认跳过只作用于事实事务（ODKU no-op），**outbox writer 仍覆盖该订单并补写**（见「入参集合修正」） |
| outbox 写成功、schedule 前 | 全 | 在 | 在 | recovery scan（`outbox-timer-wakeup-design.md` §9）按 `next_retry_time` 补排 timer；若发生重投，重消费会对全部 selected 重 schedule（同 eventId 替换幂等）提前覆盖 |
| schedule 成功、markDbPersisted 前 | 全 | 在 | 在 | pending 补投重消费：事实 ODKU 跳过，outbox ODKU no-op，重 schedule，重 mark |
| 全部成功 | 全 | 在 | 清 | 终态 |

关键不变量：**outbox writer 无条件写**（与当前 `persistBatch` 对 `dispatchOutboxRequired()`/`balanceReserved()` 的过滤一致，只按快照属性过滤，不按"是否已写"过滤，也不随二次确认跳过），重复触发靠 `event_id` 唯一键 ODKU 去重。该不变量在 `MybatisBatchRechargeOrderPersistenceAdapter.java:214-231` 已成立，移出后 writer 必须保持。

## 异步 Outbox Writer 契约

新增 application 端口 `RechargePersistOutboxAsyncWriter`（或复用 `RechargeOrderBatchPersistencePort` 拆分），infrastructure 实现 `MybatisRechargePersistOutboxAsyncWriter`：

- 入参：本批 **全部 selected** 的 `List<OrderPersistAggregate>`（与第一段输入的父集相同，但不随二次确认跳过收缩；已提交事实的订单重放 ODKU 为 no-op，未提交的不可能到达第二段——第一段失败整批已回滚重试）。
- 写入：调用现有 `insertDispatchOutboxBatchIfAbsent` + `insertBalanceDeltaBatchIfAbsent`，**不改 SQL 与表顺序**。
- 事务：独立 `@Transactional`（或显式 `SqlSession`），与 persist consumer 事务隔离。
- 连接池：独立 Hikari `DataSource`，配置项 `trade.engine.persist.outbox-writer-hikari-max`（默认 8），与 persist consumer 的 `spring.datasource.hikari.maximum-pool-size` 隔离，避免 outbox 写争用回压到事实事务连接获取。
- 并发：writer 在 consumer 线程内同步调用（不 fire-and-forget），但因其独立连接池，outbox 锁等待不阻塞 persist consumer 的连接池。批内 outbox 仍整批一次 ODKU，保持批写收益。
- 排序：沿用 `STABLE_ORDER`（`orgCode → outOrderSn → orderId`），与分片键一致。
- phase 指标：保留 `recharge_dispatch_outbox` / `recharge_balance_delta_outbox` phase 上报，标签不变，便于对照。

**不做 fire-and-forget** 的理由：fire-and-forget 需要"outbox 未写"的独立持久追踪（新表或 Redis set），否则 markDbPersisted 后崩溃将永久丢失 outbox 行。同步 await + 现有 pending 恢复是更小且可证明的变更。若 await 版本验证后 outbox 写本身成为新瓶颈（落库仍 <100/s），再评估 fire-and-forget + outbox-pending 追踪作为二期。

## Timer 调度时机

`PersistRechargeOrderConsumerService.persistBatch`（`cardshop-trade-engine-application/.../PersistRechargeOrderConsumerService.java:302-397`）当前在事务提交后、对每个 `persistedCandidate` 调 `scheduleDispatchAfterMysqlCommit`（`:369`）。

调整：`scheduleDispatchAfterMysqlCommit` 移到 **outbox writer 写成功之后**，且调用集合从 `persistedCandidates` 扩为 **全部 selected aggregates**（重投场景下补 schedule，timer 同 eventId 替换幂等；过滤条件不变）。理由：

- timer 热路径 `processEvent` 用 `findDueByEventId` 按 `event_id` 点查 outbox 行（`RechargeDispatchOutboxMapper.java:272-312`）。若在 outbox 行可见前 schedule，timer 到点 claim 会 miss，退化为 recovery scan 补排，增加派单延迟与 scan 压力。
- schedule 本身仍只是内存唤醒（`ScheduledExecutorRechargeDispatchTimer.schedule`），失败不回滚 outbox 已写事实，靠 recovery 兜底（`outbox-timer-wakeup-design.md` §9 已定义此语义）。

`scheduleDispatchAfterMysqlCommit` 内部对 `aggregate.dispatchOutboxRequired() && aggregate.snapshot().balanceReserved()` 的过滤（`PersistRechargeOrderConsumerService.java:429-431`）不变。

## 配置项

| 配置键 | 默认 | 说明 |
| --- | --- | --- |
| `trade.engine.persist.outbox-async-enabled` | `false` | 灰度开关。`false` 时 persistBatch 仍走原单事务 5 表写入，行为与今日一致 |
| `trade.engine.persist.outbox-writer-hikari-max` | `8` | outbox writer 独立连接池上限 |
| `trade.engine.persist.outbox-writer-batch-enabled` | `true` | writer 是否整批 ODKU；`false` 时退化为逐条（仅排障） |

`outbox-async-enabled=false` 时，`persistBatch` 必须保留原 5 表同事务路径，作为回滚兜底；实现上用同一个 adapter 的两个方法（`persistBatchWithOutbox` / `persistBatchFactsOnly`）按开关选择，避免两套 rowConverter。

## 可观测性

phase 指标保留并新增分段标签：

- `trade.engine.persist.batch.mysql.phase.duration{phase}`：`mc_order` / `recharge_order_ext` / `mc_depart_record` 不变；`recharge_dispatch_outbox` / `recharge_balance_delta_outbox` 现在度量的是 **writer 独立事务** 耗时，与事实事务解耦。
- 新增 `trade.engine.persist.outbox.writer.duration`：writer 端到端（含连接获取）。
- writer 独立连接池水位读取 Spring Boot 标准 Hikari 指标 `hikaricp.connections.active/pending{pool="persist-outbox-writer"}`（writer `DataSource` 是 `HikariDataSource`，由 `DataSourcePoolMetadataProvider` 自动绑定），不新增自定义 `trade.engine.persist.outbox.writer.hikari.*` 指标。
- 保留 `persist.batch.orders.total{result}`、批事务总 duration。

告警：writer duration 持续高于事实事务 duration ×N，或 `hikaricp.connections.pending{pool="persist-outbox-writer"}` 持续 >0（说明 outbox 写成为新瓶颈，需评估 fire-and-forget 二期）。

## 验收标准（pro143 / A39）

1. `outbox-async-enabled=true`，`timer-enabled=true`，`outbox-parallelism=4`，`persist-batch-enabled=true`。
2. 入口持续 ≥150 QPS 至少 3 分钟。
3. persist ready 之和 **不持续单调上涨**。
4. 全局落库成功速率 **≥100/s**（`persist.batch.orders.total{result="success"}` 窗口速率）。
5. 批事务均值（事实事务）**≤300ms**；`dispatch_outbox` phase（writer 事务）单独观测，允许高于事实事务但不应回压到事实事务 hikari pending。
6. 崩溃恢复：构造"事实事务提交后、writer 写前"杀进程场景，重启后 pending 补投能补齐 outbox 行，派单最终成功；构造"writer 写后、schedule 前"杀进程，recovery scan 补排 timer，派单最终成功。
7. 相对基线（timer ON + outbox-parallelism=4，~25/s）有明确提升；若未达 100/s，记录 writer duration / hikari / 各 phase，评估是否需 fire-and-forget 二期。

## 风险与回滚

| 风险 | 缓解 |
| --- | --- |
| 两段之间崩溃丢 outbox | 已由 Redis pending + MQ 重投 + ODKU 幂等覆盖（见崩溃恢复证明）；验收第 6 条实测 |
| writer 独立连接池配置错误导致死锁 | 启动期 fail-fast 校验 writer `DataSource` 与 persist consumer `DataSource` 不同实例；`outbox-writer-hikari-max` 必须 >0 |
| schedule 时机后移增加派单延迟 | 仅增加 outbox 写耗时（预期百毫秒级）；recovery scan 兜底；派单 P99 需纳入观测 |
| outbox 写成为新瓶颈 | 先 await 版本验证；若 writer duration 持续打满，二期 fire-and-forget + outbox-pending 追踪 |
| 回滚 | `outbox-async-enabled=false` 重启回单事务 5 表路径，无需 DDL 回滚 |

## 与 persist 8 分片方案的关系

本方案与 `2026-07-23-persist-shard-8-design.md` **正交互补**，建议 **本方案先行**：

- 本方案消除 outbox 写对事实事务的锁回压，是分片方案生效的前提之一。若不先收缩事务边界，分片后 persist 批事务仍含 outbox 写，timer claim 仍会跨分片阻塞批事务（next-key lock 不按 persist 的 `crc32(orgCode+outOrderSn)` 分片）。
- 本方案落地后，事实事务只写 `mc_order`/`ext`/`depart`，分片方案对这些表的 INSERT 打散才真正生效，两者叠加目标 150/s。
- 两者配置开关独立（`outbox-async-enabled` vs `persist-shard-count`），可分别灰度。

## 文档同步（实现时）

实现合并前必须同步：

- `docs/交行直充业务系统详细设计.md`「8.3 异步落库 Consumer」：事务边界从 5 表同事务改为事实事务 + outbox 异步两段；补充崩溃恢复窗口表。
- `docs/交行直充业务系统高可用方案设计.md`「14.x 落库与履约边界」：明确 outbox 写不再与订单事实同事务，依赖 Redis pending + MQ 重投保证最终一致。
- `docs/部署手册.md`：新增 `outbox-async-enabled` 灰度步骤、writer 独立连接池 Nacos 项、回滚命令。
- `docs/集成测试.md`：下一轮 pro143 对照压测记录 outbox-async on/off、timer on/off 四象限的落库、批事务、writer duration、派单成功数。

# Redis→MQ→MySQL 批消费落库设计

> 日期：2026-07-17  
> 状态：待实现（已通过方案评审：Rabbit 原生批消费）  
> 关联：`docs/交行直充业务系统详细设计.md`「8.3 异步落库 Consumer」  
> 压测基线：`PERF-A39-T5-RATE50-20260717213507`（直连 50 QPS×10min，同步 30000/30000；异步稳态约 9 persist QPS）

## 1. 背景与目标

直连压测证明同步受理链路有余量，但异步落库跟不上：

- Consumer 成功吞吐约 **9 msg/s**（8 并发 × 单次 listener 平均约 0.88s）
- Hikari `pending=0`，主瓶颈不在拿连接，而在 **每单一次 MySQL 事务/网络往返**
- 伴随 `SQLSTATE(08S01)` / `SocketTimeoutException: Read timed out`（≈30s）与 `JDBC rollback failed`

成功标准（分阶段，用户确认 **C**）：

1. **阶段 1**：拆薄提交成本（批写），降低 30s 读超时；吞吐先抬到 **≥20 persist QPS**
2. **阶段 2**：调参后稳态 **≥50 persist QPS**；停灌后 `persist:pending` + `recharge.persist.mysql.queue` 归零，入库与受理成功数对齐

一致性边界（用户确认 **C**）：

- **不拆事务表集合**：每笔订单仍在同一事务内写 `mc_order`、`recharge_order_ext`，并按需要写 `mc_depart_record` / `recharge_outbox`
- **不做跨订单业务合并**；批只是「N 笔订单共享一次 commit」

推荐方案（用户确认）：**Rabbit 原生批消费（方案 2）**，并以 JDBC `rewriteBatchedStatements` 等作为底座。

## 2. 非目标

- 不把 outbox / PRE_DEDUCT 挪出订单落库事务（那是另一条「拆薄语义」路线，本次不做）
- 不把 `recharge_order_ext` 改成最终一致后补
- 不把同步下单路径改成同步写 MySQL
- 不在阶段 1 引入 per-item savepoint / 批内部分提交

## 3. 架构与数据流

```text
Rabbit 主队列 recharge.persist.mysql.queue
  └─ SimpleMessageListenerContainer (consumerBatchEnabled=true)
       └─ onMessage(List<Message>)  // 批大小由 container 聚合
            ├─ 反序列化 → List<PersistTradeOrderMessage>
            ├─ 逐条 selectSnapshot（Redis 优先 / MQ fallback / redis_order_written=false）
            ├─ 一次 @Transactional：对 N 条执行现有 persist 写入集合
            │     MyBatis ExecutorType.BATCH + JDBC rewriteBatchedStatements=true
            ├─ 事务成功后：逐条（或管道）markDbPersisted（redis_order_written=false 跳过）
            └─ 整批 ack；MySQL 失败则整批走现有 retry/DLQ（幂等消化重复）
```

开关：

- `trade.engine.mq.persist-batch-enabled`（默认建议先 `false`，灰度打开）
- `false` 时保持现有单消息 listener 行为，作为回退

默认参数（Nacos 可调，阶段 1 起值）：

| 参数 | 建议 | 说明 |
| --- | --- | --- |
| `spring.rabbitmq.listener.simple.consumer-batch-enabled` | `true`（批模式开启时） | 与开关联动或同配置面 |
| batch size | `10` | 不超过 prefetch |
| `prefetch` | 保持 `20`（或 ≥ batch size） | 避免饿批 |
| `concurrency` / `max-concurrency` | 保持 `4` / `8` | 先靠批写抬吞吐，避免盲目加并发打爆 RDS |
| Hikari `maximum-pool-size` | 保持 `16`（阶段 1） | 阶段 2 再按实测调整 |

## 4. 组件职责

| 组件 | 职责 |
| --- | --- |
| `RabbitPersistRechargeOrderMessageListener`（或批专用 listener） | 协议层：批反序列化、整批 ack/retry/DLQ；不承载业务事务细节 |
| `PersistRechargeOrderConsumerService`（扩展批入口） | 批内逐条选快照；调用批写端口；事务后 mark；汇总批结果 |
| `RechargeOrderPersistencePort` / MyBatis adapter | 新增批写入能力：同一事务内对 N 个 `OrderPersistAggregate` 执行与单笔相同的表写入集合，使用 BATCH executor |
| Redis reliability port | 行为不变；批提交后逐条 mark；失败策略见下节 |

单笔 `persist(message)` API 保留，供回退路径与单测复用。

## 5. 失败 / ack / 幂等

### 5.1 整批原子（阶段 1）

| 阶段 | 成功 | 失败 |
| --- | --- | --- |
| MySQL 批事务 | 继续 mark | 整批 rollback；不 mark；listener 走现有 retry（未超限）或 reject→DLQ（超限） |
| Redis mark（MySQL 已提交） | 该条成功 | **不回滚 MySQL**（与现网一致）；**整批仍 ack**；依赖 pending / 二次确认修 Redis；采样 error |
| 反序列化坏消息 | — | 阶段 1 允许「坏消息导致整批 retry」；阶段 2 再优化为坏消息单独 DLQ、其余继续 |

禁止阶段 1 使用 per-item savepoint 做批内部分提交。

### 5.2 幂等

以下约束必须继续成立，以支持整批重投：

- `mc_order`：`insertIfAbsent`
- `recharge_order_ext`：版本前进 upsert
- `mc_depart_record` / `recharge_outbox`：业务唯一键幂等
- 已 `DB_PERSISTED` 的重复消息：写库无害，mark 幂等，ack

### 5.3 与 republish 的关系

- 批消费变快后，pending 应主要由主路径清空
- `rechargePersistMqRepublishJob` 语义不变，仅作补偿
- 验收时要求：主队列消耗速率可持续高于 republish 灌入，避免「pending 降、queue 不降」长期僵持

## 6. JDBC / MyBatis 底座

阶段 1 必做：

- 数据源 URL 启用 `rewriteBatchedStatements=true`（若尚未启用）
- 批写路径使用 MyBatis `ExecutorType.BATCH`（或等价 SqlSession 批模式），在**单个** Spring 事务内 flush
- 单笔路径也可受益于 `rewriteBatchedStatements`，但不改变一消息一事务模型

说明：单笔多语句伪批量（仅方案 3）不足以达到 50 QPS，但作为批模式的底座保留。

## 7. 观测

| 指标 / 日志 | 含义 |
| --- | --- |
| `trade.engine.persist.batch.size`（summary） | 实际批大小；Prometheus 为 `trade_engine_persist_batch_size` |
| `trade.engine.persist.batch.mysql.duration` | 批事务耗时；Prometheus 为 `trade_engine_persist_batch_mysql_duration_seconds` |
| `trade.engine.persist.batch.orders.total{result}` | 批内订单 success/fail 计数 |
| 现有 `spring_rabbitmq_listener_seconds` | 批模式下按批计时；报告换算 per-order |
| Hikari active/pending、`08S01` / timeout 采样 error | 确认读超时风暴收敛 |

日志需带：`batchSize`、首末 messageId/deliveryTag、失败下标、失败 deliveryTag、body 长度与 SHA-256 摘要（禁止完整 body）、耗时；高频失败继续走 `LogStormGuard`。

## 8. 验收口径

相对基线 `PERF-A39-T5-RATE50-20260717213507`：

### 阶段 1

- 同负载下 `Communications link failure` / `Read timed out(≈30s)` 明显下降
- listener 成功吞吐 **≥20 persist QPS**
- 压测结束后 15 分钟内 pending 显著下降（不必已清零）
- 同步直连 50 QPS 不回归（成功数与 p99 不明显变差）

### 阶段 2

- 稳态 **≥50 persist QPS**
- 停灌后 `persist:pending` + `recharge.persist.mysql.queue` **归零**
- Redis `DB_PERSISTED` / MySQL 与受理成功数一致（允许极少量失败最终被补偿打平）

## 9. 测试计划

- **单测**：批服务整批成功；整批 MySQL 失败不 mark；mark 失败仍 ack；幂等重投；开关关闭走单笔路径
- **集成**：batch=5、约 20 单，Rabbit + MySQL（或现有 test-support），断言队列清空与 `DB_PERSISTED`
- **压测**：复用 A39 simulator 直连 50 QPS×10min，对比 persist QPS、pending、queue、超时错误

## 10. 文档与实现顺序

1. 本 spec 评审通过后，写实现计划（`docs/superpowers/plans/`）
2. 实现前同步更新：
   - `docs/交行直充业务系统详细设计.md`「8.3」
   - 如涉及高可用参数默认值，同步 `docs/交行直充业务系统高可用方案设计.md` 相关段落
3. 代码：配置开关 → 批端口/服务 → 批 listener → 单测/集成 → 灰度打开 → 压测验收
4. 将结果记入 `docs/集成测试.md`

## 11. 风险与开放问题

| 风险 | 缓解 |
| --- | --- |
| 大批事务更易触碰 RDS 慢查询 / 锁时间 | 阶段 1 batch=10；观察后再增 |
| 整批失败放大重试流量 | 幂等 + 有限 retry + 指标告警；必要时降 batch |
| 批 listener 与现有单测/集成夹具不兼容 | 保留单笔回退开关；双路径测试 |
| JDBC socketTimeout=30s 仍在 | 批写降低单连接占用时长；若仍超时，另案调超时与 RDS |

开放问题（实现前可默认如下，不必阻塞）：

- 阶段 1 坏消息策略：默认「整批 retry」；阶段 2 再拆单 DLQ
- Redis mark 是否做 pipeline：阶段 1 逐条即可；若 mark 成新瓶颈再优化

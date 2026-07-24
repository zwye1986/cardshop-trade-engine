# Persist 8 分片落库设计

## 目标与边界

**目标**：异步落库吞吐在单商户压测下达到 **≥100 单/s**（争取 ≥150，追平入口 ~150–200 QPS 需二期方案 C 配合），使 `recharge.persist.*.queue` 的 ready **不持续上涨**。

**背景证据（pro143，2026-07-23）**：

- 入口 ~180–200 QPS / HTTP ~7ms 正常。
- 落库稳态约 **37–45 单/s**；`concurrency` 从 8 提到 16 几乎无收益，批事务均值反而升高。
- `timer-enabled=true` 且 `outbox-parallelism=32` 时落库跌至 ~7/s。
- 根因是并发批写同一套 MySQL 表的唯一键/二级索引争用，不是 Hikari/CPU。

**本期做**：

1. Persist 主路径改为 **8 分片队列**（方案 B）。
2. 配套减争用（方案 A）：默认每分片低并发；**timer/履约并行与落库隔离是分片生效的物理前置，不是可选运维建议**（根因见下文「timer 争用与落库耦合根因」）。

**本期不做**：

- 不拆热事务（方案 C：outbox 延后写出）——列为二期候选项。
- 不改 `mc_order` / `recharge_order_ext` 唯一键业务语义。
- 不引入 RabbitMQ consistent-hash 插件（首期用显式 routing key，便于运维）。

## 分片契约

| 项 | 约定 |
| --- | --- |
| 分片数 `N` | **固定 8**（配置项存在，但首期只允许 `1` 或 `8`；非 1/8 启动失败） |
| 分片键 | `orgCode + '\0' + outOrderSn` 的字节 UTF-8 |
| 分片函数 | `Math.floorMod(crc32(shardKeyBytes), 8)`，与现有 XXL `crc32` 分片风格一致 |
| 稳定性 | 同一 `(orgCode, outOrderSn)` **永远**进同一分片；retry / pending 补投必须复用同一 routing key |
| 为何含 orgCode | 多商户时避免不同商户同 `out_order_sn` 误碰撞；单商户压测仍靠 `out_order_sn` 打散 |

`orderId` 不作为分片键：受理时可能尚未稳定，且补投路径以商户订单号为主。

## MQ 拓扑

保留现有 exchange / DLX / retry 机制，将 **主队列** 从 1 条扩展为 8 条。

### 命名

| 角色 | 名称模式 | 示例 |
| --- | --- | --- |
| 主 exchange | 沿用 `trade.engine.mq.persist-exchange` | 现网名不变 |
| 分片 routing key | `{persist-routing-key-prefix}.{shard}` | `recharge.persist.mysql.0` … `.7` |
| 分片主队列 | `{persist-queue-prefix}.{shard}` | `recharge.persist.mysql.queue.0` … `.7` |
| Retry exchange/queue | **仍共用 1 套**（见下） | 现网 retry 名可保留 |
| DLQ | **仍共用 1 条** | `recharge.persist.mysql.dlq` |

当 `persist-shard-count=1`（兼容回退）：routing key / queue 继续使用现有 `persist-routing-key` / `persist-queue`（无 `.0` 后缀），行为与今日一致。

### 绑定与 DLX

- 每个分片队列：Quorum、durable、persistent；`x-dead-letter-exchange` 指向现有 persist DLX（与今日主队列一致）。
- Publisher：按消息计算出的 `shard` 发布到对应 routing key。
- **Retry 回主**：retry 消息 TTL 到期后必须回到 **同一分片** 主队列。实现要求：
  - 发布到 retry 时在 header 携带 `x-persist-shard`（0–7）及原始 routing key；或
  - retry 使用 **per-shard retry queue**（`…retry.queue.{shard}`），DLX routing 回 `…mysql.{shard}`。
  - **推荐 per-shard retry queue**，避免共享 retry 回主时丢失分片信息。首期采用该方案。

### 旧单队列迁移

1. 配置 `persist-shard-count=1` 时不创建分片队列（兼容）。
2. 切换到 8 前：停入口 → 排空旧 `recharge.persist.mysql.queue` → 部署分片声明与代码 → `persist-shard-count=8` 重启。
3. 旧队列保留但不订阅；确认无流量后人工删除（不自动 drop）。
4. 不双写旧+新队列（避免双倍落库压力）。

## 发布与消费

### Publisher

- `RabbitPersistRechargeOrderMessagePublisher`（及 pending 补投）在 confirm 前计算 shard，使用分片 routing key。
- 日志/指标带 `shard`（低基数 0–7）。
- `PersistTradeOrderMessage` body **不必**改 schema；shard 只影响路由。

### Consumer

- 注册 **8** 个分片队列订阅（批模式仍由 `persist-batch-enabled` 控制）。
- 每个分片独立 `SimpleMessageListenerContainer`（或等价），避免一个 container 混吃多队列导致不均。
- 每分片：
  - `concurrency = persist-shard-concurrency`（默认 **1**）
  - `max-concurrency = persist-shard-max-concurrency`（默认 **2**）
  - `prefetch >= persist-batch-size`
- 全局最大同时批写 ≈ `8 * max-concurrency`；默认上限 **16**，但键空间打散后争用应低于今日「单队列 16 并发打同一索引」。

### 批写 SQL

- `MybatisBatchRechargeOrderPersistenceAdapter` **不改写表顺序与 ODKU 语义**。
- 同批消息保证来自同一分片（单队列消费），稳定排序规则保持 `orgCode + outOrderSn + orderId`。

## 配置项

| 配置键 | 默认 | 说明 |
| --- | --- | --- |
| `trade.engine.mq.persist-shard-count` | `1` | 仅允许 `1` 或 `8`；`8` 启用分片 |
| `trade.engine.mq.persist-shard-concurrency` | `1` | 每分片 listener concurrency |
| `trade.engine.mq.persist-shard-max-concurrency` | `2` | 每分片 max-concurrency |
| `trade.engine.mq.persist-routing-key` | 现网值 | `count=1` 时用；`count=8` 时作 prefix |
| `trade.engine.mq.persist-queue` | 现网值 | `count=1` 时用；`count=8` 时作 prefix（去掉逻辑上的最终名，实际队列为 `prefix.0`…） |

命名细则（实现时写死拼装，避免配置 8 份 queue 名）：

- `queue(shard) = persist-queue + "." + shard`（当 count=8 且 persist-queue 以 `.queue` 结尾时，结果为 `recharge.persist.mysql.queue.0`）
- `routingKey(shard) = persist-routing-key + "." + shard`

配套减争用（方案 A）：

**硬性前置（验收必须满足，写入 Nacos 与部署手册）**：

- `timer-enabled=false`，或 `outbox-parallelism≤4`。集成测试 `PERF-DIAG-A39-PRO143-20260723` 已证：timer ON + `outbox-parallelism=32` 时落库仅 ~7/s，关 timer 后 ~40-50/s。timer 未关时分片收益被进程内履约热路径争用吞没，甚至因正反馈环净负（根因见下文）。
- 全局 `spring.rabbitmq.listener.simple.concurrency/max-concurrency` 在 count=8 时 **不再**作为 persist 主并发来源；persist 以分片配置为准（实现上分片 container 显式 setConcurrency，不被全局 16 带跑偏）。

**软性建议（写入运维手册）**：

- 压测窗口避开共享 RDS 邻库（如 `aifuli-mall-test.product_spu`）写高峰。
- Hikari max 需 ≥ `8 × persist-shard-max-concurrency` + 履约/补投余量；pro143 实测 max=64 足够默认 8×2=16。

## 可观测性

新增/扩展低基数标签 `shard="0"…"7"`（仅 count=8 时）：

- `trade.engine.persist.batch.orders.total{result,shard}`
- `trade.engine.persist.batch.mysql.duration{shard}`
- `trade.engine.persist.batch.mysql.phase.duration{phase,shard}`
- 队列 ready/unacked：按 8 个队列分别抓（Rabbit 管理 API / 导出）

告警：任一 shard ready 持续上涨，或某 shard 落库速率显著低于其它（倾斜）。

## timer 争用与落库耦合根因

persist 批写与 timer worker 共享 `recharge_dispatch_outbox`，但访问模式正交。分片只隔离主键 range 上的 INSERT 争用，治不了 timer 侧的二级索引争用。

**争用点（落到 SQL/索引）**：

- scan：`RechargeDispatchOutboxMapper.selectDueForShardByEventTypesSql` 走 `idx_dispatch_outbox_due(status, next_retry_time, id)`，条件 `status IN('INIT','FAILED') AND next_retry_time <= now`。`status` 仅 4 值是低基数前缀，`next_retry_time <= now` 是范围扫描，所有 due 行聚集在索引右端同一组页；REPEATABLE READ 下范围扫描对区间加 gap lock，阻塞 persist INSERT 往同区间落新条目。
- claim：`claimProcessingSql` 按 `uk_dispatch_outbox_event_id(event_id)` 定位行持 X 锁 + next-key lock；persist 的 `INSERT ... ODKU event_id=event_id` 按同一唯一键操作，两者在唯一索引页正面相撞。
- 正反馈环：`PersistRechargeOrderConsumerService.persistBatch` 提交后对每单调用 `scheduleDispatchAfterMysqlCommit` → `dispatchTimerPort.schedule` 立即唤醒刚写入的行。persist 吞吐越高 → timer 喂入越多 → 争用越重 → persist INSERT 被阻塞。timer ON 时分片提吞吐反而喂大环的输入端，可能净负。
- 线程放大：`RechargeFulfillmentConfigurationSupport.rechargeDispatchTimer` 把 timer 线程池直接绑死 `outboxParallelism`，`ScheduledExecutorRechargeDispatchTimer` 的 worker 池与 timer 池同大小。`outbox-parallelism=32` → 32 timer + 32 worker = 64 个 `recharge-dispatch-*` 线程，对应 32 行 PROCESSING 同时持锁。

**与 persist 分片的关系**：persist MQ 分片键 `crc32(orgCode+'\0'+outOrderSn)`，outbox scan 分片键 `crc32(shard_key=event_id)`，两者函数不同、键不同、正交。分片只把主键 range INSERT 争用扩散到 8 段，但 timer scan/claim 仍在二级索引右端和唯一键页上争用。要让分片真正生效，必须同时治理 timer 侧。

**分层治理（按见效快慢）**：

1. 断正反馈环：`timer-enabled=false`（零代码，立即，7/s→40-50/s）——本期硬前置。履约延迟退化为 XXL-Job 扫描周期级，直充业务可接受。
2. persist 8 分片上线（本 spec 主体，40-50/s→80-120/s），前置是步骤 1。
3. timer 池/worker 池独立配置（配套，建议本期同步）：新增 `trade.engine.fulfillment.timer-pool-size`（默认 4），与 `outbox-parallelism` 解耦；worker 池从 32 降到 4-8，claim 并发降 8×，为重开 timer 不回退落库铺路。
4. 拆 scan 索引右端热点（配套，建议本期同步）：`idx_dispatch_outbox_due(status, next_retry_time, id)` 改为 `idx_dispatch_outbox_due_shard(status, shard_key, next_retry_time, id)`，scan SQL 的 `mod(crc32(shard_key), shardTotal)=shardIndex` 利用 `shard_key` 前缀等值收窄，timer 不同分片访问不同 B+ 树子树，与 persist INSERT 物理页解耦。需 Liquibase changeSet。
5. 重开 timer 恢复秒级履约（二期，需步骤 3-4 先落地并复压验证）。

步骤 1-4 预期到 80-120/s；追平入口 150/s 的剩余差距留给二期方案 C（热事务去掉 dispatch/balance outbox 同事务写出，提交后异步写入）。

## 验收标准（pro143 / A39）

1. **硬性前置**：`timer-enabled=false`（或 `outbox-parallelism≤4`）；`persist-shard-count=8`，每分片 concurrency 1/max 2。timer 未关视为配置错误，不进入压测。
2. 入口持续 ≥150 QPS 至少 3 分钟。
3. 8 个分片 ready **之和**不持续单调上涨（允许短抖动）。
4. 全局落库成功速率 **≥100/s**（`persist.batch.orders.total{result="success"}` 窗口速率）；争取 ≥150/s。基线单队列 cns=8 timer OFF 约 40-50/s，分片预期 80-120/s；150/s 需二期方案 C 配合，单独分片大概率不达标。
5. 落库 listener 真实起 8 个 container 消费（RabbitMQ management 验证 8 队列各有 consumer），避免改错注册点导致生产仍单队列。
6. 若未达标，记录各 shard phase 与 per-shard ready，对照「timer 争用与落库耦合根因」分层治理步骤定位瓶颈。

## 风险与回滚

| 风险 | 缓解 |
| --- | --- |
| timer 未关导致分片收益被吞甚至净负 | 验收前置硬性拦截 `timer-enabled=false`；根因见「timer 争用与落库耦合根因」正反馈环 |
| 改错 listener 注册点（infrastructure `@Configuration` vs bootstrap `@AutoConfiguration` 双注册） | 改 bootstrap 层 `TradeEngineRabbitListenerAutoConfiguration`；集成测试验证 8 队列各有 consumer |
| 批模式参数不传编程式 container | 分片 container 注册代码显式 set `consumerBatchEnabled`/`batchSize`/`receiveTimeout`/`batchReceiveTimeout`，不依赖 `PersistBatchRabbitListenerContainerFactoryPostProcessor` |
| 分片倾斜 | crc32 打散；观测 per-shard；极端倾斜再加 salt（需双读双写迁移，首期不做） |
| 单商户压测 orgCode 恒定，打散全靠 outOrderSn | 压测前实测 8 片均匀度，倾斜超阈值先排查订单号规律再上线 |
| Retry 回错片 | per-shard retry queue + 单测覆盖 |
| Hikari 打满 | max ≥ `8 × persist-shard-max-concurrency` + 余量；观测 active/pending |
| 切换窗口丢消息 | 先排空旧队列再切；pending 补投仍可用 |
| 回滚 | `persist-shard-count=1` 重启回单队列（需旧队列仍在） |

## 二期（不在本期）

- 方案 C：热事务去掉 dispatch/balance outbox 同事务写出，提交后异步写入。这是从 ~100/s 追平入口 150/s 的关键，因 `dispatch_outbox` 在 timer OFF 后仍是 persist 事务内相对最慢 phase。
- 重开 timer 恢复秒级履约：需「timer 争用与落库耦合根因」步骤 3（timer/worker 池解耦）、步骤 4（scan 索引按 shard_key 收窄）先落地并复压验证。
- 专用 MySQL / 降低共享实例噪声。

## 文档同步（实现时）

实现合并前必须同步：

- `docs/交行直充业务系统高可用方案设计.md`（队列拓扑、监控、timer 争用与落库耦合根因）
- `docs/交行直充业务系统详细设计.md`（8.3 Consumer、配置表、timer/worker 池与 outbox scan 索引）
- `docs/部署手册.md`（切换步骤、Nacos 项、`timer-enabled=false` 硬前置与 `outbox-parallelism` 约束）

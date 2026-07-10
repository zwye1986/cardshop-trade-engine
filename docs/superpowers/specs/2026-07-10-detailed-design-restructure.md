# 详细设计文档全文重排设计

## 目标

将 `docs/交行直充业务系统详细设计.md` 从当前“总体设计 + 实施细节大章 + V1 追加说明”混合结构，重排为按系统分层和业务模块组织的详细设计文档，并把文档中出现的字段定义同步到当前代码、DDL COMMENT、Redis key/hash、MQ payload 和配置类。

## 背景与问题

当前详细设计文档约 3380 行，主要问题有三类：

1. 结构重复：第 1-19 章已经按技术面展开，第 20 章又重新包含 Maven、API、Redis、MyBatis、RabbitMQ、Nacos、XXL-Job、V1 细节，读者需要在多个位置拼接同一能力。
2. 责任边界不清：V1 接入、Application 用例、Domain 状态、Infrastructure 适配、Job 补偿、Migration DDL 等内容交叉出现，难以从某个模块快速定位设计约束。
3. 字段定义不完整：部分表格只列字段名或示例，缺少字段含义、来源、代码字段、落库/Redis/MQ 位置、约束和敏感性说明，且一些字段已落代码但文档未更新。

## 设计原则

1. 以分层和模块为主线，而不是以实现阶段或历史追加顺序为主线。
2. 保留已经验证的业务约束和联调结论，不因为重排丢失 test188 dev/test 证据对应的设计依据。
3. 字段字典以当前代码和 DDL 为准：Java DTO/record/DO、`TradeEngineProperties`、Liquibase COMMENT、Redis key builder/Lua、MQ publisher/consumer、outbox/notify worker。
4. 不在本次文档整理中改变业务设计、接口语义或代码行为；若发现代码与设计明显矛盾，先在文档中标记“当前实现/后续事项”，不顺手改代码。
5. 不写入密钥、密码、真实 host 等敏感信息；涉及 `.env`、Nacos、部署密钥时继续要求同类密钥值必须使用单引号包裹，且文档只写变量名和示例占位。

## 目标目录结构

重排后的 `docs/交行直充业务系统详细设计.md` 使用以下主目录：

1. 文档说明、当前实现边界与术语
2. 系统模块与依赖关系
3. API/V1 接入层
4. Application 用例编排层
5. Domain 状态与业务规则
6. Redis 受理与缓存层
7. RabbitMQ 持久化消息层
8. MySQL/Liquibase 持久化层
9. Outbox、供应商派单、作废与商户回调
10. XXL-Job 与 dev/test 人工任务入口
11. 配置中心、部署与健康检查
12. 观测、日志与告警
13. 测试与验收标准
14. 字段字典总表
15. 后续事项与当前不纳入范围的能力

现有第 20 章“实施级详细设计补充”不再作为独立巨章保留。其内容将迁移到对应主章节：

| 当前内容 | 新位置 |
| --- | --- |
| Maven 模块 POM、模块职责 | 第 2 章 |
| API 契约、V1 协议、签名、路由 | 第 3 章 |
| 领域对象、状态码、错误码 | 第 5 章 |
| 下单服务、Consumer、outbox 编排 | 第 4 章、第 9 章 |
| Redis Lua、Key 构造、pending、锁 | 第 6 章 |
| MyBatis Mapper、Liquibase 字段 | 第 8 章、第 14 章 |
| RabbitMQ 声明和 Confirm | 第 7 章 |
| Nacos 全量模板 | 第 11 章 |
| XXL-Job Handler | 第 10 章 |
| 观测指标 | 第 12 章 |
| V1 测试清单、验收标准 | 第 13 章 |

## 字段同步范围

### V1 API 字段

同步以下代码：

| 类型 | 文件 |
| --- | --- |
| 创建订单请求 | `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/dto/V1RechargeCreateOrderRequest.java` |
| 查单请求 | `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/dto/V1RechargeOrderQueryRequest.java` |
| 作废请求 | `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/dto/V1RechargeOrderInvalidRequest.java` |
| token 请求 | `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/dto/V1IssueTokenRequest.java` |
| 通用响应 | `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/dto/V1CardApiResponse.java` |
| 订单响应 | `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/dto/V1RechargeOrderResponse.java` |

字段表统一包含：外部 JSON 字段、Java 字段、必填性、来源、业务含义、签名/验签参与方式、落库或 Redis 映射、注意事项。

### Application 业务对象字段

同步以下核心 record/enum：

| 类型 | 文件 |
| --- | --- |
| 下单命令 | `AcceptRechargeOrderCommand.java` |
| 下单结果 | `AcceptRechargeOrderResult.java`、`AcceptRechargeOrderStatus.java` |
| 持久化消息 | `PersistTradeOrderMessage.java` |
| Consumer 快照 | `PersistOrderSnapshot.java` |
| pending 快照 | `PendingPersistOrderSnapshot.java` |
| outbox 事件 | `RechargeOutboxEvent.java`、`RechargeOutboxBatchResult.java` |
| 供应商请求/响应 | `SupplierDispatchGatewayRequest.java`、`SupplierInvalidGatewayRequest.java`、`SupplierGatewayResponse.java` |
| 通知任务 | `RechargeNotifyTask.java`、`RechargeNotifyTaskDraft.java`、`MerchantNotifyRequest.java` |
| 余额释放/刷账/补偿 | `BalanceReservationReleaseCommand.java`、`BalanceDeltaFlushOutboxCommand.java`、`RechargeCompensationBatchResult.java` |
| 异常核查 | `AcceptAnomalyRecord.java`、`AcceptAnomalyCase.java`、`AcceptAnomalyCheckEvidence.java` |

字段表统一包含：Java 字段、来源、输出目标、幂等/状态约束、敏感性或脱敏要求。

### MySQL 和 Liquibase 字段

同步以下实体和 changelog：

| 表/实体 | 文件 |
| --- | --- |
| `mc_order` 旧表映射 | `McOrderDO.java` |
| `recharge_order_ext` | `RechargeOrderExtDO.java`、`db.changelog-master.yaml` |
| `recharge_outbox` | `RechargeOutboxDO.java`、`db.changelog-master.yaml` |
| `recharge_notify_task` | `RechargeNotifyTaskDO.java`、`db.changelog-master.yaml` |
| `recharge_accept_anomaly` | `RechargeAcceptAnomalyDO.java`、`db.changelog-master.yaml` |
| `mc_order_supplier_flow` | `McOrderSupplierFlowDO.java` |
| `mc_depart_record` | `McDepartRecordDO.java`、生成列/唯一键 changelog |
| `mc_ext_depart` | `McExtDepartDO.java` |
| `mc_product` / `mc_depart_product` | `McProductDO.java`、`McDepartProductDO.java` |

字段表统一包含：数据库字段、Java DO 字段、DDL COMMENT 或旧表语义、读写职责、索引/唯一键、状态值来源。

### Redis 字段

同步以下来源：

| 类型 | 文件 |
| --- | --- |
| key 构造 | `RechargeRedisKeyBuilder.java` |
| 下单 Lua 入参/返回 | `AcceptRechargeOrderLuaArguments.java`、`AcceptRechargeOrderLuaResultMapper.java`、`accept_recharge_order.lua` |
| pending/mark 脚本 | `RedisPendingPersistOrderAdapter.java`、`mark_db_persisted.lua`、`mark_persist_queued.lua`、`mark_persist_failed.lua` |
| 余额释放 | `RedisBalanceReservationReleaseAdapter.java`、`release_balance_reservation.lua` |
| 缓存修复 | `RedisRechargeCacheRepairAdapter.java` |

字段表统一包含：Redis key、类型、member/hash field、写入方、读取方、TTL/锁语义、失败恢复语义。

### MQ、Outbox、Notify、Job 字段

同步以下来源：

| 类型 | 文件 |
| --- | --- |
| MQ publisher/listener | `RabbitPersistRechargeOrderMessagePublisher.java`、`RabbitPersistRechargeOrderMessageListener.java` |
| Outbox worker | `RechargeOutboxWorkerService.java`、`MybatisRechargeOutboxAdapter.java` |
| Notify worker/client | `RechargeNotifyWorkerService.java`、`HttpMerchantNotifyClientAdapter.java` |
| Job facade | `cardshop-trade-engine-job/src/main/java/com/cardshop/trade/engine/job/recharge/*.java` |
| 人工入口 | `ManualRechargeJobController.java` |

字段表统一包含：payload 字段、事件/任务状态、retry/lock 字段、workerId 语义、分片字段、失败和人工介入口径。

### 配置字段

同步 `TradeEngineProperties.java`，覆盖以下配置族：

| 配置族 | 文档要求 |
| --- | --- |
| `redis` | 写入开关、WAIT、副本数、pending bucket、key prefix、Lua 慢阈值 |
| `mq` | exchange/queue/routing key、retry/DLQ、confirm、breaker |
| `order` | pending TTL、终态 TTL、request_hash 元数据、配置变化重试 |
| `routing` | 旧链路代理、灰度 app/org、超时 |
| `v1` | SM4、timestamp、nonce、token、签名密钥来源 |
| `balance` | owner、差异阈值 |
| `fulfillment` | outbox 租约、退避、同账号限频 |
| `notify` | 回调租约、退避、HTTP 超时 |
| `supplier` | HTTP 开关、渠道 URL、超时 |
| `integration` | dev/test 人工入口超时和开关 |
| `xxl-job` | executor、admin、token、日志路径 |

## 迁移策略

1. 先在目标文档顶部建立新目录和“当前实现边界”。
2. 按分层迁移现有内容，保留现有重要段落，但移动到新位置并去除重复表达。
3. 建立第 14 章字段字典总表，先覆盖当前代码已落地且联调证据依赖的字段：V1 DTO、`PersistOrderSnapshot`、`recharge_order_ext`、`recharge_outbox`、`recharge_notify_task`、`recharge_accept_anomaly`、Redis order/reservation/balance/pending、MQ initial snapshot、outbox payload、notify payload、`TradeEngineProperties`。
4. 对历史设计但当前未落地的内容，统一移动到第 15 章，并标明“未纳入当前实现”或“后续事项”。
5. 完成后执行 Markdown 结构检查：目录标题不重复、没有孤立的旧第 20 章、关键字段表都有含义和代码来源。

## 验收标准

1. `docs/交行直充业务系统详细设计.md` 的一级/二级标题能体现系统分层和模块边界。
2. 文档不再依赖一个巨大的“实施级详细设计补充”承载主内容。
3. V1、Application、Redis、RabbitMQ、MySQL、Outbox、Notify、Job、配置字段均有字段注释和代码来源。
4. 所有敏感配置只出现变量名或占位说明，不出现真实 secret。
5. 与 `AGENTS.md` 一致：代码关键注释仍能引用详细设计中的稳定章节；如章节号发生变化，需要保留章节标题或在文档中提供迁移说明，避免已有注释引用完全失效。
6. `git diff --check` 通过。
7. 文档重排完成后，运行标题和字段覆盖检查命令，确认关键类/表在文档中可检索。

## 非目标

1. 不修改 Java 生产代码、测试代码或 DDL。
2. 不重新设计业务流程。
3. 不把 dev/test 集成测试执行记录搬入详细设计；执行证据仍保留在 `docs/集成测试.md`，详细设计只保留设计约束和字段定义。
4. 不把 `docs/交行直充业务系统高可用方案设计.md` 合并进详细设计；只在必要位置互相引用。

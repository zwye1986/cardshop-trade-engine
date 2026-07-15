# Outbox 订单归属边界设计

## 背景

`cardshop-trade-engine` 与旧卡管系统共享 `mc_order`、`mc_ext_depart`、`mc_depart_record` 等旧表。
新服务的 outbox 任务会根据 `recharge_outbox` 事件调用供应商，并回写共享订单、余额和流水。
因此，任务不能仅凭共享表中的订单号、状态或商品类型判断是否有权处理，必须显式确认订单属于新交易引擎。

## 目标

所有读取和推进 `recharge_outbox` 的自动任务，只处理由 `cardshop-trade-engine` 完成 MySQL 持久化的订单；
旧卡管系统订单以及缺少新系统持久化事实的异常 outbox 记录不得被扫描、抢占或产生共享表副作用。

## 归属判定

`recharge_order_ext` 是新交易引擎独占维护的订单扩展表，作为订单归属的权威哨兵。
一条 outbox 事件必须同时满足以下条件才属于新系统可处理范围：

1. `recharge_order_ext.order_id = recharge_outbox.order_id`。
2. `recharge_order_ext.org_code = recharge_outbox.org_code`。
3. `recharge_order_ext.out_order_sn = recharge_outbox.out_order_sn`。
4. `recharge_order_ext.persist_status = 'DB_PERSISTED'`。

三个业务定位字段必须同时匹配，避免异常数据只复用订单号时跨商户或跨订单误关联。
`DB_PERSISTED` 表示新系统订单事实已完成 MySQL 提交，满足 outbox 执行前置条件。

## 扫描与抢占边界

归属条件必须同时落在两个阶段：

- 扫描阶段：`selectDueForShardByEventTypes` 只返回满足归属判定的到期或租约失效事件。
- 抢占阶段：`claimProcessing` 再次校验相同归属条件，防止扫描后扩展记录状态变化或调用方绕过扫描直接抢占。

扫描过滤用于控制任务输入范围，抢占过滤是执行副作用前的最终数据库防线。任何不满足边界的事件保持原状态，
不自动标记成功、失败或人工处理，避免新服务修改不属于自己的数据。此类异常记录通过低频监控或人工 SQL 排查。

## 处理顺序

outbox 任务先使用状态、`next_retry_time` 和租约条件筛选当前具备执行资格的事件，再在每个分片的可执行集合内按
`created_at ASC, id ASC` 扫描和抢占。`created_at` 表示事件生成时间，`id` 用于同一时间值下提供稳定顺序，保证先生成的
事件优先获得处理机会。

`next_retry_time` 只作为执行资格条件，不再作为主排序字段。尚未到达退避时间的早期失败事件不得提前执行；到期后重新进入
可执行集合，并按原始 `created_at` 恢复其优先级。该约束保证单分片、单批次的获取顺序，不承诺多分片、多节点并发处理后的
全局完成顺序。

## 其它 Outbox 读取

所有影响资金判断或 outbox 可观测性的查询也必须使用相同归属边界：

- 未完成余额 delta 汇总只能计入新系统订单事件，避免旧数据或误写事件影响余额对账。
- outbox pending/最大滞留时间指标只统计新系统订单事件，避免告警被边界外数据污染。

事件写入仍由新系统事务和专用适配器负责，不新增 `source_system` 字段。`recharge_order_ext` 已能证明订单归属和持久化完成状态，
避免增加需要回填的重复来源字段。payload 可保留业务排障信息，但不得作为权限边界。

## 数据库实现

Mapper 中使用 `exists` 子查询表达归属条件，避免改动现有 DO、应用端口和事件模型：

```sql
exists (
    select 1
    from recharge_order_ext ext
    where ext.order_id = recharge_outbox.order_id
      and ext.org_code = recharge_outbox.org_code
      and ext.out_order_sn = recharge_outbox.out_order_sn
      and ext.persist_status = 'DB_PERSISTED'
)
```

MyBatis-Plus LambdaWrapper 无法稳定表达带当前表字段关联的 `exists` 子查询时，Mapper 使用参数化 SQL 方法承载扫描和抢占，
保持状态、租约、重试上限和分片语义不变，并将扫描排序固定为 `created_at ASC, id ASC`。不得拼接事件类型或业务值；
`limit` 继续在 Java 侧限制为 `1..1000`。

## 文档与注释

实现前同步更新：

- `docs/交行直充业务系统详细设计.md` 的「9.1 派单 Outbox」及字段/任务边界说明。
- `docs/交行直充业务系统高可用方案设计.md` 的 outbox 扫描、抢占和共享表风险说明。

关键 Mapper 方法增加中文注释，并链接详细设计对应章节。

## 测试

测试覆盖以下合同：

1. 扫描 SQL 同时校验 `order_id`、`org_code`、`out_order_sn` 和 `DB_PERSISTED`。
2. 抢占 SQL 重复校验相同归属条件。
3. 余额 delta 汇总与可观测性聚合使用相同归属边界。
4. 扫描按 `created_at ASC, id ASC` 稳定排序，不再按 `next_retry_time` 决定已到期事件的处理先后。
5. 原有事件类型过滤、到期/僵尸判断、重试上限、分片和 CAS 版本条件保持不变。
6. 运行 infrastructure 相关单元测试，并在条件允许时运行全量 Maven 测试。

## 非目标

- 不修改旧卡管系统代码。
- 不给 `mc_order` 增加来源字段。
- 不迁移或自动关闭历史边界外 outbox 数据。
- 不改变供应商、余额、回调和缓存修复的业务状态机。

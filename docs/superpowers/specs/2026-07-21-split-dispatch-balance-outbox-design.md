# 派单 Outbox 与余额刷账 Outbox 拆表设计

> 日期：2026-07-21
> 状态：设计更新，待实现
> 关联：`docs/交行直充业务系统详细设计.md`「8.3 异步落库 Consumer」「9.1 派单 Outbox」「9.2 余额 Delta 刷账 Outbox」
> 触发证据：`docs/集成测试.md` 中 `PERF-A39-C100-R100-T5-5M-PHASE-20260721105037`

## 1. 背景

100 并发、100 QPS、5 秒超时、5 分钟压测显示，显式 multi-row 批 adapter 未继续提升异步落库吞吐。新增 phase 指标拆解出 `recharge_outbox` 写入占批事务均值约 54.5%，明显高于 `mc_order`、`recharge_order_ext` 和 `mc_depart_record`。

当前 `recharge_outbox` 同时承载供应商派单/查单/作废事件和余额刷账事件。两类事件生命周期不同：

- `SUPPLIER_DISPATCH`、`SUPPLIER_QUERY`、`INVALID_ORDER` 需要供应商 HTTP 重试、查单、租约、人工处理和派单幂等。
- `BALANCE_DELTA_FLUSH` 是资金沉淀事件，目标是把 Redis 实时余额变化批量刷入旧表 `mc_ext_depart.balance`，更适合按商户聚合、轻索引扫描和批量标记。

把它们放在一张表中，会让订单落库事务每单写两条 outbox，并为两种生命周期共同维护同一组二级索引，压测下写入、扫描和锁等待互相放大。

## 2. 目标

1. 将供应商履约 outbox 与余额刷账 outbox 拆成两张表：`recharge_dispatch_outbox` 和 `recharge_balance_delta_outbox`。
2. 让订单落库事务里的 outbox 写入更轻：派单表保留履约所需字段和索引；余额表只保留资金 delta 所需字段和轻量索引。
3. 保持可靠性语义不下降：订单事实提交后，派单和余额刷账仍都有可恢复入口；重复 MQ、pending 补投、worker 重试不得造成重复派单或重复刷账。
4. 由于该链路尚未正式上线，拆表按上线前目标模型直接替换旧混合 outbox，不设计生产数据迁移、旧表双写、旧表读取或回填。

## 3. 非目标

- 不在本设计中把 `SUPPLIER_QUERY` 或 `INVALID_ORDER` 拆到第三张表；它们属于供应商履约生命周期，继续放入 `recharge_dispatch_outbox`。
- 不把 `BALANCE_RELEASE` 或 `CACHE_REPAIR` 纳入本轮 outbox 拆表目标实现：余额释放通过 `recharge_balance_delta_outbox(delta_type=RELEASE)` 表达，缓存修复继续走 `RechargeCacheRepairService` 独立链路。现有枚举若仍保留这两个值，只作为历史/预留值，不得被 dispatch worker 扫描或写入。
- 不改变 Redis-first 下单、RabbitMQ 落库、MySQL 订单事实的主链路。
- 不把 `mc_ext_depart.balance` 改回下单接口同步更新。
- 不设计旧 `recharge_outbox` 存量数据迁移；dev/test 历史压测数据按测试数据清理或重建环境处理。

## 4. 表职责

| 表 | 事件类型 | 写入方 | 扫描/处理方 | 主要索引目标 |
| --- | --- | --- | --- | --- |
| `recharge_dispatch_outbox` | `SUPPLIER_DISPATCH`、`SUPPLIER_QUERY`、`INVALID_ORDER` | 落库 Consumer、派单/查单/作废 worker | `RechargeOutboxWorkerService` | 供应商事件重试、租约回收、FIFO 扫描、事件幂等 |
| `recharge_balance_delta_outbox` | `BALANCE_DELTA_FLUSH` | 落库 Consumer、失败终态释放补偿 | `RechargeBalanceDeltaFlushService`、余额对账 | 按商户聚合、delta 幂等、轻量待刷账扫描 |
| `recharge_outbox` | 旧混合事件 | 未上线旧实现 | 无目标处理方 | 作为问题背景和待废弃实现，不作为新代码目标表 |

## 5. 目标 DDL 口径

`recharge_dispatch_outbox` 保留完整重试/租约字段：

```sql
create table recharge_dispatch_outbox (
  id bigint not null auto_increment comment '表主键',
  event_id varchar(100) not null comment '履约事件唯一ID',
  event_type varchar(64) not null comment '履约事件类型',
  org_code varchar(64) not null comment '商户编码',
  order_id varchar(100) not null comment '平台订单号',
  out_order_sn varchar(100) not null comment '商户订单号',
  payload json not null comment '履约事件参数JSON',
  status varchar(32) not null comment '事件状态',
  retry_count int not null default 0 comment '已重试次数',
  max_retry_count int not null default 10 comment '最大重试次数',
  next_retry_time datetime not null comment '下次允许执行时间',
  locked_by varchar(100) default null comment '当前抢占该事件的应用实例ID',
  locked_until datetime default null comment '抢占租约到期时间',
  version bigint not null default 0 comment '乐观锁版本号',
  last_error varchar(1000) default null comment '最近一次失败原因',
  shard_key varchar(128) not null comment '固定使用event_id，避免不同任务分片语义冲突',
  created_at datetime not null default current_timestamp comment '创建时间',
  updated_at datetime not null default current_timestamp on update current_timestamp comment '更新时间',
  primary key (id),
  unique key uk_dispatch_outbox_event_id (event_id),
  key idx_dispatch_outbox_due (status, next_retry_time, id),
  key idx_dispatch_outbox_lease (status, locked_until, id)
) engine=InnoDB default charset=utf8mb4 comment='交易订单供应商履约可靠事件表';
```

`recharge_balance_delta_outbox` 只保留余额刷账所需字段：

```sql
create table recharge_balance_delta_outbox (
  id bigint not null auto_increment comment '表主键',
  event_id varchar(120) not null comment '余额刷账事件唯一ID',
  org_code varchar(64) not null comment '商户编码',
  order_id varchar(100) not null comment '平台订单号',
  out_order_sn varchar(100) not null comment '商户订单号',
  delta_type varchar(32) not null comment '余额变动类型，例如 PRE_DEDUCT 或 RELEASE',
  delta_amount bigint not null comment '余额变动金额，单位分；扣减为负，释放为正',
  status varchar(32) not null comment '刷账状态',
  retry_count int not null default 0 comment '已重试次数',
  max_retry_count int not null default 10 comment '最大重试次数',
  next_retry_time datetime not null comment '下次允许执行时间',
  locked_by varchar(100) default null comment '当前抢占该事件的应用实例ID',
  locked_until datetime default null comment '抢占租约到期时间',
  version bigint not null default 0 comment '乐观锁版本号',
  last_error varchar(1000) default null comment '最近一次失败原因',
  created_at datetime not null default current_timestamp comment '创建时间',
  updated_at datetime not null default current_timestamp on update current_timestamp comment '更新时间',
  primary key (id),
  unique key uk_balance_delta_event_id (event_id),
  key idx_balance_delta_due (status, next_retry_time, org_code, id),
  key idx_balance_delta_lease (status, locked_until, id),
  key idx_balance_delta_org_status (org_code, status, id)
) engine=InnoDB default charset=utf8mb4 comment='交易订单余额增量刷账事件表';
```

说明：

- 余额表不再保存通用 `event_type`、`payload`、`shard_key`，避免 JSON 写入和无效通用索引。
- 余额表以 `org_code` 为主要聚合维度，扫描后仍必须通过 CAS 抢占事件，刷账时按商户行锁串行更新 `mc_ext_depart.balance`。
- `idx_balance_delta_lease` 用于恢复 `PROCESSING` 租约过期事件，避免大批刷账任务中断后按状态全表扫描。
- 余额刷账首期分片固定使用 `crc32(event_id)` 作为 residual predicate，与 dispatch 分片认知保持一致；若压测显示 residual filter 扫描过重，再单独设计 `id` range 或显式 shard 字段。

## 6. 写入流程

订单落库事务目标顺序：

```text
mc_order
  -> recharge_order_ext
  -> mc_depart_record
  -> recharge_dispatch_outbox
  -> recharge_balance_delta_outbox
```

校验通过且允许派单的订单写 `recharge_dispatch_outbox(SUPPLIER_DISPATCH)`；已完成 Redis 余额预扣的订单写 `recharge_balance_delta_outbox(PRE_DEDUCT)`。两张表都使用稳定 `event_id` 做幂等：

- `dispatch-{orderId}`：供应商派单事件。
- `supplier-query-{orderId}`：供应商查单事件。
- `invalid-{orderId}` 或业务已约定的作废事件 ID：作废事件。
- `balance-flush-{orderId}`：预扣刷账事件。
- `balance-flush-release-{orderId}`：释放刷账事件。

批 adapter 必须分别调用显式 multi-row mapper，并分别上报 phase：

- `recharge_dispatch_outbox`
- `recharge_balance_delta_outbox`

旧 `recharge_outbox` 的 phase 只保留为历史压测对比背景，不作为目标实现的性能指标。

## 7. 处理流程

### 7.1 派单履约

`RechargeOutboxWorkerService` 改为扫描 `recharge_dispatch_outbox`，且首期只扫描 `SUPPLIER_DISPATCH`、`SUPPLIER_QUERY`、`INVALID_ORDER`。扫描、抢占、退避、人工处理和供应商幂等规则保持不变。处理前仍以 `recharge_order_ext` 作为归属哨兵：同一 `order_id + org_code + out_order_sn` 必须存在且 `persist_status=DB_PERSISTED`。

### 7.2 余额刷账

`RechargeBalanceDeltaFlushService` 改为扫描 `recharge_balance_delta_outbox`。流程为：

1. 按 `status + next_retry_time` 扫描到期 delta，并按 `status + locked_until` 恢复租约过期的 `PROCESSING` delta。
2. CAS 抢占为 `PROCESSING`。
3. 按 `org_code` 聚合本轮抢占成功的 `delta_amount`。
4. 在同一 MySQL 事务中锁定 `mc_ext_depart` 商户余额行，执行 `balance = balance + sum(delta_amount)`。
5. 同事务将参与聚合的 delta 标记为 `SUCCESS`；任一 `markSuccess` CAS miss 必须回滚整个商户组事务，不能留下余额已更新但 delta 未成功的状态。
6. 商户组事务失败后，在事务外逐条标记 `FAILED` 并按退避序列推进 `next_retry_time`，达到上限转 `MANUAL_REQUIRED`；若这些失败标记本身因数据库抖动失败，事件保留在 `PROCESSING`，依赖租约过期后的下轮 reclaim。

余额对账只汇总 `recharge_balance_delta_outbox` 中未 `SUCCESS` 的 delta，不再扫描供应商履约表。

## 8. 配置与运行参数

拆表不新增旧表兼容开关。目标代码只写双表；若 dev/test 需要回退，应停止新服务、清理测试数据并回滚版本，而不是运行双写或旧表读取。

余额 delta 刷账新增 `trade.engine.outbox.*` 配置族：

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `balance-delta-flush-batch-size` | `500` | 余额 delta 刷账单轮抢占上限，按 `mc_ext_depart` 行锁和任务耗时压测调优。 |
| `balance-delta-claim-lease-seconds` | `120` | 余额刷账租约，必须覆盖单轮刷账事务 P99。 |
| `balance-delta-retry-backoff-seconds` | `5,30,120,600` | 余额刷账失败退避序列，逗号分隔。 |

## 9. 上线前替换策略

1. **DDL 调整**：新增 `recharge_dispatch_outbox` 和 `recharge_balance_delta_outbox`；旧 `recharge_outbox` 不承载目标业务写入。
2. **代码替换**：落库 adapter、派单 worker、余额刷账 job 一次性切到双表；不保留旧表 reader、dual-write adapter 或 backfill job。
3. **测试数据处理**：dev/test 历史压测数据在下一轮压测前清空 Redis、MQ 和测试库相关业务数据；不把历史旧表数据迁入新表。
4. **回退策略**：若双表版本不可用，回退应用版本并清理 dev/test 新产生数据；生产正式上线前不设计在线数据回滚流程。

## 10. 可观测性

新增或调整指标：

- `trade.engine.persist.batch.mysql.phase.duration{phase="recharge_dispatch_outbox"}`
- `trade.engine.persist.batch.mysql.phase.duration{phase="recharge_balance_delta_outbox"}`
- `trade.engine.dispatch.outbox.pending.total{event_type,status}`
- `trade.engine.balance.delta.pending.total{status}`
- `trade.engine.balance.delta.amount.pending{org_code}` 不进入应用内高基数指标；如需按商户排障，由 SQL/脚本临时采样，不进 Prometheus label。

目标实现不再把新的 pending 聚合写入旧 `trade.engine.outbox.pending`。若短期保留该旧 gauge，只能作为 dispatch 兼容视图或历史 dashboard 过渡，不得作为拆表后压测结论来源。

测试辅助前端的观测聚合当前仍硬编码旧 `recharge_outbox` 证据模板；本变更落地时必须同步更新 `/Users/mac/Documents/codeSpace/frontend/cardshop-trade-engine-test-console` 的观测 SQL、证据解析和 README/AGENTS，或在实施记录中证明前端没有使用这些表名/指标。

压测报告必须比较：

- 两张 outbox 表的 phase 耗时。
- 批事务总耗时。
- Rabbit 主队列 ready/unacked。
- `recharge_dispatch_outbox` 与 `recharge_balance_delta_outbox` 未终态积压。
- `Read timed out`、`Communications link failure`、lock wait。

## 11. 代码注释与文档引用要求

后续代码修改必须在关键类/方法中文注释中引用以下稳定章节：

- 批落库 adapter、mapper：`docs/交行直充业务系统详细设计.md`「8.3 异步落库 Consumer」。
- 派单履约表、worker、mapper：`docs/交行直充业务系统详细设计.md`「9.1 派单 Outbox」。
- 余额刷账表、worker、mapper：`docs/交行直充业务系统详细设计.md`「9.2 余额 Delta 刷账 Outbox」。

不要只引用本 spec；本 spec 用于解释变更背景，主约束以两份正式设计文档为准。

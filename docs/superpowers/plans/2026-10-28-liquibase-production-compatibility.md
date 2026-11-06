# 生产历史订单兼容与 Liquibase 迁移详细方案

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不删除、改号或迁移历史业务订单的前提下，消除生产 `cardshop` 的历史重复键对 Liquibase 的阻断，并保持新引擎订单唯一性及查询、持久化的一致性。

**Architecture:** 新增明确的主单归属字段，将现有生成列唯一键收窄为新引擎直充订单专用。新增迁移入口，先执行兼容前置步骤，再 include 原 master，最后校验完整结构；原已发布 changeset 的身份和内容保持不变。应用同步校验主单、扩展及事件归属，不把历史一对多当作一条新订单。

**Tech Stack:** Java 21、Maven、Liquibase 4.29.2、MySQL 8.0.28、MyBatis、JUnit 5、AssertJ。

**状态（2026-10-28）：待评审、未实施。** 本文 SQL 表达目标合同，不是可直接在生产执行的脚本；所有勾选项均须后续实现并取得实际证据。当前只允许生产只读核查，不授权 DDL、配置发布、数据修复、停启服务或切流。

---

## 1. 范围、事实与不做事项

生产现有生成列按所有 `product_type=1` 生成外部单幂等键，原 `0003` 会因 96 组重复而 HALT，原 `0004` 尚不能建立唯一索引。96 组共 236 条主单、140 条超出每组首条的记录：27 组拆单（82 条）、58 组补发（130 条）、11 组待历史核实（24 条）。拆单/补发已具有关联证据；11 组不能定性为重复扣款或履约。

证据索引：`.amp/in/artifacts/production-duplicate-analysis-20261028.jsonl`、`production-duplicate-classification-check-20261028.log`；明细解释见《部署手册》16.1。数字是该日只读快照，正式迁移前必须重新统计，不能硬编码成迁移前提。

本次保留全部历史平台单号、外部单号、金额、状态、履约及资金流水。不删除 140 条，不把合法子单合并，不按状态、日期、渠道或 `order_type=0` 猜测新引擎归属，不在此次迁移中裁定 11 组的售后责任。

`cardshop_test_new` 的测试订单、扩展、券、资金流水、outbox、回调和迁移账本 **均不复制到 `cardshop`**。生产只执行本库迁移。测试库已有订单的归属回填仅用于该测试库自身升级演练，不是跨库搬迁。

用户已明确不会发生新端超时后转旧端重新下单/发券。本方案不为该场景新增跨端请求锁、共享 claim 或资金 owner；也不实施透支、Lua 余额门禁或旧后台余额编辑改造。后文的同号冲突处理针对新端自身重试、MQ 重放与历史号码占用，不推翻用户给定的业务边界。

## 2. 目标数据合同

### 2.1 主单归属

新增普通字段，放在表末尾，不使用相对位置增加改表成本：

```sql
trade_engine_owned tinyint NOT NULL DEFAULT 0
  COMMENT '新引擎订单归属标记：0旧系统，1新引擎'
```

- 旧端 insert 不传字段时为 0；新增主单不要求旧端增加必填配置。
- 新端共享转换器显式写 1，单条、multi-row 批写、facts-only、MQ 快照兜底、业务校验失败事实全部覆盖。
- 该字段是订单归属，不是商户资金归属，也不是派单许可。普通更新不得将 1 改成 0。
- 迁移时仅按同库可信 `recharge_order_ext.order_id` 关联回填；必须同时核对商户、外部单号和 `product_type=1`。存在 ext 却无匹配主单、身份冲突或归属值不在 0/1 时 HALT，不能静默忽略。
- 生产主单存在 `update_time ... ON UPDATE CURRENT_TIMESTAMP`，回填SQL必须显式保留 `update_time=update_time`，并保持 `create_time/edit_time/end_time/success_time` 等业务时间不变；不能把归属迁移误记成订单状态更新时间。
- 无 ext 的历史订单保持 0；已有标记 1 却无 ext 也 HALT，由事实核查解决，不能自动降成 0 规避冲突。
- 不将该字段作为删改历史事实的手段；新端不支持同一请求生成拆单/补发尝试的多条主单，旧端合法多行仍归旧端处理。

### 2.2 稀疏唯一键

保留字段名、长度、字符集/排序规则及索引名，目标表达式如下：

```sql
direct_recharge_out_order_key varchar(191)
GENERATED ALWAYS AS (
  CASE
    WHEN trade_engine_owned = 1 AND product_type = 1
      AND org_code IS NOT NULL AND org_code <> ''
      AND out_orderId IS NOT NULL AND out_orderId <> ''
    THEN concat(org_code, ':', out_orderId)
    ELSE NULL
  END
) STORED COMMENT '新引擎直充订单幂等唯一键'
```

`uk_direct_recharge_out_order_key` 仍唯一。旧订单派生值变为 NULL，MySQL 唯一索引允许多条 NULL；历史业务列不变。这会重算生成列、可能重建表/索引，不可描述为完全无数据变化或零锁 DDL。

新端写入前必须要求有效直充身份，不能借 product_type 非 1、空订单号或标记 0 逃过约束。保留当前冒号拼接及排序规则，不顺带升级幂等规范；演练须校验允许的商户/外单字符范围不会出现不同元组拼接碰撞，存在不兼容值则 HALT 并另行评审，不能静默放宽。长度边界以实际 DDL 及接口合同校验，不靠字符串截断。

继续保留平台 `order_Id` 唯一、ext 平台单号唯一、ext `(org_code,out_order_sn)` 唯一、资金流水唯一及所有事件唯一约束。旧端拆单/补发继续可以复用外部单号，新端最多持有一条。

## 3. Liquibase 入口、执行顺序与重入

### 3.1 文件安排及账本身份

新增资源：

- `cardshop-trade-engine-migration/src/main/resources/db/changelog/db.changelog-production-compatible.yaml`：统一升级入口，顺序为前置 → 原 master → 后置。
- 同目录 `0026-legacy-order-idempotency-prelude.yaml`：环境合同、归属字段与回填、生成列/索引兼容。
- 同目录 `0026-legacy-order-idempotency-postconditions.yaml`：在全部原迁移执行后验证目标模型。

原 `db.changelog-master.yaml`、`0024-bocom-notify.yaml`、`0025-miniso-no-release-decision.yaml` **不修改、不搬移、不重编号**。新 changeset ID 使用 `0026-compat-*`，编号不表示排序，实际由 include 顺序决定。

```yaml
databaseChangeLog:
  - include:
      file: 0026-legacy-order-idempotency-prelude.yaml
      relativeToChangelogFile: true
  - include:
      file: db.changelog-master.yaml
      relativeToChangelogFile: true
  - include:
      file: 0026-legacy-order-idempotency-postconditions.yaml
      relativeToChangelogFile: true
```

修改 bootstrap `application.yml` 和 migration `pom.xml` 的入口引用，测试加载新入口。Nacos 如显式设置 `spring.liquibase.change-log`，须同步指向 `classpath:/db/changelog/db.changelog-production-compatible.yaml`；若未覆盖则随制品默认值。实际生效值必须在启动前核对，不能只看仓库 YAML。

测试必须比较新入口解析后的每个原 changeset `(id, author, filePath, checksum)` 与原入口结果，及真实旧账本升级前后值；include 解析路径有差异就修正新入口，不能清 checksum、改账本或用 changelogSync 掩盖。保留原账本行，新增行数按实际解析结果计算，不硬编码总行数。

### 3.2 前置步骤

按以下次序实施，每一项有独立稳定 ID，任何异常身份/结构均 HALT：

1. **环境与结构守卫。** 必须已存在旧系统 `mc_order`、`mc_depart_record` 及必要业务列、平台单号唯一索引；这是“空业务数据旧库”的初始化，不是从零建卡管库。完全空 schema 明确失败，不生成残缺卡管结构。核对 MySQL 版本、字符集、索引定义和 ext 是否存在；新字段/生成列若已有未知异构定义，不凭同名视为兼容。允许的起始生成列包括本次生产实测版本（没有 `org_code<>''` 条件且COMMENT不同）、原master版本及目标版本；按等价表达式/存储属性识别，不能仅字符串逐字比较，亦不能因生产与原master略有不同就错误HALT。
2. **增加归属列。** 不存在才增加；已存在须验证类型/default/null/comment 和值域。采用条件分支 changeset `onFail: CONTINUE` 跳过不适用的正常分支，配合无条件结构守卫；不将未知结构 MARK_RAN。目标环境是否可用 INSTANT ADD COLUMN 由隔离演练确认，不承诺全部分支在线。
3. **核对并回填已有新端归属。** ext 不存在时不发出引用该表的 SQL；存在时先校验完整身份及唯一性，然后按平台号关联更新 0→1。批量按主键区间执行、记录总数并可重入，未通过校验的行不回填。新端写入者须暂停，防止旧制品继续写默认 0。生产首次接入无 ext 则不导入、不构造 ext。
4. **范围内重复预检。** 在改生成列前，直接按“标记 1、有效直充身份”的目标表达式分组，要求零重复。标记 0 的 96 组不阻断；新端范围的真实重复必须阻断。检查索引同名异构、额外依赖该生成列的索引，未知依赖 HALT，不自动删除其他索引。
5. **生成列/索引兼容 DDL。** 无生成列时直接按目标定义增加；有列无索引（当前生产）时修改表达式；已有旧全局唯一索引（已迁移测试库）时在同一个 ALTER 中 drop 该索引、修改生成列、重建同名目标唯一索引，不将约束缺口暴露为可正常运行状态。目标列与索引已正确则不再重建。各分支均以实际元数据为前提，不做“遇错继续”。
6. **前置终检。** 确认生成列真实表达式/存储方式/排序规则正确，标记 0 的派生值全 NULL、标记 1 身份合法、目标重复为零。该终检不能只检查列名存在。

无索引生产分支由原 `0004` 正常建立唯一索引；已有索引测试分支由前置 ALTER 保持唯一。原 `0002a` 在列已存在时按其原有条件自然跳过，原 `0003` 对新范围真实执行；原 `0004` 依其原有索引存在条件执行或跳过。这些是原已发布 precondition 的自然行为，不是篡改执行结果绕过重复检查。

**重入要求：** MySQL DDL 不随 Liquibase 事务整体回滚，可能 DDL 成功而账本尚未记录。每项先验证目标状态，已正确即无副作用成功；冲突状态 HALT。归属回填允许重复更新相同值；所有条件分支后均有终检，防止 CONTINUE 漏掉必需步骤。迁移锁不等于业务停写锁；连接中断先读元数据、账本、锁持有者及活动 DDL，再决定续跑，不盲目释放锁或重放。

### 3.3 原迁移与后置验收

原 master 继续负责 fail 字段、ext、资金流水约束、双 outbox、MINISO/交行各表字段等。`0011` 对资金流水重复仍 HALT，不因主单范围调整而弱化；`0017` 的多语句要求仍保留。实际 JDBC URL 必须匹配原迁移需要的 `allowMultiQueries=true`，批写启用时同时满足 `rewriteBatchedStatements=true`。

后置验收检查：目标主单字段/唯一索引、ext 双唯一、资金流水唯一、全部预期表和关键新增列均具备；主单↔ext 平台号/商户/外单身份一致，1 无 ext、ext 无 1 的记录均为零。只核验、不凭空补订单、资金、券或回调任务。后置失败同样不启动业务。

## 4. 应用必须同步完成的配套

### 4.1 写入路径：不把 ODKU 当作本单落库证明

修改基础目录 `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/` 下：

- `entity/McOrderDO.java`：新增字段映射，中文注释与新 DDL COMMENT 一致。
- `RechargeOrderPersistenceRowConverter.java`：所有新端事实统一置 1。
- `mapper/McOrderMapper.java`：单条、multi-row SQL 显式带标记；增加按平台号/目标键的身份读取及批量读取，避免批路径逐条查。
- `mapper/RechargeOrderExtMapper.java`：为冲突裁决提供稳定身份读取，不能让同号异平台的 upsert 偷推进另一订单版本。
- `MybatisRechargeOrderPersistenceAdapter.java`、`MybatisBatchRechargeOrderPersistenceAdapter.java`：主单插入后、扩展 upsert 后各核对身份，随后才写资金流水和事件。全部核验在事实事务内，锁顺序保持主单→ext；不只依赖 affected rows。
- `MybatisRechargePersistOutboxAsyncWriter.java`：独立事务先读取并核对已提交主单/ext，再补写 outbox，不能为未提交/身份冲突的 aggregate 创建事件。
- `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/PersistRechargeOrderConsumerService.java`：已持久化跳过判定必须包含可信完整身份，不绕过 hash/归属核对；合法重复仍保留异步 outbox 补写和 Redis mark 恢复。

事务裁决固定如下：

```text
写 mc_order（带 owned=1，按既有唯一键串行）
读取并验证：orderId/orgCode/outOrderSn/owned/productType/金额匹配
写 ext（仅允许同一身份的版本前进）
读取并验证：orderId/orgCode/outOrderSn/requestHash/version/alg 匹配
写幂等流水、派单和 delta；提交
```

同号同平台同摘要允许重放；同平台不同商户/外单/金额或同键不同平台号均抛出身份冲突，整笔/整批事务回滚，不自动把失败者重写到胜者 orderId。摘要比较包括算法和规范版本，不能仅比较字符串。新端已有主单但 ext 缺失的异常事实不能靠新请求自动认领。

异步模式仍保持事实提交→独立 writer→调度/Redis mark/ack；writer 核验的是原始不可变身份及已提交版本覆盖关系，不要求当前履约状态仍等于旧快照。writer 失败不 ack，事实保留、重投补齐；事实事务冲突不调用 writer。不得借本次修改调整履约、扣退时点或 ack 边界。

### 4.2 查询：先确定新单身份，历史多行不随机挑选

修改 `MybatisRechargeOrderQueryAdapter.java` 及 mapper，保留 `FallbackRechargeOrderQueryAdapter.java` 的受理期 Redis 优先规则：

1. MySQL 路径先查 ext 的 `(org_code,out_order_sn)`。命中时按其唯一平台号查主单，再核对归属 1、商户、外单、直充类型。缺主单/不匹配返回技术冲突，不丢弃 ext 后回落旧单。
2. 无 ext 时读取最多两条符合商户/外单的旧直充候选：零条不存在；仅一条且归属 0 可沿用现有只读展示；多条为历史歧义，返回明确技术冲突，映射既有 SYSTEM_BUSY 并提示人工核查，不返回“订单不存在”。不能按成功优先或最新优先猜出可作废、可退款的唯一订单。
3. 候选中发现归属 1 但无 ext 也是事实异常，不按旧单展示。新单和旧单同号并存时，ext 定位的新单不会被旧单遮蔽。
4. 查单不触发供应商、资金或自动历史修复；作废仍须经过既有平台号/资金/券归属裁决，不因展示兼容获得新端操作资格。

异常使用应用层明确的订单身份冲突异常，基础设施抛出，API 查单/作废映射既有技术失败；高频日志使用既有 `LogStormGuard` 采样并带平台号等非敏感定位信息。数据库不可用也不能返回不存在。

### 4.3 创建前的持久占用检查

Redis 清理/过期后，不能仅依赖 Lua 空幂等键把历史号码再次受理。在验签及语法检查后、任何 Redis 受理/失败事实发布前，查询可信 MySQL 的外单占用；有任一旧/新直充主单或 ext 即返回既有 `ORDER_DUPLICATE`，不产生新预扣、订单或事件。这是占用检查，不从历史记录重建请求摘要或 Redis 订单。

现有 `RechargeOrderQueryPort` 同时服务Redis受理期查询、MySQL最终查询及组合查询，不能把它的所有实现都解释为可信持久读。新增应用层端口 `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/RechargeOrderExistencePort.java`，唯一方法 `boolean existsPersistedOrder(String orgCode, String outOrderSn)`；复用现有 `MybatisRechargeOrderQueryAdapter` 实现该接口和存在性SQL，不再建独立数据库wrapper。存在性不依赖“能否得到一个无歧义展示快照”，历史多行仍明确返回true。

修改基础设施 `TradeEngineInfrastructureConfiguration`、`TradeEngineRedisPendingAutoConfiguration` 的相应装配分支，让同一MySQL adapter同时提供持久占用能力；修改 bootstrap `V1CardApiConfiguration` 及API `V1CreateRechargeOrderUseCaseService` 注入、在prepare的验签/语法检查后调用。Redis-only未装配持久库时不得以默认false开启生产写入口，缺能力应启动拒绝或保持写入关闭；不改现有查询端口的函数式合同，不为所有Redis查询实现增加假数据库能力。接口、装配及用例测试一并更新，不建设跨系统请求服务。

主单占用及历史候选SQL使用 `out_orderId=? AND org_code=? AND product_type=1` 并分别limit 1/2，不能用收窄后全为NULL的旧订单生成列做历史占用查询。实测生产已有 `out_orderId_code_index(out_orderId,org_code)`，优先复用；ext复用现有复合唯一索引。隔离库用EXPLAIN确认索引命中、无全表扫描，不因新增一次占用查询顺带增加冗余索引。

此检查与并发新端落库间仍可能有窗口，最终由 scoped 主单唯一键及 ext 双唯一兜底；它不宣称对正在运行的旧端提供跨系统原子 claim。用户已排除跨端重发，不扩大工程范围。创建检查走已有 `tradeDbTaskExecutor`，不可阻塞 WebFlux event loop；记录新增数据库查询延迟，若生产容量不满足须在演练后调整，不能先声称性能无影响。

## 5. 环境升级、Nacos 与操作边界

### 5.1 三类环境

- **生产旧库首次接入：** 有旧表和历史重复、无新端 ext；加默认归属→收窄生成列→原 0003/0004→其余原迁移→终检。历史 96 组仍保留，不导入测试数据。
- **已完成旧版迁移的测试库：** 暂停该测试库所有新端写入者及消费/补偿任务；按本库 ext 回填→原全局索引改成 scoped→原 master 的已执行项跳过→终检。测试历史保持在测试环境。
- **中途失败或部分升级：** 不以 changeset 数量猜状态，逐项核对元数据和账本；目标状态已存在可续跑，半成品/异构/身份异常 HALT。完全空 schema 因缺旧业务表而拒绝，只有具备旧表结构但零业务数据的库才属于支持初始化路径。

### 5.2 配置准备清单（实施后才可发布）

本方案不新增业务 Nacos 开关，也不需要旧端为归属字段配置开关。需核对新端现有 Nacos namespace/group/DataId 中实际生效的 `spring.datasource.url`、用户名权限、`spring.liquibase.enabled`、`spring.liquibase.change-log`，以及环境独立的 Redis/MQ namespace/vhost/队列、供应商与商户回调。配置值不在本文放凭据。

推荐生产单独执行受控迁移，应用常态 `spring.liquibase.enabled=false`；迁移使用新入口，完成后以新配套制品启动业务。若选择启动期自动迁移，必须另行授权单一迁移实例，其他节点与业务任务保持停止；不能三个节点直接换库启动。迁移锁仅序列化 DDL，不会阻止应用复用测试 MQ 或执行旧积压任务。

特别注意现场当前 Nacos 是 `spring.liquibase.enabled=true`，不能在核验/授权前仅将 datasource 改为 `cardshop` 后重启。所有仍写旧版默认 0 的新引擎制品不得连接已升级库；新制品也不能提前连未升级库。新代码与新入口同批交付，schema 就绪后才允许业务启动。

### 5.3 生产执行前门禁

需单独取得生产变更授权后才执行：可恢复的 RDS 备份/PITR 水位与恢复演练；确认目标库名、迁移制品及 checksum；只读重查重复、目标结构和长事务；确认旧写入 SQL 显式列清单兼容新增末尾字段；记录表容量、磁盘、复制延迟及 DDL 锁预算；关闭新端接单并停止其写入者/消费者/补偿；隔离测试 Redis/MQ，不将测试消息送进生产。

对 STORED 表达式变更不预设 `LOCK=NONE` 或 INPLACE 必然可用。在生产近似数据量的隔离库确认实际算法、耗时、MDL 和旧端写入阻塞；若需要 COPY/阻塞写入，则申请业务低峰维护窗口及相应旧端停写范围。设置会话级锁等待预算并观测 metadata lock/连接池/复制延迟，达到演练批准阈值即停止推进、先核对现场。不能偷偷去掉在线 DDL 约束改成长时间锁表执行。

## 6. 验证矩阵与成功标准

### 6.1 永久回归

扩展现有 migration `RechargeD2ChangelogContractTest`，比较新入口的原迁移身份/checksum、顺序及最终唯一约束合同。H2/解析测试不能替代 MySQL DDL 验收。

扩展现有 infrastructure 的 `MybatisRechargeOrderPersistenceAdapterTest`、`MybatisBatchRechargeOrderPersistenceAdapterTest`、`MybatisRechargePersistOutboxAsyncWriterTest`、`MybatisRechargeOrderQueryAdapterTest`、`FallbackRechargeOrderQueryAdapterTest`，以及 API 创建/查单/作废用例测试。新增真实 MySQL 回归用例放 migration 的 `ProductionCompatibilityMigrationTest`（结构）及 infrastructure 的 `ProductionOrderIdentityMysqlTest`（事务）；沿用环境可用性条件启用，但发布验收必须实际运行，跳过不算通过。

重点断言可观察结果，不保留只验证 mapper 调用次数的测试：

- 旧库带拆单、补发、未明三种同号多行，迁移成功且历史业务字段逐条不变；旧端随后再写同号子单仍成功、标记仍为 0。
- 两个新平台号并发争同一外单，仅一个完整事实提交；失败者没有 ext、资金流水、派单或 delta。同平台号改变金额/商户/摘要必须失败，不修改胜者。
- 同单重放、批内相同键不同平台号、两个并发批次冲突，覆盖单条、全事务批写、facts-only+独立 writer；批冲突整批回滚。
- facts 提交后 writer 前进程退出，重投只补齐原事件；跳过已落库路径不会放过身份/摘要冲突；writer 不为不存在主单补事件。
- 新旧同号共存时查新单；无 ext 单条旧单保持只读展示；两条旧单不随机挑选；ext 指向不存在/错误主单返回技术失败而非查无订单。
- Redis 无该单幂等键时，已存在的旧/新号码仍不再次受理；持久库查询失败不写 Redis、不谎称不存在。既有同号重复请求响应仍为 ORDER_DUPLICATE。

### 6.2 MySQL 8.0.28 隔离演练

使用与生产一致的 READ-COMMITTED、utf8/general_ci、lower_case_table_names 和 SQL mode，使用脱敏 DDL/账本与合成业务数据，不复制真实券码或账户。分别运行：

1. 旧表零业务数据、生成列缺失。
2. 当前生产式：有旧生成列、无唯一索引、有三类历史重复、无 ext。
3. 已升级测试式：全局唯一索引和新端 ext 已存在，含校验失败单及不同业务状态。
4. 每个前置步骤成功后停止、重新执行；DDL 成功但账本缺该项的模拟；重复 update 无结构/业务漂移。
5. 归属 1 重复、孤儿 ext、主扩展身份错配、同名异构索引/字段、原0011检查范围的资金流水重复、缺必需旧表：均在预期守卫 HALT，不伪造账本通过。
6. 保留原 26 行卡管迁移账本及所有已执行新端 changeset 身份/checksum，原 master 未执行项正常完成；重复执行没有待执行变更或意外重建。
7. 旧端真实 mapper/代表性 SQL 持续插入、查询、更新兼容；大数据量测 DDL 时长、MDL、磁盘放大和复制影响。小夹具通过不当作大表在线能力证明。

运行定向测试后运行全量 `mvn -B test`（JDK 21/Maven 3.9+）。真实 MySQL 合同通过与全量结果分别记录，不能用静态文本断言替代事务竞争测试。更新 `docs/集成测试.md` 记录日期、配置、夹具范围、完整命令及证据，不提前填“通过”。此前旧方案的无重复库演练不证明本方案通过。

**成功标准：** 原 0003 不再因归属 0 历史重复阻断，0004/终检确认目标唯一约束有效；全部历史业务事实不变；新单并发不产生孤儿/重扣/错查；原迁移账本身份保持；失败可续跑；没有测试业务数据或消息进入生产。全部满足后仅可表述“该迁移及订单兼容合同通过验收”，不等于已授权或完成生产切换。

## 7. 失败、回退与恢复

- **尚未运行生产迁移：** 可撤回本地代码/制品计划，不改生产。
- **只执行部分前置、未开新端业务：** 停止推进，保留现场与账本；优先修正可重入步骤后续跑。归属回填后若未改列，仍按守卫校验；不能删除回填痕迹“重来”。
- **结构完成、未开新端业务：** 旧端可在兼容结构上继续运行；保留新增字段/表。不要用 rollbackCount 把混合旧账本中的原表/列删除。可回退应用入口配置到已批准状态，但不重新启用旧全局索引。
- **新端已产生生产订单：** 先停新接单，保留并收敛 pending、MQ、事实、派单、delta 和通知；只能使用理解归属字段的兼容制品处理存量。原旧引擎制品会漏写归属，不允许直接回退继续接单；“修复版上一版本”需先通过同样合同验收。
- **不能恢复全局唯一键：** 旧拆单/补发的历史重复仍在，旧生成表达式加全局唯一键必然可能失败。回退不以删除、改号这些订单作为条件，不把清除新增表当作安全回退。
- **不可恢复数据库故障：** PITR/备份恢复影响整个共享库及旧端新增交易，只能按单独灾备授权执行；普通迁移失败不能擅自整库回滚。记录故障时点、已提交 DDL、账本及业务水位后人工裁决。

新 changeset 的 rollback 需明确为保留数据的前向恢复策略，不提供看似成功却丢归属/订单证据的破坏性 SQL。自动回滚不能保证还原本次多 DDL；运维文档必须明确这一点。

## 8. 实施拆分与交付门禁

- [ ] **阶段一：固定合同。** 评审本方案及两份主设计修订；先写能复现 0003 阻断和错查/孤儿问题的失败用例，再实施。确认旧号码占用返回 ORDER_DUPLICATE、历史歧义返回技术失败的兼容语义，不擅改 V1 业务码。
- [ ] **阶段二：迁移。** 新增两个兼容资源和一个统一入口，更新 bootstrap/Maven 入口；完成元数据守卫、回填、分支 DDL、后置校验及账本身份回归。只有目标范围重复为零才允许原 0003/0004 继续。
- [ ] **阶段三：应用。** 实体/转换器/两个 insert→事实身份裁决→async writer→跳过路径→ext 优先查询→创建持久占用检查；每一步先观察回归在旧实现失败，再实现最小改动。保持原事务和日志降噪约定，不扩展余额或其他旧业务。
- [ ] **阶段四：隔离验收。** 运行 6.1/6.2，核对部署文档及可复现证据；不提交生产地址密码的脚本，不留下临时容器/夹具文件。
- [ ] **阶段五：交付生产变更单。** 给出确切制品版本、DDL 算法/耗时/锁预算、Nacos 现有键差异、单实例迁移命令、备份水位、停止/续跑条件与切换后核验 SQL。此阶段之后仍需取得生产写操作的明确授权，才可执行 DDL/配置/部署。

本轮只完成详细方案及设计引用，不执行上述实现或演练步骤，不作提交、推送或生产变更。

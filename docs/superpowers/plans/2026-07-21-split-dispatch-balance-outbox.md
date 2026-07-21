# Split Dispatch / Balance Outbox Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the mixed `recharge_outbox` with `recharge_dispatch_outbox` (supplier fulfillment) and `recharge_balance_delta_outbox` (balance flush), with no dual-write or old-table read path.

**Architecture:** Keep application ports (`RechargeOutboxPort`, `BalanceDeltaFlushOutboxPort`, `RechargeBalanceDeltaFlushPort`) but point adapters/mappers at the two new tables. Persist adapters write dispatch and balance rows separately (batch phases split). Flush worker scans the balance table, CAS-claims, aggregates by `org_code`, and updates `mc_ext_depart.balance` in one transaction per merchant group. Config for balance flush moves to `trade.engine.outbox.*`.

**Resolved review decisions:** dispatch worker scans only `SUPPLIER_DISPATCH` / `SUPPLIER_QUERY` / `INVALID_ORDER`; `BALANCE_RELEASE` and `CACHE_REPAIR` remain historical/reserved enum values and are not written or scanned in this split. Balance release is represented by `recharge_balance_delta_outbox(delta_type=RELEASE)`. Balance flush uses `idx_balance_delta_lease` for `PROCESSING` reclaim, `crc32(event_id)` as the initial residual shard predicate, and all balance increment + group `SUCCESS` marks share one transaction.

**Tech Stack:** Java 21, Spring Boot, MyBatis/MyBatis-Plus, Liquibase, Micrometer, JUnit 5, AssertJ.

**Spec:** `docs/superpowers/specs/2026-07-21-split-dispatch-balance-outbox-design.md`

**Canonical constraints:** `docs/交行直充业务系统详细设计.md` §8.3 / §9.1 / §9.2；`docs/交行直充业务系统高可用方案设计.md`（已随本变更同步）

---

## Files

### Already updated (working tree; do not re-author unless drift found)
- `docs/交行直充业务系统详细设计.md`
- `docs/交行直充业务系统高可用方案设计.md`
- `docs/superpowers/specs/2026-07-17-persist-batch-consumer-design.md`
- `docs/superpowers/specs/2026-07-21-split-dispatch-balance-outbox-design.md`

### Create
- `cardshop-trade-engine-infrastructure/.../entity/RechargeDispatchOutboxDO.java`
- `cardshop-trade-engine-infrastructure/.../entity/RechargeBalanceDeltaOutboxDO.java`
- `cardshop-trade-engine-infrastructure/.../mapper/RechargeDispatchOutboxMapper.java`
- `cardshop-trade-engine-infrastructure/.../mapper/RechargeBalanceDeltaOutboxMapper.java`
- Liquibase changeSets `0014` / `0015` in `db.changelog-master.yaml`

### Rename / replace (delete old after switch)
- `RechargeOutboxDO` → replaced by two DOs above
- `RechargeOutboxMapper` → split into two mappers
- `MybatisRechargeOutboxAdapter` keeps dispatch write/scan ports; balance enqueue moves to balance mapper
- `MybatisRechargeBalanceDeltaFlushAdapter` → balance table + org aggregation
- Phase metrics whitelist and batch adapter phases

### Modify (high-touch)
- `RechargeOrderPersistenceRowConverter` — `toDispatchOutbox` / `toBalanceDeltaFlushOutbox` return new DOs
- `MybatisRechargeOrderPersistenceAdapter` / `MybatisBatchRechargeOrderPersistenceAdapter`
- `MybatisRechargeBalanceReconcileAdapter` — sum unfinished from balance table columns
- `TradeEngineProperties` — add nested `Outbox`
- `TradeEngineMybatisPersistenceAutoConfiguration` / `TradeEngineFulfillmentAutoConfiguration` / Redis pending / observability wiring
- `TradeEngineObservabilityMetrics` + snapshot provider gauges
- Migration contract test + all outbox-related unit tests
- Comments that still say「9.2 作废设计」for balance flush →「9.2 余额 Delta 刷账 Outbox」
- `cardshop-trade-engine-bootstrap/src/main/resources/application.yml`
- `deploy/nacos/cardshop-trade-engine-dev.example.yaml`
- `scripts/publish-dev-nacos.sh`
- `/Users/mac/Documents/codeSpace/frontend/cardshop-trade-engine-test-console/src/views/ObservationDashboard.vue`
- `/Users/mac/Documents/codeSpace/frontend/cardshop-trade-engine-test-console/src/utils/evidence.js`
- `/Users/mac/Documents/codeSpace/frontend/cardshop-trade-engine-test-console/README.md`
- `/Users/mac/Documents/codeSpace/frontend/cardshop-trade-engine-test-console/AGENTS.md`

---

## Tasks

### Task 1: Liquibase DDL `0014` / `0015`

**Files:**
- Modify: `cardshop-trade-engine-migration/src/main/resources/db/changelog/db.changelog-master.yaml`
- Modify: `cardshop-trade-engine-migration/src/test/java/.../RechargeD2ChangelogContractTest.java`

- [ ] **Step 1: Add failing contract assertions** for table names `recharge_dispatch_outbox` and `recharge_balance_delta_outbox`, unique keys `uk_dispatch_outbox_event_id` / `uk_balance_delta_event_id`, balance columns `delta_type` / `delta_amount` (no `payload` / `event_type` / `shard_key` on balance table), `idx_balance_delta_lease`, and parsed changeSet ids `0013` / `0014` / `0015`.

- [ ] **Step 2: Run contract test — expect FAIL**

```bash
mvn -pl cardshop-trade-engine-migration test -Dtest=RechargeD2ChangelogContractTest
```

- [ ] **Step 3: Append changeSets** (exact DDL from spec §5):

```yaml
  - changeSet:
      id: 0014-d2-create-recharge-dispatch-outbox
      author: codex
      changes:
        - sql:
            splitStatements: false
            stripComments: false
            sql: |
              create table if not exists recharge_dispatch_outbox (
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
              ) engine=InnoDB default charset=utf8mb4 comment='交易订单供应商履约可靠事件表'
      rollback:
        - sql:
            sql: |
              drop table if exists recharge_dispatch_outbox
  - changeSet:
      id: 0015-d2-create-recharge-balance-delta-outbox
      author: codex
      changes:
        - sql:
            splitStatements: false
            stripComments: false
            sql: |
              create table if not exists recharge_balance_delta_outbox (
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
              ) engine=InnoDB default charset=utf8mb4 comment='交易订单余额增量刷账事件表'
      rollback:
        - sql:
            sql: |
              drop table if exists recharge_balance_delta_outbox
```

Leave `0006` / `recharge_outbox` in place (legacy unused). Do **not** add drop/migrate changeSets.

- [ ] **Step 4: Re-run contract test — expect PASS**

- [ ] **Step 5: Commit** (only if user asked to commit; otherwise leave staged work uncommitted)

---

### Task 2: Domain DOs + Mappers

**Files:**
- Create: `RechargeDispatchOutboxDO.java` (`@TableName("recharge_dispatch_outbox")`) — same fields as old outbox DO; Chinese class comment cites 详细设计「9.1 派单 Outbox」
- Create: `RechargeBalanceDeltaOutboxDO.java` (`@TableName("recharge_balance_delta_outbox")`) — fields from balance DDL; cite 「9.2 余额 Delta 刷账 Outbox」
- Create: `RechargeDispatchOutboxMapper.java` — move dispatch SQL from `RechargeOutboxMapper`:
  - `insertIfAbsent` / `insertDispatchOutboxBatchIfAbsent`
  - `selectDueForShard` / `selectDueForShardByEventTypes` with active event types fixed to `SUPPLIER_DISPATCH`, `SUPPLIER_QUERY`, `INVALID_ORDER` only
  - `claimProcessing` / lease reclaim helpers used by dispatch worker
  - `selectObservabilityDispatchOutboxMetrics` (event_type + status)
  - Ownership exists-subquery on `recharge_order_ext.persist_status = 'DB_PERSISTED'` unchanged
- Create: `RechargeBalanceDeltaOutboxMapper.java`:
  - `insertIfAbsent` / `insertBalanceDeltaBatchIfAbsent` (columns: no payload/event_type/shard_key)
  - `selectDueForFlush(shardTotal, shardIndex, limit, now)` — due by status/next_retry_time plus `PROCESSING` lease recovery by locked_until; shard by `crc32(event_id)` as a residual predicate; still require ownership exists subquery
  - `claimProcessing`
  - `sumUnfinishedBalanceDelta(orgCode)` — `sum(delta_amount)` where status <> SUCCESS (+ ownership)
  - `selectObservabilityBalanceDeltaMetrics(now)` — group by status only
- Delete: `RechargeOutboxDO.java`, `RechargeOutboxMapper.java` after callers switched
- Update: `MapperContractTest` for new SQL strings / table names
- Do **not** add `RechargeBalanceDeltaEvent` domain record in this implementation. The detailed-design record is a field-contract view; infrastructure may operate directly on `RechargeBalanceDeltaOutboxDO`.

- [ ] **Step 1: Write failing mapper contract tests** asserting SQL targets new tables and balance insert lists `delta_type`,`delta_amount` not `payload`.

- [ ] **Step 2: Implement DOs + mappers; remove old mapper usage from compile path.**

- [ ] **Step 3: Run**

```bash
mvn -pl cardshop-trade-engine-infrastructure test -Dtest=MapperContractTest
```

Expected: PASS

---

### Task 3: Config `trade.engine.outbox.*`

**Files:**
- Modify: `TradeEngineProperties.java` — add:

```java
/**
 * 余额 Delta 刷账 outbox 运行参数；参见详细设计「9.2 余额 Delta 刷账 Outbox」。
 */
private final Outbox outbox = new Outbox();

public Outbox getOutbox() {
    return outbox;
}

public static class Outbox {
    private int balanceDeltaFlushBatchSize = 500;
    private int balanceDeltaClaimLeaseSeconds = 120;
    /** 逗号分隔退避秒数，默认 5,30,120,600 */
    private String balanceDeltaRetryBackoffSeconds = "5,30,120,600";

    public List<Duration> balanceDeltaRetryBackoffDurations() {
        // parse CSV → List<Duration>; empty/invalid → default sequence
    }
    // getters/setters with kebab binding via relaxed Spring names
}
```

- Change `MybatisRechargeBalanceDeltaFlushAdapter` constructor from `(claimLease, retryBackoff)` to `(claimLease, retryBackoffSequence, defaultBatchSize)` and wire construction to use `properties.getOutbox()` (job limit may still pass `limit`; when limit <=0 use config batch size).
- Keep `trade.engine.fulfillment.outbox-*` for **dispatch** worker only.
- Add/adjust `TradeEnginePropertiesTest` (or equivalent) for defaults and CSV parse.
- Add `trade.engine.outbox` defaults to `application.yml`, `deploy/nacos/cardshop-trade-engine-dev.example.yaml`, and `scripts/publish-dev-nacos.sh`. Use environment variables `TRADE_ENGINE_OUTBOX_BALANCE_DELTA_FLUSH_BATCH_SIZE`, `TRADE_ENGINE_OUTBOX_BALANCE_DELTA_CLAIM_LEASE_SECONDS`, and `TRADE_ENGINE_OUTBOX_BALANCE_DELTA_RETRY_BACKOFF_SECONDS` in generated Nacos YAML.

- [ ] **Step 1: Failing property binding test** for defaults 500 / 120 / `5,30,120,600`.

- [ ] **Step 2: Implement nested `Outbox` + wiring.**

- [ ] **Step 3: Run properties test — PASS**

---

### Task 4: Persist path — write two tables + split phases

**Files:**
- Modify: `RechargeOrderPersistenceRowConverter`
  - `toDispatchOutbox` → `RechargeDispatchOutboxDO`; `event_id=dispatch-{orderId}`; `shard_key=event_id`
  - `toBalanceDeltaFlushOutbox` → `RechargeBalanceDeltaOutboxDO`; `event_id=balance-flush-{orderId}`; `delta_type=PRE_DEDUCT`; `delta_amount=-orderPrice` (same sign semantics as today); **no JSON payload**
- Modify: `MybatisRechargeOrderPersistenceAdapter` — insert dispatch via dispatch mapper; insert balance via balance mapper when `balanceReserved`
  - Split `RechargeOutboxPersistWriter` into dispatch and balance writers with concrete DO types.
- Modify: `MybatisBatchRechargeOrderPersistenceAdapter`
  - Phases: `PHASE_RECHARGE_DISPATCH_OUTBOX = "recharge_dispatch_outbox"`, `PHASE_RECHARGE_BALANCE_DELTA_OUTBOX = "recharge_balance_delta_outbox"`
  - Remove `PHASE_RECHARGE_OUTBOX`
  - Separate writers / multi-row inserts; order: `mc_order → ext → depart_record → dispatch → balance`
  - Writer interfaces: `insertDispatchOutboxBatchIfAbsent` / `insertBalanceDeltaBatchIfAbsent`
- Modify: `TradeEngineObservabilityMetrics.PERSIST_BATCH_MYSQL_PHASES` whitelist accordingly
- Update tests:
  - `MybatisRechargeOrderPersistenceAdapterTest`
  - `MybatisBatchRechargeOrderPersistenceAdapterTest` (assert both phase names recorded)
  - `TradeEngineObservabilityMetricsTest`

- [ ] **Step 1: Update failing tests** expecting table/phase names and balance row fields without payload.

- [ ] **Step 2: Implement converter + single + batch adapters + metrics whitelist.**

- [ ] **Step 3: Run**

```bash
mvn -pl cardshop-trade-engine-infrastructure test -Dtest=MybatisRechargeOrderPersistenceAdapterTest,MybatisBatchRechargeOrderPersistenceAdapterTest,TradeEngineObservabilityMetricsTest
```

Expected: PASS

---

### Task 5: Dispatch adapter / worker stay on dispatch table

**Files:**
- Modify: `MybatisRechargeOutboxAdapter` (keep class name or rename to `MybatisRechargeDispatchOutboxAdapter` if compile churn is acceptable; prefer **rename** for clarity)
  - Implements `RechargeOutboxPort`, `SupplierQueryOutboxPort`, `InvalidRechargeOrderOutboxPort` only against dispatch mapper
  - Remove `BalanceDeltaFlushOutboxPort` from this class
- Create or modify: balance enqueue adapter implementing `BalanceDeltaFlushOutboxPort` using balance mapper:

```java
// event_id rules (unchanged):
// PRE_DEDUCT persist path: balance-flush-{orderId}
// RELEASE via port: balance-flush-release-{orderId}
outbox.setDeltaType(normalizedDeltaType);
outbox.setDeltaAmount(command.deltaAmount());
// no payload / event_type / shard_key
```

- Update Spring bean wiring so `BalanceDeltaFlushOutboxPort` bean is the balance adapter (can be same class as flush adapter or a small `MybatisBalanceDeltaOutboxWriteAdapter`).
- Normalize `deltaType` to the closed set `PRE_DEDUCT` / `RELEASE`; blank port input defaults to `RELEASE`. Do not retain the old generic `balance-flush-{type}-{orderId}` event id rule.
- Comments: cite 「9.1」for dispatch; 「9.2 余额 Delta 刷账 Outbox」for balance enqueue (fix old「9.2 作废」refs on balance flush).
- `RechargeOutboxWorkerService` remains scanning active dispatch events only (`SUPPLIER_DISPATCH`, `SUPPLIER_QUERY`, `INVALID_ORDER`) — update comments; no `BALANCE_DELTA_FLUSH`, `BALANCE_RELEASE`, or `CACHE_REPAIR` handling.
- Update: `MybatisRechargeOutboxAdapterTest` → dispatch-only; add balance enqueue test on new adapter.

- [ ] **Step 1: Failing tests for enqueue targets.**

- [ ] **Step 2: Split adapters + auto-config.**

- [ ] **Step 3: Run outbox adapter + worker tests — PASS**

```bash
mvn -pl cardshop-trade-engine-infrastructure,cardshop-trade-engine-application test -Dtest=MybatisRechargeOutboxAdapterTest,RechargeOutboxWorkerServiceTest
```

---

### Task 6: Balance flush — org aggregation + new table

**Files:**
- Rewrite: `MybatisRechargeBalanceDeltaFlushAdapter`

**Target algorithm (spec §7.2):**

1. `selectDue` with `limit = max(1, configured batch size or job limit)`
2. For each candidate: if retry exhausted → `MANUAL_REQUIRED`; else CAS `claimProcessing`
3. Group successfully claimed events by `org_code`
4. For each org group, one transaction:
   - `incrementBalance(orgCode, sum(delta_amount))` (must update 1 row)
   - mark each claimed event `SUCCESS` with version CAS
   - if any `markSuccess` returns 0, throw and roll back the whole org transaction
5. On group failure: outside the failed transaction, mark each claimed event in the group `FAILED` with next retry from backoff sequence indexed by `retry_count` (clamp to last), or `MANUAL_REQUIRED` if next retry would exhaust. If failure marking also fails, leave the event in `PROCESSING`; `idx_balance_delta_lease` + locked_until reclaim will retry it later.

```java
Duration backoff = backoffs.get(Math.min(retryCount, backoffs.size() - 1));
```

- Remove JSON payload parsing; read `deltaAmount` from DO column.
- Reuse or repurpose `TransactionRunner` so it wraps one org group at a time; remove it only if tests still prove the same transaction boundary.
- Update: `MybatisRechargeBalanceDeltaFlushAdapterTest` — cover:
  - two events same org → one balance increment with sum
  - claim miss → skipped
  - `markSuccess` CAS miss → org transaction rolls back balance increment
  - failure → FAILED + backoff from sequence
  - exhausted → MANUAL_REQUIRED
- Update: `MybatisRechargeBalanceReconcileAdapter` + test to use `sumUnfinishedBalanceDelta` on balance mapper
- Update observability snapshot provider constructor and auto-config to inject both `RechargeDispatchOutboxMapper` and `RechargeBalanceDeltaOutboxMapper`.
- Add new pending gauges as the target metric contract:
  - `trade.engine.dispatch.outbox.pending.total{event_type,status}`
  - `trade.engine.balance.delta.pending.total{status}`
  - Do **not** add `org_code` as a Prometheus label. If old `trade.engine.outbox.pending` remains temporarily, it may only represent a dispatch compatibility view and must not be used in new pressure-test conclusions.

- [ ] **Step 1: Failing aggregation test**

- [ ] **Step 2: Implement flush + reconcile + gauges**

- [ ] **Step 3: Run**

```bash
mvn -pl cardshop-trade-engine-infrastructure test -Dtest=MybatisRechargeBalanceDeltaFlushAdapterTest,MybatisRechargeBalanceReconcileAdapterTest,RedisMysqlTradeEngineObservabilitySnapshotProviderTest
```

Expected: PASS

---

### Task 7: Wiring, jobs, bootstrap smoke

**Files:**
- `TradeEngineMybatisPersistenceAutoConfiguration` — register both mappers/adapters
- `TradeEngineRedisPendingAutoConfiguration` — reconcile uses balance mapper
- `TradeEngineFulfillmentAutoConfiguration` — comments cite 9.1 / 9.2 correctly; balance flush bean uses `Outbox` props
- `TradeEngineObservabilityAutoConfiguration` — snapshot provider uses both dispatch and balance mappers
- Job/bootstrap tests that reference outbox mapper types — update imports
- Grep purge: no production code may still reference `recharge_outbox` table name except comments marking legacy / migration `0006`

- [ ] **Step 1: Grep for leftover `recharge_outbox` / `RechargeOutboxMapper` / `RechargeOutboxDO` in `src/main`**

```bash
rg -n "recharge_outbox|RechargeOutboxMapper|RechargeOutboxDO" cardshop-trade-engine-*/src/main --glob '!**/db.changelog-master.yaml'
```

Allowed leftovers: changelog `0006`, explicit “legacy unused” comments, docs.

- [ ] **Step 2: Fix leftovers**

- [ ] **Step 3: Module tests**

```bash
mvn -pl cardshop-trade-engine-bootstrap -am test -Dtest=TradeEngineReliabilityConfigurationTest,TradeEngineXxlJobConfigurationTest,ManualRechargeJobControllerTest,TradeEngineMybatisPersistenceAutoConfigurationTest,TradeEngineInfrastructureConfigurationTest,TradeEngineRedisPendingAutoConfigurationTest
```

---

### Task 8: Full verification

- [ ] **Step 1: Run focused suite**

```bash
mvn -pl cardshop-trade-engine-migration,cardshop-trade-engine-application,cardshop-trade-engine-infrastructure,cardshop-trade-engine-job,cardshop-trade-engine-bootstrap -am test -Dtest=RechargeD2ChangelogContractTest,MapperContractTest,MybatisRechargeOrderPersistenceAdapterTest,MybatisBatchRechargeOrderPersistenceAdapterTest,MybatisRechargeOutboxAdapterTest,MybatisRechargeBalanceDeltaFlushAdapterTest,MybatisRechargeBalanceReconcileAdapterTest,RechargeOutboxWorkerServiceTest,TradeEngineObservabilityMetricsTest,RechargeCompensationJobsTest,TradeEngineReliabilityConfigurationTest
```

- [ ] **Step 2: If green, broader**

```bash
mvn -pl cardshop-trade-engine-bootstrap -am test
```

- [ ] **Step 3: Confirm** no dual-write, no old-table read in adapters; phases whitelist matches design; comments cite 8.3 / 9.1 / 9.2.

---

### Task 9: Cross-project test-console observation update

**Files:**
- `/Users/mac/Documents/codeSpace/frontend/cardshop-trade-engine-test-console/src/views/ObservationDashboard.vue`
- `/Users/mac/Documents/codeSpace/frontend/cardshop-trade-engine-test-console/src/utils/evidence.js`
- `/Users/mac/Documents/codeSpace/frontend/cardshop-trade-engine-test-console/README.md`
- `/Users/mac/Documents/codeSpace/frontend/cardshop-trade-engine-test-console/AGENTS.md`

- [ ] **Step 1: Update observation SQL templates** from old `recharge_outbox` to both `recharge_dispatch_outbox` and `recharge_balance_delta_outbox`. Use `order_id` / `out_order_sn` columns, not the old `aggregate_id` template.

- [ ] **Step 2: Update evidence parsing** so MySQL evidence accepts both split tables. The old “MySQL 四表” wording becomes the split-table evidence set: `mc_order`, `recharge_order_ext`, `mc_depart_record`, `recharge_dispatch_outbox`, and `recharge_balance_delta_outbox`.

- [ ] **Step 3: Update frontend README/AGENTS** to mention split outbox observation, new Prometheus metric names, and the no-old-table/no-migration assumption.

- [ ] **Step 4: Run frontend unit/build checks**

```bash
cd /Users/mac/Documents/codeSpace/frontend/cardshop-trade-engine-test-console
npm run test:unit
npm run build
```

---

## Self-Review

| Spec requirement | Task |
| --- | --- |
| DDL two tables, no prod migration of old data | Task 1 |
| Persist writes dispatch + balance separately; batch phases split | Task 4 |
| Worker scans dispatch only | Task 5 |
| Flush scans balance; org aggregate; CAS; mark SUCCESS | Task 6 |
| `trade.engine.outbox.*` config | Task 3 |
| Observability phase + pending metrics; no org_code Prometheus label | Task 4 + 6 + 9 |
| Comments cite 8.3 / 9.1 / 9.2 | Tasks 2–6 |
| Frontend observation templates no longer hard-code `recharge_outbox` | Task 9 |
| Docs already updated | pre-done |

**Placeholder scan:** none intentional.

**Type consistency:** `insertDispatchOutboxBatchIfAbsent` / `insertBalanceDeltaBatchIfAbsent` used in design §8.3 and Task 4; backoff is `List<Duration>` from CSV string property.

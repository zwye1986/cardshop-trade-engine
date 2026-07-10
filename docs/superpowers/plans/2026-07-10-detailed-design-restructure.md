# Detailed Design Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Rewrite `docs/交行直充业务系统详细设计.md` into a layered/module-based detailed design and synchronize field definitions with current code, Redis scripts, configuration, and Liquibase comments.

**Architecture:** Keep a single canonical detailed-design Markdown file, organized by system layer. Move the old implementation appendix content into the relevant layer chapters, add a field dictionary chapter, and keep execution evidence in `docs/集成测试.md` instead of duplicating it here.

**Tech Stack:** Markdown, Java source as reference, Liquibase YAML as DDL source, Redis Lua scripts as runtime field source, shell verification with `rg`, `git diff --check`, and optional Maven package verification.

---

### Task 1: Capture Current Structure And Source Field Inventory

**Files:**
- Read: `docs/交行直充业务系统详细设计.md`
- Read: `docs/superpowers/specs/2026-07-10-detailed-design-restructure.md`
- Read: `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/dto/*.java`
- Read: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/*.java`
- Read: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/entity/*.java`
- Read: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineProperties.java`
- Read: `cardshop-trade-engine-migration/src/main/resources/db/changelog/db.changelog-master.yaml`
- Read: `cardshop-trade-engine-infrastructure/src/main/resources/redis/lua/*.lua`

- [x] **Step 1: Snapshot current heading structure**

Run:

```bash
rg -n "^(#|##|###|####) " docs/交行直充业务系统详细设计.md
```

Expected: output shows the current mixed structure, including the old `20. 实施级详细设计补充` heading.

- [x] **Step 2: List field source files**

Run:

```bash
rg --files cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/dto \
  cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge \
  cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/entity \
  cardshop-trade-engine-infrastructure/src/main/resources/redis/lua
```

Expected: output includes V1 DTOs, application records, persistence DOs, and Lua scripts referenced by the spec.

- [x] **Step 3: Confirm branch state**

Run:

```bash
git status --short --branch
```

Expected: current branch is `reliability/balance-release-compensation`; any existing changes are understood before editing.

### Task 2: Rewrite Detailed Design Into Layered Structure

**Files:**
- Modify: `docs/交行直充业务系统详细设计.md`

- [x] **Step 1: Replace the old document with the layered skeleton**

Use `apply_patch` to rewrite `docs/交行直充业务系统详细设计.md` with these top-level headings:

```markdown
# cardshop-trade-engine 系统详细设计

## 1. 文档说明、当前实现边界与术语
## 2. 系统模块与依赖关系
## 3. API/V1 接入层
## 4. Application 用例编排层
## 5. Domain 状态与业务规则
## 6. Redis 受理与缓存层
## 7. RabbitMQ 持久化消息层
## 8. MySQL/Liquibase 持久化层
## 9. Outbox、供应商派单、作废与商户回调
## 10. XXL-Job 与 dev/test 人工任务入口
## 11. 配置中心、部署与健康检查
## 12. 观测、日志与告警
## 13. 测试与验收标准
## 14. 字段字典总表
## 15. 后续事项与当前不纳入范围的能力
```

Expected: no separate `## 20. 实施级详细设计补充` remains.

- [x] **Step 2: Populate chapters 1-5**

Add concise content for document scope, module responsibilities, V1 API flow, application use cases, and domain state rules. Include these code references in prose:

```text
V1RechargeOrderController
V1CreateRechargeOrderUseCaseService
PersistRechargeOrderConsumerService
RechargeOutboxWorkerService
LegacyOrderStatusCode
PersistStatus
OutboxEventType
TaskStatus
```

Expected: `rg -n "V1RechargeOrderController|PersistRechargeOrderConsumerService|LegacyOrderStatusCode|PersistStatus" docs/交行直充业务系统详细设计.md` finds all items.

- [x] **Step 3: Populate chapters 6-10**

Add Redis, RabbitMQ, MySQL, Outbox/Notify, and Job design sections. Include current D12 late-binding and auto-configuration constraints from the existing design. Include these code/table references:

```text
RechargeRedisKeyBuilder
RedisPendingPersistOrderAdapter
RabbitPersistRechargeOrderMessageListener
recharge_order_ext
recharge_outbox
recharge_notify_task
recharge_accept_anomaly
RechargeBalanceDeltaFlushJob
RechargeBalanceReconcileJob
RechargeCacheRepairJob
ManualRechargeJobController
```

Expected: each reference is searchable in the rewritten document.

- [x] **Step 4: Populate chapters 11-13**

Add configuration, deployment, observability, logging, testing, and acceptance sections. Keep integration evidence referenced to `docs/集成测试.md` rather than copied inline.

Expected: document includes `docs/集成测试.md`, `docs/集成测试方案.md`, and `TradeEngineProperties`.

### Task 3: Synchronize Field Dictionary With Code And DDL

**Files:**
- Modify: `docs/交行直充业务系统详细设计.md`

- [x] **Step 1: Add V1 DTO field tables**

In chapter 14, add tables for:

```text
V1RechargeCreateOrderRequest
V1RechargeOrderQueryRequest
V1RechargeOrderInvalidRequest
V1IssueTokenRequest
V1RechargeOrderResponse
```

Each row must include `JSON 字段`, `Java 字段`, `来源/含义`, `约束`, and `落点/参与逻辑`.

Expected: `rg -n "V1RechargeCreateOrderRequest|nonce_str|notify_url|order_status_desc" docs/交行直充业务系统详细设计.md` finds the synchronized fields.

- [x] **Step 2: Add application object field tables**

Add tables for:

```text
AcceptRechargeOrderCommand
PersistTradeOrderMessage
PersistOrderSnapshot
PendingPersistOrderSnapshot
RechargeOutboxEvent
RechargeNotifyTask
AcceptAnomalyRecord
```

Each row must include Java field, source, output target, and constraint.

Expected: `rg -n "PersistOrderSnapshot|mqPublishCount|dispatchAllowed|balanceReserved|RechargeOutboxEvent" docs/交行直充业务系统详细设计.md` finds documented fields.

- [x] **Step 3: Add MySQL table dictionary**

Add field tables for:

```text
mc_order
recharge_order_ext
recharge_outbox
recharge_notify_task
recharge_accept_anomaly
mc_order_supplier_flow
mc_depart_record
mc_ext_depart
mc_product
mc_depart_product
```

For new tables, use Liquibase COMMENT wording as the field meaning.

Expected: `rg -n "DDL COMMENT|request_hash_version|locked_until|redis_check_result|trade_engine_record_key" docs/交行直充业务系统详细设计.md` finds the table dictionary details.

- [x] **Step 4: Add Redis/MQ/config dictionaries**

Add tables for Redis keys/hash fields, MQ initial snapshot, outbox payload, notify payload, and `TradeEngineProperties` configuration groups.

Expected: `rg -n "recharge:\\{orgCode\\}:balance|persist:pending|initialOrderSnapshot|manual-job-enabled|wait-replicas" docs/交行直充业务系统详细设计.md` finds these definitions.

### Task 4: Verify Structure And Field Coverage

**Files:**
- Verify: `docs/交行直充业务系统详细设计.md`

- [x] **Step 1: Check Markdown whitespace**

Run:

```bash
git diff --check
```

Expected: no output and exit code 0.

- [x] **Step 2: Check top-level headings**

Run:

```bash
rg -n "^(## [0-9]+\\.|## 20\\.|实施级详细设计补充)" docs/交行直充业务系统详细设计.md
```

Expected: headings `## 1.` through `## 15.` appear; `## 20.` and `实施级详细设计补充` do not appear.

- [x] **Step 3: Check field coverage**

Run:

```bash
rg -n "V1RechargeCreateOrderRequest|PersistOrderSnapshot|recharge_order_ext|recharge_outbox|recharge_notify_task|recharge_accept_anomaly|RechargeRedisKeyBuilder|TradeEngineProperties|ManualRechargeJobController" docs/交行直充业务系统详细设计.md
```

Expected: all listed terms appear in meaningful sections, not only in a file list.

- [x] **Step 4: Check sensitive data safety**

Run:

```bash
rg -n "(password|secret|token|access-token|jwt-secret|app_secret|aes_key)" docs/交行直充业务系统详细设计.md
```

Expected: matches are explanatory field names or placeholders only; no real secret values appear.

### Task 5: Commit The Documentation Restructure

**Files:**
- Modify: `docs/交行直充业务系统详细设计.md`
- Modify: `docs/superpowers/plans/2026-07-10-detailed-design-restructure.md`

- [x] **Step 1: Review diff summary**

Run:

```bash
git diff --stat
git diff -- docs/交行直充业务系统详细设计.md | sed -n '1,220p'
```

Expected: diff shows the detailed design rewrite and plan updates only.

- [x] **Step 2: Commit changes**

Run:

```bash
git add docs/交行直充业务系统详细设计.md docs/superpowers/plans/2026-07-10-detailed-design-restructure.md
git commit -m "docs: restructure detailed design by module"
```

Expected: commit succeeds.

- [x] **Step 3: Report remaining state**

Run:

```bash
git status --short --branch
git log -2 --oneline
```

Expected: worktree is clean or only known unrelated files remain; latest commit is the documentation restructure.

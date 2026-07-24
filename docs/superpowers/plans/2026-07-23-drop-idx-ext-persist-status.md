# Drop idx_ext_persist_status Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 通过 Liquibase 删除 `recharge_order_ext.idx_ext_persist_status`，并同步两份主设计文档与迁移契约测试。

**Architecture:** 保留 `0005` 历史 CREATE 不变；追加 `0016` DROP INDEX。契约测试断言 DROP 存在且 parse 含 `0016`。业务代码零改动。

**Tech Stack:** Liquibase YAML、JUnit 5、AssertJ、Markdown 设计文档。

**Spec:** `docs/superpowers/specs/2026-07-23-drop-idx-ext-persist-status-design.md`

---

## Files

- Modify: `cardshop-trade-engine-migration/src/main/resources/db/changelog/db.changelog-master.yaml`
- Modify: `cardshop-trade-engine-migration/src/test/java/com/cardshop/trade/engine/migration/RechargeD2ChangelogContractTest.java`
- Modify: `docs/交行直充业务系统高可用方案设计.md`
- Modify: `docs/交行直充业务系统详细设计.md`

---

### Task 1: 契约测试先改（TDD）

**Files:**
- Modify: `cardshop-trade-engine-migration/src/test/java/com/cardshop/trade/engine/migration/RechargeD2ChangelogContractTest.java`

- [x] **Step 1: 更新断言**

在 `changelogKeepsTechnicalStateOutOfMcOrderAndIndexesRecoveryTables` 中：

- 保留 `.contains("key idx_ext_persist_status (persist_status, updated_at)")`（历史 0005 仍会出现）
- 新增 `.contains("0016-d2-drop-recharge-order-ext-persist-status-index")`
- 新增 `.contains("DROP INDEX idx_ext_persist_status")`

在 `changelogCanBeParsedByLiquibase` 的 id 列表末尾追加：

```java
"0016-d2-drop-recharge-order-ext-persist-status-index"
```

- [x] **Step 2: 跑测试确认失败**

Inline 执行时与 Task 2 合并落地；未单独保留红灯现场。

---

### Task 2: 追加 Liquibase 0016

**Files:**
- Modify: `cardshop-trade-engine-migration/src/main/resources/db/changelog/db.changelog-master.yaml`（文件末尾、0015 之后）

- [x] **Step 1: 追加 changeSet**

```yaml
  # 0016：删除低基数二级索引 idx_ext_persist_status，降低批写 ODKU 热点。
  # 对应 docs/superpowers/specs/2026-07-23-drop-idx-ext-persist-status-design.md
  - changeSet:
      id: 0016-d2-drop-recharge-order-ext-persist-status-index
      author: codex
      changes:
        - sql:
            sql: |
              ALTER TABLE recharge_order_ext
                DROP INDEX idx_ext_persist_status
      rollback:
        - sql:
            sql: |
              ALTER TABLE recharge_order_ext
                ADD INDEX idx_ext_persist_status (persist_status, updated_at)
```

- [x] **Step 2: 跑测试确认通过**

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home mvn -pl cardshop-trade-engine-migration test
```

Expected: PASS

---

### Task 3: 同步主设计文档

**Files:**
- Modify: `docs/交行直充业务系统高可用方案设计.md`（建议约束代码块，约 811–818 行）
- Modify: `docs/交行直充业务系统详细设计.md`（14.3 表字段，`persist_status` 行）

- [x] **Step 1: 高可用方案**

将建议约束改为：

```sql
unique key uk_ext_order_id (order_id)
unique key uk_ext_org_out_order (org_code, out_order_sn)
key idx_ext_msg_id (persist_msg_id)
```

在约束块后增加说明：曾规划的 `idx_ext_persist_status (persist_status, updated_at)` 因低基数 + 高频批写热点，由 Liquibase `0016` 删除；正式查询与补偿不依赖该索引。

- [x] **Step 2: 详细设计**

将 `persist_status` 行的「索引/约束」从 `` `idx_ext_persist_status` `` 改为 `无二级索引（低基数热点已删）`。

---

### Task 4: 验证

- [x] **Step 1: 再跑 migration 模块测试**

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home mvn -pl cardshop-trade-engine-migration test
```

Expected: PASS

- [ ] **Step 2: 提交（仅当用户明确要求时）**

本仓库约定：未收到用户 commit 指令前不自动提交。

# Outbox 履约并行与公平调度 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 拆分 DISPATCH/QUERY XXL Job、Job 内并行、账号公平+频控预检，并暴露 Outbox 双年龄指标。

**Architecture:** Application Worker 按 eventTypes overfetch → created_at 序公平过滤 → 串行 claim → 线程池并行履约；Bootstrap 注册三 Handler；观测 SQL 增加 due 年龄。

**Tech Stack:** Java 21, Spring, XXL-Job, Redis, MyBatis, Micrometer

**Spec:** `docs/superpowers/specs/2026-07-22-outbox-dispatch-parallel-fair-design.md`

---

### Task 1: BatchResult.deferred + Frequency peek + Settings

**Files:**
- Modify: `RechargeOutboxBatchResult.java`, `DirectRechargeFrequencyPort.java`, `RedisDirectRechargeFrequencyAdapter.java`, `RechargeOutboxWorkerSettings.java`, `TradeEngineProperties.Fulfillment`

- [x] Add `deferred` to batch result + Outcome.DEFERRED
- [x] Add `Optional<String> currentOwner(org, account)` on frequency port + Redis GET
- [x] Settings: `outboxParallelism`, `outboxScanOverfetchFactor`
- [x] Properties defaults 8 / 3
- [x] Unit tests for settings validation and batch result

### Task 2: Port selectDue by eventTypes + mapper default DISPATCH for PublishJob path

**Files:**
- Modify: `RechargeOutboxPort`, `MybatisRechargeDispatchOutboxAdapter`, fake ports in tests

- [x] `selectDueForShard(..., List<OutboxEventType>)`
- [x] No-arg overload → DISPATCH only (adapter)
- [x] Mapper `selectDueForShard` default list → DISPATCH only (align production scan)

### Task 3: Worker fair filter + parallel execute

**Files:**
- Modify: `RechargeOutboxWorkerService.java`, `RechargeOutboxWorkerServiceTest.java`

- [x] `processDue(..., eventTypes, parallelism)`
- [x] 4-arg → DISPATCH + settings.parallelism
- [x] Overfetch, account dedupe, frequency deferred, claim, parallel with account lock
- [x] Tests: dedupe, deferred, eventTypes filter, parallel same-account serial

### Task 4: XXL Jobs + Handlers

**Files:**
- Create: `RechargeSupplierDispatchJob.java`, `RechargeSupplierQueryJob.java`（组合委托，不继承 PublishJob）
- Modify: `RechargeOutboxPublishJob`, `TradeEngineXxlJobHandlers`, config, tests, `docs/部署手册.md`

- [x] 新增 Dispatch/Query Job 门面（组合委托）
- [x] 注册三 Handler；Publish 默认 DISPATCH；日志含 deferred/parallelism/eventTypes
- [x] 部署手册任务表更新

### Task 5: Observability dual age

**Files:**
- Modify: mapper metrics SQL, `CountAge` / snapshot provider, `TradeEngineObservabilityMetrics`

- [x] SQL `max_age_since_due_seconds`
- [x] Gauge `max.age.since.created` / `max.age.since.due`（保留旧 `max.age.seconds`）

### Task 6: Design docs sync + compile tests

- [x] Update 详细设计 9.1/10.1, 高可用 17.2/指标
- [x] `mvn -pl application,job,infrastructure,bootstrap -am` 聚焦测试通过

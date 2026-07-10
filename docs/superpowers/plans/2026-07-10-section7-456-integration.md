# Section 7 Items 4-6 Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete `docs/开发进度.md` section 7 items 4, 5, and 6 by making supplier dispatch, invalid/notify, and balance flush/reconcile/cache repair executable in dev/test and traceable in integration documents.

**Architecture:** Keep business orchestration in application/job facades and expose only fixed dev/test manual endpoints from bootstrap. Reuse existing MyBatis, Redis, supplier HTTP, and merchant notify adapters; add only the missing execution bridge and minimal real ports needed for dev/test evidence. Integration documents remain the source of truth for test cases and execution records.

**Tech Stack:** Java 21, Spring Boot WebFlux, MyBatis-Plus, Redis session abstractions, RabbitMQ/outbox, JUnit 5, AssertJ, Maven.

---

## File Structure

- Modify `cardshop-trade-engine-bootstrap/src/main/java/com/cardshop/trade/engine/bootstrap/admin/ManualRechargeJobController.java`: add fixed dev/test manual endpoints for outbox publish, merchant notify retry, and balance delta flush.
- Modify `cardshop-trade-engine-bootstrap/src/test/java/com/cardshop/trade/engine/bootstrap/admin/ManualRechargeJobControllerTest.java`: add RED/GREEN tests proving endpoint registration, loopback enforcement, parameter validation, response fields, and service delegation.
- Modify `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/RechargeNotifyBatchResult.java`: make factory/accessor helpers public only if controller response mapping needs them.
- Create `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeBalanceReconcileAdapter.java`: minimal MySQL-side balance reconciliation port that scans test merchant rows and reports observed differences without mutating money.
- Create `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeBalanceReconcileAdapterTest.java`: tests for shard filtering, outbox pending delta calculation, and non-mutating result counts.
- Create `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/redis/RedisRechargeCacheRepairAdapter.java`: minimal cache repair/preheat port for merchant balance baseline, guarded by repairing lock.
- Create `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/redis/RedisRechargeCacheRepairAdapterTest.java`: tests for lock acquisition, balance hash rewrite, and skipped result when lock is held.
- Modify `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineMybatisPersistenceAutoConfiguration.java`: register balance reconcile port when MyBatis is available.
- Modify `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineRedisPendingAutoConfiguration.java` or nearest Redis auto-configuration: register cache repair port when Redis session factory and MyBatis readers are available.
- Modify `docs/集成测试方案.md`: define SUP/INV/CB/BAL dev/test cases and evidence fields.
- Modify `docs/集成测试.md`: add execution-record templates for section 7 items 4, 5, and 6.
- Modify `docs/开发进度.md`: keep items open until dev/test evidence exists; update statuses to show code readiness and exact next commands.
- Modify `docs/跨项目联调记忆.md`: record backend endpoints and test-console follow-up needs.

## Task 1: Manual Dev/Test Job Endpoints

**Files:**
- Modify: `cardshop-trade-engine-bootstrap/src/main/java/com/cardshop/trade/engine/bootstrap/admin/ManualRechargeJobController.java`
- Modify: `cardshop-trade-engine-bootstrap/src/test/java/com/cardshop/trade/engine/bootstrap/admin/ManualRechargeJobControllerTest.java`

- [ ] **Step 1: Write failing controller tests**

Add tests that construct the controller with recording `RechargeOutboxPublishJob`, `RechargeBalanceDeltaFlushJob`, and `RechargeNotifyWorkerService` collaborators, then call:

```text
POST /internal/integration/recharge/jobs/outbox-publish/run-once
POST /internal/integration/recharge/jobs/notify-retry/run-once
POST /internal/integration/recharge/jobs/balance-delta-flush/run-once
```

Expected behavior:
- non-loopback request returns `403` and does not call the job.
- invalid shard/limit/worker parameters throw `IllegalArgumentException`.
- valid loopback request returns status `OK` with scanned/succeeded/failed/skipped counts.

- [ ] **Step 2: Run RED verification**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH mvn -pl cardshop-trade-engine-bootstrap -am -Dtest=ManualRechargeJobControllerTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the new endpoints and constructor dependencies do not exist.

- [ ] **Step 3: Implement minimal controller endpoints**

Add optional constructor dependencies for fixed jobs/services, keep current Redis pending endpoint unchanged, reuse loopback and parameter validation, and return simple JSON response objects. Do not add generic bean invocation.

- [ ] **Step 4: Run GREEN verification**

Run the same Maven command. Expected: all `ManualRechargeJobControllerTest` tests pass.

## Task 2: Minimal Balance Reconcile Port

**Files:**
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeBalanceReconcileAdapter.java`
- Create: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeBalanceReconcileAdapterTest.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineMybatisPersistenceAutoConfiguration.java`

- [ ] **Step 1: Write failing adapter tests**

Test desired behavior with fake operations:
- scans org rows by shard and limit.
- counts success when MySQL balance equals baseline plus successful `BALANCE_DELTA_FLUSH` facts.
- counts failed when difference is detected and logs context.
- never updates `mc_ext_depart.balance`.

- [ ] **Step 2: Run RED verification**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH mvn -pl cardshop-trade-engine-infrastructure -am -Dtest=MybatisRechargeBalanceReconcileAdapterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the adapter does not exist.

- [ ] **Step 3: Implement minimal adapter**

Implement `RechargeBalanceReconcilePort` with Chinese comments referencing `docs/交行直充业务系统高可用方案设计.md` section 17.5. Keep it non-mutating in this phase; it returns counts and writes sampled error logs for differences.

- [ ] **Step 4: Register bean**

Register the port after MyBatis auto-configuration when `SqlSessionFactory` exists and no custom `RechargeBalanceReconcilePort` bean exists.

- [ ] **Step 5: Run GREEN verification**

Run the same Maven command. Expected: adapter tests pass.

## Task 3: Minimal Cache Repair/Preheat Port

**Files:**
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/redis/RedisRechargeCacheRepairAdapter.java`
- Create: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/redis/RedisRechargeCacheRepairAdapterTest.java`
- Modify: Redis infrastructure auto-configuration that already creates Redis pending/reliability adapters.

- [ ] **Step 1: Write failing Redis adapter tests**

Test desired behavior against fake Redis session operations:
- when repairing lock is acquired, writes balance hash fields `available`, `frozen`, and `version` for one org.
- when lock is held by another worker, returns skipped count.
- releases only its own lock.

- [ ] **Step 2: Run RED verification**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH mvn -pl cardshop-trade-engine-infrastructure -am -Dtest=RedisRechargeCacheRepairAdapterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the adapter does not exist.

- [ ] **Step 3: Implement minimal adapter**

Implement `RechargeCacheRepairPort` for dev/test baseline preheat only: merchant balance hash rewrite under `balance:repairing_lock`. Product config repair can remain outside this first implementation if no current mapper exposes the needed full product source fields; document that as a follow-up, not as completed.

- [ ] **Step 4: Register bean**

Register the port only when Redis session factory and the merchant balance reader are available. Do not register a noop bean globally.

- [ ] **Step 5: Run GREEN verification**

Run the same Maven command. Expected: Redis adapter tests pass.

## Task 4: Documentation And Integration Records

**Files:**
- Modify: `docs/集成测试方案.md`
- Modify: `docs/集成测试.md`
- Modify: `docs/开发进度.md`
- Modify: `docs/跨项目联调记忆.md`

- [ ] **Step 1: Update test plan**

Add precise dev/test-only cases:
- `SUP-001`: supplier dispatch success via mock supplier.
- `SUP-002`: supplier timeout/5xx retry.
- `INV-001`: invalid order creates outbox and supplier invalid succeeds.
- `CB-001`: merchant notify success.
- `CB-002`: merchant notify non-200 retry.
- `BAL-001`: balance delta flush updates `mc_ext_depart.balance`.
- `BAL-002`: balance reconcile reports no difference.
- `CACHE-001`: merchant balance baseline preheat under repairing lock.

- [ ] **Step 2: Update execution record templates**

Add empty sections in `docs/集成测试.md` with command, order id, outbox event id, mock supplier URL, callback URL, MySQL evidence, Redis evidence, and conclusion fields.

- [ ] **Step 3: Update progress ledger**

Do not mark items 4/5/6 as fully verified until dev/test evidence is pasted. Mark code/test readiness and list the exact remaining dev/test run sequence.

## Task 5: Final Verification

**Files:** all touched code and docs.

- [ ] **Step 1: Run focused test suite**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH mvn -pl cardshop-trade-engine-bootstrap,cardshop-trade-engine-infrastructure,cardshop-trade-engine-application -am -Dtest=ManualRechargeJobControllerTest,MybatisRechargeBalanceReconcileAdapterTest,RedisRechargeCacheRepairAdapterTest,RechargeOutboxWorkerServiceTest,RechargeNotifyWorkerServiceTest,MybatisRechargeBalanceDeltaFlushAdapterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: selected tests pass.

- [ ] **Step 2: Run package**

Run:

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH ./scripts/package.sh
```

Expected: build success and deployable zip produced.

- [ ] **Step 3: Inspect diff**

Run:

```bash
git diff --check
git status --short
```

Expected: no whitespace errors; only intentional files changed.

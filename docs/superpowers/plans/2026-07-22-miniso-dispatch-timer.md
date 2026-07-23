# MINISO Dispatch Timer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a low-DB-cost supplier fulfillment timer path for MINISO/direct recharge while keeping `recharge_dispatch_outbox` as the durable recovery source.

**Architecture:** Persisted outbox rows remain the only durable task facts. A new timer port schedules specific `event_id` values after commit or after a worker creates a query event; timer execution claims the row by `event_id + version` and reuses the existing outbox state machine. XXL-Job scanning stays as restart/multi-node recovery, not the hot path.

**Tech Stack:** Java 21, Spring Boot, MyBatis, JUnit 5, AssertJ, `ScheduledThreadPoolExecutor`.

---

### Task 1: Event-Targeted Worker Entry

**Files:**
- Modify: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/RechargeOutboxPort.java`
- Modify: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/RechargeOutboxWorkerService.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeDispatchOutboxAdapter.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeDispatchOutboxMapper.java`
- Test: `cardshop-trade-engine-application/src/test/java/com/cardshop/trade/engine/application/recharge/RechargeOutboxWorkerServiceTest.java`

- [x] Add a failing test that calls `processEvent("dispatch-ORDER-1", SUPPLIER_DISPATCH, "worker-A")` and verifies no shard scan is used.
- [x] Add `RechargeOutboxPort.findDueByEventId(eventId, eventType)`.
- [x] Implement worker event lookup, retry exhaustion check, CAS claim, and existing `handleClaimed` reuse.
- [x] Implement mapper SQL that selects one due event by `event_id`, event type, due/expired lease status, and `recharge_order_ext.persist_status=DB_PERSISTED`.
- [x] Run `mvn -pl cardshop-trade-engine-application -Dtest=RechargeOutboxWorkerServiceTest test`.

### Task 2: Timer Port And Scheduled Executor

**Files:**
- Create: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/RechargeDispatchTimerPort.java`
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/timer/ScheduledExecutorRechargeDispatchTimer.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineFulfillmentAutoConfiguration.java`
- Test: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/timer/ScheduledExecutorRechargeDispatchTimerTest.java`

- [x] Add a failing test that schedules the same event twice and verifies only the latest handle remains active while execution delegates to `processEvent`.
- [x] Implement `RechargeDispatchTimerPort.schedule(eventId, eventType, delay)` and `noop()`.
- [x] Implement a `ScheduledThreadPoolExecutor` adapter with named daemon threads, `setRemoveOnCancelPolicy(true)`, idempotent per-event future replacement, and bounded shutdown.
- [x] Register the timer bean only when `RechargeOutboxWorkerService` is available.

### Task 3: Persist-Commit Hot Dispatch Scheduling

**Files:**
- Modify: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/PersistRechargeOrderConsumerService.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/RechargeOrderPersistenceRowConverter.java`
- Test: `cardshop-trade-engine-application/src/test/java/com/cardshop/trade/engine/application/recharge/PersistRechargeOrderConsumerServiceTest.java`

- [x] Add a failing test that a dispatchable persisted order schedules `dispatch-{orderId}` after one second.
- [x] Inject `RechargeDispatchTimerPort` into `PersistRechargeOrderConsumerService` with a noop default for existing tests.
- [x] Schedule dispatch events after MySQL persist returns, using selected snapshots with `dispatchAllowed=true` and `balanceReserved=true`.
- [x] Set initial dispatch `next_retry_time` to persisted time plus `dispatchInitialDelay`, so timer and recovery scan share one due fact.

### Task 4: Query Timer Rhythm

**Files:**
- Modify: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/RechargeOutboxWorkerSettings.java`
- Modify: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/SupplierQueryOutboxCommand.java`
- Modify: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/RechargeOutboxWorkerService.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeDispatchOutboxAdapter.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineProperties.java`
- Test: `cardshop-trade-engine-application/src/test/java/com/cardshop/trade/engine/application/recharge/RechargeOutboxWorkerServiceTest.java`
- Test: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeOutboxAdapterTest.java`

- [x] Add failing tests that `PROCESSING` dispatch writes query event with two-second initial delay and schedules the query timer.
- [x] Add query delays `2,4,6,8,16,32` seconds to settings/properties.
- [x] Make query outbox rows use `max_retry_count=queryDelays.size()` and `next_retry_time=now+firstDelay`.
- [x] When query returns `PROCESSING` or `RETRYABLE_FAILED`, use the next configured query delay instead of the generic dispatch retry delay.

### Task 5: Documentation And Verification

**Files:**
- Modify: `docs/交行直充业务系统详细设计.md`
- Modify: `docs/交行直充业务系统高可用方案设计.md`
- Modify: `CONTEXT.md`

- [x] Update design docs to state: timer is hot path, outbox is durable fact/recovery path, MINISO remains direct recharge without local card delivery.
- [x] Run focused module tests, then at least `mvn -pl cardshop-trade-engine-application,cardshop-trade-engine-infrastructure -am test`.
- [x] Summarize residual risks: exact MINISO business failure code mapping still belongs in the supplier-specific adapter.

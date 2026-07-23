# Dispatch Timer Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the P1/P2 review findings before enabling the supplier dispatch timer path.

**Architecture:** Keep `recharge_dispatch_outbox` as the durable task fact. Add shared event-id/delay contracts in application, make timer scheduling configurable and disabled by default, and make the timer executor only submit work to a bounded worker executor so slow supplier I/O cannot starve delayed wakeups.

**Tech Stack:** Java 21, Spring Boot configuration properties, JUnit 5, AssertJ, `ScheduledThreadPoolExecutor` plus bounded `ThreadPoolExecutor`.

---

### Task 1: Shared Dispatch Event Contract

**Files:**
- Create: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/RechargeDispatchEventIds.java`
- Modify: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/PersistRechargeOrderConsumerService.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/RechargeOrderPersistenceRowConverter.java`
- Test: `cardshop-trade-engine-application/src/test/java/com/cardshop/trade/engine/application/recharge/RechargeDispatchEventIdsTest.java`
- Test: existing persistence/consumer tests

- [x] Add a failing test that `dispatchEventId("ORDER-1")` is `dispatch-ORDER-1` and rejects blank order id.
- [x] Move `dispatch-` and the default dispatch initial delay to `RechargeDispatchEventIds`.
- [x] Use the shared factory in the consumer and row converter.
- [x] Run the event id test plus `PersistRechargeOrderConsumerServiceTest` and relevant persistence tests.

### Task 2: Timer Grey Switch

**Files:**
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineProperties.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineFulfillmentAutoConfiguration.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineInfrastructureConfiguration.java`
- Test: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/config/TradeEnginePropertiesTest.java`
- Test: existing auto-configuration tests

- [x] Add failing property tests: `trade.engine.fulfillment.timer-enabled` defaults false and can be overridden true.
- [x] Add conditional timer bean creation based on `timer-enabled=true`; false must provide no-op timer so outbox remains recovery-backed.
- [x] Update docs to state timer defaults off until grey validation.
- [x] Run properties and auto-configuration tests.

### Task 3: Timer Wakeup Executor Isolation

**Files:**
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/timer/ScheduledExecutorRechargeDispatchTimer.java`
- Test: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/timer/ScheduledExecutorRechargeDispatchTimerTest.java`

- [x] Add a failing test where a blocked worker does not block a later timer wakeup from being submitted.
- [x] Change timer callback to submit `processEvent` to a bounded worker executor and return immediately to the scheduled executor thread.
- [x] Add sampled logging and rejection accounting for worker executor rejections.
- [x] Run timer tests.

### Task 4: Dispatch Unknown Result Queries Before Re-dispatch

**Files:**
- Modify: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/RechargeOutboxWorkerService.java`
- Test: `cardshop-trade-engine-application/src/test/java/com/cardshop/trade/engine/application/recharge/RechargeOutboxWorkerServiceTest.java`

- [x] Add a failing test: when dispatch returns `RETRYABLE_FAILED`, worker writes `supplier-query-{orderId}` and marks dispatch success instead of re-dispatching.
- [x] Reuse `enqueueSupplierQuery` for dispatch不可确认失败 so the next action is supplier query.
- [x] Add Chinese comments linking to detailed design「9.1 派单 Outbox」for query-first recovery and query retry delay.
- [x] Run worker tests.

### Task 5: Configuration Duplication Cleanup And Verification

**Files:**
- Modify: duplicated auto-configuration methods as needed.
- Modify: `docs/交行直充业务系统详细设计.md`
- Modify: `docs/交行直充业务系统高可用方案设计.md`
- Test: full module tests

- [x] Remove auto-configuration duplicate risk where practical without changing module boundaries.
- [x] Remove unreachable config branch and add/adjust Chinese comments for changed defaults.
- [x] Run focused tests, `mvn -pl cardshop-trade-engine-application,cardshop-trade-engine-infrastructure -am test`, `git diff --check`, and full `mvn test` if time permits.

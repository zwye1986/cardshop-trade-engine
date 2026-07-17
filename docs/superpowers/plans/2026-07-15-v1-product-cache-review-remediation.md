# V1 Product Cache Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve the confirmed AGENTS.md violations, duplicated cache-contract code, and the local-bypass/Lua contract inconsistency found in the review.

**Architecture:** Keep the application port Redis-free while centralizing Java-side Redis field names and ordered argument contracts in infrastructure. Extend the existing `LogStormGuard` with a keyed time-window mode, and only permit local direct-MySQL selection when Redis writes are disabled. Cross-language Java/Lua compatibility remains protected by focused contract tests.

**Tech Stack:** Java 21, Maven, JUnit 5, AssertJ, Spring Boot `ApplicationContextRunner`, Redis Lua.

---

### Task 1: Time-window log throttling

**Files:**
- Modify: `cardshop-trade-engine-application/src/test/java/com/cardshop/trade/engine/application/logging/LogStormGuardTest.java`
- Modify: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/logging/LogStormGuard.java`
- Modify: the three V1 product-cache hot-path callers in api/application/infrastructure.

- [x] Add a failing test proving the first event logs, repeated events inside the window are suppressed, and the event logs again after the window.
- [x] Add `LogStormGuard.atMostOncePer(Duration, Clock)` and preserve the existing every-N behavior.
- [x] Replace the three hand-written `AtomicLong.compareAndSet` throttles with stable event keys and the shared guard.
- [x] Run `mvn -pl cardshop-trade-engine-application -am -Dtest=LogStormGuardTest test`.

### Task 2: Local fail-closed boundary

**Files:**
- Modify: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineInfrastructureConfigurationTest.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineInfrastructureConfiguration.java`
- Modify: `docs/交行直充业务系统详细设计.md`
- Modify: `docs/交行直充业务系统高可用方案设计.md`

- [x] Add a failing context test proving local direct-MySQL bypass is rejected when Redis writes are enabled and the cache port is unavailable.
- [x] Restrict local bypass to `environment=local && redis.write-enabled=false`.
- [x] Clarify the same rule in both design documents before relying on the code behavior.
- [x] Run the focused configuration test.

### Task 3: Redis selection contract cleanup

**Files:**
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/redis/V1ProductSelectionRedisContract.java`
- Modify: both V1 Redis adapters and `accept_recharge_order.lua`.
- Modify: Redis adapter and Lua contract tests.

- [x] Add contract assertions for the complete required snapshot and child field sets.
- [x] Move Java field constants, ordered field lists, value conversion, and argument appending into the shared contract class.
- [x] Name Lua field literals once per script and use those names for HGET/HSET operations.
- [x] Run the focused Redis adapter and Lua contract tests.

### Task 4: Documentation and minor standards fixes

**Files:**
- Modify: `TradeEngineInfrastructureConfiguration.java`, `TradeEngineObservabilityMetrics.java`, and `PersistOrderSnapshot.java`.
- Modify: both design documents.

- [x] Add the required document link to the active-sync TODO.
- [x] Add observability-section links to `recordRefresh` and `recordMysqlLoad`.
- [x] Name persist-consumer command sentinel values.
- [x] Add `product_id` and `face_value` to the snapshot field list and product-cache metrics to detailed-design section 12.

### Task 5: Verification

- [x] Run focused application and infrastructure tests.
- [x] Run the full reactor `mvn test`.
- [x] Re-scan for duplicated CAS throttles, unlinked TODOs, and inlined V1 product-cache field names.

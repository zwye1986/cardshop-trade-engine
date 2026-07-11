# Observability Minimum Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first production-safe business observability loop for Redis pending, outbox, anomaly, balance reconcile, WAIT/Lua, and MQ publish reliability without introducing an independent MySQL schema.

**Architecture:** Keep facts in existing Redis/MySQL/RabbitMQ paths and add a thin Micrometer layer. Redis/MySQL aggregate gauges are sampled through a TTL-cached snapshot provider so Prometheus scrape does not scan shared `cardshop_test` on every request. Hot-path WAIT/Lua/MQ events update low-cardinality counters/timers directly.

**Tech Stack:** Java 21, Spring Boot 3.3, Micrometer, Spring Data Redis reactive, MyBatis-Plus, JUnit 5, AssertJ.

---

### Task 1: Add metric contracts and TDD tests

**Files:**
- Create: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/observability/TradeEngineObservabilityMetricsTest.java`
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/observability/TradeEngineObservabilityMetrics.java`
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/observability/TradeEngineObservabilitySnapshot.java`
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/observability/TradeEngineObservabilitySnapshotProvider.java`
- Modify: `cardshop-trade-engine-infrastructure/pom.xml`

- [x] **Step 1: Write failing Micrometer test**

Run: `mvn -pl cardshop-trade-engine-infrastructure -Dtest=TradeEngineObservabilityMetricsTest test`

Expected before implementation: compilation fails because the observability classes do not exist.

- [x] **Step 2: Implement minimal metric registry**

Register gauges with names:

```text
trade.engine.redis.pending.total
trade.engine.redis.pending.due
trade.engine.redis.pending.max.age.seconds
trade.engine.outbox.pending
trade.engine.outbox.max.age.seconds
trade.engine.accept.anomaly.open
trade.engine.balance.reconcile.diff.orgs
trade.engine.mq.publish.breaker.open
```

Register counters/timers with names:

```text
trade.engine.redis.wait.total
trade.engine.redis.lua.slow.total
trade.engine.redis.lua.duration
trade.engine.mq.publish.confirm.total
```

All tags must be low-cardinality: `environment`, `status`, `event_type`, `anomaly_type`, `operation`, `result`, `fail_code`.

- [x] **Step 3: Re-run focused test**

Run: `mvn -pl cardshop-trade-engine-infrastructure -Dtest=TradeEngineObservabilityMetricsTest test`

Expected after implementation: focused test exits `0`.

### Task 2: Add TTL-cached Redis/MySQL snapshot provider

**Files:**
- Create: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/observability/RedisMysqlTradeEngineObservabilitySnapshotProviderTest.java`
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/observability/RedisMysqlTradeEngineObservabilitySnapshotProvider.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeOutboxMapper.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeAcceptAnomalyMapper.java`

- [x] **Step 1: Write failing snapshot tests**

Run: `mvn -pl cardshop-trade-engine-infrastructure -Dtest=RedisMysqlTradeEngineObservabilitySnapshotProviderTest test`

Expected before implementation: compilation fails because the provider does not exist.

- [x] **Step 2: Implement aggregate queries and cache**

Snapshot must compute:

```text
Redis pending total / due / max age by scanning org_index buckets and per-org pending ZSET only up to configured limits.
Outbox due / failed / manual_required / max age from recharge_outbox.
Accept anomaly open / manual_required from recharge_accept_anomaly.
Balance reconcile diff org count by comparing sampled merchant balances to Redis available plus unfinished BALANCE_DELTA_FLUSH.
```

The provider must cache snapshots for at least 15 seconds by default and return the last snapshot when a refresh fails.

- [x] **Step 3: Re-run focused test**

Run: `mvn -pl cardshop-trade-engine-infrastructure -Dtest=RedisMysqlTradeEngineObservabilitySnapshotProviderTest test`

Expected after implementation: focused test exits `0`.

### Task 3: Wire metrics into existing hot paths and auto-configuration

**Files:**
- Create: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineObservabilityAutoConfigurationTest.java`
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineObservabilityAutoConfiguration.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/redis/RedisSessionAcceptReliabilityAdapter.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/mq/RabbitPersistRechargeOrderMessagePublisher.java`
- Modify: `cardshop-trade-engine-bootstrap/src/main/java/com/cardshop/trade/engine/bootstrap/config/TradeEngineReliabilityConfiguration.java`

- [x] **Step 1: Write failing auto-configuration and hot-path tests**

Run: `mvn -pl cardshop-trade-engine-infrastructure -Dtest=TradeEngineObservabilityAutoConfigurationTest,RedisSessionAcceptReliabilityAdapterTest,RabbitPersistRechargeOrderMessagePublisherTest test`

Expected before implementation: new auto-configuration test fails because metrics beans are missing.

- [x] **Step 2: Implement wiring**

Create `TradeEngineObservabilityMetrics` only when a `MeterRegistry` is present. Inject it into Redis session and RabbitMQ publisher constructors with a no-op fallback so existing tests and disabled observability contexts keep working.

- [x] **Step 3: Re-run focused tests**

Run: `mvn -pl cardshop-trade-engine-infrastructure -Dtest=TradeEngineObservabilityAutoConfigurationTest,RedisSessionAcceptReliabilityAdapterTest,RabbitPersistRechargeOrderMessagePublisherTest test`

Expected after implementation: focused tests exit `0`.

### Task 4: Update docs and verify

**Files:**
- Modify: `docs/集成测试.md`
- Modify: `docs/开发进度.md`
- Modify: `docs/集成测试方案.md`

- [x] **Step 1: Run verification**

Run:

```bash
mvn -pl cardshop-trade-engine-infrastructure -am test
git diff --check
```

Expected: both commands exit `0`.

- [x] **Step 2: Document test-profile evidence command**

Add the exact test188 commands for checking `/actuator/prometheus` business metrics and RabbitMQ management queue depth without printing passwords.

# V1 Product Selection Cache Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Redis-first V1 product-selection cache whose price and product attributes become effective within 30 minutes without querying MySQL for every order.

**Architecture:** Keep `MybatisV1ProductCandidateSelector` as the source-of-truth reader and wrap it with an application-layer cache-first selector. A Redis application port owns snapshot reads, parent-product single-flight locks, negative entries, and lock-fenced atomic writes of the parent selection snapshot plus selected-child Lua configuration. The existing child-config preheat port becomes a compare-and-repair operation that verifies the parent snapshot before changing the child key, preventing an old in-flight request from overwriting a newer price.

**Tech Stack:** Java 21, Spring Boot 3, Maven, Spring Data Reactive Redis, Redis Lua, Micrometer, JUnit 5, AssertJ, Testcontainers Redis 7.

## Review Remediation Addendum

Independent review after the first green implementation found four correctness gaps. The remaining implementation must be test-driven in this order:

1. Add parent-selection fencing to `accept_recharge_order.lua`: include the parent key in `KEYS`, add `productSnapshotAvailable` to the command/ARGV contract, compare parent/child/price/version/type/cache kind, and require `expire_at > now` atomically. A negative/no-snapshot business failure may skip the child key and must persist as `ACCEPTED_FAILED` without balance reservation.
2. Re-read `Clock.instant()` after a soft refresh failure. Falling back to the old entry is legal only while the new time remains before `expireAt`.
3. Extend the refresh-lock contract with token-based renewal. Run a renewal watchdog while the blocking MySQL selector executes, and add a concurrency test whose source duration exceeds the initial lock TTL.
4. Restrict Redis-less direct MySQL selection to `trade.engine.environment=local`; dev/test/prod keep the cache decorator and fail closed.
5. Split source and Redis completion exception boundaries: source failures record `mysql_failed`; Redis read/write/fencing failures record `redis_failed`; negative-cache write failures return the MySQL-confirmed business failure.
6. Add focused tests for wait timeout, negative-cache expiry/reload, metrics classification, parent switch between repair and accept, and real-Lua negative-cache behavior before the full verification task.
7. Fence the complete parent snapshot with a stable `selection_fingerprint`, not only the child/price/version/type subset; add a regression where only base price and candidate validation change.

---

### Task 1: Define cache contracts and validated settings

**Files:**
- Create: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/V1ProductCacheSettings.java`
- Create: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/V1ProductSelectionCacheEntry.java`
- Create: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/V1ProductSelectionCachePort.java`
- Create: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/V1ProductCacheObserver.java`
- Create: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/V1ProductCacheUnavailableException.java`
- Test: `cardshop-trade-engine-application/src/test/java/com/cardshop/trade/engine/application/v1/V1ProductCacheSettingsTest.java`

- [ ] **Step 1: Write failing settings tests**

```java
@Test
void acceptsOrderedPositiveDurations() {
    assertThatCode(() -> new V1ProductCacheSettings(
            Duration.ofMinutes(25), Duration.ofMinutes(30), Duration.ofMinutes(60),
            Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ofMinutes(1)))
            .doesNotThrowAnyException();
}

@Test
void rejectsRefreshAfterAtOrBeyondMaxStaleAge() {
    assertThatThrownBy(() -> new V1ProductCacheSettings(
            Duration.ofMinutes(30), Duration.ofMinutes(30), Duration.ofMinutes(60),
            Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ofMinutes(1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("refreshAfter");
}
```

- [ ] **Step 2: Verify RED**

Run: `mvn -pl cardshop-trade-engine-application -am -Dtest=V1ProductCacheSettingsTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL because `V1ProductCacheSettings` does not exist.

- [ ] **Step 3: Implement the records and ports**

```java
public record V1ProductCacheSettings(
        Duration refreshAfter,
        Duration maxStaleAge,
        Duration physicalTtl,
        Duration refreshLockTtl,
        Duration refreshWaitTimeout,
        Duration negativeCacheTtl) {
    public V1ProductCacheSettings {
        requirePositive(refreshAfter, "refreshAfter");
        requirePositive(maxStaleAge, "maxStaleAge");
        requirePositive(physicalTtl, "physicalTtl");
        requirePositive(refreshLockTtl, "refreshLockTtl");
        requirePositive(refreshWaitTimeout, "refreshWaitTimeout");
        requirePositive(negativeCacheTtl, "negativeCacheTtl");
        if (refreshAfter.compareTo(maxStaleAge) >= 0) {
            throw new IllegalArgumentException("refreshAfter must be less than maxStaleAge");
        }
        if (maxStaleAge.compareTo(physicalTtl) >= 0) {
            throw new IllegalArgumentException("maxStaleAge must be less than physicalTtl");
        }
    }
}
```

`V1ProductSelectionCachePort` exposes `find`, `tryAcquireRefreshLock`, `writeSnapshotIfLockOwned`, `writeNegativeIfLockOwned`, `releaseRefreshLock`, and `awaitNewerEntry`. `V1ProductSelectionCacheEntry` stores the selection, `cacheKind`, `loadedAt`, `refreshAfter`, and `expireAt`. `V1ProductCacheObserver` supplies no-op defaults and records cache result, refresh result/duration, age, and MySQL loads.

- [ ] **Step 4: Verify GREEN**

Run: `mvn -pl cardshop-trade-engine-application -am -Dtest=V1ProductCacheSettingsTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS.

### Task 2: Implement cache-first selection behavior

**Files:**
- Create: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/CachedV1ProductCandidateSelector.java`
- Test: `cardshop-trade-engine-application/src/test/java/com/cardshop/trade/engine/application/v1/CachedV1ProductCandidateSelectorTest.java`

- [ ] **Step 1: Write failing behavior tests**

Cover these independent cases with a fake cache port and counting source selector:

```java
@Test void freshSnapshotDoesNotLoadMysql() { /* find returns age 10m; assert source calls == 0 */ }
@Test void softStaleLockLoserUsesOldSnapshot() { /* age 26m, lock false; source calls == 0 */ }
@Test void softStaleLockOwnerRefreshesAndUsesNewPrice() { /* lock true; assert new price and one source call */ }
@Test void hardStaleLockLoserWaitsForNewerSnapshot() { /* age 30m; await returns refreshed entry */ }
@Test void hardStaleRefreshFailureThrowsSystemBusy() { /* source throws; assert V1ProductCacheUnavailableException */ }
@Test void missingSelectionWritesNegativeEntry() { /* source returns PRODUCT_NO_PERMISSION */ }
@Test void negativeEntryAvoidsMysqlUntilItsExpiry() { /* negative hit; source calls == 0 */ }
```

- [ ] **Step 2: Verify RED**

Run: `mvn -pl cardshop-trade-engine-application -am -Dtest=CachedV1ProductCandidateSelectorTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL because the decorator does not exist.

- [ ] **Step 3: Implement the selector state machine**

```java
if (entry.isPresent() && entry.get().negative() && now.isBefore(entry.get().expireAt())) {
    observer.recordHit("negative", entry.get().age(now));
    return entry.get().selection();
}
if (entry.isPresent() && now.isBefore(entry.get().refreshAfter())) {
    observer.recordHit("fresh", entry.get().age(now));
    return entry.get().selection();
}
if (entry.isPresent() && now.isBefore(entry.get().expireAt())) {
    return refreshSoftOrUseOld(orgCode, productId, account, entry.get());
}
return refreshHardOrWait(orgCode, productId, account, entry.orElse(null));
```

The refresh owner calls MySQL once and completes Redis using its lock token. Soft-refresh failures return the still-younger-than-30-min snapshot; hard/missing failures throw `V1ProductCacheUnavailableException`. A lock loser never queries MySQL. All `finally` paths call compare-and-delete lock release, which is harmless after a successful lock-fenced Lua write has already removed the lock.

- [ ] **Step 4: Verify GREEN**

Run: `mvn -pl cardshop-trade-engine-application -am -Dtest=CachedV1ProductCandidateSelectorTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS with exactly one source load in lock-owner cases.

### Task 3: Add Redis keys and atomic snapshot adapter

**Files:**
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/redis/RechargeRedisKeyBuilder.java`
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/redis/RedisV1ProductSelectionCacheAdapter.java`
- Test: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/redis/RechargeRedisKeyBuilderTest.java`
- Test: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/redis/RedisV1ProductSelectionCacheAdapterTest.java`

- [ ] **Step 1: Write failing key and serialization tests**

```java
assertThat(keyBuilder.productSelectionKey("A39", "80100022"))
        .isEqualTo("cardshop:trade:recharge:{A39}:product-selection:80100022");
assertThat(keyBuilder.productSelectionRefreshLockKey("A39", "80100022"))
        .isEqualTo("cardshop:trade:recharge:{A39}:product-selection:80100022:refresh-lock");
```

Adapter tests verify full field round-trip, malformed hashes treated as misses, tokenized lock acquire/release, negative serialization, and rejected writes when the lock token no longer owns the lock.

- [ ] **Step 2: Verify RED**

Run: `mvn -pl cardshop-trade-engine-infrastructure -am -Dtest=RechargeRedisKeyBuilderTest,RedisV1ProductSelectionCacheAdapterTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL because key methods and adapter are missing.

- [ ] **Step 3: Implement Redis operations and lock-fenced Lua**

The snapshot completion Lua receives three same-slot keys: selection hash, selected child config hash, and refresh lock. It first checks `GET lock == token`; only then does it `HSET` both hashes, apply physical TTLs, and delete the lock. The negative completion Lua performs the same token check, writes a `cache_kind=NEGATIVE` hash with the business failure, applies the one-minute TTL, and deletes the lock. Lock release uses compare-and-delete Lua. `awaitNewerEntry` polls at a small bounded interval until `refreshWaitTimeout`, returning only an entry whose `loadedAt` is newer than the caller's observed value.

- [ ] **Step 4: Verify GREEN**

Run: `mvn -pl cardshop-trade-engine-infrastructure -am -Dtest=RechargeRedisKeyBuilderTest,RedisV1ProductSelectionCacheAdapterTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS.

### Task 4: Upgrade child-config preheat to compare-and-repair

**Files:**
- Modify: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/V1ProductConfigPreheatPort.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/redis/RedisV1ProductConfigPreheatAdapter.java`
- Modify: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/redis/RedisV1ProductConfigPreheatAdapterTest.java`

- [ ] **Step 1: Replace legacy insertion-only expectations with failing repair tests**

```java
@Test void repairsMissingChildWhenParentSnapshotStillMatches() { /* result REPAIRED */ }
@Test void repairsDriftedPriceAndVersionWhenParentSnapshotStillMatches() { /* rewrite all four fields */ }
@Test void doesNotOverwriteChildWhenParentSnapshotHasMovedToNewPrice() { /* result PARENT_CHANGED */ }
@Test void skipsSelectionWithoutResolvedSnapshot() { /* no Redis call */ }
```

- [ ] **Step 2: Verify RED**

Run: `mvn -pl cardshop-trade-engine-infrastructure -am -Dtest=RedisV1ProductConfigPreheatAdapterTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL against the legacy child-config preheat contract.

- [ ] **Step 3: Implement `refreshIfMissingOrStale`**

The Lua checks parent `selected_product_id`, `sale_price`, `product_version`, and `product_type` against the request snapshot before changing the child hash. If the parent moved, return without writing. If it still matches and any child field differs or is missing, rewrite all four child fields and refresh its physical TTL. Redis failures throw `V1ProductCacheUnavailableException`; they are not swallowed.

- [ ] **Step 4: Verify GREEN**

Run: `mvn -pl cardshop-trade-engine-infrastructure -am -Dtest=RedisV1ProductConfigPreheatAdapterTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS.

### Task 5: Bind properties and wire the decorator

**Files:**
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineProperties.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineInfrastructureConfiguration.java`
- Modify: `cardshop-trade-engine-bootstrap/src/main/resources/application.yml`
- Modify: `scripts/publish-dev-nacos.sh`
- Modify: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineInfrastructureConfigurationTest.java`

- [ ] **Step 1: Write failing configuration tests**

Assert default values `25m/30m/60m/5s/1s/1m`, invalid ordering fails context startup, and Redis plus MyBatis readers yields `CachedV1ProductCandidateSelector` with `RedisV1ProductSelectionCacheAdapter`. Without Redis, retain the direct MyBatis selector for local/test isolation.

- [ ] **Step 2: Verify RED**

Run: `mvn -pl cardshop-trade-engine-infrastructure -am -Dtest=TradeEngineInfrastructureConfigurationTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL because product-cache properties and beans do not exist.

- [ ] **Step 3: Implement properties and wiring**

```yaml
v1:
  product-cache:
    refresh-after: ${TRADE_ENGINE_V1_PRODUCT_CACHE_REFRESH_AFTER:25m}
    max-stale-age: ${TRADE_ENGINE_V1_PRODUCT_CACHE_MAX_STALE_AGE:30m}
    physical-ttl: ${TRADE_ENGINE_V1_PRODUCT_CACHE_PHYSICAL_TTL:60m}
    refresh-lock-ttl: ${TRADE_ENGINE_V1_PRODUCT_CACHE_REFRESH_LOCK_TTL:5s}
    refresh-wait-timeout: ${TRADE_ENGINE_V1_PRODUCT_CACHE_REFRESH_WAIT_TIMEOUT:1s}
    negative-cache-ttl: ${TRADE_ENGINE_V1_PRODUCT_CACHE_NEGATIVE_CACHE_TTL:1m}
```

Construct `V1ProductCacheSettings` during bean creation so invalid values fail startup. Add the repository-mandated deferred-work comment linked to the detailed design, stating that legacy-card active synchronization via outbox/CDC/incremental polling remains future work.

- [ ] **Step 4: Verify GREEN**

Run: `mvn -pl cardshop-trade-engine-infrastructure -am -Dtest=TradeEngineInfrastructureConfigurationTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS.

### Task 6: Integrate request error handling and compare-and-repair

**Files:**
- Modify: `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/usecase/V1CreateRechargeOrderUseCaseService.java`
- Modify: `cardshop-trade-engine-api/src/test/java/com/cardshop/trade/engine/api/v1/usecase/V1CreateRechargeOrderUseCaseServiceTest.java`

- [ ] **Step 1: Write failing use-case tests**

Add tests proving `refreshIfMissingOrStale` runs before accept Lua, cache refresh unavailability returns V1 system-busy without invoking accept/persist/balance paths, and a resolved validation-failed selection is still repaired.

- [ ] **Step 2: Verify RED**

Run: `mvn -pl cardshop-trade-engine-api -am -Dtest=V1CreateRechargeOrderUseCaseServiceTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL because the use case still calls the legacy preheat method and does not map cache exceptions.

- [ ] **Step 3: Implement the use-case changes**

Wrap selection plus child repair in a `try/catch (V1ProductCacheUnavailableException)` inside `prepare`, emit a rate-limited error with only `orgCode/productId`, and return `V1CardApiResultStatus.SYSTEM_BUSY`. Call `refreshIfMissingOrStale` for every resolved child snapshot before request hash and accept-command construction.

- [ ] **Step 4: Verify GREEN**

Run: `mvn -pl cardshop-trade-engine-api -am -Dtest=V1CreateRechargeOrderUseCaseServiceTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS and no accept invocation in cache-unavailable tests.

### Task 7: Add product-cache observability

**Files:**
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/observability/TradeEngineObservabilityMetrics.java`
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineInfrastructureConfiguration.java`
- Modify: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/observability/TradeEngineObservabilityMetricsTest.java`

- [ ] **Step 1: Write failing metrics tests**

Bind a `SimpleMeterRegistry`, record `fresh`, `success`, duration, age and MySQL load, and assert the meters exist with only low-cardinality `environment/result` labels.

- [ ] **Step 2: Verify RED**

Run: `mvn -pl cardshop-trade-engine-infrastructure -am -Dtest=TradeEngineObservabilityMetricsTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL because product-cache metrics are absent.

- [ ] **Step 3: Implement observer methods**

Register counters/timers/summaries corresponding to the approved names: cache hits, refresh outcomes, refresh duration, cache age, and MySQL load. Do not label by merchant, product, account, or order. Soft-refresh failure logs are sampled; hard-expiry failures remain diagnosable without producing one error per competing request.

- [ ] **Step 4: Verify GREEN**

Run: `mvn -pl cardshop-trade-engine-infrastructure -am -Dtest=TradeEngineObservabilityMetricsTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS.

### Task 8: Prove real-Redis atomicity and single-flight behavior

**Files:**
- Replace/Modify: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/redis/RedisV1ProductConfigPreheatIntegrationTest.java`
- Create: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/redis/RedisV1ProductSelectionCacheIntegrationTest.java`

- [ ] **Step 1: Add gated Testcontainers tests**

Use 16 concurrent callers on the same `A39 + parentProductId`. Assert only one source-selector call, all hard-expired callers receive the same refreshed price, parent and selected child hashes switch together, a stale lock token cannot overwrite a newer refresh, and a deleted child config is repaired only while the parent snapshot still matches.

- [ ] **Step 2: Run real Redis tests**

Run: `mvn -pl cardshop-trade-engine-infrastructure -am -DredisTestcontainers.enabled=true -Dtest=RedisV1ProductSelectionCacheIntegrationTest,RedisV1ProductConfigPreheatIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS when Docker is available. If Docker is unavailable, record that fact and retain unit-test evidence.

### Task 9: Full verification and documentation consistency

**Files:**
- Modify if needed: `docs/集成测试.md`
- Review: `docs/交行直充业务系统详细设计.md`
- Review: `docs/交行直充业务系统高可用方案设计.md`
- Review: `docs/superpowers/specs/2026-07-15-v1-product-selection-cache-refresh-design.md`

- [ ] **Step 1: Run focused module suites**

Run: `mvn -pl cardshop-trade-engine-application,cardshop-trade-engine-infrastructure,cardshop-trade-engine-api,cardshop-trade-engine-bootstrap -am test`

Expected: BUILD SUCCESS.

- [ ] **Step 2: Run the full repository suite**

Run: `mvn test`

Expected: BUILD SUCCESS.

- [ ] **Step 3: Run static diff checks**

Run: `git diff --check`

Expected: no output.

Run: `rg -n "preheatIfMissing|put-if-absent" cardshop-trade-engine-*`

Expected: no stale production contract references and no plan placeholders; historical integration-test evidence may retain old wording only when clearly dated.

- [ ] **Step 4: Update integration evidence when a real environment run is performed**

Record date, test188 profile/Nacos DataIds, non-secret Redis/MySQL topology, parent and selected child IDs, before/after cache timestamps and prices, concurrency evidence, results, and follow-up issues in `docs/集成测试.md`. Never record credentials or full tokens.

- [ ] **Step 5: Review the uncommitted diff**

Run: `git status --short && git diff --stat`

Expected: only the cache-refresh implementation, approved design documents, plan/spec, configuration templates, tests, and integration evidence are changed. Do not create a commit unless the user explicitly requests it.

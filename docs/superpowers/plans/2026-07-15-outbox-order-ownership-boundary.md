# Outbox Order Ownership Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make all outbox task reads and claims process only `cardshop-trade-engine` owned orders, and process eligible events in generation-time order.

**Architecture:** Use `recharge_order_ext` as the ownership sentinel. `RechargeOutboxMapper` applies an `exists` predicate against `recharge_order_ext` for scan, claim, balance delta aggregation, and observability aggregation. Scanning keeps existing status, retry, lease, event-type, shard, and limit rules, then orders eligible rows by `created_at ASC, id ASC`.

**Tech Stack:** Java 21, Spring Boot, MyBatis-Plus, MyBatis annotation SQL, Maven, JUnit 5, AssertJ.

---

## File Structure

- Modify: `docs/交行直充业务系统详细设计.md`
  - Add the outbox ownership boundary and FIFO scanning rule to「9.1 派单 Outbox」and the `recharge_outbox` field notes.
- Modify: `docs/交行直充业务系统高可用方案设计.md`
  - Add the same boundary to outbox design points, scan SQL, and shared-table risk notes.
- Modify: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/persistence/MapperContractTest.java`
  - Add contract tests that fail until mapper SQL uses the `recharge_order_ext` sentinel and `created_at ASC, id ASC` ordering.
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeOutboxMapper.java`
  - Replace scan and claim LambdaWrapper logic with MyBatis annotation SQL that can express correlated `exists` predicates.
  - Update balance delta and observability aggregate queries to use the same ownership boundary.
- Verify only; no source edit expected: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeOutboxAdapter.java`
  - Existing adapter calls mapper methods, so no application port changes are needed.
- Verify only; no source edit expected: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeBalanceDeltaFlushAdapter.java`
  - Existing adapter delegates balance event scanning and claiming to the mapper, so mapper boundary covers it.
- Verify only; no source edit expected: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/observability/RedisMysqlTradeEngineObservabilitySnapshotProvider.java`
  - Existing provider delegates metrics and unfinished delta to the mapper, so mapper boundary covers it.

---

### Task 1: Update Design Documents

**Files:**
- Modify: `docs/交行直充业务系统详细设计.md`
- Modify: `docs/交行直充业务系统高可用方案设计.md`

- [ ] **Step 1: Add the boundary text to detailed design**

In `docs/交行直充业务系统详细设计.md`, update「9.1 派单 Outbox」so it contains this text after the first paragraph about `recharge_outbox`:

```markdown
由于新交易引擎与旧卡管系统共享 `mc_order` 等旧表，outbox 自动任务必须先确认事件关联订单属于新系统处理范围。归属边界以新系统独占维护的 `recharge_order_ext` 为准：扫描和抢占 `recharge_outbox` 时，必须同时匹配 `order_id + org_code + out_order_sn`，且 `recharge_order_ext.persist_status = DB_PERSISTED`。不满足该边界的事件不得被扫描、抢占、派单、刷账、作废或写回共享表，避免处理旧卡管订单或异常历史数据。

outbox 任务先使用状态、`next_retry_time` 和租约条件筛选当前具备执行资格的事件，再在每个分片的可执行集合内按 `created_at ASC, id ASC` 处理，保证先生成的事件优先获得处理机会。`next_retry_time` 只作为执行资格条件，不作为到期事件的主排序字段。
```

In the outbox rules table, add these rows:

```markdown
| 归属边界 | `recharge_order_ext` 中必须存在同 `order_id + org_code + out_order_sn` 且 `persist_status=DB_PERSISTED` 的记录。 |
| 处理顺序 | 已具备执行资格的事件按 `created_at ASC, id ASC` 稳定扫描，先生成先处理。 |
```

In the `recharge_outbox` field table, update `created_at、updated_at` to mention scan ordering:

```markdown
| `created_at`、`updated_at` | `createdAt`、`updatedAt` | DDL COMMENT: 创建时间/更新时间；`created_at` 是 outbox 扫描 FIFO 顺序依据 | 自动时间 |
```

- [ ] **Step 2: Add the boundary text to high-availability design**

In `docs/交行直充业务系统高可用方案设计.md`, under the outbox design points around the section that begins with `` `outbox` 任务指的是：``, add:

```markdown
7. 新旧系统共享 `mc_order` 等旧表期间，outbox Worker 的处理边界不以 `mc_order` 订单状态、商品类型或订单号前缀判断，而以 `recharge_order_ext` 归属哨兵判断：同一 `order_id + org_code + out_order_sn` 必须存在扩展记录，且 `persist_status=DB_PERSISTED`。扫描、抢占、余额 delta 汇总和 outbox 监控聚合都必须使用同一边界。
8. 可执行 outbox 事件按生成时间优先处理。SQL 先过滤状态、退避时间、租约、分片和归属边界，再按 `created_at asc, id asc` 排序；`next_retry_time` 不作为已到期事件之间的主排序字段。
```

Replace the MySQL outbox scan SQL in the same document with:

```sql
select *
from recharge_outbox o
where (
    (o.status in ('INIT', 'FAILED') and o.next_retry_time <= now())
    or (o.status = 'PROCESSING' and (o.locked_until < now() or o.locked_until is null))
  )
  and exists (
    select 1
    from recharge_order_ext ext
    where ext.order_id = o.order_id
      and ext.org_code = o.org_code
      and ext.out_order_sn = o.out_order_sn
      and ext.persist_status = 'DB_PERSISTED'
  )
  and mod(crc32(o.shard_key), ?) = ?
order by o.created_at asc, o.id asc
limit ?
```

Replace the claim SQL in the same document with:

```sql
update recharge_outbox o
set o.status = 'PROCESSING',
    o.locked_by = ?,
    o.locked_until = date_add(now(), interval 2 minute),
    o.updated_at = now(),
    o.version = o.version + 1
where o.event_id = ?
  and o.version = ?
  and (
      (o.status in ('INIT', 'FAILED') and o.retry_count < o.max_retry_count)
      or (o.status = 'PROCESSING' and (o.locked_until < now() or o.locked_until is null))
  )
  and exists (
    select 1
    from recharge_order_ext ext
    where ext.order_id = o.order_id
      and ext.org_code = o.org_code
      and ext.out_order_sn = o.out_order_sn
      and ext.persist_status = 'DB_PERSISTED'
  )
```

- [ ] **Step 3: Verify docs changed**

Run:

```bash
git diff -- docs/交行直充业务系统详细设计.md docs/交行直充业务系统高可用方案设计.md
```

Expected: diff contains the `recharge_order_ext` ownership sentinel and `created_at ASC, id ASC` ordering in both documents.

---

### Task 2: Write Failing Mapper Contract Tests

**Files:**
- Modify: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/persistence/MapperContractTest.java`

- [ ] **Step 1: Update the existing outbox mapper contract test**

Replace `outboxMapperScansDueAndZombieEventsWithShardPredicate` with this stricter contract:

```java
    @Test
    void outboxMapperScopesDueScanAndClaimToTradeEngineOwnedOrders() throws IOException {
        String mapper = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeOutboxMapper.java"));

        assertThat(mapper)
                .contains("OutboxEventType.SUPPLIER_QUERY.name()")
                .contains("from recharge_outbox o")
                .contains("from recharge_order_ext ext")
                .contains("ext.order_id = o.order_id")
                .contains("ext.org_code = o.org_code")
                .contains("ext.out_order_sn = o.out_order_sn")
                .contains("ext.persist_status = 'DB_PERSISTED'")
                .contains("TaskStatus.INIT.name()")
                .contains("TaskStatus.FAILED.name()")
                .contains("TaskStatus.PROCESSING.name()")
                .contains("retry_count < max_retry_count")
                .contains("mod(crc32(o.shard_key), #{shardTotal}) = #{shardIndex}")
                .contains("version = version + 1")
                .doesNotContainIgnoringCase(" join ");
    }
```

- [ ] **Step 2: Add FIFO and aggregate contract tests**

Add these tests next to the outbox mapper contract:

```java
    @Test
    void outboxMapperScansEligibleEventsByCreationTime() throws IOException {
        String mapper = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeOutboxMapper.java"));
        String scanSql = mapper.substring(mapper.indexOf("List<RechargeOutboxDO> selectDueForShardByEventTypesSql"));

        assertThat(scanSql)
                .contains("o.next_retry_time <= #{now}")
                .contains("order by o.created_at asc, o.id asc")
                .doesNotContain("orderByAsc(RechargeOutboxDO::getNextRetryTime)")
                .doesNotContain("order by o.next_retry_time asc");
    }

    @Test
    void outboxMapperScopesBalanceAndObservabilityAggregatesToOwnedOrders() throws IOException {
        String mapper = Files.readString(Path.of("src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeOutboxMapper.java"));
        String balanceSql = mapper.substring(mapper.indexOf("Long sumUnfinishedBalanceDelta"), mapper.indexOf("List<Map<String, Object>> selectObservabilityOutboxMetrics"));
        String observabilitySql = mapper.substring(mapper.indexOf("List<Map<String, Object>> selectObservabilityOutboxMetrics"), mapper.indexOf("default List<RechargeOutboxDO> selectDueForShard"));

        assertThat(balanceSql)
                .contains("from recharge_outbox o")
                .contains("from recharge_order_ext ext")
                .contains("ext.persist_status = 'DB_PERSISTED'");
        assertThat(observabilitySql)
                .contains("from recharge_outbox o")
                .contains("from recharge_order_ext ext")
                .contains("ext.persist_status = 'DB_PERSISTED'");
    }
```

- [ ] **Step 3: Run test and verify RED**

Run:

```bash
mvn -pl cardshop-trade-engine-infrastructure -Dtest=MapperContractTest test
```

Expected: FAIL because current mapper still uses LambdaWrapper scan/claim, lacks `recharge_order_ext` `exists`, and orders by `next_retry_time`.

---

### Task 3: Implement Owned FIFO Outbox SQL

**Files:**
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeOutboxMapper.java`

- [ ] **Step 1: Update mapper imports**

Remove unused imports after the rewrite:

```java
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
```

Add the MyBatis update annotation:

```java
import org.apache.ibatis.annotations.Update;
```

Keep `BaseMapper`, `OutboxEventType`, `TaskStatus`, `RechargeOutboxDO`, `LocalDateTime`, `List`, `Map`, `Insert`, `Mapper`, `Param`, and `Select`.

- [ ] **Step 2: Scope balance delta aggregation to owned orders**

Replace `sumUnfinishedBalanceDelta` with:

```java
    @Select("""
            select coalesce(sum(cast(json_unquote(json_extract(o.payload, '$.delta_amount')) as signed)), 0)
            from recharge_outbox o
            where o.org_code = #{orgCode}
              and o.event_type = 'BALANCE_DELTA_FLUSH'
              and o.status <> 'SUCCESS'
              and exists (
                select 1
                from recharge_order_ext ext
                where ext.order_id = o.order_id
                  and ext.org_code = o.org_code
                  and ext.out_order_sn = o.out_order_sn
                  and ext.persist_status = 'DB_PERSISTED'
              )
            """)
    Long sumUnfinishedBalanceDelta(@Param("orgCode") String orgCode);
```

- [ ] **Step 3: Scope observability aggregation to owned orders**

Replace `selectObservabilityOutboxMetrics` with:

```java
    @Select("""
            select
              o.event_type,
              o.status,
              count(*) as count,
              coalesce(timestampdiff(second, min(coalesce(o.created_at, o.updated_at, #{now})), #{now}), 0) as max_age_seconds
            from recharge_outbox o
            where o.status in ('INIT', 'PROCESSING', 'FAILED', 'MANUAL_REQUIRED')
              and exists (
                select 1
                from recharge_order_ext ext
                where ext.order_id = o.order_id
                  and ext.org_code = o.org_code
                  and ext.out_order_sn = o.out_order_sn
                  and ext.persist_status = 'DB_PERSISTED'
              )
            group by o.event_type, o.status
            """)
    List<Map<String, Object>> selectObservabilityOutboxMetrics(@Param("now") LocalDateTime now);
```

- [ ] **Step 4: Add the owned FIFO scan SQL method**

Add this method below the default overloads and before `claimProcessing`:

```java
    /**
     * 扫描新交易引擎拥有的到期事件和租约失效事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。新旧系统共享
     * {@code mc_order} 期间，只有能在 {@code recharge_order_ext} 中确认 `DB_PERSISTED` 的订单事件
     * 才允许进入 outbox 自动任务；可执行集合按 {@code created_at, id} 排序，保证先生成的事件优先处理。
     */
    @Select("""
            <script>
            select
              o.id,
              o.event_id,
              o.event_type,
              o.org_code,
              o.order_id,
              o.out_order_sn,
              o.payload,
              o.status,
              o.retry_count,
              o.max_retry_count,
              o.next_retry_time,
              o.locked_by,
              o.locked_until,
              o.version,
              o.last_error,
              o.shard_key,
              o.created_at,
              o.updated_at
            from recharge_outbox o
            where o.event_type in
              <foreach collection="eventTypes" item="eventType" open="(" separator="," close=")">
                #{eventType}
              </foreach>
              and (
                (o.status in ('INIT', 'FAILED') and o.next_retry_time &lt;= #{now})
                or (o.status = 'PROCESSING' and (o.locked_until &lt; #{now} or o.locked_until is null))
              )
              and exists (
                select 1
                from recharge_order_ext ext
                where ext.order_id = o.order_id
                  and ext.org_code = o.org_code
                  and ext.out_order_sn = o.out_order_sn
                  and ext.persist_status = 'DB_PERSISTED'
              )
              and mod(crc32(o.shard_key), #{shardTotal}) = #{shardIndex}
            order by o.created_at asc, o.id asc
            limit #{limit}
            </script>
            """)
    List<RechargeOutboxDO> selectDueForShardByEventTypesSql(
            @Param("shardTotal") int shardTotal,
            @Param("shardIndex") int shardIndex,
            @Param("limit") int limit,
            @Param("eventTypes") List<String> eventTypes,
            @Param("now") LocalDateTime now);
```

- [ ] **Step 5: Keep Java-side limit safety and delegate to SQL scan**

Replace the body of the existing default `selectDueForShardByEventTypes(..., LocalDateTime now)` with:

```java
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        return selectDueForShardByEventTypesSql(shardTotal, shardIndex, safeLimit, eventTypes, now);
```

- [ ] **Step 6: Add owned claim SQL method**

Add this method below the default `claimProcessing(..., LocalDateTime now)` overload:

```java
    /**
     * 基于事件 ID、版本号和订单归属哨兵抢占事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。抢占阶段重复校验
     * {@code recharge_order_ext}，避免扫描后扩展状态变化或绕过扫描的调用方处理旧系统订单。
     */
    @Update("""
            update recharge_outbox o
            set o.status = 'PROCESSING',
                o.locked_by = #{workerId},
                o.locked_until = #{lockedUntil},
                o.updated_at = #{now},
                o.version = o.version + 1
            where o.event_id = #{eventId}
              and o.version = #{currentVersion}
              and (
                (o.status in ('INIT', 'FAILED') and o.retry_count &lt; o.max_retry_count)
                or (o.status = 'PROCESSING' and (o.locked_until &lt; #{now} or o.locked_until is null))
              )
              and exists (
                select 1
                from recharge_order_ext ext
                where ext.order_id = o.order_id
                  and ext.org_code = o.org_code
                  and ext.out_order_sn = o.out_order_sn
                  and ext.persist_status = 'DB_PERSISTED'
              )
            """)
    int claimProcessingSql(
            @Param("eventId") String eventId,
            @Param("currentVersion") long currentVersion,
            @Param("workerId") String workerId,
            @Param("lockedUntil") LocalDateTime lockedUntil,
            @Param("now") LocalDateTime now);
```

- [ ] **Step 7: Delegate default claim to owned SQL**

Replace the body of the existing default `claimProcessing(..., LocalDateTime now)` with:

```java
        return claimProcessingSql(eventId, currentVersion, workerId, lockedUntil, now);
```

- [ ] **Step 8: Run formatter if the project has one**

Run Maven test compilation rather than introducing a new formatter:

```bash
mvn -pl cardshop-trade-engine-infrastructure -DskipTests test
```

Expected: module compiles. If this command runs tests despite `-DskipTests`, that is acceptable; all executed tests should pass or failures should be unrelated to this syntax change.

---

### Task 4: Verify Red-Green and Broader Test Scope

**Files:**
- Test: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/persistence/MapperContractTest.java`

- [ ] **Step 1: Run mapper contract test and verify GREEN**

Run:

```bash
mvn -pl cardshop-trade-engine-infrastructure -Dtest=MapperContractTest test
```

Expected: PASS. The earlier RED failures are now fixed by mapper SQL.

- [ ] **Step 2: Run outbox-adjacent infrastructure tests**

Run:

```bash
mvn -pl cardshop-trade-engine-infrastructure -Dtest=MapperContractTest,MybatisRechargeBalanceDeltaFlushAdapterTest,MybatisRechargeBalanceReconcileAdapterTest,TradeEngineObservabilityMetricsTest test
```

Expected: PASS. These tests exercise the contracts around outbox scan delegation, balance projection, and observability gauge consumers.

- [ ] **Step 3: Run full infrastructure module tests**

Run:

```bash
mvn -pl cardshop-trade-engine-infrastructure test
```

Expected: PASS. If unrelated dirty-worktree changes cause failures, record the exact failing test class and first failure message before deciding whether the failure is related.

- [ ] **Step 4: Run full Maven tests if time permits**

Run:

```bash
mvn test
```

Expected: PASS. If the full suite is too slow or blocked by unrelated local changes, keep the infrastructure module result as required evidence and report the full-suite blocker.

---

### Task 5: Final Review and Commit

**Files:**
- Review all changed files from Tasks 1-4.

- [ ] **Step 1: Inspect final diff**

Run:

```bash
git diff -- docs/交行直充业务系统详细设计.md docs/交行直充业务系统高可用方案设计.md cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/persistence/MapperContractTest.java cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeOutboxMapper.java
```

Expected: diff is limited to docs, mapper contract tests, and `RechargeOutboxMapper`.

- [ ] **Step 2: Check for accidental joins**

Run:

```bash
rg -n " join | left join | right join | inner join " cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeOutboxMapper.java cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/persistence/MapperContractTest.java docs/交行直充业务系统详细设计.md docs/交行直充业务系统高可用方案设计.md
```

Expected: no output. `exists` subqueries are allowed; joins remain disallowed.

- [ ] **Step 3: Commit implementation only**

Because the worktree contains unrelated existing changes, stage only files changed by this plan:

```bash
git add docs/交行直充业务系统详细设计.md docs/交行直充业务系统高可用方案设计.md cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/persistence/MapperContractTest.java cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeOutboxMapper.java
git commit -m "fix: scope outbox tasks to trade engine orders"
```

Expected: commit succeeds without staging unrelated dirty files.

---

## Self-Review

- Spec coverage: Ownership sentinel, scan boundary, claim boundary, balance/observability aggregation boundary, FIFO ordering, docs updates, and tests are all mapped to tasks.
- Placeholder scan: No deferred implementation markers are present in this plan.
- Type consistency: Method names are consistent across test and implementation steps: `selectDueForShardByEventTypesSql`, `claimProcessingSql`, `sumUnfinishedBalanceDelta`, and `selectObservabilityOutboxMetrics`.

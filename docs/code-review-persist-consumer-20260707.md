# 异步落库 Consumer 代码评审报告

- **Commit**: `a157d8d` — `recharge: add persist consumer core`
- **变更规模**: 25 文件，+1798/-34 行
- **评审时间**: 2026-07-07

---

## 🔴 严重

### 1. `.toCompletableFuture().join()` 无限阻塞，可能耗尽 MQ 消费线程池

- **文件**: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/PersistRechargeOrderConsumerService.java`，`markDbPersisted()` 和 `markPersistFailed()`
- **代码**:

```java
return reliabilityPort.markDbPersisted(...)
        .toCompletableFuture()
        .join();
```

- **问题**: 如果 Redis 连接 hang 或 Lua 脚本执行卡住，`CompletableFuture` 永不完成，调用线程（RabbitMQ consumer 线程）将永久阻塞。连续多次发生后，RabbitMQ consumer 线程池耗尽，整个异步落库链路被拖垮。
- **修复**: 使用 `get(timeout, TimeUnit.SECONDS)` 或 `orTimeout()`，catch `TimeoutException` 并降级为 `markPersistFailed`。

---

### 2. `persistedVersion` 错误赋值为 `orderVersion`，版本博弈逻辑失效

- **文件**: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeOrderPersistenceAdapter.java`，`toExt()` 方法
- **代码**:

```java
ext.setPersistedVersion(snapshot.orderVersion());   // 错误：应该是 persistedVersion()
```

- **问题**: `PersistOrderSnapshot` 有两个独立字段：
  - `orderVersion`: Redis 中订单的 CAS 版本号
  - `persistedVersion`: 该快照已成功落库的次数（初始为 0）
  
  MySQL upsert 中的版本比较逻辑（`values(persisted_version) > persisted_version`）依赖 `persistedVersion` 判断是否应更新状态和来源。错误地赋值为 `orderVersion` 后，MQ 重投同一消息（`orderVersion` 不变）时，`persistedVersion` 不会前进，可能导致正确的重试更新被错误的版本判断拦截。
- **修复**: 改为 `snapshot.persistedVersion()`，并确认 Consumer 在每次落库成功后（如 MQ 重投时）是否需要递增该值。

---

## 🟡 中等

### 3. `LocalDateTime + ZoneOffset.UTC` 与 MySQL `serverTimezone=GMT+8` 不匹配，影响补偿调度时效

- **文件**: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeOrderPersistenceAdapter.java`，`persistedAt()` 方法
- **代码**:

```java
private static LocalDateTime persistedAt(OrderPersistAggregate aggregate) {
    return LocalDateTime.ofInstant(aggregate.persistedAt(), ZoneOffset.UTC);
}
```

- **问题**: `Clock.instant()` 返回 UTC 时刻，`ZoneOffset.UTC` 转换后得到 UTC 表象的 `LocalDateTime`。MySQL JDBC 驱动对 `datetime` 列将 `LocalDateTime` 以字面值传入，不做时区转换。若 JDBC 连接串配置为 `serverTimezone=GMT%2B8`（本项目确认如此），MySQL 会将该值解释为 CST 时间。存储的 `created_at`、`next_retry_time` 等字段与实际 UTC 时间偏移 +8 小时，outbox 轮的补偿调度时效将受影响。
- **修复**: 统一时区策略，二选一：
  - 改为 `ZoneOffset.ofHours(8)` / `ZoneId.of("Asia/Shanghai")`
  - 或将 JDBC 连接串改为 `serverTimezone=UTC`

---

### 4. `McOrderDO.couponType` 从 `Integer` 改为 `String`，缺少 DB 兼容性确认与 Liquibase 变更

- **文件**: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/entity/McOrderDO.java`
- **变更**: `Integer couponType` → `String couponType`
- **问题**:
  1. 旧系统 `McOrderMapper.xml` 中使用 `WHEN mvlo.coupon_type = 'url' ...` 进行比较，说明数据库列为 `varchar`，之前 `Integer` 类型属于错误，本次修复方向正确。
  2. 但本次 commit 未附带 Liquibase changelog 确认 DDL 兼容性。
  3. **关键风险**: 若旧系统曾通过其他路径写入了数字形式的 `coupon_type`（如 `1`、`2`），新代码写入字符串值（如 `"codekey"`）后，依赖 `coupon_type` 做路由的查询将无法匹配历史记录。需确认生产库 `mc_order` 表 `coupon_type` 列是否仅包含 `url`/`code`/`codekey` 等字符串值。
  4. 若存在 `McOrderDO` 的 MyBatis-Plus `LambdaQueryWrapper` 中对 `couponType` 做了数值比较，编译期不会报错但运行期 SQL 会变成字符串比较。
- **修复**: 添加 Liquibase changelog 做 DDL 验证（`columnExists` + 类型检查），并确认生产数据不包含数字值。

---

### 5. `intValue` / `longValue` 解析失败时日志丢失字段信息

- **文件**: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/redis/RedisPersistOrderSnapshotAdapter.java`
- **代码**:

```java
private static long longValue(Map<String, String> fields, String key, long fallback) {
    String value = fields.get(key);
    return StringUtils.hasText(value) ? Long.parseLong(value) : fallback;
}
```

- **问题**: 如果 Redis hash 中某字段被意外写入非数字值（如 `"N/A"`），`Long.parseLong` 抛出 `NumberFormatException` → 外层 `catch(RuntimeException)` 捕获 → 返回 `Optional.empty()` → Consumer 退回 MQ 快照兜底。降级路径本身 OK，但日志仅记录了 `"Redis读取落库完整订单快照失败"`，丢失了具体是哪个字段解析失败。线上排查 Redis 数据质量问题时无法快速定位。
- **修复**: 在 `catch(NumberFormatException)` 时单独打一条包含 `key` 和 `rawValue` 的 error 日志，再重新抛出。

---

## 🟢 轻微

### 6. `fromInitialSnapshot` 中 `couponType` 默认空字符串，旧消息下游影响待确认

- **文件**: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/PersistOrderSnapshot.java`，`fromInitialSnapshot()`
- **代码**:

```java
text(snapshot, "coupon_type", ""),
```

- **问题**: 旧 MQ 消息的 `initialOrderSnapshot` 中不包含 `coupon_type`（本次新增字段），默认设为 `""`。写入 `mc_order.coupon_type` 后成为空字符串。若下游卡管查询依赖 `coupon_type` 做卡密类型路由（如 `url`/`code`/`codekey`），空值可能导致这些订单被跳过。
- **修复**: 确认空值的影响范围；若下游对空值敏感，可设一个明确的兜底值如 `"UNKNOWN"`。

---

### 7. `markPersistFailed` 的 `cause.getMessage()` 可能超长

- **文件**: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/PersistRechargeOrderConsumerService.java`，`markPersistFailed()`
- **代码**:

```java
reliabilityPort.markPersistFailed(
    ...
    cause.getMessage(),   // MySQL 异常消息可能包含完整 SQL，超长
    ...
```

- **问题**: MySQL batch insert 异常的 message 可能非常长（包含完整 SQL 语句和参数），`recharge_order_ext.last_error` 列定义是 `varchar(1000)`，超长会被 MySQL 截断。Redis Lua 侧没有截断逻辑。虽然不会崩溃，但可能写入不完整的错误信息。
- **修复**: 传入前做 `cause.getMessage()` 的长度截断（如前 500 字符 + `"...[truncated]"`）。

---

## 总结

| 严重程度 | 问题 | 文件 | 修复方向 |
|--------|------|------|---------|
| 🔴 严重 | `.join()` 无超时，可能耗尽 MQ 消费线程 | `PersistRechargeOrderConsumerService.java` | 加 `get(timeout, SECONDS)` |
| 🔴 严重 | `persistedVersion` 赋错值 | `MybatisRechargeOrderPersistenceAdapter.java` | 改为 `snapshot.persistedVersion()` |
| 🟡 中等 | `ZoneOffset.UTC` 与 `serverTimezone=GMT+8` 不匹配 | `MybatisRechargeOrderPersistenceAdapter.java` | 统一时区策略 |
| 🟡 中等 | `couponType` 改型无 DB 兼容性确认 | `McOrderDO.java` | 加 Liquibase 验证 + 确认生产数据 |
| 🟡 中等 | Redis number 解析失败日志丢失字段信息 | `RedisPersistOrderSnapshotAdapter.java` | 细化异常日志 |
| 🟢 轻微 | `couponType` 空值下游影响待确认 | `PersistOrderSnapshot.java` | 确认影响面或设兜底值 |
| 🟢 轻微 | error 消息可能超长 | `PersistRechargeOrderConsumerService.java` | 截断 |

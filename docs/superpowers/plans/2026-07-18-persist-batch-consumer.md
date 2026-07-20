# Redis MQ MySQL Batch Persist Consumer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a gray-switchable RabbitMQ batch consumer that persists multiple recharge orders in one MySQL transaction while keeping the existing single-message consumer as the default rollback path.

**Architecture:** Keep `RechargeOrderPersistencePort` and `MybatisRechargeOrderPersistenceAdapter` as the single-message path. Add a separate batch persistence port and MyBatis batch adapter, then add a batch application entrypoint and Rabbit batch listener guarded by `trade.engine.mq.persist-batch-enabled`. The batch path uses whole-batch retry for MySQL failures and acks after MySQL commit even if Redis `markDbPersisted` later fails.

**Tech Stack:** Java 21, Spring Boot, Spring AMQP manual ack, MyBatis/MyBatis-Plus, RabbitMQ, Redis reliability ports, JUnit 5, AssertJ.

---

## Files

- Modify: `docs/交行直充业务系统详细设计.md` - document batch consumer contract and all config fields.
- Modify: `docs/交行直充业务系统高可用方案设计.md` - document HA behavior, tuning, and rollback.
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineProperties.java` - add batch switch, size, receive timeout, and config comments.
- Modify: `cardshop-trade-engine-bootstrap/src/main/resources/application.yml` - add default-off batch business config and conservative listener batch settings.
- Create: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/RechargeOrderBatchPersistencePort.java` - batch write port.
- Create: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/PersistRechargeOrderBatchResult.java` - batch ack/retry result.
- Modify: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/PersistRechargeOrderConsumerService.java` - add batch entrypoint that selects snapshots, calls batch port, then marks Redis.
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisBatchRechargeOrderPersistenceAdapter.java` - separate batch adapter.
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/RechargeOrderPersistenceRowConverter.java` - shared row conversion so single and batch adapters keep one field contract without inheriting from each other.
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeOrderPersistenceAdapter.java` - keep single-message transaction control flow and delegate row conversion to the shared converter.
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/mq/RabbitPersistRechargeOrderBatchMessageListener.java` - batch listener with whole-batch ack/retry/DLQ.
- Modify: `cardshop-trade-engine-bootstrap/src/main/java/com/cardshop/trade/engine/bootstrap/config/TradeEngineConsumerAutoConfiguration.java` - inject optional batch persistence port.
- Modify: `cardshop-trade-engine-bootstrap/src/main/java/com/cardshop/trade/engine/bootstrap/config/TradeEngineRabbitListenerAutoConfiguration.java` - switch single and batch listeners by config.
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineMybatisPersistenceAutoConfiguration.java` - create batch adapter independently from single adapter.
- Test: application batch service, batch listener, properties binding, auto-configuration, and batch adapter tests.

## Tasks

### Task 1: Documentation And Config Contract

- [x] Update detailed design `8.3` to state that batch mode is whole-batch transactional, single-message mode remains default, and `redis_order_written=false` still skips Redis mark.
- [x] Update detailed design config table with:
  - `trade.engine.mq.persist-batch-enabled`, default `false`, enables batch business listener.
  - `trade.engine.mq.persist-batch-size`, default `10`, maximum messages per batch and must be `<= spring.rabbitmq.listener.simple.prefetch`.
  - `trade.engine.mq.persist-batch-receive-timeout-ms`, default `200`, maps to Spring AMQP `batchReceiveTimeout` (partial-batch fill deadline).
  - `trade.engine.mq.persist-batch-poll-timeout-ms`, default `200`, maps to Spring AMQP `receiveTimeout` (per `nextMessage` poll).
  - JDBC URL must include `rewriteBatchedStatements=true` when batch mode is enabled (startup fail-fast).
  - default Spring Rabbit simple listener factory is synchronized by bootstrap auto-configuration when the business batch switch is enabled.
- [x] Update high availability design to state that batch insert does not intentionally lock tables, but batch size and concurrency bound transaction time, lock waits, and retry amplification.

### Task 2: Failing Tests

- [x] Add application tests for batch success, MySQL batch failure without Redis mark, Redis mark failure still acking, and mixed `redis_order_written=false`.
- [x] Add infrastructure tests for `TradeEngineProperties.Mq` binding defaults and explicit batch config.
- [x] Add Rabbit batch listener tests for whole-batch ack, retry publish of every body on batch failure, DLQ on exhausted retry, and malformed payload reject behavior.
- [x] Add batch adapter tests proving it writes `mc_depart_record` by DB idempotency instead of relying on immediate `mc_order` row counts.

### Task 3: Application Batch Contract

- [x] Add `RechargeOrderBatchPersistencePort.persistBatch(List<OrderPersistAggregate>)`.
- [x] Add `PersistRechargeOrderBatchResult` with `shouldAck`, `batchSize`, `persistedCount`, `redisMarkFailedCount`, `failCode`, and `failReason`.
- [x] Extend `PersistRechargeOrderConsumerService` with a constructor accepting an optional batch port while preserving the existing constructor.
- [x] Implement `persistBatch(List<PersistTradeOrderMessage>)`:
  - select snapshots in input order,
  - call the batch port once,
  - on MySQL failure call `markPersistFailed` for Redis-backed messages and return retryable result,
  - after MySQL success call `markDbPersisted` per Redis-backed message and return ackable result even when any mark fails.

### Task 4: Independent Batch Adapter

- [x] Add `MybatisBatchRechargeOrderPersistenceAdapter` with `@Transactional` batch method.
- [x] Keep single adapter behavior unchanged.
- [x] In the batch adapter, sort aggregates by `orgCode`, `outOrderSn`, `orderId`.
- [x] Write in fixed table groups after sorting aggregates: all `mc_order`, then all `recharge_order_ext`, then all `mc_depart_record`, then all `recharge_outbox`.
- [x] Do not branch on the immediate `mc_order.insertIfAbsent` result for `mc_depart_record`; call `insertPreDeductIfAbsent` whenever the snapshot requires a pre-deduct record and rely on `uk_mc_depart_record_trade_engine_key`.

### Task 5: Rabbit Batch Listener And Auto Configuration

- [x] Add batch listener `onMessage(List<Message>, Channel)` with manual ack using the last delivery tag and `multiple=true`.
- [x] On batch failure before max retry, publish every message body to retry exchange with incremented retry count, then ack original batch with `multiple=true`.
- [x] On retry publish failure, `basicNack(lastDeliveryTag, true, true)` so the broker requeues the whole batch.
- [x] On exhausted retry or malformed payload, reject messages individually with `requeue=false` to route them to DLQ.
- [x] Guard single listener with `@ConditionalOnProperty(name="trade.engine.mq.persist-batch-enabled", havingValue="false", matchIfMissing=true)`.
- [x] Guard batch listener with `@ConditionalOnProperty(name="trade.engine.mq.persist-batch-enabled", havingValue="true")`.

### Task 6: Verification

- [x] Run application tests:
  `mvn -pl cardshop-trade-engine-application test -Dtest=PersistRechargeOrderConsumerServiceTest`
- [x] Run infrastructure tests:
  `mvn -pl cardshop-trade-engine-infrastructure test -Dtest=RabbitPersistRechargeOrderMessageListenerTest,RabbitPersistRechargeOrderBatchMessageListenerTest,MybatisRechargeOrderPersistenceAdapterTest,MybatisBatchRechargeOrderPersistenceAdapterTest,TradeEnginePropertiesTest`
- [x] Run bootstrap config tests:
  `mvn -pl cardshop-trade-engine-bootstrap -am test -Dtest=TradeEngineRabbitListenerAutoConfigurationTest,TradeEngineConsumerAutoConfigurationTest,ProfileResourceIsolationTest`
- [x] If those pass, run:
  `mvn -pl cardshop-trade-engine-bootstrap -am test`

## Self-Review

- The plan covers the reviewed spec, the user's decision to implement the batch adapter separately, and the repository rule to update both design documents before code changes.
- No per-item savepoints or partial MySQL commit are introduced in phase 1.
- The plan keeps batch mode default-off and preserves the current single-message rollback path.

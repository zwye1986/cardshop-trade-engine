package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/**
 * Redis/MQ 到 MySQL 异步落库 Consumer 核心编排测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
 * D7A 先验证不绑定 RabbitMQ listener 的核心服务：快照选择、事务写库端口调用和 Redis mark 调用顺序。
 */
class PersistRechargeOrderConsumerServiceTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-07-07T01:30:00Z"), ZoneOffset.UTC);

    @Test
    void mqFallbackSnapshotAndAcceptCommandDoNotBindSigningKey() {
        var snapshot = PersistOrderSnapshot.fromInitialSnapshot(message(1L));
        assertThat(java.util.Arrays.stream(snapshot.getClass().getRecordComponents()).map(java.lang.reflect.RecordComponent::getName))
                .doesNotContain("signingKeyId");
        assertThat(java.util.Arrays.stream(snapshot.toAcceptCommand(1000L, 2000L).getClass().getRecordComponents())
                .map(java.lang.reflect.RecordComponent::getName)).doesNotContain("signingKeyId");
    }

    @Test
    void persistsMqInitialSnapshotAndMarksRedisAfterMysqlCommit() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSISTED);
        assertThat(result.shouldAck()).isTrue();
        assertThat(persistencePort.lastAggregate.persistSource()).isEqualTo(PersistSource.MQ_SNAPSHOT_FALLBACK);
        assertThat(persistencePort.lastAggregate.snapshot().productName()).isEqualTo("交行话费100元");
        assertThat(reliabilityPort.markDbPersistedCalls).isEqualTo(1);
        // 对应详细设计「8.3 异步落库 Consumer」：Redis 快照缺失时，MQ initialOrderSnapshot 的版本就是已落库版本。
        assertThat(reliabilityPort.lastPersistedVersion).isEqualTo(1L);
        assertThat(reliabilityPort.markPersistFailedCalls).isZero();
        assertThat(persistencePort.events).containsExactly("persist");
        assertThat(reliabilityPort.events).containsExactly("markDbPersisted");
    }

    @Test
    void schedulesDispatchTimerAfterMysqlCommitForDispatchablePersistedOrder() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        RecordingDispatchTimerPort dispatchTimerPort = new RecordingDispatchTimerPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                null,
                PersistedOrderStatePort.noop(),
                reliabilityPort,
                dispatchTimerPort,
                CLOCK,
                Duration.ofSeconds(3),
                Duration.ofSeconds(5));

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSISTED);
        assertThat(dispatchTimerPort.calls)
                .containsExactly(new TimerCall(
                        "dispatch-ORDER-1001",
                        OutboxEventType.SUPPLIER_DISPATCH,
                        Duration.ofSeconds(5)));
        assertThat(persistencePort.events).containsExactly("persist");
        assertThat(reliabilityPort.events).containsExactly("markDbPersisted");
    }

    @Test
    void prefersRedisSnapshotWhenRedisHasNewerOrderVersion() {
        PersistOrderSnapshot redisSnapshot = snapshot(2L, "Redis更新商品名", true);
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.of(redisSnapshot));
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSISTED);
        assertThat(persistencePort.lastAggregate.persistSource()).isEqualTo(PersistSource.REDIS_SNAPSHOT);
        assertThat(persistencePort.lastAggregate.snapshot().orderVersion()).isEqualTo(2L);
        assertThat(persistencePort.lastAggregate.snapshot().productName()).isEqualTo("Redis更新商品名");
        // 对应详细设计「8.3 异步落库 Consumer」：选择 Redis 更新版本快照后，markDbPersisted 必须推进到该快照版本。
        assertThat(reliabilityPort.lastPersistedVersion).isEqualTo(2L);
    }

    @Test
    void fallsBackToMqInitialSnapshotWhenRedisSnapshotReadFails() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        snapshotPort.failure = new IllegalStateException("redis snapshot malformed");
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSISTED);
        assertThat(result.shouldAck()).isTrue();
        assertThat(persistencePort.lastAggregate.persistSource()).isEqualTo(PersistSource.MQ_SNAPSHOT_FALLBACK);
        assertThat(persistencePort.lastAggregate.snapshot().productName()).isEqualTo("交行话费100元");
        assertThat(reliabilityPort.markDbPersistedCalls).isEqualTo(1);
        // Redis 快照读取异常会降级使用 MQ 初始快照，因此 persistedVersion 仍应等于 MQ 消息中的 orderVersion。
        assertThat(reliabilityPort.lastPersistedVersion).isEqualTo(1L);
    }

    @Test
    void marksPersistFailedAndKeepsMessageRetryableWhenMysqlWriteFails() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        persistencePort.failure = new IllegalStateException("db unavailable");
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSIST_FAILED);
        assertThat(result.shouldAck()).isFalse();
        assertThat(result.failCode()).isEqualTo("MYSQL_PERSIST_FAILED");
        assertThat(reliabilityPort.markDbPersistedCalls).isZero();
        assertThat(reliabilityPort.markPersistFailedCalls).isEqualTo(1);
        assertThat(reliabilityPort.lastPersistFailedReason).contains("db unavailable");
    }

    @Test
    void persistsMysqlOnlyConfigChangedSnapshotWithoutRedisMark() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(mysqlOnlyConfigChangedMessage());

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSISTED);
        assertThat(result.shouldAck()).isTrue();
        assertThat(persistencePort.lastAggregate.snapshot().validationStatus()).isEqualTo("FAILED");
        assertThat(persistencePort.lastAggregate.snapshot().failCode()).isEqualTo("MERCHANT_CONFIG_STALE");
        assertThat(persistencePort.lastAggregate.snapshot().failReason()).isEqualTo("商户配置缺失或版本不一致");
        assertThat(reliabilityPort.markDbPersistedCalls).isZero();
        assertThat(reliabilityPort.markPersistFailedCalls).isZero();
        assertThat(persistencePort.events).containsExactly("persist");
        assertThat(reliabilityPort.events).isEmpty();
    }

    @Test
    void skipsPersistFailedRedisMarkForMysqlOnlyConfigChangedSnapshotWhenMysqlWriteFails() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        persistencePort.failure = new IllegalStateException("db unavailable");
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(mysqlOnlyConfigChangedMessage());

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSIST_FAILED);
        assertThat(result.shouldAck()).isFalse();
        assertThat(reliabilityPort.markDbPersistedCalls).isZero();
        assertThat(reliabilityPort.markPersistFailedCalls).isZero();
    }

    @Test
    void returnsAckedRedisMarkFailureWhenMarkDbPersistedTimesOutAfterMysqlCommit() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        reliabilityPort.markDbPersistedFuture = new CompletableFuture<>();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK,
                Duration.ofMillis(10));

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSISTED_REDIS_MARK_FAILED);
        assertThat(result.shouldAck()).isTrue();
        assertThat(result.failCode()).isEqualTo(PersistRechargeOrderResult.FAIL_CODE_REDIS_MARK_DB_PERSISTED_FAILED);
        assertThat(result.failReason()).contains("timeout");
        assertThat(reliabilityPort.markDbPersistedCalls).isEqualTo(1);
        assertThat(reliabilityPort.markPersistFailedCalls).isZero();
        assertThat(reliabilityPort.markDbPersistedFuture.toCompletableFuture()).isCancelled();
    }

    @Test
    void cancelsMarkPersistFailedFutureWhenRedisMarkTimesOutAfterMysqlFailure() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        persistencePort.failure = new IllegalStateException("db unavailable");
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        reliabilityPort.markPersistFailedFuture = new CompletableFuture<>();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK,
                Duration.ofMillis(10));

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSIST_FAILED);
        assertThat(result.shouldAck()).isFalse();
        assertThat(reliabilityPort.markPersistFailedCalls).isEqualTo(1);
        assertThat(reliabilityPort.markPersistFailedFuture.toCompletableFuture()).isCancelled();
    }

    @Test
    void persistsSnapshotWithBlankNotifyUrlSoCallbackCanStayDisabled() {
        PersistOrderSnapshot redisSnapshot = snapshot(2L, "Redis更新商品名", true, "");
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.of(redisSnapshot));
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSISTED);
        assertThat(persistencePort.lastAggregate.snapshot().notifyUrl()).isEmpty();
        assertThat(reliabilityPort.markDbPersistedCalls).isEqualTo(1);
    }

    @Test
    void truncatesMysqlExceptionReasonBeforeWritingPersistFailedState() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        persistencePort.failure = new IllegalStateException("x".repeat(1_500));
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderResult result = service.persist(message(1L));

        assertThat(result.status()).isEqualTo(PersistRechargeOrderStatus.PERSIST_FAILED);
        assertThat(result.failReason()).endsWith("...[truncated]");
        assertThat(result.failReason()).hasSizeLessThanOrEqualTo(520);
        assertThat(reliabilityPort.lastPersistFailedReason).isEqualTo(result.failReason());
    }

    @Test
    void persistBatchWritesAggregatesOnceAndMarksEveryRedisBackedMessage() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingBatchPersistencePort batchPersistencePort = new RecordingBatchPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderBatchResult result = service.persistBatch(List.of(
                message(1L),
                message("persist-ORDER-1002-01", "ORDER-1002", "OUT-1002", 1L)));

        assertThat(result.shouldAck()).isTrue();
        assertThat(result.batchSize()).isEqualTo(2);
        assertThat(result.persistedCount()).isEqualTo(2);
        assertThat(result.redisMarkFailedCount()).isZero();
        assertThat(persistencePort.events).isEmpty();
        assertThat(batchPersistencePort.withOutboxCalls).hasSize(1);
        assertThat(batchPersistencePort.withOutboxCalls.getFirst())
                .extracting(aggregate -> aggregate.snapshot().orderId())
                .containsExactly("ORDER-1001", "ORDER-1002");
        assertThat(batchPersistencePort.factsOnlyCalls).isEmpty();
        assertThat(reliabilityPort.markDbPersistedCalls).isEqualTo(2);
        assertThat(reliabilityPort.markPersistFailedCalls).isZero();
    }

    @Test
    void persistBatchReturnsRetryableResultAndDoesNotMarkDbPersistedWhenMysqlBatchFails() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingBatchPersistencePort batchPersistencePort = new RecordingBatchPersistencePort();
        batchPersistencePort.failure = new IllegalStateException("batch db unavailable");
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderBatchResult result = service.persistBatch(List.of(
                message(1L),
                message("persist-ORDER-1002-01", "ORDER-1002", "OUT-1002", 1L)));

        assertThat(result.shouldAck()).isFalse();
        assertThat(result.failCode()).isEqualTo("MYSQL_PERSIST_FAILED");
        assertThat(result.failReason()).contains("batch db unavailable");
        assertThat(reliabilityPort.markDbPersistedCalls).isZero();
        assertThat(reliabilityPort.markPersistFailedCalls).isEqualTo(2);
    }

    @Test
    void persistBatchAcksAfterMysqlCommitWhenRedisMarkFails() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingBatchPersistencePort batchPersistencePort = new RecordingBatchPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        reliabilityPort.markDbPersistedFuture = CompletableFuture.completedFuture(
                MarkPersistStateResult.failed("REDIS_DOWN", "redis unavailable"));
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderBatchResult result = service.persistBatch(List.of(message(1L)));

        assertThat(result.shouldAck()).isTrue();
        assertThat(result.persistedCount()).isEqualTo(1);
        assertThat(result.redisMarkFailedCount()).isEqualTo(1);
        assertThat(result.failCode()).isEqualTo(PersistRechargeOrderResult.FAIL_CODE_REDIS_MARK_DB_PERSISTED_FAILED);
        assertThat(reliabilityPort.markDbPersistedCalls).isEqualTo(1);
        assertThat(reliabilityPort.markPersistFailedCalls).isZero();
    }

    @Test
    void persistBatchSkipsRedisMarkForMysqlOnlyConfigChangedMessageInMixedBatch() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingBatchPersistencePort batchPersistencePort = new RecordingBatchPersistencePort();
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderBatchResult result = service.persistBatch(List.of(message(1L), mysqlOnlyConfigChangedMessage()));

        assertThat(result.shouldAck()).isTrue();
        assertThat(result.batchSize()).isEqualTo(2);
        assertThat(result.persistedCount()).isEqualTo(2);
        assertThat(reliabilityPort.markDbPersistedCalls).isEqualTo(1);
        assertThat(reliabilityPort.markPersistFailedCalls).isZero();
        assertThat(batchPersistencePort.withOutboxCalls.getFirst())
                .extracting(aggregate -> aggregate.snapshot().failCode())
                .containsExactly("", "MERCHANT_CONFIG_STALE");
    }

    @Test
    void persistBatchSkipsAlreadyPersistedDuplicatesAndStillRepairsRedisMark() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingBatchPersistencePort batchPersistencePort = new RecordingBatchPersistencePort();
        RecordingPersistedOrderStatePort persistedStatePort = new RecordingPersistedOrderStatePort();
        persistedStatePort.alreadyPersisted("A36", "OUT-1001", "ORDER-1001", 1L);
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                persistedStatePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderBatchResult result = service.persistBatch(List.of(
                message(1L),
                message("persist-ORDER-1002-01", "ORDER-1002", "OUT-1002", 1L)));

        assertThat(result.shouldAck()).isTrue();
        assertThat(result.batchSize()).isEqualTo(2);
        assertThat(result.persistedCount()).isEqualTo(1);
        assertThat(result.skippedAlreadyPersistedCount()).isEqualTo(1);
        assertThat(persistedStatePort.batchLookupCalls).isEqualTo(1);
        assertThat(batchPersistencePort.withOutboxCalls).hasSize(1);
        assertThat(batchPersistencePort.withOutboxCalls.getFirst())
                .extracting(aggregate -> aggregate.snapshot().orderId())
                .containsExactly("ORDER-1002");
        assertThat(reliabilityPort.markDbPersistedOrderIds)
                .containsExactly("ORDER-1001", "ORDER-1002");
        assertThat(reliabilityPort.markPersistFailedCalls).isZero();
    }

    @Test
    void persistBatchCountsEverySkippedMessageWhenSamePersistedOrderAppearsMoreThanOnce() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingBatchPersistencePort batchPersistencePort = new RecordingBatchPersistencePort();
        RecordingPersistedOrderStatePort persistedStatePort = new RecordingPersistedOrderStatePort();
        persistedStatePort.alreadyPersisted("A36", "OUT-1001", "ORDER-1001", 1L);
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                persistedStatePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderBatchResult result = service.persistBatch(List.of(
                message("persist-ORDER-1001-01", "ORDER-1001", "OUT-1001", 1L),
                message("persist-ORDER-1001-02", "ORDER-1001", "OUT-1001", 1L)));

        assertThat(result.shouldAck()).isTrue();
        assertThat(result.persistedCount()).isZero();
        assertThat(result.skippedAlreadyPersistedCount()).isEqualTo(2);
        assertThat(batchPersistencePort.withOutboxCalls).isEmpty();
        assertThat(reliabilityPort.markDbPersistedOrderIds).containsExactly("ORDER-1001", "ORDER-1001");
    }

    @Test
    void persistBatchUsesSelectedRedisSnapshotVersionBeforeSkippingAlreadyPersistedMessage() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(
                Optional.of(snapshot(2L, "Redis更新商品名", true)));
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingBatchPersistencePort batchPersistencePort = new RecordingBatchPersistencePort();
        RecordingPersistedOrderStatePort persistedStatePort = new RecordingPersistedOrderStatePort();
        persistedStatePort.alreadyPersisted("A36", "OUT-1001", "ORDER-1001", 1L);
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                persistedStatePort,
                reliabilityPort,
                CLOCK);

        PersistRechargeOrderBatchResult result = service.persistBatch(List.of(message(1L)));

        assertThat(result.shouldAck()).isTrue();
        assertThat(result.persistedCount()).isEqualTo(1);
        assertThat(result.skippedAlreadyPersistedCount()).isZero();
        assertThat(persistedStatePort.probes)
                .containsExactly(new PersistedOrderProbe("A36", "OUT-1001", "ORDER-1001", 2L));
        assertThat(batchPersistencePort.withOutboxCalls).hasSize(1);
        assertThat(batchPersistencePort.withOutboxCalls.getFirst().getFirst().snapshot().orderVersion()).isEqualTo(2L);
        assertThat(reliabilityPort.lastPersistedVersion).isEqualTo(2L);
    }

    @Test
    void persistBatchAsyncWritesFactsOnlyForCandidatesButOutboxAndTimerForAllSelectedMessages() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingBatchPersistencePort batchPersistencePort = new RecordingBatchPersistencePort();
        RecordingPersistedOrderStatePort persistedStatePort = new RecordingPersistedOrderStatePort();
        persistedStatePort.alreadyPersisted("A36", "OUT-1001", "ORDER-1001", 1L);
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        RecordingDispatchTimerPort dispatchTimerPort = new RecordingDispatchTimerPort();
        RecordingOutboxAsyncWriter outboxAsyncWriter = new RecordingOutboxAsyncWriter();
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                persistedStatePort,
                reliabilityPort,
                dispatchTimerPort,
                CLOCK,
                Duration.ofSeconds(3),
                Duration.ofSeconds(5),
                outboxAsyncWriter,
                true);

        PersistRechargeOrderBatchResult result = service.persistBatch(List.of(
                message(1L),
                message("persist-ORDER-1002-01", "ORDER-1002", "OUT-1002", 1L)));

        assertThat(result.shouldAck()).isTrue();
        assertThat(result.persistedCount()).isEqualTo(1);
        assertThat(result.skippedAlreadyPersistedCount()).isEqualTo(1);
        assertThat(batchPersistencePort.withOutboxCalls).isEmpty();
        assertThat(batchPersistencePort.factsOnlyCalls).hasSize(1);
        assertThat(batchPersistencePort.factsOnlyCalls.getFirst())
                .extracting(aggregate -> aggregate.snapshot().orderId())
                .containsExactly("ORDER-1002");
        assertThat(outboxAsyncWriter.batchCalls).hasSize(1);
        assertThat(outboxAsyncWriter.batchCalls.getFirst())
                .extracting(aggregate -> aggregate.snapshot().orderId())
                .containsExactly("ORDER-1001", "ORDER-1002");
        assertThat(dispatchTimerPort.calls).containsExactly(
                new TimerCall("dispatch-ORDER-1001", OutboxEventType.SUPPLIER_DISPATCH, Duration.ofSeconds(5)),
                new TimerCall("dispatch-ORDER-1002", OutboxEventType.SUPPLIER_DISPATCH, Duration.ofSeconds(5)));
        assertThat(reliabilityPort.markDbPersistedOrderIds)
                .containsExactly("ORDER-1001", "ORDER-1002");
        assertThat(reliabilityPort.markPersistFailedCalls).isZero();
    }

    @Test
    void persistBatchAsyncWriterFailureKeepsWholeBatchRetryableAndDoesNotMarkDbPersisted() {
        RecordingSnapshotPort snapshotPort = new RecordingSnapshotPort(Optional.empty());
        RecordingPersistencePort persistencePort = new RecordingPersistencePort();
        RecordingBatchPersistencePort batchPersistencePort = new RecordingBatchPersistencePort();
        RecordingPersistedOrderStatePort persistedStatePort = new RecordingPersistedOrderStatePort();
        persistedStatePort.alreadyPersisted("A36", "OUT-1001", "ORDER-1001", 1L);
        RecordingReliabilityPort reliabilityPort = new RecordingReliabilityPort();
        RecordingDispatchTimerPort dispatchTimerPort = new RecordingDispatchTimerPort();
        RecordingOutboxAsyncWriter outboxAsyncWriter = new RecordingOutboxAsyncWriter();
        outboxAsyncWriter.failure = new IllegalStateException("writer unavailable");
        PersistRechargeOrderConsumerService service = new PersistRechargeOrderConsumerService(
                snapshotPort,
                persistencePort,
                batchPersistencePort,
                persistedStatePort,
                reliabilityPort,
                dispatchTimerPort,
                CLOCK,
                Duration.ofSeconds(3),
                Duration.ofSeconds(5),
                outboxAsyncWriter,
                true);

        PersistRechargeOrderBatchResult result = service.persistBatch(List.of(
                message(1L),
                message("persist-ORDER-1002-01", "ORDER-1002", "OUT-1002", 1L)));

        assertThat(result.shouldAck()).isFalse();
        assertThat(result.failCode()).isEqualTo("MYSQL_PERSIST_FAILED");
        assertThat(result.failReason()).contains("writer unavailable");
        assertThat(batchPersistencePort.factsOnlyCalls).hasSize(1);
        assertThat(outboxAsyncWriter.batchCalls).hasSize(1);
        assertThat(reliabilityPort.markDbPersistedCalls).isZero();
        assertThat(reliabilityPort.markPersistFailedOrderIds)
                .containsExactly("ORDER-1001", "ORDER-1002");
        assertThat(dispatchTimerPort.calls).isEmpty();
    }

    private static PersistTradeOrderMessage message(long orderVersion) {
        return message("persist-ORDER-1001-01", "ORDER-1001", "OUT-1001", orderVersion);
    }

    private static PersistTradeOrderMessage message(
            String messageId,
            String orderId,
            String outOrderSn,
            long orderVersion) {
        return new PersistTradeOrderMessage(
                messageId,
                "A36",
                orderId,
                outOrderSn,
                "request-hash-" + orderId,
                orderVersion,
                Map.ofEntries(
                        Map.entry("order_id", orderId),
                        Map.entry("org_code", "A36"),
                        Map.entry("app_id", "app-gray"),
                        Map.entry("signing_key_id", "bound-key"),
                        Map.entry("out_order_sn", outOrderSn),
                        Map.entry("product_id", "PARENT-100"),
                        Map.entry("selected_product_id", "SKU-100"),
                        Map.entry("account", "13800138000"),
                        Map.entry("notify_url", "https://merchant.example/callback"),
                        Map.entry("num", 1),
                        Map.entry("order_price", 5200L),
                        Map.entry("product_name", "交行话费100元"),
                        Map.entry("product_value", 10000L),
                        Map.entry("product_type", 1),
                        Map.entry("coupon_type", "1"),
                        Map.entry("supplier_channel", "BOCOM"),
                        Map.entry("request_hash", "request-hash-" + orderId),
                        Map.entry("request_hash_version", "v1"),
                        Map.entry("request_hash_alg", "SHA-256"),
                        Map.entry("order_status", "ACCEPTED"),
                        Map.entry("validation_status", "PASSED"),
                        Map.entry("fail_code", ""),
                        Map.entry("fail_reason", ""),
                        Map.entry("dispatch_allowed", true),
                        Map.entry("balance_reserved", true),
                        Map.entry("persist_status", "PERSIST_QUEUED"),
                        Map.entry("order_version", orderVersion),
                        Map.entry("persisted_version", 0L),
                        Map.entry("mq_publish_count", 1)));
    }

    private static PersistTradeOrderMessage mysqlOnlyConfigChangedMessage() {
        return new PersistTradeOrderMessage(
                "persist-ORDER-1001-01",
                "A36",
                "ORDER-1001",
                "OUT-1001",
                "request-hash-1001",
                1L,
                Map.ofEntries(
                        Map.entry("order_id", "ORDER-1001"),
                        Map.entry("org_code", "A36"),
                        Map.entry("app_id", "app-gray"),
                        Map.entry("signing_key_id", "bound-key"),
                        Map.entry("out_order_sn", "OUT-1001"),
                        Map.entry("product_id", "PARENT-100"),
                        Map.entry("selected_product_id", "SKU-100"),
                        Map.entry("account", "13800138000"),
                        Map.entry("notify_url", "https://merchant.example/callback"),
                        Map.entry("num", 1),
                        Map.entry("order_price", 5200L),
                        Map.entry("product_name", "交行话费100元"),
                        Map.entry("product_value", 10000L),
                        Map.entry("product_type", 1),
                        Map.entry("coupon_type", "1"),
                        Map.entry("supplier_channel", "BOCOM"),
                        Map.entry("request_hash", "request-hash-1001"),
                        Map.entry("request_hash_version", "v1"),
                        Map.entry("request_hash_alg", "SHA-256"),
                        Map.entry("order_status", "VALIDATION_FAILED"),
                        Map.entry("validation_status", "FAILED"),
                        Map.entry("fail_code", "MERCHANT_CONFIG_STALE"),
                        Map.entry("fail_reason", "商户配置缺失或版本不一致"),
                        Map.entry("dispatch_allowed", false),
                        Map.entry("balance_reserved", false),
                        Map.entry("persist_status", "PENDING_MQ"),
                        Map.entry("order_version", 1L),
                        Map.entry("persisted_version", 0L),
                        Map.entry("mq_publish_count", 1),
                        Map.entry("redis_order_written", false)));
    }

    private static PersistOrderSnapshot snapshot(long orderVersion, String productName, boolean dispatchAllowed) {
        return snapshot(orderVersion, productName, dispatchAllowed, "https://merchant.example/callback");
    }

    private static PersistOrderSnapshot snapshot(
            long orderVersion,
            String productName,
            boolean dispatchAllowed,
            String notifyUrl) {
        return new PersistOrderSnapshot(
                "A36",
                "app-gray",
                "ORDER-1001",
                "OUT-1001",
                "PARENT-100",
                "SKU-100",
                "13800138000",
                notifyUrl,
                1,
                5200L,
                productName,
                10000L,
                1,
                "1",
                "BOCOM",
                "request-hash-1001",
                "v1",
                "SHA-256",
                "ACCEPTED",
                "PASSED",
                "",
                "",
                dispatchAllowed,
                dispatchAllowed,
                "PERSIST_QUEUED",
                orderVersion,
                0L,
                1);
    }

    private static final class RecordingSnapshotPort implements PersistOrderSnapshotPort {

        private final Optional<PersistOrderSnapshot> snapshot;
        private RuntimeException failure;

        private RecordingSnapshotPort(Optional<PersistOrderSnapshot> snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public Optional<PersistOrderSnapshot> findLatestSnapshot(String orgCode, String outOrderSn, String orderId) {
            if (failure != null) {
                throw failure;
            }
            return snapshot;
        }
    }

    private static final class RecordingPersistencePort implements RechargeOrderPersistencePort {

        private final List<String> events = new ArrayList<>();
        private OrderPersistAggregate lastAggregate;
        private RuntimeException failure;

        @Override
        public void persist(OrderPersistAggregate aggregate) {
            events.add("persist");
            lastAggregate = aggregate;
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class RecordingBatchPersistencePort implements RechargeOrderBatchPersistencePort {

        private final List<List<OrderPersistAggregate>> withOutboxCalls = new ArrayList<>();
        private final List<List<OrderPersistAggregate>> factsOnlyCalls = new ArrayList<>();
        private RuntimeException failure;

        @Override
        public void persistBatchWithOutbox(List<OrderPersistAggregate> aggregates) {
            withOutboxCalls.add(List.copyOf(aggregates));
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public void persistBatchFactsOnly(List<OrderPersistAggregate> aggregates) {
            factsOnlyCalls.add(List.copyOf(aggregates));
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class RecordingOutboxAsyncWriter implements RechargePersistOutboxAsyncWriter {

        private final List<List<OrderPersistAggregate>> batchCalls = new ArrayList<>();
        private RuntimeException failure;

        @Override
        public void writeBatch(List<OrderPersistAggregate> aggregates) {
            batchCalls.add(List.copyOf(aggregates));
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class RecordingPersistedOrderStatePort implements PersistedOrderStatePort {

        private final Set<PersistedOrderProbe> alreadyPersisted = new HashSet<>();
        private final List<PersistedOrderProbe> probes = new ArrayList<>();
        private int batchLookupCalls;

        private void alreadyPersisted(String orgCode, String outOrderSn, String orderId, long orderVersion) {
            alreadyPersisted.add(new PersistedOrderProbe(orgCode, outOrderSn, orderId, orderVersion));
        }

        @Override
        public boolean isPersistedAtLeast(String orgCode, String outOrderSn, String orderId, long orderVersion) {
            PersistedOrderProbe probe = new PersistedOrderProbe(orgCode, outOrderSn, orderId, orderVersion);
            probes.add(probe);
            return alreadyPersisted.contains(probe);
        }

        @Override
        public Set<PersistedOrderProbe> findPersistedAtLeast(Collection<PersistedOrderProbe> requestedProbes) {
            batchLookupCalls++;
            probes.addAll(requestedProbes);
            Set<PersistedOrderProbe> result = new HashSet<>(requestedProbes);
            result.retainAll(alreadyPersisted);
            return result;
        }
    }

    private static final class RecordingReliabilityPort implements RechargeAcceptReliabilityPort {

        private final List<String> events = new ArrayList<>();
        private final List<String> markDbPersistedOrderIds = new ArrayList<>();
        private final List<String> markPersistFailedOrderIds = new ArrayList<>();
        private int markDbPersistedCalls;
        private int markPersistFailedCalls;
        private long lastPersistedVersion = -1L;
        private String lastPersistFailedReason;
        private CompletionStage<MarkPersistStateResult> markDbPersistedFuture =
                CompletableFuture.completedFuture(MarkPersistStateResult.success());
        private CompletionStage<MarkPersistStateResult> markPersistFailedFuture =
                CompletableFuture.completedFuture(MarkPersistStateResult.success());

        @Override
        public CompletionStage<ReplicationWaitResult> waitForReplication(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult) {
            throw new UnsupportedOperationException("D7A consumer does not call WAIT");
        }

        @Override
        public CompletionStage<CancelUnconfirmedAcceptResult> cancelUnconfirmedAccept(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String reason) {
            throw new UnsupportedOperationException("D7A consumer does not cancel accept");
        }

        @Override
        public CompletionStage<MarkPersistQueuedResult> markPersistQueued(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String persistMessageId,
                int publishCount) {
            throw new UnsupportedOperationException("D7A consumer does not mark queued");
        }

        @Override
        public CompletionStage<MarkPersistStateResult> markDbPersisted(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                long persistedVersion,
                long nowEpochMillis) {
            events.add("markDbPersisted");
            markDbPersistedOrderIds.add(acceptResult.orderId());
            markDbPersistedCalls++;
            lastPersistedVersion = persistedVersion;
            return markDbPersistedFuture;
        }

        @Override
        public CompletionStage<MarkPersistStateResult> markPersistFailed(
                AcceptRechargeOrderCommand command,
                AcceptRechargeOrderResult acceptResult,
                String failCode,
                String failReason,
                int retryCount,
                long nextRetryEpochMillis,
                long nowEpochMillis) {
            events.add("markPersistFailed");
            markPersistFailedCalls++;
            markPersistFailedOrderIds.add(acceptResult.orderId());
            lastPersistFailedReason = failReason;
            return markPersistFailedFuture;
        }
    }

    private record TimerCall(String eventId, OutboxEventType eventType, Duration delay) {}

    private static final class RecordingDispatchTimerPort implements RechargeDispatchTimerPort {

        private final List<TimerCall> calls = new ArrayList<>();

        @Override
        public void schedule(String eventId, OutboxEventType eventType, Duration delay) {
            calls.add(new TimerCall(eventId, eventType, delay));
        }
    }
}

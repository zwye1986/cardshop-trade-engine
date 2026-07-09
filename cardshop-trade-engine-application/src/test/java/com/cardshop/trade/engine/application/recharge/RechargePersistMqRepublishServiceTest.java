package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * Redis pending MQ 补投服务测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「17. 分片补偿与对账任务」
 * 和「20.13.8 异步落库 Consumer」。补投任务只能读取 Redis pending 中仍未落库的订单快照，
 * 重新发布 MySQL 落库消息；发布失败时保留 pending，等待下一轮补偿。
 */
class RechargePersistMqRepublishServiceTest {

    @Test
    void republishesPendingSnapshotsAndMarksPublishedOnConfirmAck() {
        FakePendingPort pendingPort = new FakePendingPort(List.of(pendingSnapshot("ORDER-1"), pendingSnapshot("ORDER-2")));
        FakePublisher publisher = new FakePublisher(true);
        var service = new RechargePersistMqRepublishService(pendingPort, publisher);

        RechargeCompensationBatchResult result = service.republishDue(16, 3, 20, "worker-A");

        assertThat(result.scanned()).isEqualTo(2);
        assertThat(result.succeeded()).isEqualTo(2);
        assertThat(result.failed()).isZero();
        assertThat(publisher.messages).extracting(PersistTradeOrderMessage::orderId)
                .containsExactly("ORDER-1", "ORDER-2");
        assertThat(pendingPort.selectCalls).containsExactly("16:3:20:worker-A");
        assertThat(pendingPort.markedPublished).containsExactly("ORDER-1", "ORDER-2");
        assertThat(pendingPort.markedFailed).isEmpty();
    }

    @Test
    void keepsPendingWhenPublishFails() {
        FakePendingPort pendingPort = new FakePendingPort(List.of(pendingSnapshot("ORDER-1")));
        FakePublisher publisher = new FakePublisher(false);
        var service = new RechargePersistMqRepublishService(pendingPort, publisher);

        RechargeCompensationBatchResult result = service.republishDue(16, 3, 20, "worker-A");

        assertThat(result.scanned()).isEqualTo(1);
        assertThat(result.succeeded()).isZero();
        assertThat(result.failed()).isEqualTo(1);
        assertThat(pendingPort.markedPublished).isEmpty();
        assertThat(pendingPort.markedFailed).containsExactly("ORDER-1");
    }

    private static PendingPersistOrderSnapshot pendingSnapshot(String orderId) {
        return new PendingPersistOrderSnapshot(
                "MSG-" + orderId,
                "A36",
                orderId,
                "OUT-" + orderId,
                "HASH-" + orderId,
                1L,
                Map.of("order_id", orderId));
    }

    private static final class FakePendingPort implements PendingPersistOrderPort {

        private final List<PendingPersistOrderSnapshot> snapshots;
        private final List<String> selectCalls = new ArrayList<>();
        private final List<String> markedPublished = new ArrayList<>();
        private final List<String> markedFailed = new ArrayList<>();

        private FakePendingPort(List<PendingPersistOrderSnapshot> snapshots) {
            this.snapshots = snapshots;
        }

        @Override
        public List<PendingPersistOrderSnapshot> selectDueForShard(
                int shardTotal,
                int shardIndex,
                int limit,
                String workerId) {
            selectCalls.add(shardTotal + ":" + shardIndex + ":" + limit + ":" + workerId);
            return snapshots;
        }

        @Override
        public void markRepublished(PendingPersistOrderSnapshot snapshot, PersistMessagePublishResult result, String workerId) {
            markedPublished.add(snapshot.orderId());
        }

        @Override
        public void markRepublishFailed(PendingPersistOrderSnapshot snapshot, String reason, String workerId) {
            markedFailed.add(snapshot.orderId());
        }
    }

    private static final class FakePublisher implements PersistRechargeOrderMessagePublisher {

        private final boolean ack;
        private final List<PersistTradeOrderMessage> messages = new ArrayList<>();

        private FakePublisher(boolean ack) {
            this.ack = ack;
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public java.util.concurrent.CompletionStage<PersistMessagePublishResult> publish(PersistTradeOrderMessage message) {
            messages.add(message);
            PersistMessagePublishResult result = ack
                    ? PersistMessagePublishResult.confirmed(message.messageId(), 1)
                    : PersistMessagePublishResult.failed(message.messageId(), 1, "BROKER_NACK", "broker nack");
            return CompletableFuture.completedFuture(result);
        }
    }
}

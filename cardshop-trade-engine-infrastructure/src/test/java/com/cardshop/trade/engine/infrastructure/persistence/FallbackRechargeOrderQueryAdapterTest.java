package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Redis 优先、MySQL 兜底查单组合端口测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。
 * 查单入口必须优先返回 Redis 受理期快照；只有 Redis 未命中时，才读取 MySQL 已落库事实。
 */
class FallbackRechargeOrderQueryAdapterTest {

    @Test
    void returnsPrimaryRedisSnapshotWithoutCallingMysqlFallback() {
        RechargeOrderSnapshot redisSnapshot = snapshot("ORDER-REDIS", "PERSIST_QUEUED");
        RecordingQueryPort primary = new RecordingQueryPort(Optional.of(redisSnapshot));
        RecordingQueryPort fallback = new RecordingQueryPort(Optional.of(snapshot("ORDER-MYSQL", "DB_PERSISTED")));
        FallbackRechargeOrderQueryAdapter adapter = new FallbackRechargeOrderQueryAdapter(primary, fallback);

        Optional<RechargeOrderSnapshot> result = adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001");

        assertThat(result).containsSame(redisSnapshot);
        assertThat(primary.calls).isEqualTo(1);
        assertThat(fallback.calls).isZero();
    }

    @Test
    void fallsBackToMysqlSnapshotWhenRedisMisses() {
        RechargeOrderSnapshot mysqlSnapshot = snapshot("ORDER-MYSQL", "DB_PERSISTED");
        RecordingQueryPort primary = new RecordingQueryPort(Optional.empty());
        RecordingQueryPort fallback = new RecordingQueryPort(Optional.of(mysqlSnapshot));
        FallbackRechargeOrderQueryAdapter adapter = new FallbackRechargeOrderQueryAdapter(primary, fallback);

        Optional<RechargeOrderSnapshot> result = adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001");

        assertThat(result).containsSame(mysqlSnapshot);
        assertThat(primary.calls).isEqualTo(1);
        assertThat(fallback.calls).isEqualTo(1);
    }

    private static RechargeOrderSnapshot snapshot(String orderId, String persistStatus) {
        return new RechargeOrderSnapshot(
                orderId,
                "OUT-1001",
                "ACCEPTED",
                persistStatus,
                "PASSED",
                "",
                "",
                true);
    }

    private static final class RecordingQueryPort implements RechargeOrderQueryPort {

        private final Optional<RechargeOrderSnapshot> result;
        private int calls;

        private RecordingQueryPort(Optional<RechargeOrderSnapshot> result) {
            this.result = result;
        }

        @Override
        public Optional<RechargeOrderSnapshot> findByOrgCodeAndOutOrderSn(String orgCode, String outOrderSn) {
            calls++;
            return result;
        }
    }
}

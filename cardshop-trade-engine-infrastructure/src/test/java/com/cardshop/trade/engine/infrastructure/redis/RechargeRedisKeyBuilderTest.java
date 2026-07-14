package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.recharge.BalanceReservationReleaseCommand;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;

/**
 * Redis Key 构造契约测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6.1 Redis Lua 入参与 Key 构造」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「5. 关键 Redis Key 设计」。
 * 下单 Lua 的 KEYS 顺序是 Java 适配器和 Lua 脚本之间的强契约，任何顺序漂移都可能导致扣错余额或写错订单。
 */
class RechargeRedisKeyBuilderTest {

    @Test
    void acceptKeysUseStableOrderAndOrgHashTag() {
        var builder = new RechargeRedisKeyBuilder("cardshop:trade", 128);
        var command = sampleCommand();
        int bucket = builder.pendingOrgBucket(command.orgCode());

        assertThat(builder.acceptKeys(command))
                .containsExactly(
                        "cardshop:trade:recharge:{A36}:balance",
                        "cardshop:trade:recharge:{A36}:idem:OUT-1001",
                        "cardshop:trade:recharge:{A36}:order:OUT-1001",
                        "cardshop:trade:recharge:{A36}:reservation:202607051337458961474292",
                        "cardshop:trade:recharge:{A36}:persist:pending",
                        "cardshop:trade:recharge:{A36}:order:event_index:202607051337458961474292",
                        "cardshop:trade:recharge:{A36}:order:event:202607051337458961474292:1",
                        "cardshop:trade:recharge:{A36}:nonce:nonce-12345678901234567890123456",
                        "cardshop:trade:recharge:{A36}:merchant:config",
                        "cardshop:trade:recharge:{A36}:product:73301289:config",
                        "cardshop:trade:recharge:{A36}:balance:repairing_lock",
                        "cardshop:trade:recharge:persist:pending:org_index:" + bucket);
    }

    @Test
    void cancelUnconfirmedAcceptKeysUseStableOrder() {
        var builder = new RechargeRedisKeyBuilder("cardshop:trade", 128);
        var command = sampleCommand();
        int bucket = builder.pendingOrgBucket(command.orgCode());

        assertThat(builder.cancelUnconfirmedAcceptKeys(command))
                .containsExactly(
                        "cardshop:trade:recharge:{A36}:balance",
                        "cardshop:trade:recharge:{A36}:idem:OUT-1001",
                        "cardshop:trade:recharge:{A36}:order:OUT-1001",
                        "cardshop:trade:recharge:{A36}:reservation:202607051337458961474292",
                        "cardshop:trade:recharge:{A36}:persist:pending",
                        "cardshop:trade:recharge:{A36}:order:event_index:202607051337458961474292",
                        "cardshop:trade:recharge:{A36}:order:event:202607051337458961474292:2",
                        "cardshop:trade:recharge:persist:pending:org_index:" + bucket);
    }

    @Test
    void markPersistQueuedKeysUseStableOrder() {
        var builder = new RechargeRedisKeyBuilder("cardshop:trade", 128);
        var command = sampleCommand();

        assertThat(builder.markPersistQueuedKeys(command))
                .containsExactly(
                        "cardshop:trade:recharge:{A36}:order:OUT-1001",
                        "cardshop:trade:recharge:{A36}:idem:OUT-1001",
                        "cardshop:trade:recharge:{A36}:order:event_index:202607051337458961474292",
                        "cardshop:trade:recharge:{A36}:order:event:202607051337458961474292:2");
    }

    @Test
    void markDbPersistedKeysUseStableOrderAndOrgIndexBucket() {
        var builder = new RechargeRedisKeyBuilder("cardshop:trade", 128);
        var command = sampleCommand();
        int bucket = builder.pendingOrgBucket(command.orgCode());

        assertThat(builder.markDbPersistedKeys(command))
                .containsExactly(
                        "cardshop:trade:recharge:{A36}:order:OUT-1001",
                        "cardshop:trade:recharge:{A36}:idem:OUT-1001",
                        "cardshop:trade:recharge:{A36}:persist:pending",
                        "cardshop:trade:recharge:{A36}:order:event_index:202607051337458961474292",
                        "cardshop:trade:recharge:{A36}:order:event:202607051337458961474292:3",
                        "cardshop:trade:recharge:persist:pending:org_index:" + bucket);
    }

    @Test
    void markPersistFailedKeysUseStableOrderAndOrgIndexBucket() {
        var builder = new RechargeRedisKeyBuilder("cardshop:trade", 128);
        var command = sampleCommand();
        int bucket = builder.pendingOrgBucket(command.orgCode());

        assertThat(builder.markPersistFailedKeys(command))
                .containsExactly(
                        "cardshop:trade:recharge:{A36}:order:OUT-1001",
                        "cardshop:trade:recharge:{A36}:idem:OUT-1001",
                        "cardshop:trade:recharge:{A36}:persist:pending",
                        "cardshop:trade:recharge:{A36}:order:event_index:202607051337458961474292",
                        "cardshop:trade:recharge:{A36}:order:event:202607051337458961474292:3",
                        "cardshop:trade:recharge:persist:pending:org_index:" + bucket);
    }

    @Test
    void releaseBalanceReservationKeysUseStableOrderAndOrgHashTag() {
        var builder = new RechargeRedisKeyBuilder("cardshop:trade", 128);

        assertThat(builder.releaseBalanceReservationKeys(sampleReleaseCommand()))
                .containsExactly(
                        "cardshop:trade:recharge:{A36}:balance",
                        "cardshop:trade:recharge:{A36}:order:OUT-1001",
                        "cardshop:trade:recharge:{A36}:reservation:202607051337458961474292",
                        "cardshop:trade:recharge:{A36}:order:event_index:202607051337458961474292",
                        "cardshop:trade:recharge:{A36}:order:event:202607051337458961474292:4",
                        "cardshop:trade:recharge:{A36}:release:dispatch-202607051337458961474292");
    }

    @Test
    void orderKeyUsesSameHashTagAsAcceptLua() {
        var builder = new RechargeRedisKeyBuilder("cardshop:trade", 128);

        assertThat(builder.orderKey("A36", "OUT-1001"))
                .isEqualTo("cardshop:trade:recharge:{A36}:order:OUT-1001");
    }

    @Test
    void pendingOrgBucketUsesCrc32FloorMod() {
        var builder = new RechargeRedisKeyBuilder("cardshop:trade", 128);

        assertThat(builder.pendingOrgBucket("A36")).isEqualTo(expectedCrc32Bucket("A36", 128));
        assertThat(builder.pendingOrgBucket("A36")).isBetween(0, 127);
    }

    @Test
    void rejectsInvalidPrefixAndBucketCount() {
        assertThatThrownBy(() -> new RechargeRedisKeyBuilder(" ", 128))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keyPrefix");
        assertThatThrownBy(() -> new RechargeRedisKeyBuilder("cardshop:trade", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pendingOrgBucketCount");
    }

    private static int expectedCrc32Bucket(String orgCode, int bucketCount) {
        CRC32 crc32 = new CRC32();
        crc32.update(orgCode.getBytes(StandardCharsets.UTF_8));
        return Math.floorMod((int) crc32.getValue(), bucketCount);
    }

    private static AcceptRechargeOrderCommand sampleCommand() {
        return new AcceptRechargeOrderCommand(
                "A36",
                "app-1",
                "OUT-1001",
                "202607051337458961474292",
                "73301280",
                "73301289",
                "18186222336",
                "http://merchant.example/callback",
                "nonce-12345678901234567890123456",
                1,
                5200L,
                "话费50元",
                5000L,
                1,
                "codekey",
                "TF_KA",
                "hash-v1",
                "v1",
                "SHA-256",
                ValidationStatus.PASSED,
                "",
                "",
                7L,
                11L,
                1783224958000L,
                1783225258000L);
    }

    private static BalanceReservationReleaseCommand sampleReleaseCommand() {
        return new BalanceReservationReleaseCommand(
                "A36",
                "202607051337458961474292",
                "OUT-1001",
                5200L,
                "RELEASE",
                "dispatch-202607051337458961474292",
                "供应商失败");
    }
}

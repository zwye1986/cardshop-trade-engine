package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Java 到 Lua 的入参映射契约测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6.1 Redis Lua 入参与 Key 构造」。
 * Redis Lua 参数是位置语义，测试必须锁住 ARGV 顺序，避免后续字段扩展造成脚本误读价格、版本或 TTL。
 */
class AcceptRechargeOrderLuaArgumentsTest {

    @Test
    void mapsAcceptedCandidateToStableLuaKeysAndArguments() {
        var keyBuilder = new RechargeRedisKeyBuilder("cardshop:trade", 128);
        var command = sampleCommand(ValidationStatus.PASSED, "", "");

        var arguments = AcceptRechargeOrderLuaArguments.from(
                command, keyBuilder, Duration.ofDays(7), Duration.ofMinutes(10));

        assertThat(arguments.keys()).isEqualTo(keyBuilder.acceptKeys(command));
        assertThat(arguments.args())
                .containsExactly(
                        "A36",
                        "app-1",
                        "OUT-1001",
                        "202607051337458961474292",
                        "73301280",
                        "73301289",
                        "18186222336",
                        "http://merchant.example/callback",
                        "nonce-12345678901234567890123456",
                        "1",
                        "5200",
                        "话费50元",
                        "5000",
                        "1",
                        "codekey",
                        "TF_KA",
                        "hash-v1",
                        "v1",
                        "SHA-256",
                        "PASSED",
                        "",
                        "",
                        "7",
                        "11",
                        "selection-fingerprint",
                        "true",
                        "1783224958000",
                        "1783225258000",
                        "604800000",
                        "600000", "order-key-id");
    }

    @Test
    void preservesCandidateFailureForLuaToWriteFailedOrder() {
        var keyBuilder = new RechargeRedisKeyBuilder("cardshop:trade", 128);
        var command = sampleCommand(ValidationStatus.FAILED, "INVALID_NUM", "购买数量不支持");

        var arguments = AcceptRechargeOrderLuaArguments.from(
                command, keyBuilder, Duration.ofDays(7), Duration.ofMinutes(10));

        assertThat(arguments.args()).containsSubsequence("FAILED", "INVALID_NUM", "购买数量不支持");
    }

    private static AcceptRechargeOrderCommand sampleCommand(
            ValidationStatus validationStatus, String failCode, String failReason) {
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
                validationStatus,
                failCode,
                failReason,
                7L,
                11L,
                "selection-fingerprint",
                true,
                1783224958000L,
                1783225258000L, "order-key-id");
    }
}

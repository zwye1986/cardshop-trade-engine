package com.cardshop.trade.engine.application.recharge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.cardshop.trade.engine.domain.order.ValidationStatus;
import org.junit.jupiter.api.Test;

/**
 * Redis-first 下单命令契约测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
 * 应用层命令必须把 Java 候选校验、配置版本、request_hash 和订单快照字段一次性带给基础设施层，
 * 避免 Redis Lua 只能看到部分上下文而无法原子写入成功/失败订单。
 */
class AcceptRechargeOrderCommandTest {

    @Test
    void carriesBusinessSnapshotAndConfigurationVersionsForLuaAccept() {
        var command = sampleCommand(ValidationStatus.PASSED, "", "");

        assertThat(command.orgCode()).isEqualTo("A36");
        assertThat(command.outOrderSn()).isEqualTo("OUT-1001");
        assertThat(command.orderId()).isEqualTo("202607051337458961474292");
        assertThat(command.productId()).isEqualTo("73301280");
        assertThat(command.selectedProductId()).isEqualTo("73301280");
        assertThat(command.requestHash()).isEqualTo("hash-v1");
        assertThat(command.requestHashVersion()).isEqualTo("v1");
        assertThat(command.candidateValidationStatus()).isEqualTo(ValidationStatus.PASSED);
        assertThat(command.merchantConfigVersion()).isEqualTo(7);
        assertThat(command.productConfigVersion()).isEqualTo(11);
    }

    @Test
    void rejectsBlankBusinessIdentityBeforeBuildingRedisKeys() {
        assertThatThrownBy(() -> new AcceptRechargeOrderCommand(
                        " ",
                        "app-1",
                        "OUT-1001",
                        "202607051337458961474292",
                        "73301280",
                        "73301280",
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
                        1783225258000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("orgCode");
    }

    @Test
    void allowsBlankNotifyUrlSoOrderCanDisableMerchantCallback() {
        var command = new AcceptRechargeOrderCommand(
                "A36",
                "app-1",
                "OUT-1001",
                "202607051337458961474292",
                "73301280",
                "73301280",
                "18186222336",
                "",
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

        assertThat(command.notifyUrl()).isEmpty();
    }

    private static AcceptRechargeOrderCommand sampleCommand(
            ValidationStatus validationStatus, String failCode, String failReason) {
        return new AcceptRechargeOrderCommand(
                "A36",
                "app-1",
                "OUT-1001",
                "202607051337458961474292",
                "73301280",
                "73301280",
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
                1783224958000L,
                1783225258000L);
    }
}

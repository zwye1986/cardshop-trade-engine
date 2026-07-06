package com.cardshop.trade.engine.application.v1;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * V1 旧协议返回码模板测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.1 协议基线」。旧协议允许不同业务语义复用同一个
 * code，例如订单重复和订单不存在都返回 505；测试在这里锁定“code 可复用、语义不可合并”的约束。
 */
class V1CardApiResultStatusTest {

    @Test
    void keepsDuplicateOrderAndOrderNotFoundAsDifferentStatusesWithSameLegacyCode() {
        V1CardApiResult duplicate = V1CardApiResultStatus.ORDER_DUPLICATE.result();
        V1CardApiResult notFound = V1CardApiResultStatus.ORDER_NOT_FOUND.result();

        assertThat(V1CardApiResultStatus.ORDER_DUPLICATE).isNotSameAs(V1CardApiResultStatus.ORDER_NOT_FOUND);
        assertThat(duplicate.code()).isEqualTo("505");
        assertThat(duplicate.msg()).isEqualTo("订单号重复");
        assertThat(notFound.code()).isEqualTo("505");
        assertThat(notFound.msg()).isEqualTo("订单不存在");
    }
}

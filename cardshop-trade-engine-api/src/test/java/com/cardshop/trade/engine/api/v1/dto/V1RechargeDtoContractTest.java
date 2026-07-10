package com.cardshop.trade.engine.api.v1.dto;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * V1 DTO 契约测试。
 *
 * <p>DTO 字段对应 {@code docs/交行直充业务系统详细设计.md} 的「14.1 V1 DTO 字段」
 * 和「3. API/V1 接入层」。D2 只固化字段载体，D3 再接 Controller、验签和路由。
 */
class V1RechargeDtoContractTest {

    @Test
    void createOrderRequestKeepsExternalProtocolNames() {
        var request = new V1RechargeCreateOrderRequest(
                "A11",
                "OUT-1",
                "74600828",
                "74600827",
                "13800138000",
                1,
                10000L,
                1,
                "https://merchant.example/callback",
                "nonce-1",
                "20260702120000",
                "sign");

        assertThat(request.orgCode()).isEqualTo("A11");
        assertThat(request.outOrderSn()).isEqualTo("OUT-1");
        assertThat(request.num()).isEqualTo(1);
        assertThat(request.orderPrice()).isEqualTo(10000L);
    }

    @Test
    void responseWrapperUsesV1CardApiShape() {
        var body = V1CardApiResponse.success(new V1RechargeOrderResponse("ORDER-1", "OUT-1", "2", "处理中"));

        assertThat(body.code()).isEqualTo("200");
        assertThat(body.msg()).isEqualTo("操作成功");
        assertThat(body.data().orderId()).isEqualTo("ORDER-1");
    }
}

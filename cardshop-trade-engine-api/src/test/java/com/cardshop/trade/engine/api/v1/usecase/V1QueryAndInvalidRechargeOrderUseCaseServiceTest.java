package com.cardshop.trade.engine.api.v1.usecase;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.api.v1.codec.V1RequestBodyCodec;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderResponse;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolPrecheckService;
import com.cardshop.trade.engine.api.v1.protocol.V1ProtocolSettings;
import com.cardshop.trade.engine.api.v1.security.V12SignatureVerifier;
import com.cardshop.trade.engine.api.v1.security.V1TimestampSettings;
import com.cardshop.trade.engine.api.v1.security.V1TimestampValidator;
import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;
import com.cardshop.trade.engine.application.v1.V1CardApiResult;
import com.cardshop.trade.engine.application.v1.V1MerchantCommand;
import com.cardshop.trade.engine.application.v1.V1MerchantProfile;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxCommand;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxPort;
import java.util.ArrayList;
import java.util.List;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * V1 查单/作废用例测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」和「9.2 作废设计」。
 * D4 先形成 Redis 快照闭环：未落库订单查单/作废都返回处理中，避免同步调用供应商或直接修改旧库状态。
 */
class V1QueryAndInvalidRechargeOrderUseCaseServiceTest {

    private static final Instant NOW = Instant.parse("2026-07-05T04:15:58Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void queryReturnsProcessingWhenRedisOrderIsStillPendingPersistence() {
        var service = new V1QueryRechargeOrderUseCaseService(
                precheckService(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "", true, 7L)),
                (orgCode, outOrderSn) -> Optional.of(snapshot("ACCEPTED", "PERSIST_QUEUED", "PASSED", "", "")));

        V1CardApiResult result = service.queryRechargeOrder(command("OUT-QUERY-1", "nonce-query-12345678901234567890"));

        assertThat(result.code()).isEqualTo("200");
        V1RechargeOrderResponse data = (V1RechargeOrderResponse) result.data();
        assertThat(data.orderStatus()).isEqualTo("2");
        assertThat(data.orderStatusDesc()).isEqualTo("处理中");
    }

    @Test
    void queryReturnsFailedWhenRedisSnapshotContainsValidationFailure() {
        var service = new V1QueryRechargeOrderUseCaseService(
                precheckService(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "", true, 7L)),
                (orgCode, outOrderSn) -> Optional.of(snapshot(
                        "VALIDATION_FAILED",
                        "PENDING_MQ",
                        "FAILED",
                        "PRODUCT_NO_PERMISSION",
                        "商品无权限")));

        V1CardApiResult result = service.queryRechargeOrder(command("OUT-QUERY-2", "nonce-query-22345678901234567890"));

        assertThat(result.code()).isEqualTo("200");
        V1RechargeOrderResponse data = (V1RechargeOrderResponse) result.data();
        assertThat(data.orderStatus()).isEqualTo("6");
        assertThat(data.orderStatusDesc()).isEqualTo("商品无权限");
    }

    @Test
    void queryReturnsSuccessWhenRedisSnapshotContainsTerminalSuccess() {
        var service = new V1QueryRechargeOrderUseCaseService(
                precheckService(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "", true, 7L)),
                (orgCode, outOrderSn) -> Optional.of(snapshot("SUCCESS", "DB_PERSISTED", "PASSED", "", "")));

        V1CardApiResult result = service.queryRechargeOrder(command("OUT-QUERY-3", "nonce-query-42345678901234567890"));

        assertThat(result.code()).isEqualTo("200");
        V1RechargeOrderResponse data = (V1RechargeOrderResponse) result.data();
        assertThat(data.orderStatus()).isEqualTo("12");
        assertThat(data.orderStatusDesc()).isEqualTo("成功");
    }

    @Test
    void queryReturnsRechargingWhenRedisSnapshotContainsSupplierAccepted() {
        var service = new V1QueryRechargeOrderUseCaseService(
                precheckService(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "", true, 7L)),
                (orgCode, outOrderSn) -> Optional.of(snapshot("SUPPLIER_ACCEPTED", "DB_PERSISTED", "PASSED", "", "")));

        V1CardApiResult result = service.queryRechargeOrder(command("OUT-QUERY-4", "nonce-query-52345678901234567890"));

        assertThat(result.code()).isEqualTo("200");
        V1RechargeOrderResponse data = (V1RechargeOrderResponse) result.data();
        assertThat(data.orderStatus()).isEqualTo("13");
        assertThat(data.orderStatusDesc()).isEqualTo("充值中");
    }

    @Test
    void queryReturnsRefundedWhenRedisSnapshotContainsRefunded() {
        var service = new V1QueryRechargeOrderUseCaseService(
                precheckService(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "", true, 7L)),
                (orgCode, outOrderSn) -> Optional.of(snapshot("REFUNDED", "DB_PERSISTED", "PASSED", "", "")));

        V1CardApiResult result = service.queryRechargeOrder(command("OUT-QUERY-5", "nonce-query-62345678901234567890"));

        assertThat(result.code()).isEqualTo("200");
        V1RechargeOrderResponse data = (V1RechargeOrderResponse) result.data();
        assertThat(data.orderStatus()).isEqualTo("11");
        assertThat(data.orderStatusDesc()).isEqualTo("已退款");
    }

    @ParameterizedTest
    @ValueSource(strings = {"2", "3"})
    void queryReturnsVoucherFactsWithoutTurningCancelledVoucherIntoFailedOrder(String stateType) {
        var service = new V1QueryRechargeOrderUseCaseService(
                precheckService(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "", true, 7L)),
                (orgCode, outOrderSn) -> Optional.of(voucherSnapshot(stateType, "3", "", "2026-10-19 10:20:30")));

        V1CardApiResult result = service.queryRechargeOrder(command("OUT-QUERY-1", "nonce-query-12345678901234567890"));

        assertThat(result.code()).isEqualTo("200");
        V1RechargeOrderResponse data = (V1RechargeOrderResponse) result.data();
        assertThat(data.orderStatus()).isEqualTo("12");
        assertThat(data.stateType()).isEqualTo(stateType);
        assertThat(data.useState()).isEqualTo("3");
        assertThat(data.useTime()).isEmpty();
        assertThat(data.invalidTime()).isEqualTo("2026-10-19 10:20:30");
    }

    @ParameterizedTest
    @ValueSource(strings = {"2", "3"})
    void queryReturnsRedeemedVoucherUseTime(String stateType) {
        var service = new V1QueryRechargeOrderUseCaseService(
                precheckService(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "", true, 7L)),
                (orgCode, outOrderSn) -> Optional.of(voucherSnapshot(stateType, "2", "2026-10-19 10:20:30", "")));

        V1RechargeOrderResponse data = (V1RechargeOrderResponse) service.queryRechargeOrder(
                command("OUT-QUERY-1", "nonce-query-12345678901234567890")).data();

        assertThat(data.orderStatus()).isEqualTo("12");
        assertThat(data.stateType()).isEqualTo(stateType);
        assertThat(data.useState()).isEqualTo("2");
        assertThat(data.useTime()).isEqualTo("2026-10-19 10:20:30");
        assertThat(data.invalidTime()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"2", "3"})
    void repeatedInvalidReturnsCompletedVoucherFactWithoutEnqueuingAgain(String stateType) {
        RecordingInvalidOutboxPort invalidOutboxPort = new RecordingInvalidOutboxPort();
        var service = new V1InvalidRechargeOrderUseCaseService(
                precheckService(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "", true, 7L)),
                (orgCode, outOrderSn) -> Optional.of(voucherSnapshot(stateType, "3", "", "2026-10-19 10:20:30")),
                invalidOutboxPort);

        V1CardApiResult result = service.invalidRechargeOrder(command("OUT-QUERY-1", "nonce-invalid-123456789012345678"));

        assertThat(result.code()).isEqualTo("200");
        V1RechargeOrderResponse data = (V1RechargeOrderResponse) result.data();
        assertThat(data.orderStatus()).isEqualTo("12");
        assertThat(data.stateType()).isEqualTo(stateType);
        assertThat(data.useState()).isEqualTo("3");
        assertThat(data.useTime()).isEmpty();
        assertThat(data.invalidTime()).isEqualTo("2026-10-19 10:20:30");
        assertThat(invalidOutboxPort.commands).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"2", "3"})
    void invalidWithoutTrustedCompletionTimeStillEnqueuesAsynchronousWork(String stateType) {
        RecordingInvalidOutboxPort invalidOutboxPort = new RecordingInvalidOutboxPort();
        var service = new V1InvalidRechargeOrderUseCaseService(
                precheckService(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "", true, 7L)),
                (orgCode, outOrderSn) -> Optional.of(voucherSnapshot(stateType, "3", "", "")),
                invalidOutboxPort);

        V1RechargeOrderResponse data = (V1RechargeOrderResponse) service.invalidRechargeOrder(
                command("OUT-QUERY-1", "nonce-invalid-123456789012345678")).data();

        assertThat(data.orderStatus()).isEqualTo("2");
        assertThat(invalidOutboxPort.commands).hasSize(1);
    }

    @Test
    void invalidReturnsProcessingForOrderThatHasNotBeenPersistedToMysql() {
        RecordingInvalidOutboxPort invalidOutboxPort = new RecordingInvalidOutboxPort();
        var service = new V1InvalidRechargeOrderUseCaseService(
                precheckService(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "", true, 7L)),
                (orgCode, outOrderSn) -> Optional.of(snapshot("ACCEPTED", "PENDING_MQ", "PASSED", "", "")),
                invalidOutboxPort);

        V1CardApiResult result = service.invalidRechargeOrder(command("OUT-INVALID-1", "nonce-invalid-123456789012345678"));

        assertThat(result.code()).isEqualTo("200");
        V1RechargeOrderResponse data = (V1RechargeOrderResponse) result.data();
        assertThat(data.orderStatus()).isEqualTo("2");
        assertThat(data.orderStatusDesc()).isEqualTo("处理中");
        assertThat(invalidOutboxPort.commands).isEmpty();
    }

    @Test
    void invalidEnqueuesInvalidOrderOutboxWhenMysqlOrderHasPersisted() {
        RecordingInvalidOutboxPort invalidOutboxPort = new RecordingInvalidOutboxPort();
        var service = new V1InvalidRechargeOrderUseCaseService(
                precheckService(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "", true, 7L)),
                (orgCode, outOrderSn) -> Optional.of(snapshot("SUPPLIER_ACCEPTED", "DB_PERSISTED", "PASSED", "", "")),
                invalidOutboxPort);

        V1CardApiResult result = service.invalidRechargeOrder(command("OUT-INVALID-2", "nonce-invalid-223456789012345678"));

        assertThat(result.code()).isEqualTo("200");
        V1RechargeOrderResponse data = (V1RechargeOrderResponse) result.data();
        assertThat(data.orderStatus()).isEqualTo("2");
        assertThat(data.orderStatusDesc()).isEqualTo("处理中");
        assertThat(invalidOutboxPort.commands).singleElement().satisfies(command -> {
            assertThat(command.orgCode()).isEqualTo("A36");
            assertThat(command.orderId()).isEqualTo("202607051337458961474292");
            assertThat(command.outOrderSn()).isEqualTo("OUT-QUERY-1");
            assertThat(command.reason()).isEqualTo("");
        });
    }

    @Test
    void queryReturnsNotFoundWhenOrderSnapshotDoesNotExist() {
        var service = new V1QueryRechargeOrderUseCaseService(
                precheckService(),
                (appId, orgCode) -> Optional.of(new V1MerchantProfile(appId, orgCode, "", true, 7L)),
                (orgCode, outOrderSn) -> Optional.empty());

        V1CardApiResult result = service.queryRechargeOrder(command("OUT-MISSING", "nonce-query-32345678901234567890"));

        assertThat(result.code()).isEqualTo("505");
        assertThat(result.msg()).isEqualTo("订单不存在");
        assertThat(result.data()).isNull();
    }

    private static V1ProtocolPrecheckService precheckService() {
        return new V1ProtocolPrecheckService(
                new V1RequestBodyCodec(),
                new V1TimestampValidator(new V1TimestampSettings(Duration.ofSeconds(300)), CLOCK),
                new V12SignatureVerifier(),
                new V1ProtocolSettings(false));
    }

    private static V1MerchantCommand command(String outOrderSn, String nonceStr) {
        String signSource = "nonce_str=%s,out_order_sn=%s,timestamp=1783224958";
        String sign = com.cardshop.trade.engine.api.v1.V12TestCredentials.signSource(signSource.formatted(nonceStr, outOrderSn));
        String rawBody = """
                {
                  "out_order_sn": "%s",
                  "timestamp": "1783224958",
                  "nonce_str": "%s",
                  "sign": "%s"
                }
                """.formatted(outOrderSn, nonceStr, sign);
        return com.cardshop.trade.engine.api.v1.V12TestCredentials.command("app-gray", "A36", rawBody);
    }

    private static RechargeOrderSnapshot snapshot(
            String orderStatus,
            String persistStatus,
            String validationStatus,
            String failCode,
            String failReason) {
        return new RechargeOrderSnapshot(
                "202607051337458961474292",
                "OUT-QUERY-1",
                orderStatus,
                persistStatus,
                validationStatus,
                failCode,
                failReason,
                true);
    }

    private static RechargeOrderSnapshot voucherSnapshot(String stateType, String useState, String useTime, String invalidTime) {
        return new RechargeOrderSnapshot(
                "202607051337458961474292", "OUT-QUERY-1", "SUCCESS", "DB_PERSISTED", "PASSED",
                "", "", true, stateType, useState, useTime, invalidTime);
    }

    private static final class RecordingInvalidOutboxPort implements InvalidRechargeOrderOutboxPort {

        private final List<InvalidRechargeOrderOutboxCommand> commands = new ArrayList<>();

        @Override
        public void enqueueInvalidOrder(InvalidRechargeOrderOutboxCommand command) {
            commands.add(command);
        }
    }
}

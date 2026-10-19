package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import java.time.LocalDateTime;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * MySQL 查单兜底适配器测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。Redis 快照丢失或已过期时，
 * 查单需要从 `mc_order` 读取订单事实，并尽量合并 `recharge_order_ext` 中的持久化状态。
 */
class MybatisRechargeOrderQueryAdapterTest {

    @Test
    void mapsPersistedSuccessOrderToSnapshot() {
        McOrderDO order = order(12, "", "");
        RechargeOrderExtDO ext = ext("DB_PERSISTED", "PASSED", true);
        MybatisRechargeOrderQueryAdapter adapter = new MybatisRechargeOrderQueryAdapter(
                (orgCode, outOrderSn) -> order,
                (orgCode, outOrderSn) -> ext);

        Optional<RechargeOrderSnapshot> result = adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001");

        assertThat(result).hasValueSatisfying(snapshot -> {
            assertThat(snapshot.orderId()).isEqualTo("ORDER-1001");
            assertThat(snapshot.outOrderSn()).isEqualTo("OUT-1001");
            assertThat(snapshot.orderStatus()).isEqualTo("SUCCESS");
            assertThat(snapshot.persistStatus()).isEqualTo("DB_PERSISTED");
            assertThat(snapshot.validationStatus()).isEqualTo("PASSED");
            assertThat(snapshot.dispatchAllowed()).isTrue();
        });
    }

    @Test
    void fallsBackToMcOrderFieldsWhenRechargeExtIsMissing() {
        McOrderDO order = order(6, "SUPPLIER_FAILED", "供应商失败");
        MybatisRechargeOrderQueryAdapter adapter = new MybatisRechargeOrderQueryAdapter(
                (orgCode, outOrderSn) -> order,
                (orgCode, outOrderSn) -> null);

        Optional<RechargeOrderSnapshot> result = adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001");

        assertThat(result).hasValueSatisfying(snapshot -> {
            assertThat(snapshot.orderStatus()).isEqualTo("FAILED");
            assertThat(snapshot.persistStatus()).isEqualTo("DB_PERSISTED");
            assertThat(snapshot.validationStatus()).isEqualTo("FAILED");
            assertThat(snapshot.failCode()).isEqualTo("SUPPLIER_FAILED");
            assertThat(snapshot.failReason()).isEqualTo("供应商失败");
            assertThat(snapshot.dispatchAllowed()).isFalse();
        });
    }

    @Test
    void returnsEmptyWhenMcOrderDoesNotExist() {
        MybatisRechargeOrderQueryAdapter adapter = new MybatisRechargeOrderQueryAdapter(
                (orgCode, outOrderSn) -> null,
                (orgCode, outOrderSn) -> ext("DB_PERSISTED", "PASSED", true));

        assertThat(adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-MISSING")).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"MINISO,3", "OTHER,2"})
    void cancelledVoucherKeepsSuccessfulOrderAndOriginalInvalidTime(String supplierChannel, String stateType) {
        RechargeOrderExtDO ext = ext("DB_PERSISTED", "PASSED", true);
        ext.setDispatchStatus("CANCELLED");
        ext.setVoucherUseState("3");
        ext.setVoucherInvalidTime(LocalDateTime.parse("2026-10-19T10:20:30"));
        McOrderDO order = order(12, "", "");
        order.setSupplierChannel(supplierChannel);
        MybatisRechargeOrderQueryAdapter adapter = new MybatisRechargeOrderQueryAdapter(
                (orgCode, outOrderSn) -> order, (orgCode, outOrderSn) -> ext);

        assertThat(adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001")).hasValueSatisfying(snapshot -> {
            assertThat(snapshot.orderStatus()).isEqualTo("SUCCESS");
            assertThat(snapshot.stateType()).isEqualTo(stateType);
            assertThat(snapshot.useState()).isEqualTo("3");
            assertThat(snapshot.useTime()).isEmpty();
            assertThat(snapshot.invalidTime()).isEqualTo("2026-10-19 10:20:30");
        });
    }

    @ParameterizedTest
    @CsvSource({"MINISO,3", "OTHER,2"})
    void redeemedVoucherExposesItsUseTimeWithoutChangingOrderSuccess(String supplierChannel, String stateType) {
        RechargeOrderExtDO ext = ext("DB_PERSISTED", "PASSED", true);
        ext.setVoucherUseState("2");
        ext.setVoucherUseTime(LocalDateTime.parse("2026-10-19T10:20:30"));
        McOrderDO order = order(12, "", "");
        order.setSupplierChannel(supplierChannel);
        MybatisRechargeOrderQueryAdapter adapter = new MybatisRechargeOrderQueryAdapter(
                (orgCode, outOrderSn) -> order, (orgCode, outOrderSn) -> ext);

        assertThat(adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001")).hasValueSatisfying(snapshot -> {
            assertThat(snapshot.orderStatus()).isEqualTo("SUCCESS");
            assertThat(snapshot.stateType()).isEqualTo(stateType);
            assertThat(snapshot.useState()).isEqualTo("2");
            assertThat(snapshot.useTime()).isEqualTo("2026-10-19 10:20:30");
            assertThat(snapshot.invalidTime()).isEmpty();
        });
    }

    @Test
    void legacyCancelledOrderIsNotReportedAsRechargeFailureOrGivenInventedVoucherFact() {
        RechargeOrderExtDO ext = ext("DB_PERSISTED", "FAILED", false);
        ext.setDispatchStatus("CANCELLED");
        MybatisRechargeOrderQueryAdapter adapter = new MybatisRechargeOrderQueryAdapter(
                (orgCode, outOrderSn) -> order(6, "", "旧作废"), (orgCode, outOrderSn) -> ext);

        assertThat(adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001")).hasValueSatisfying(snapshot -> {
            assertThat(snapshot.orderStatus()).isEqualTo("SUCCESS");
            assertThat(snapshot.validationStatus()).isEqualTo("PASSED");
            assertThat(snapshot.stateType()).isEmpty();
            assertThat(snapshot.useState()).isEmpty();
            assertThat(snapshot.useTime()).isEmpty();
            assertThat(snapshot.invalidTime()).isEmpty();
        });
    }

    @Test
    void mismatchedExtensionCannotProvideVoucherFacts() {
        RechargeOrderExtDO ext = ext("DB_PERSISTED", "PASSED", true);
        ext.setOrderId("OTHER-ORDER");
        ext.setVoucherUseState("3");
        ext.setVoucherInvalidTime(LocalDateTime.parse("2026-10-19T10:20:30"));
        MybatisRechargeOrderQueryAdapter adapter = new MybatisRechargeOrderQueryAdapter(
                (orgCode, outOrderSn) -> order(12, "", ""), (orgCode, outOrderSn) -> ext);

        assertThat(adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001")).hasValueSatisfying(snapshot -> {
            assertThat(snapshot.stateType()).isEmpty();
            assertThat(snapshot.useState()).isEmpty();
            assertThat(snapshot.invalidTime()).isEmpty();
        });
    }

    private static McOrderDO order(int orderStatus, String failCode, String failReason) {
        McOrderDO order = new McOrderDO();
        order.setOrgCode("A36");
        order.setOrderId("ORDER-1001");
        order.setOutOrderId("OUT-1001");
        order.setOrderStatus(orderStatus);
        order.setProductType(1);
        order.setFailCode(failCode);
        order.setFailReason(failReason);
        order.setCreateTime(LocalDateTime.parse("2026-07-06T09:30:00"));
        return order;
    }

    private static RechargeOrderExtDO ext(String persistStatus, String validationStatus, boolean dispatchAllowed) {
        RechargeOrderExtDO ext = new RechargeOrderExtDO();
        ext.setOrderId("ORDER-1001");
        ext.setOrgCode("A36");
        ext.setOutOrderSn("OUT-1001");
        ext.setPersistStatus(persistStatus);
        ext.setValidationStatus(validationStatus);
        ext.setDispatchAllowed(dispatchAllowed);
        return ext;
    }
}

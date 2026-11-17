package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import com.cardshop.trade.engine.application.recharge.RechargeOrderIdentityConflictException;
import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import java.util.List;
import java.time.LocalDateTime;
import java.util.Optional;
import org.apache.ibatis.builder.MapperBuilderAssistant;
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

    private final McOrderMapper orders = mock(McOrderMapper.class);
    private final RechargeOrderExtMapper extensions = mock(RechargeOrderExtMapper.class);

    @org.junit.jupiter.api.BeforeAll
    static void initializeLambdaMetadata() {
        TableInfoHelper.initTableInfo(new MapperBuilderAssistant(new MybatisConfiguration(), "query-test"), McOrderDO.class);
    }

    @Test
    void mapsPersistedSuccessOrderToSnapshot() {
        McOrderDO order = order(12, "", "");
        RechargeOrderExtDO ext = ext("DB_PERSISTED", "PASSED", true);
        MybatisRechargeOrderQueryAdapter adapter = adapter(order, ext);

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
        order.setTradeEngineOwned(0);
        MybatisRechargeOrderQueryAdapter adapter = adapter(order, null);

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
    void rejectsOrphanExtensionInsteadOfReportingOrderMissing() {
        MybatisRechargeOrderQueryAdapter adapter = adapter(null, ext("DB_PERSISTED", "PASSED", true));

        assertThatThrownBy(() -> adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001"))
                .isInstanceOf(RechargeOrderIdentityConflictException.class);
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
        MybatisRechargeOrderQueryAdapter adapter = adapter(order, ext);

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
        MybatisRechargeOrderQueryAdapter adapter = adapter(order, ext);

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
        MybatisRechargeOrderQueryAdapter adapter = adapter(order(6, "", "旧作废"), ext);

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
        MybatisRechargeOrderQueryAdapter adapter = adapter(order(12, "", ""), ext);

        assertThatThrownBy(() -> adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001"))
                .isInstanceOf(RechargeOrderIdentityConflictException.class);
    }

    @Test
    void extensionLocatesNewOrderWithoutReadingAmbiguousLegacyCandidates() {
        var adapter = adapter(order(12, "", ""), ext("DB_PERSISTED", "PASSED", true));

        assertThat(adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001")).isPresent();
        verify(orders).selectByOrderId("ORDER-1001");
        verify(orders, never()).selectList(any());
        verify(orders, never()).selectByOrgAndOutOrder(any(), any());
    }

    @Test
    void rejectsMultipleLegacyCandidatesButStillReportsPersistedOccupation() {
        var order = order(12, "", "");
        order.setTradeEngineOwned(0);
        var adapter = adapter(order, null);
        when(orders.selectList(any())).thenReturn(List.of(order, order));

        assertThatThrownBy(() -> adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001"))
                .isInstanceOf(RechargeOrderIdentityConflictException.class);
        assertThat(adapter.existsPersistedOrder("A36", "OUT-1001")).isTrue();
    }

    @Test
    void rejectsOwnedOrderWithoutExtensionButKeepsIdentityOccupied() {
        var adapter = adapter(order(12, "", ""), null);

        assertThatThrownBy(() -> adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001"))
                .isInstanceOf(RechargeOrderIdentityConflictException.class);
        assertThat(adapter.existsPersistedOrder("A36", "OUT-1001")).isTrue();
    }

    @Test
    void reportsOrphanExtensionAsOccupiedWithoutRequiringDisplaySnapshot() {
        var adapter = adapter(null, ext("DB_PERSISTED", "PASSED", true));

        assertThat(adapter.existsPersistedOrder("A36", "OUT-1001")).isTrue();
        verifyNoInteractions(orders);
    }

    @Test
    void returnsMissingOnlyWhenBothTablesMissAndPropagatesDatabaseFailures() {
        var adapter = adapter(null, null);
        assertThat(adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001")).isEmpty();
        assertThat(adapter.existsPersistedOrder("A36", "OUT-1001")).isFalse();
        when(orders.selectList(any())).thenThrow(new IllegalStateException("db down"));
        assertThatThrownBy(() -> adapter.existsPersistedOrder("A36", "OUT-1001"))
                .isInstanceOf(IllegalStateException.class).hasMessage("db down");
        assertThatThrownBy(() -> adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001"))
                .isInstanceOf(IllegalStateException.class).hasMessage("db down");
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"org", "out", "owned", "type", "ext-org", "ext-out"})
    void rejectsEveryExtensionIdentityMismatch(String mismatch) {
        var order = order(12, "", "");
        var ext = ext("DB_PERSISTED", "PASSED", true);
        switch (mismatch) {
            case "org" -> order.setOrgCode("OTHER");
            case "out" -> order.setOutOrderId("OTHER");
            case "owned" -> order.setTradeEngineOwned(0);
            case "type" -> order.setProductType(0);
            case "ext-org" -> ext.setOrgCode("OTHER");
            case "ext-out" -> ext.setOutOrderSn("OTHER");
            default -> throw new AssertionError(mismatch);
        }
        var adapter = adapter(order, ext);
        assertThatThrownBy(() -> adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001"))
                .isInstanceOf(RechargeOrderIdentityConflictException.class);
    }

    @Test
    void boundsCandidateAndOccupationSqlWithoutUsingScopedGeneratedColumn() {
        var adapter = adapter(null, null);
        var wrappers = new java.util.ArrayList<com.baomidou.mybatisplus.core.conditions.Wrapper<McOrderDO>>();
        when(orders.selectList(any())).thenAnswer(invocation -> {
            wrappers.add(invocation.getArgument(0));
            return List.of();
        });
        adapter.findByOrgCodeAndOutOrderSn("A36", "OUT-1001");
        adapter.existsPersistedOrder("A36", "OUT-1001");

        assertThat(wrappers.getFirst().getSqlSegment()).contains("out_orderId =", "org_code =", "product_type =", "ORDER BY id ASC", "limit 2")
                .doesNotContain("recharge_idempotency_key");
        assertThat(wrappers.getLast().getSqlSegment()).contains("out_orderId =", "org_code =", "product_type =", "limit 1")
                .doesNotContain("recharge_idempotency_key");
        assertThat(wrappers.getLast().getSqlSelect()).isEqualTo("order_Id");
    }

    private MybatisRechargeOrderQueryAdapter adapter(McOrderDO order, RechargeOrderExtDO ext) {
        when(orders.selectByOrderId(any())).thenReturn(order);
        when(orders.selectList(any())).thenReturn(order == null ? List.of() : List.of(order));
        when(extensions.selectByOrgAndOutOrder(any(), any())).thenReturn(ext);
        return new MybatisRechargeOrderQueryAdapter(orders, extensions);
    }

    private static McOrderDO order(int orderStatus, String failCode, String failReason) {
        McOrderDO order = new McOrderDO();
        order.setTradeEngineOwned(McOrderDO.TRADE_ENGINE_OWNED);
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

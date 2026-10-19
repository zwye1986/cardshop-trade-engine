package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.cardshop.trade.engine.application.recharge.FulfillmentDispatchStatus;
import com.cardshop.trade.engine.application.recharge.OrderFulfillmentStatusUpdate;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderSupplierFlowMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;

/**
 * 券状态与订单履约状态的真实MyBatisPlus/H2事务验证。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」和「9.6 券状态」。
 * 使用真实Mapper执行适配器的CASE/COALESCE参数绑定，通过独立JDBC查询验证提交及回滚；
 * 不用Mapper桩或仅检查Wrapper字符串替代数据库执行。最小表仅包含本次UPDATE实际使用的字段。
 */
class RechargeFulfillmentVoucherStatePersistenceTest {

    private static final String ORDER_ID = "GENERIC-ORDER-1";
    private static final Instant FIRST_INVALID_TIME = Instant.parse("2026-10-19T02:30:45.987654321Z");
    private static final LocalDateTime FIRST_INVALID_LOCAL_TIME = LocalDateTime.parse("2026-10-19T10:30:45");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-19T00:00:00Z"), ZoneOffset.UTC);

    private JdbcTemplate jdbc;
    private RechargeFulfillmentOrderPort port;

    @BeforeEach
    void setUpRealMappersAndTransactionalAdapter() throws Exception {
        var dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("""
                create table mc_order (
                    order_Id varchar(100) primary key,
                    order_status int,
                    data_status varchar(32),
                    flow_id bigint,
                    fail_code varchar(128),
                    fail_reason varchar(512),
                    remark varchar(512),
                    edit_time timestamp(6)
                )
                """);
        jdbc.execute("""
                create table recharge_order_ext (
                    order_id varchar(100) primary key,
                    dispatch_status varchar(32),
                    last_error varchar(512),
                    updated_at timestamp(6),
                    voucher_use_state varchar(1),
                    voucher_use_time timestamp(6),
                    voucher_invalid_time timestamp(6)
                )
                """);
        jdbc.update("insert into mc_order(order_Id, order_status, data_status, remark) values (?, 12, ?, ?)",
                ORDER_ID, "PROCESSED", "before-update");
        jdbc.update("""
                insert into recharge_order_ext(order_id, dispatch_status, voucher_use_state)
                values (?, 'SUCCESS', '1')
                """, ORDER_ID);

        var configuration = new MybatisConfiguration();
        configuration.addMapper(McOrderMapper.class);
        configuration.addMapper(RechargeOrderExtMapper.class);
        configuration.addMapper(McOrderSupplierFlowMapper.class);
        var factory = new MybatisSqlSessionFactoryBean();
        factory.setDataSource(dataSource);
        factory.setConfiguration(configuration);
        var session = new SqlSessionTemplate(factory.getObject());
        var target = new MybatisRechargeFulfillmentOrderAdapter(
                session.getMapper(McOrderMapper.class),
                session.getMapper(RechargeOrderExtMapper.class),
                session.getMapper(McOrderSupplierFlowMapper.class),
                new ObjectMapper(), CLOCK);
        // 直接调用端口代理，确保生产@Transactional生效，而不是由测试事务掩盖提交边界。
        var proxyFactory = new ProxyFactory(target);
        proxyFactory.addAdvice(new TransactionInterceptor(new DataSourceTransactionManager(dataSource),
                new AnnotationTransactionAttributeSource()));
        port = (RechargeFulfillmentOrderPort) proxyFactory.getProxy();
    }

    @AfterEach
    void closeDatabase() {
        jdbc.execute("shutdown");
    }

    @Test
    void cancellationKeepsSuccessfulOrderAndFirstTrustedInvalidTimeAtSecondPrecision() {
        port.updateFulfillmentStatus(update(FulfillmentDispatchStatus.SUCCESS));
        assertThat(orderStatus()).isEqualTo(12);
        assertThat(voucherState()).isEqualTo("1");

        port.updateFulfillmentStatus(update(FulfillmentDispatchStatus.CANCELLED).withInvalidTime(FIRST_INVALID_TIME));

        assertThat(orderStatus()).isEqualTo(12);
        assertThat(voucherState()).isEqualTo("3");
        assertThat(dispatchStatus()).isEqualTo("CANCELLED");
        assertThat(invalidTime()).isEqualTo(FIRST_INVALID_LOCAL_TIME);

        // 重试携带另一个时间仍必须保留首次事实；这里执行的是数据库COALESCE及真实时间参数绑定。
        port.updateFulfillmentStatus(update(FulfillmentDispatchStatus.CANCELLED)
                .withInvalidTime(FIRST_INVALID_TIME.plusSeconds(600)));

        assertThat(orderStatus()).isEqualTo(12);
        assertThat(voucherState()).isEqualTo("3");
        assertThat(invalidTime()).isEqualTo(FIRST_INVALID_LOCAL_TIME);
    }

    @Test
    void missingVoucherFactRowRejectsCancellationAndRollsBackMainOrder() {
        jdbc.update("delete from recharge_order_ext where order_id=?", ORDER_ID);
        assertThatThrownBy(() -> port.updateFulfillmentStatus(
                update(FulfillmentDispatchStatus.CANCELLED).withInvalidTime(FIRST_INVALID_TIME)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("select remark from mc_order where order_Id=?", String.class, ORDER_ID))
                .isEqualTo("before-update");
    }

    @Test
    void successfulRechargeInitializesMissingVoucherStateToUnused() {
        jdbc.update("update recharge_order_ext set voucher_use_state=null where order_id=?", ORDER_ID);

        port.updateFulfillmentStatus(update(FulfillmentDispatchStatus.SUCCESS));

        assertThat(orderStatus()).isEqualTo(12);
        assertThat(voucherState()).isEqualTo("1");
        assertThat(dispatchStatus()).isEqualTo("SUCCESS");
        assertThat(invalidTime()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"2", "3"})
    void successReplayCannotResetRedeemedOrCancelledVoucher(String voucherState) {
        LocalDateTime useTime = "2".equals(voucherState) ? LocalDateTime.parse("2026-10-19T09:30:00") : null;
        LocalDateTime invalidTime = "3".equals(voucherState) ? FIRST_INVALID_LOCAL_TIME : null;
        jdbc.update("""
                update recharge_order_ext set voucher_use_state=?, voucher_use_time=?, voucher_invalid_time=?
                where order_id=?
                """, voucherState, useTime, invalidTime, ORDER_ID);

        port.updateFulfillmentStatus(update(FulfillmentDispatchStatus.SUCCESS));

        assertThat(orderStatus()).isEqualTo(12);
        assertThat(voucherState()).isEqualTo(voucherState);
        assertThat(invalidTime()).isEqualTo(invalidTime);
        assertThat(jdbc.queryForObject("select voucher_use_time from recharge_order_ext where order_id=?",
                Timestamp.class, ORDER_ID)).isEqualTo(useTime == null ? null : Timestamp.valueOf(useTime));
    }

    @Test
    void actualExtConstraintFailureRollsBackPreviouslyUpdatedMainOrder() {
        jdbc.update("update mc_order set order_status=13 where order_Id=?", ORDER_ID);
        // 由数据库拒绝第二条真实UPDATE，验证第一条主单UPDATE也回滚，避免Mock异常绕过SQL执行。
        jdbc.execute("""
                alter table recharge_order_ext add constraint reject_cancellation_for_rollback_test
                check (dispatch_status <> 'CANCELLED')
                """);

        assertThatThrownBy(() -> port.updateFulfillmentStatus(
                update(FulfillmentDispatchStatus.CANCELLED).withInvalidTime(FIRST_INVALID_TIME)))
                .isInstanceOf(DataIntegrityViolationException.class);

        assertThat(orderStatus()).isEqualTo(13);
        assertThat(jdbc.queryForObject("select remark from mc_order where order_Id=?", String.class, ORDER_ID))
                .isEqualTo("before-update");
        assertThat(jdbc.queryForObject("select edit_time from mc_order where order_Id=?", Timestamp.class, ORDER_ID))
                .isNull();
        assertThat(dispatchStatus()).isEqualTo("SUCCESS");
        assertThat(voucherState()).isEqualTo("1");
        assertThat(invalidTime()).isNull();
    }

    @Test
    void cancellationWithoutTrustedTimeIsRejectedBeforeAnyWrite() {
        assertThatThrownBy(() -> port.updateFulfillmentStatus(update(FulfillmentDispatchStatus.CANCELLED)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trusted invalid time");

        assertThat(orderStatus()).isEqualTo(12);
        assertThat(voucherState()).isEqualTo("1");
        assertThat(dispatchStatus()).isEqualTo("SUCCESS");
        assertThat(invalidTime()).isNull();
        assertThat(jdbc.queryForObject("select remark from mc_order where order_Id=?", String.class, ORDER_ID))
                .isEqualTo("before-update");
        assertThat(jdbc.queryForObject("select updated_at from recharge_order_ext where order_id=?",
                Timestamp.class, ORDER_ID)).isNull();
    }

    private static OrderFulfillmentStatusUpdate update(FulfillmentDispatchStatus status) {
        return new OrderFulfillmentStatusUpdate(ORDER_ID, status, 12, "PROCESSED", null, null, null,
                "履约状态更新-" + status.name());
    }

    private Integer orderStatus() {
        return jdbc.queryForObject("select order_status from mc_order where order_Id=?", Integer.class, ORDER_ID);
    }

    private String voucherState() {
        return jdbc.queryForObject("select voucher_use_state from recharge_order_ext where order_id=?",
                String.class, ORDER_ID);
    }

    private String dispatchStatus() {
        return jdbc.queryForObject("select dispatch_status from recharge_order_ext where order_id=?",
                String.class, ORDER_ID);
    }

    private LocalDateTime invalidTime() {
        Timestamp value = jdbc.queryForObject("select voucher_invalid_time from recharge_order_ext where order_id=?",
                Timestamp.class, ORDER_ID);
        return value == null ? null : value.toLocalDateTime();
    }
}

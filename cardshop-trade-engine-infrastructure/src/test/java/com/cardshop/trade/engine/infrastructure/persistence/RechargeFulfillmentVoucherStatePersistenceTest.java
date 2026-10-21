package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.cardshop.trade.engine.application.recharge.FulfillmentDispatchStatus;
import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.cardshop.trade.engine.application.recharge.MinisoWriteOffException;
import com.cardshop.trade.engine.application.recharge.OrderFulfillmentStatusUpdate;
import com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort;
import com.cardshop.trade.engine.infrastructure.config.TradeEngineMybatisPersistenceAutoConfiguration;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoCouponGuardMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoReceiptMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoSharedCallbackInboxMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoWriteOffEventMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderSupplierFlowMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeDispatchOutboxMapper;
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
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
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
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-19T00:00:00.123456789Z"), ZoneOffset.UTC);
    private static final Instant AUTHORIZED_AT = Instant.parse("2026-10-19T00:00:00.123Z");

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
                    org_code varchar(64), out_orderId varchar(100), supplier_channel varchar(32),
                    order_price bigint, account varchar(64), num int,
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
                    org_code varchar(64), out_order_sn varchar(100), supplier_batch_id varchar(64),
                    persist_status varchar(32), balance_reserved boolean,
                    balance_release_authorized_at timestamp(3), balance_release_event_id varchar(100),
                    balance_release_amount bigint,
                    dispatch_status varchar(32),
                    last_error varchar(512),
                    updated_at timestamp(6),
                    voucher_use_state varchar(1),
                    voucher_use_time timestamp(6),
                    voucher_invalid_time timestamp(6)
                )
                """);
        jdbc.execute("""
                create table recharge_miniso_receipt (
                    order_id varchar(100) primary key, org_code varchar(64), batch_id int,
                    coupon_code varchar(128) unique, phone varchar(64), begin_time varchar(19),
                    end_time varchar(19), coupon_status int, use_time timestamp,
                    updated_at timestamp default current_timestamp
                )
                """);
        jdbc.execute("create table mc_miniso_coupon_guard(coupon_code varchar(128) primary key, created_at timestamp)");
        jdbc.execute("create table mc_miniso_callback_inbox(event_id varchar(96) primary key,"
                + "coupon_code varchar(128) unique, use_time timestamp, status varchar(32))");
        jdbc.execute("create table recharge_miniso_writeoff_event(event_id varchar(96) primary key,"
                + "coupon_code varchar(128) unique, use_time timestamp, status varchar(32), order_id varchar(100),"
                + "org_code varchar(64), retry_count int default 0, next_retry_time timestamp,"
                + "last_error varchar(128), version bigint default 0)");
        jdbc.execute("create table recharge_dispatch_outbox(event_id varchar(100) primary key,"
                + "event_type varchar(32), org_code varchar(64), order_id varchar(100), out_order_sn varchar(100))");
        jdbc.execute("create table mc_depart_record(id bigint auto_increment primary key, org_code varchar(64),"
                + "order_id varchar(100), type varchar(1), `value` bigint)");
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
        configuration.addMapper(MinisoReceiptMapper.class);
        configuration.addMapper(MinisoCouponGuardMapper.class);
        configuration.addMapper(MinisoSharedCallbackInboxMapper.class);
        configuration.addMapper(MinisoWriteOffEventMapper.class);
        configuration.addMapper(McDepartRecordMapper.class);
        configuration.addMapper(RechargeDispatchOutboxMapper.class);
        var factory = new MybatisSqlSessionFactoryBean();
        factory.setDataSource(dataSource);
        factory.setConfiguration(configuration);
        var session = new SqlSessionTemplate(factory.getObject());
        var tm = new DataSourceTransactionManager(dataSource);
        var receiptMapper = session.getMapper(MinisoReceiptMapper.class);
        var guards = session.getMapper(MinisoCouponGuardMapper.class);
        var protection = new MybatisMinisoWriteOffProtection(session.getMapper(MinisoWriteOffEventMapper.class),
                receiptMapper, guards, session.getMapper(MinisoSharedCallbackInboxMapper.class), tm, CLOCK, 5);
        var beans = new DefaultListableBeanFactory();
        beans.registerSingleton("minisoReceiptStore", new MinisoReceiptStore(receiptMapper, guards, CLOCK, () -> protection));
        // 使用生产晚装配工厂，再包生产注解事务代理，防止兜底构造器漏传Store或资金事实Mapper。
        var target = new TradeEngineMybatisPersistenceAutoConfiguration().rechargeFulfillmentOrderPort(
                session.getMapper(McOrderMapper.class),
                session.getMapper(RechargeOrderExtMapper.class),
                session.getMapper(McOrderSupplierFlowMapper.class),
                beans.getBeanProvider(MinisoReceiptStore.class),
                session.getMapper(McDepartRecordMapper.class), session.getMapper(RechargeDispatchOutboxMapper.class),
                new ObjectMapper(), CLOCK);
        // 直接调用端口代理，确保生产@Transactional生效，而不是由测试事务掩盖提交边界。
        var proxyFactory = new ProxyFactory(target);
        proxyFactory.addAdvice(new TransactionInterceptor(tm,
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

    @ParameterizedTest
    @ValueSource(strings = {"CANCELLED", "FAILED"})
    void receiptlessMinisoTerminalWriteCannotBypassSharedEvidence(String status) {
        seedMiniso();
        jdbc.update("insert into mc_miniso_callback_inbox values('shared-C1','C1',?,'TRANSFERRED_NEW')",
                Timestamp.from(CLOCK.instant()));

        assertThatThrownBy(() -> port.updateFulfillmentStatus(
                update(FulfillmentDispatchStatus.valueOf(status)).withInvalidTime(FIRST_INVALID_TIME)))
                .isInstanceOf(MinisoWriteOffException.class).hasMessage("REDEEM_VOID_CONFLICT");

        assertThat(dispatchStatus()).isEqualTo("SUCCESS");
        assertThat(voucherState()).isEqualTo("1");
        assertNoAuthorization();
    }

    @Test
    void authorizationWriteFailureRollsBackReceiptMainOrderAndTerminalTogether() {
        seedMiniso();
        jdbc.update("update mc_order set order_status=13 where order_Id=?", ORDER_ID);
        jdbc.update("update recharge_miniso_receipt set coupon_status=0 where order_id=?", ORDER_ID);
        jdbc.execute("alter table recharge_order_ext add constraint reject_authorization "
                + "check (balance_release_authorized_at is null)");

        assertThatThrownBy(() -> port.authorizeMinisoTerminalRelease(cancellation().withMinisoReceipt(
                new MinisoFulfillmentReceipt(ORDER_ID, "A39", 100023, "C1", "13800138000",
                        "2026-10-01 00:00:00", "2026-12-01 00:00:00", 3)), "event-A"))
                .isInstanceOf(DataIntegrityViolationException.class);

        assertThat(orderStatus()).isEqualTo(13);
        assertThat(dispatchStatus()).isEqualTo("SUCCESS");
        assertThat(voucherState()).isEqualTo("1");
        assertThat(invalidTime()).isNull();
        assertThat(jdbc.queryForObject("select coupon_status from recharge_miniso_receipt", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from mc_miniso_coupon_guard", Integer.class)).isZero();
        assertNoAuthorization();
    }

    @ParameterizedTest
    @ValueSource(strings = {"CANCELLED", "FAILED"})
    void terminalAndFrozenAuthorizationCommitTogetherAndRetrySurvivesLateEvidence(String status) {
        seedMiniso();
        var command = update(FulfillmentDispatchStatus.valueOf(status)).withInvalidTime(FIRST_INVALID_TIME);
        var first = port.authorizeMinisoTerminalRelease(command, "event-A").orElseThrow();
        assertThat(first.orderId()).isEqualTo(ORDER_ID);
        assertThat(first.orgCode()).isEqualTo("A39");
        assertThat(first.outOrderSn()).isEqualTo("EXT-1");
        assertThat(first.sourceEventId()).isEqualTo("event-A");
        assertThat(first.amount()).isEqualTo(137);
        assertThat(first.authorizedAt()).isEqualTo(AUTHORIZED_AT);
        assertThat(dispatchStatus()).isEqualTo(status);
        assertThat(jdbc.queryForObject("select balance_reserved from recharge_order_ext", Boolean.class)).isTrue();
        if ("CANCELLED".equals(status)) {
            assertThat(voucherState()).isEqualTo("3");
            assertThat(invalidTime()).isEqualTo(FIRST_INVALID_LOCAL_TIME);
        }
        jdbc.update("insert into mc_miniso_callback_inbox values('late-C1','C1',?,'MANUAL_REQUIRED')",
                Timestamp.from(CLOCK.instant()));
        assertThat(port.authorizeMinisoTerminalRelease(command, "event-B")).contains(first);
        port.updateFulfillmentStatus(command);
        assertThat(port.findMinisoReleaseAuthorization(ORDER_ID)).contains(first);
        // 若重试错误地重写授权，过去时间哨兵和event-A都会丢失；同时覆盖完成释放后的只读恢复。
        jdbc.update("update recharge_order_ext set balance_release_authorized_at=?,balance_reserved=false",
                Timestamp.from(AUTHORIZED_AT.minusSeconds(60)));
        var frozen = port.findMinisoReleaseAuthorization(ORDER_ID).orElseThrow();
        assertThat(frozen.authorizedAt()).isEqualTo(AUTHORIZED_AT.minusSeconds(60));
        assertThat(port.authorizeMinisoTerminalRelease(command.withInvalidTime(FIRST_INVALID_TIME.plusSeconds(600)),
                "event-B")).contains(frozen);
        assertThat(port.findMinisoReleaseAuthorization(ORDER_ID)).contains(frozen);

        assertThatThrownBy(() -> port.updateFulfillmentStatus(update(FulfillmentDispatchStatus.SUCCESS)))
                .isInstanceOf(MinisoWriteOffException.class).hasMessage("RELEASE_TERMINAL_CONFLICT");
        assertThat(dispatchStatus()).isEqualTo(status);
    }

    @ParameterizedTest
    @ValueSource(strings = {"inbox", "event", "used", "useTime", "voucher"})
    void anyCurrentRedemptionEvidenceBlocksFirstAuthorization(String evidence) {
        seedMiniso();
        switch (evidence) {
            case "inbox" -> jdbc.update("insert into mc_miniso_callback_inbox values('shared','C1',?,'MANUAL_REQUIRED')",
                    Timestamp.from(CLOCK.instant()));
            case "event" -> jdbc.update("insert into recharge_miniso_writeoff_event(event_id,coupon_code,status) "
                    + "values('local','C1','INIT')");
            case "used" -> jdbc.update("update recharge_miniso_receipt set coupon_status=1");
            case "useTime" -> jdbc.update("update recharge_miniso_receipt set use_time=?", Timestamp.from(CLOCK.instant()));
            case "voucher" -> jdbc.update("update recharge_order_ext set voucher_use_state='2'");
            default -> throw new AssertionError(evidence);
        }
        assertThatThrownBy(() -> port.authorizeMinisoTerminalRelease(cancellation(), "event-A"))
                .isInstanceOf(MinisoWriteOffException.class).hasMessage("REDEEM_VOID_CONFLICT");
        assertThat(dispatchStatus()).isEqualTo("SUCCESS");
        assertNoAuthorization();
    }

    @ParameterizedTest
    @ValueSource(strings = {"org", "outOrder", "amount", "eventOrg", "eventOrder", "eventType", "missingEvent", "missingDeduct"})
    void authorizationRejectsDamagedOwnershipAmountOrSourceEvent(String damage) {
        seedMiniso();
        switch (damage) {
            case "org" -> jdbc.update("update recharge_order_ext set org_code='OTHER'");
            case "outOrder" -> jdbc.update("update recharge_order_ext set out_order_sn='OTHER'");
            case "amount" -> jdbc.update("update mc_depart_record set `value`=139");
            case "eventOrg" -> jdbc.update("update recharge_dispatch_outbox set org_code='OTHER'");
            case "eventOrder" -> jdbc.update("update recharge_dispatch_outbox set order_id='OTHER'");
            case "eventType" -> jdbc.update("update recharge_dispatch_outbox set event_type='BALANCE_DELTA_FLUSH'");
            case "missingEvent" -> jdbc.update("delete from recharge_dispatch_outbox");
            case "missingDeduct" -> jdbc.update("delete from mc_depart_record");
            default -> throw new AssertionError(damage);
        }
        assertThatThrownBy(() -> port.authorizeMinisoTerminalRelease(cancellation(), "event-A"))
                .isInstanceOf(MinisoWriteOffException.class);
        assertThat(dispatchStatus()).isEqualTo("SUCCESS");
        assertNoAuthorization();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6})
    void partiallyPopulatedAuthorizationFailsOnReadAndBothWriteEntrypoints(int mask) {
        seedMiniso();
        jdbc.update("update recharge_order_ext set balance_release_authorized_at=?,"
                        + "balance_release_event_id=?,balance_release_amount=?",
                (mask & 1) == 0 ? null : Timestamp.from(CLOCK.instant()),
                (mask & 2) == 0 ? null : "event-A", (mask & 4) == 0 ? null : 137L);
        assertThatThrownBy(() -> port.findMinisoReleaseAuthorization(ORDER_ID))
                .isInstanceOf(MinisoWriteOffException.class).hasMessage("RELEASE_AUTHORIZATION_INCONSISTENT");
        assertThatThrownBy(() -> port.authorizeMinisoTerminalRelease(cancellation(), "event-A"))
                .isInstanceOf(MinisoWriteOffException.class).hasMessage("RELEASE_AUTHORIZATION_INCONSISTENT");
        assertThatThrownBy(() -> port.updateFulfillmentStatus(update(FulfillmentDispatchStatus.SUCCESS)))
                .isInstanceOf(MinisoWriteOffException.class).hasMessage("RELEASE_AUTHORIZATION_INCONSISTENT");
        assertThat(dispatchStatus()).isEqualTo("SUCCESS");
    }

    @ParameterizedTest
    @ValueSource(strings = {"receiptMissing", "receiptUnused", "receiptOrg", "receiptPhone", "receiptBatch", "historical", "pending"})
    void absenceOfTrustedRefundEvidenceNeverCreatesAuthorization(String damage) {
        seedMiniso();
        switch (damage) {
            case "receiptMissing" -> jdbc.update("delete from recharge_miniso_receipt");
            case "receiptUnused" -> jdbc.update("update recharge_miniso_receipt set coupon_status=0");
            case "receiptOrg" -> jdbc.update("update recharge_miniso_receipt set org_code='OTHER'");
            case "receiptPhone" -> jdbc.update("update recharge_miniso_receipt set phone='13800138001'");
            case "receiptBatch" -> jdbc.update("update recharge_miniso_receipt set batch_id=100024");
            case "historical" -> jdbc.update("update recharge_order_ext set dispatch_status='CANCELLED'");
            case "pending" -> jdbc.update("update recharge_order_ext set persist_status='PENDING_MQ'");
            default -> throw new AssertionError(damage);
        }
        assertThatThrownBy(() -> port.authorizeMinisoTerminalRelease(cancellation(), "event-A"))
                .isInstanceOf(MinisoWriteOffException.class);
        assertNoAuthorization();
    }

    @Test
    void unreservedOrderReturnsEmptyWithoutInventingRefundButStillCommitsTerminal() {
        seedMiniso();
        jdbc.update("update recharge_order_ext set balance_reserved=false");
        jdbc.update("delete from mc_depart_record");
        assertThat(port.findMinisoReleaseAuthorization(ORDER_ID)).isEmpty();
        assertThat(port.authorizeMinisoTerminalRelease(cancellation(), "event-A")).isEmpty();
        assertThat(dispatchStatus()).isEqualTo("CANCELLED");
        assertNoAuthorization();
    }

    @Test
    void oldTerminalEntrypointWithoutEventCannotCreateUnlicensedMinisoTerminal() {
        seedMiniso();
        assertThatThrownBy(() -> port.updateFulfillmentStatus(cancellation()))
                .isInstanceOf(MinisoWriteOffException.class).hasMessage("RELEASE_EVIDENCE_MISSING");
        assertThat(dispatchStatus()).isEqualTo("SUCCESS");
        assertNoAuthorization();
    }

    @Test
    void existingAuthorizationRejectsChangedAmountAndIdentityInsteadOfFreezingCorruption() {
        seedMiniso();
        var first = port.authorizeMinisoTerminalRelease(cancellation(), "event-A").orElseThrow();
        jdbc.update("update mc_order set order_price=139");
        assertThatThrownBy(() -> port.authorizeMinisoTerminalRelease(cancellation(), "event-B"))
                .isInstanceOf(MinisoWriteOffException.class);
        jdbc.update("update mc_order set order_price=137");
        jdbc.update("update recharge_order_ext set org_code='OTHER'");
        assertThatThrownBy(() -> port.findMinisoReleaseAuthorization(ORDER_ID))
                .isInstanceOf(MinisoWriteOffException.class);
        jdbc.update("update recharge_order_ext set org_code='A39'");
        assertThat(port.findMinisoReleaseAuthorization(ORDER_ID)).contains(first);
    }

    @ParameterizedTest
    @ValueSource(strings = {"CANCELLED", "FAILED"})
    void historicalTerminalCannotBeReopenedToCircumventNoBackfillRule(String terminal) {
        seedMiniso();
        jdbc.update("update recharge_order_ext set dispatch_status=?", terminal);
        assertThatThrownBy(() -> port.updateFulfillmentStatus(update(FulfillmentDispatchStatus.SUCCESS)))
                .isInstanceOf(MinisoWriteOffException.class).hasMessage("RELEASE_TERMINAL_CONFLICT");
        assertThat(dispatchStatus()).isEqualTo(terminal);
        assertNoAuthorization();
    }

    @Test
    void authorizationCannotPairCancelledDispatchWithFailedMainOrder() {
        seedMiniso();
        var mismatched = new OrderFulfillmentStatusUpdate(ORDER_ID, FulfillmentDispatchStatus.CANCELLED,
                6, "PROCESSED", null, null, null, "wrong main status").withInvalidTime(FIRST_INVALID_TIME);
        assertThatThrownBy(() -> port.authorizeMinisoTerminalRelease(mismatched, "event-A"))
                .isInstanceOf(MinisoWriteOffException.class).hasMessage("RELEASE_TERMINAL_CONFLICT");
        assertNoAuthorization();
    }

    @ParameterizedTest
    @ValueSource(ints = {6, 11})
    void oldEntrypointCannotHideFinancialTerminalBehindSuccessfulDispatch(int mainStatus) {
        seedMiniso();
        var mismatched = new OrderFulfillmentStatusUpdate(ORDER_ID, FulfillmentDispatchStatus.SUCCESS,
                mainStatus, "PROCESSED", null, null, null, "wrong dispatch status");
        assertThatThrownBy(() -> port.updateFulfillmentStatus(mismatched))
                .isInstanceOf(MinisoWriteOffException.class).hasMessage("RELEASE_TERMINAL_CONFLICT");
        assertThat(orderStatus()).isEqualTo(12);
        assertThat(dispatchStatus()).isEqualTo("SUCCESS");
        assertNoAuthorization();
    }

    @Test
    void readingAuthorizationRejectsReopenedMainOrder() {
        seedMiniso();
        port.authorizeMinisoTerminalRelease(cancellation(), "event-A").orElseThrow();
        jdbc.update("update mc_order set order_status=13");
        assertThatThrownBy(() -> port.findMinisoReleaseAuthorization(ORDER_ID))
                .isInstanceOf(MinisoWriteOffException.class).hasMessage("RELEASE_AUTHORIZATION_INCONSISTENT");
    }

    private static OrderFulfillmentStatusUpdate cancellation() {
        return update(FulfillmentDispatchStatus.CANCELLED).withInvalidTime(FIRST_INVALID_TIME);
    }

    private void seedMiniso() {
        jdbc.update("update mc_order set org_code='A39',out_orderId='EXT-1',supplier_channel='MINISO',"
                + "order_price=137,account='13800138000',num=1 where order_Id=?", ORDER_ID);
        jdbc.update("update recharge_order_ext set org_code='A39',out_order_sn='EXT-1',supplier_batch_id='100023',"
                + "persist_status='DB_PERSISTED',balance_reserved=true where order_id=?", ORDER_ID);
        jdbc.update("insert into recharge_miniso_receipt(order_id,org_code,batch_id,coupon_code,phone,"
                + "begin_time,end_time,coupon_status) values(?,'A39',100023,'C1','13800138000',"
                + "'2026-10-01 00:00:00','2026-12-01 00:00:00',3)", ORDER_ID);
        jdbc.update("insert into mc_depart_record(org_code,order_id,type,`value`) values('A39',?,'1',137)", ORDER_ID);
        jdbc.update("insert into recharge_dispatch_outbox values('event-A','INVALID_ORDER','A39',?,'EXT-1')", ORDER_ID);
        jdbc.update("insert into recharge_dispatch_outbox values('event-B','SUPPLIER_QUERY','A39',?,'EXT-1')", ORDER_ID);
    }

    private void assertNoAuthorization() {
        assertThat(jdbc.queryForObject("select balance_release_authorized_at from recharge_order_ext where order_id=?",
                Timestamp.class, ORDER_ID)).isNull();
        assertThat(jdbc.queryForObject("select balance_release_event_id from recharge_order_ext where order_id=?",
                String.class, ORDER_ID)).isNull();
        assertThat(jdbc.queryForObject("select balance_release_amount from recharge_order_ext where order_id=?",
                Long.class, ORDER_ID)).isNull();
    }

    private static OrderFulfillmentStatusUpdate update(FulfillmentDispatchStatus status) {
        return new OrderFulfillmentStatusUpdate(ORDER_ID, status,
                status == FulfillmentDispatchStatus.FAILED ? 6 : 12, "PROCESSED", null, null, null,
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

package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.application.v1.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.*;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

/** 受理归属、唯一事件和非due观察的真实SQL验证；MySQL隔离语义另行验证，见详细设计9.3.2。 */
class RechargeOrderInvalidationTransactionTest {
    private JdbcTemplate jdbc;
    private MybatisRechargeDispatchOutboxAdapter port;
    private SqlSessionTemplate session;
    private DataSourceTransactionManager tm;
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-05T02:00:00Z"), ZoneOffset.UTC);

    @BeforeEach void setup() throws Exception {
        var ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        jdbc = new JdbcTemplate(ds); tm = new DataSourceTransactionManager(ds);
        jdbc.execute("create table mc_order(order_Id varchar(64) primary key,org_code varchar(32),out_orderId varchar(64),product_type int,num int,account varchar(64),supplier_channel varchar(32),order_status int,order_price bigint,fail_code varchar(32),fail_reason varchar(64))");
        jdbc.execute("create table recharge_order_ext(order_id varchar(64) primary key,org_code varchar(32),out_order_sn varchar(64),persist_status varchar(32),validation_status varchar(32),dispatch_allowed boolean,dispatch_status varchar(32),supplier_batch_id varchar(32),balance_reserved boolean,voucher_use_state varchar(1),voucher_use_time timestamp,voucher_invalid_time timestamp,balance_release_authorized_at timestamp,balance_release_event_id varchar(100),balance_release_amount bigint,balance_no_release_decided_at timestamp,balance_no_release_event_id varchar(100))");
        jdbc.execute("create table recharge_dispatch_outbox(id bigint auto_increment primary key,event_id varchar(100) unique,event_type varchar(32),org_code varchar(32),order_id varchar(64),out_order_sn varchar(64),payload clob,status varchar(32),retry_count int,max_retry_count int,next_retry_time timestamp,locked_by varchar(64),locked_until timestamp,version bigint,last_error varchar(1000),shard_key varchar(100),created_at timestamp,updated_at timestamp)");
        var cfg = new MybatisConfiguration(); cfg.setMapUnderscoreToCamelCase(true);
        for (var c : List.of(McOrderMapper.class, RechargeOrderExtMapper.class, RechargeDispatchOutboxMapper.class,
                MinisoReceiptMapper.class, MinisoWriteOffEventMapper.class, MinisoCouponGuardMapper.class)) cfg.addMapper(c);
        var f = new MybatisSqlSessionFactoryBean(); f.setDataSource(ds); f.setConfiguration(cfg);
        session = new SqlSessionTemplate(f.getObject());
        port = new MybatisRechargeDispatchOutboxAdapter(session.getMapper(RechargeDispatchOutboxMapper.class),
                new ObjectMapper(), CLOCK, session.getMapper(McOrderMapper.class), session.getMapper(RechargeOrderExtMapper.class),
                session.getMapper(MinisoWriteOffEventMapper.class), session.getMapper(MinisoReceiptMapper.class),
                session.getMapper(MinisoCouponGuardMapper.class), () -> mock(MinisoWriteOffProtection.class),
                () -> mock(RechargeFulfillmentOrderPort.class), tm);
        seed("A", "OA"); seed("B", "OB");
    }
    @AfterEach void close() { jdbc.execute("shutdown"); }
    private void seed(String org, String id) {
        jdbc.update("insert into mc_order values(?,?, 'SAME',1,1,'10000000000','FNK_CGS_BX',12,137,'','')", id, org);
        jdbc.update("insert into recharge_order_ext(order_id,org_code,out_order_sn,persist_status,validation_status,dispatch_allowed,dispatch_status,balance_reserved,voucher_use_state) values(?,?,'SAME','DB_PERSISTED','PASSED',true,'SUCCESS',true,'1')", id, org);
    }
    @Test void concurrentSameMerchantCreatesOneOriginalEventAndNeverTouchesOtherMerchant() throws Exception {
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = pool.<RechargeOrderInvalidationState>invokeAll(List.of(() -> port.prepareInvalidOrder(command("A", "OA")),
                    () -> port.prepareInvalidOrder(command("A", "OA"))));
            for (var t : tasks) assertThat(t.get().status()).isEqualTo(RechargeOrderInvalidationState.Status.PENDING);
        }
        assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox", Integer.class)).isEqualTo(1);
        assertThat(port.findInvalidOrder("A", "SAME").orElseThrow().eventId()).isEqualTo("invalid-OA");
        assertThat(port.findInvalidOrder("B", "SAME").orElseThrow().status()).isEqualTo(RechargeOrderInvalidationState.Status.NOT_ACCEPTED);
    }
    @Test void legacyMissingExtensionAndUnissuedOrderAreNotAdmitted() {
        jdbc.update("delete from recharge_order_ext where order_id='OA'");
        jdbc.update("update mc_order set order_status=13 where order_Id='OB'");
        assertThat(port.prepareInvalidOrder(command("A", "OA")).status()).isEqualTo(RechargeOrderInvalidationState.Status.NOT_ACCEPTED);
        assertThat(port.prepareInvalidOrder(command("B", "OB")).status()).isEqualTo(RechargeOrderInvalidationState.Status.NOT_ACCEPTED);
        assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox", Integer.class)).isZero();
    }
    @Test void notDueAndManualOriginalEventsRemainVisibleWithoutFirstAdmissionChecks() {
        port.prepareInvalidOrder(command("A", "OA"));
        jdbc.update("update recharge_dispatch_outbox set status='FAILED',next_retry_time='2030-01-01',retry_count=4");
        jdbc.update("update mc_order set order_status=13 where order_Id='OA'");
        assertThat(port.prepareInvalidOrder(command("A", "OA")).status()).isEqualTo(RechargeOrderInvalidationState.Status.PENDING);
        jdbc.update("update recharge_dispatch_outbox set status='MANUAL_REQUIRED'");
        assertThat(port.findInvalidOrder("A", "SAME").orElseThrow().status()).isEqualTo(RechargeOrderInvalidationState.Status.MANUAL_REQUIRED);
        assertThat(jdbc.queryForObject("select retry_count from recharge_dispatch_outbox", Integer.class)).isEqualTo(4);
    }
    @Test void historicalCancelledWithoutTimeDoesNotBecomeCompletedOrGetNewEvent() {
        jdbc.update("update recharge_order_ext set dispatch_status='CANCELLED',voucher_use_state='3' where order_id='OA'");
        assertThat(port.prepareInvalidOrder(command("A", "OA")).status()).isEqualTo(RechargeOrderInvalidationState.Status.MANUAL_REQUIRED);
        assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox", Integer.class)).isZero();
    }

    @Test void conflictingOriginalEventIdentityMustNotBeHandedToWorker() {
        jdbc.update("insert into recharge_dispatch_outbox(event_id,event_type,org_code,order_id,out_order_sn,status) "
                + "values('invalid-OA','INVALID_ORDER','B','OB','SAME','INIT')");
        assertThatThrownBy(() -> port.prepareInvalidOrder(command("A", "OA")))
                .isInstanceOf(InvalidRechargeOrderOutboxPort.AdmissionRolledBackException.class);
        assertThat(jdbc.queryForObject("select org_code from recharge_dispatch_outbox where event_id='invalid-OA'",
                String.class)).isEqualTo("B");
        assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox", Integer.class)).isEqualTo(1);
    }

    private static InvalidRechargeOrderOutboxCommand command(String org, String id) {
        return new InvalidRechargeOrderOutboxCommand(org, id, "SAME", "fixture");
    }
}

package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.*;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeNotifyTaskMapper;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.h2.jdbcx.JdbcDataSource;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.junit.jupiter.api.*;
import java.time.*;
import java.util.*;

/** 真实SQL验证旧Worker回写隔离，不能用内存mock代替租约CAS。参见详细设计2104实施细化。 */
public class RechargeNotifyClaimCasTest {
    JdbcTemplate jdbc;
    MybatisRechargeNotifyTaskAdapter adapter;
    MutableClock clock;
    @BeforeEach void setup() throws Exception {
        var ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:"+UUID.randomUUID()+";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        jdbc = new JdbcTemplate(ds);
        jdbc.execute("""
            create table recharge_notify_task(id bigint auto_increment primary key,task_id varchar(100) unique,
              notify_type varchar(32),org_code varchar(64),order_id varchar(100),out_order_sn varchar(100),
              notify_url varchar(500),biz_status varchar(32),payload clob,status varchar(32),retry_count int,
              max_retry_count int,next_retry_time timestamp,locked_by varchar(100),locked_until timestamp,
              last_error varchar(1000),created_at timestamp,updated_at timestamp,
              delivery_channel varchar(32) default 'DIRECT_JSON',route_snapshot clob,encoded_payload clob,
              claim_version bigint default 0)
            """);
        jdbc.execute("""
            create table recharge_notify_attempt(id bigint auto_increment primary key,task_id varchar(100),
              claim_version bigint,msg_id varchar(32) unique,started_at timestamp,status varchar(32),http_status int,
              biz_state varchar(8),rsp_code varchar(64),business_code varchar(32),ref_msg_id varchar(64),finished_at timestamp)
            """);
        var cfg = new MybatisConfiguration(); cfg.addMapper(RechargeNotifyTaskMapper.class);
        var factory = new MybatisSqlSessionFactoryBean(); factory.setDataSource(ds); factory.setConfiguration(cfg);
        var session = new SqlSessionTemplate(factory.getObject());
        clock = new MutableClock();
        adapter = new MybatisRechargeNotifyTaskAdapter(session.getMapper(RechargeNotifyTaskMapper.class),new ObjectMapper(),clock);
        adapter.insertIfAbsent(draft());
    }
    @Test void expiredWorkerSuccessCannotOverwriteNewLease() {
        var a = adapter.claim(task(),"A",Duration.ofSeconds(10)).orElseThrow();
        clock.advance();
        adapter.claim(a,"B",Duration.ofSeconds(10)).orElseThrow();
        adapter.markSuccess(a);
        assertThat(status()).isEqualTo("PROCESSING");
        assertThat(jdbc.queryForObject("select locked_by from recharge_notify_task",String.class)).isEqualTo("B");
    }
    @Test void expiredWorkerFailureOrManualCannotOverwriteNewLease() {
        var a = adapter.claim(task(),"A",Duration.ofSeconds(10)).orElseThrow();
        clock.advance();
        adapter.claim(a,"B",Duration.ofSeconds(10)).orElseThrow();
        adapter.markFailed(a,"old",Duration.ofSeconds(10));
        adapter.markManualRequired(a,"old");
        assertThat(status()).isEqualTo("PROCESSING");
        assertThat(jdbc.queryForObject("select retry_count from recharge_notify_task",Integer.class)).isZero();
    }
    @Test void onlyDirectBlankTargetTasksAreClosedAsSuccess() {
        jdbc.update("update recharge_notify_task set notify_url='',delivery_channel='BOCOM_OPENAPI'");
        assertThat(adapter.closeBlankNotifyUrlTasks()).isZero();
        assertThat(status()).isEqualTo("INIT");
        jdbc.update("update recharge_notify_task set delivery_channel='DIRECT_JSON'");
        assertThat(adapter.closeBlankNotifyUrlTasks()).isEqualTo(1);
    }
    @Test void scanUsesExplicitShanghaiClockEvenWhenJvmDefaultZoneIsUtc() {
        var previous=java.util.TimeZone.getDefault();
        try {
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));
            jdbc.update("update recharge_notify_task set next_retry_time=?",LocalDateTime.now(clock).minusSeconds(1));
            assertThat(adapter.selectDueTasks(10)).hasSize(1);
        } finally { java.util.TimeZone.setDefault(previous); }
    }
    @Test void currentWorkerCanDeferWithoutConsumingRetriesAndOldScanCannotReclaim() {
        var scan=task();
        var claimed=adapter.claim(scan,"A",Duration.ofSeconds(10)).orElseThrow();
        assertThat(adapter.defer(claimed,"BANK_PAUSED",Duration.ofSeconds(60))).isTrue();
        assertThat(jdbc.queryForObject("select retry_count from recharge_notify_task",Integer.class)).isZero();
        assertThat(adapter.claim(scan,"OLD",Duration.ofSeconds(10))).isEmpty();
        clock.now=clock.now.plusSeconds(61);
        var fresh=adapter.selectDueTasks(1).getFirst();
        var next=adapter.claim(fresh,"B",Duration.ofSeconds(10)).orElseThrow();
        assertThat(next.claimVersion()).isEqualTo(2);
        assertThat(adapter.markSuccess(claimed)).isFalse();
        assertThat(adapter.markSuccess(next)).isTrue();
    }
    @Test void bankReplayKeepsFirstBodyNonceAndProtocolEvenAfterCurrentRouteChanges() throws Exception {
        var order=new RechargeFulfillmentOrder("O2","OUT2","A35","synthetic","P","C","MINISO",100,"",12,"DB_PERSISTED","PASSED",true,true);
        var current=new java.util.concurrent.atomic.AtomicReference<>(new MerchantNotifyRoute(NotifyDeliveryChannel.BOCOM_OPENAPI,
                "A35","r1","https://bank.invalid/api/trade/J_IEP_001_2104/v1","p1","app","RSA","pk1","bk1"));
        var preparation=new MerchantNotifyPreparationService((org,url)->Optional.of(current.get()),
                new V1MerchantNotifyPayloadSigner(com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort.unavailable()),null,clock);
        var first=preparation.orderStatus(order,"12",10).orElseThrow();adapter.insertIfAbsent(first);
        clock.advance();current.set(new MerchantNotifyRoute(NotifyDeliveryChannel.BOCOM_OPENAPI,
                "A35","r2","https://changed.invalid/api/trade/J_IEP_001_2104/v1","p2","app2","RSA","pk2","bk2"));
        adapter.insertIfAbsent(preparation.orderStatus(order,"12",10).orElseThrow());
        assertThat(jdbc.queryForObject("select encoded_payload from recharge_notify_task where order_id='O2'",String.class)).isEqualTo(first.encodedPayload());
        var json=new ObjectMapper();
        assertThat(json.readTree(jdbc.queryForObject("select route_snapshot from recharge_notify_task where order_id='O2'",String.class))
                .path("protocol").asText()).isEqualTo("p1");
        assertThat(jdbc.queryForObject("select notify_url from recharge_notify_task where order_id='O2'",String.class))
                .isEqualTo(first.notifyUrl());
    }
    private String status() { return jdbc.queryForObject("select status from recharge_notify_task",String.class); }
    private RechargeNotifyTaskDraft draft() { return new RechargeNotifyTaskDraft("T1","ORDER_STATUS","A35","O1","OUT1",
            "https://merchant.invalid","12",Map.of("test","value"),10,LocalDateTime.now(clock).minusDays(1)); }
    private RechargeNotifyTask task() { return new RechargeNotifyTask("T1","ORDER_STATUS","A35","O1","OUT1",
            "https://merchant.invalid","12","{}",0,10,LocalDateTime.now(clock).minusDays(1),""); }
    static class MutableClock extends Clock {
        Instant now = Instant.parse("2026-10-21T03:00:00Z");
        void advance() { now = now.plusSeconds(11); }
        public ZoneId getZone() { return ZoneId.of("Asia/Shanghai"); }
        public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return now; }
    }
}

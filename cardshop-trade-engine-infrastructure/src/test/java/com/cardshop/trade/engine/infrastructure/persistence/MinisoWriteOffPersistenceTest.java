package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.application.v1.crypto.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.ibatis.session.Configuration;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.*;
import org.mybatis.spring.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.CRC32;

/** 真实H2/MyBatis事务验证接收、幂等、恢复及退款防护；参见详细设计9.5。 */
public class MinisoWriteOffPersistenceTest {
    JdbcTemplate jdbc;
    MybatisMinisoWriteOffStore store;
    MinisoReceiptStore receipts;
    MinisoWriteOffProtection protection;
    MinisoWriteOffEventMapper events;
    MinisoReceiptMapper receiptMapper;
    TransactionTemplate tx;
    MutableClock clock;
    V12SigningCredentialStorePort credentials;
    static final LocalDateTime USED = LocalDateTime.of(2026, 10, 17, 9, 29);
    static final String KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    @BeforeEach
    void setup() throws Exception {
        var ds = new JdbcDataSource();
        ds.setURL(
                "jdbc:h2:mem:"
                        + UUID.randomUUID()
                        + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=1000");
        jdbc = new JdbcTemplate(ds);
        jdbc.execute(
                "CREATE ALIAS CRC32 FOR"
                    + " 'com.cardshop.trade.engine.infrastructure.persistence.MinisoWriteOffPersistenceTest.crc32'");
        jdbc.execute(
                "create table mc_order(order_Id varchar(100) primary key,org_code"
                        + " varchar(64),out_orderId varchar(100),account varchar(64),num"
                        + " int,supplier_channel varchar(32),order_status int,callback_url"
                        + " varchar(500),data_status varchar(32),remark varchar(100),edit_time"
                        + " timestamp,data clob)");
        jdbc.execute(
                "create table recharge_order_ext(order_id varchar(100) primary key,org_code"
                        + " varchar(64),out_order_sn varchar(100),supplier_batch_id"
                        + " varchar(64),persist_status varchar(32),dispatch_status"
                        + " varchar(32),last_error varchar(128),updated_at timestamp,"
                        + "voucher_use_state varchar(1) default '',voucher_use_time timestamp,"
                        + "voucher_invalid_time timestamp)");
        jdbc.execute(
                "create table recharge_miniso_receipt(order_id varchar(100) primary key,org_code"
                        + " varchar(64),batch_id int,coupon_code varchar(128) unique,phone"
                        + " varchar(64),begin_time varchar(19),end_time varchar(19),coupon_status"
                        + " int,use_time timestamp,created_at timestamp default"
                        + " current_timestamp,updated_at timestamp default current_timestamp)");
        jdbc.execute(
                "create table recharge_miniso_writeoff_event(event_id varchar(80) primary"
                    + " key,coupon_code varchar(128) unique,use_time timestamp,order_id"
                    + " varchar(100),org_code varchar(64),status varchar(24),retry_count"
                    + " int,next_retry_time timestamp,last_error varchar(128),version"
                    + " bigint,received_at timestamp,processed_at timestamp,updated_at timestamp)");
        jdbc.execute(
                "create table recharge_notify_task(task_id varchar(100) primary key,notify_type"
                    + " varchar(32),org_code varchar(64),order_id varchar(100),out_order_sn"
                    + " varchar(100),notify_url varchar(500),biz_status varchar(32),payload"
                    + " clob,status varchar(32),retry_count int,max_retry_count int,next_retry_time"
                    + " timestamp,locked_by varchar(100),locked_until timestamp,last_error"
                    + " varchar(128),created_at timestamp,updated_at timestamp,delivery_channel varchar(32) default 'DIRECT_JSON',route_snapshot clob,encoded_payload clob,claim_version bigint default 0)");
        jdbc.execute(
                "create table recharge_dispatch_outbox(event_id varchar(100) primary key,status"
                        + " varchar(32),locked_until timestamp,last_error varchar(128),updated_at"
                        + " timestamp)");
        var cfg = new Configuration();
        cfg.addMapper(MinisoReceiptMapper.class);
        cfg.addMapper(MinisoWriteOffEventMapper.class);
        cfg.addMapper(RechargeNotifyTaskMapper.class);
        var factory = new SqlSessionFactoryBean();
        factory.setDataSource(ds);
        factory.setConfiguration(cfg);
        var session = new SqlSessionTemplate(factory.getObject());
        events = session.getMapper(MinisoWriteOffEventMapper.class);
        receiptMapper = session.getMapper(MinisoReceiptMapper.class);
        clock = new MutableClock();
        var tm = new DataSourceTransactionManager(ds);
        tx = new TransactionTemplate(tm);
        credentials = mock(V12SigningCredentialStorePort.class);
        when(credentials.findCurrent("A39"))
                .thenReturn(Optional.of(new V12SigningCredential("K1", "A39", KEY)));
        var tasks =
                new MybatisRechargeNotifyTaskAdapter(
                        session.getMapper(RechargeNotifyTaskMapper.class),
                        new ObjectMapper(),
                        clock);
        store =
                new MybatisMinisoWriteOffStore(
                        events,
                        receiptMapper,
                        tasks,
                        new V12RedemptionNotifyPayloadSigner(credentials),
                        tm,
                        clock,
                        2,
                        Duration.ofSeconds(60),
                        5);
        protection = new MybatisMinisoWriteOffProtection(events, receiptMapper, tm, clock, 5);
        receipts = new MinisoReceiptStore(receiptMapper, () -> protection);
    }

    @Test
    void reliableReceptionDuplicatesAndConflicts() {
        store.accept(notice());
        store.accept(notice());
        assertThat(count("recharge_miniso_writeoff_event")).isEqualTo(1);
        assertThatThrownBy(
                        () ->
                                store.accept(
                                        new MinisoWriteOffNotification("C1", USED.plusSeconds(1))))
                .hasMessage("CONFLICT");
        assertThat(eventStatus()).isEqualTo("INIT");
    }

    @Test void bankRedemptionWithBlankUrlCommitsWithoutMerchantSigningKey() throws Exception {
        seed(0, "");
        reset(credentials);
        var route=new MerchantNotifyRoute(NotifyDeliveryChannel.BOCOM_OPENAPI,"A39","r1",
                "https://bank.invalid/api/trade/J_IEP_001_2104/v1","p","app","RSA","key","bank");
        var tasks=new MybatisRechargeNotifyTaskAdapter(
                new SqlSessionTemplate(sqlSessionFactory()).getMapper(RechargeNotifyTaskMapper.class),new ObjectMapper(),clock);
        var preparation=new MerchantNotifyPreparationService((org,url)->Optional.of(route),
                new V1MerchantNotifyPayloadSigner(credentials),new V12RedemptionNotifyPayloadSigner(credentials),clock);
        var bankStore=new MybatisMinisoWriteOffStore(events,receiptMapper,tasks,new V12RedemptionNotifyPayloadSigner(credentials),
                new DataSourceTransactionManager(jdbc.getDataSource()),clock,2,Duration.ofSeconds(60),5,
                MinisoWriteOffObserver.NOOP,preparation);
        bankStore.accept(notice());
        bankStore.processBatch(1,0,10,"worker");
        assertThat(eventStatus()).isEqualTo("PROCESSED");
        assertThat(count("recharge_notify_task")).isEqualTo(1);
        assertThat(jdbc.queryForObject("select encoded_payload from recharge_notify_task",String.class))
                .startsWith("operate_type:2,").contains("state_type:3,use_state:2");
        verifyNoInteractions(credentials);
    }

    private org.apache.ibatis.session.SqlSessionFactory sqlSessionFactory() throws Exception {
        var cfg=new Configuration(); cfg.addMapper(RechargeNotifyTaskMapper.class);
        var factory=new SqlSessionFactoryBean();factory.setDataSource(jdbc.getDataSource());factory.setConfiguration(cfg);
        return factory.getObject();
    }
    @Test void missingBankRouteRollsBackRedemptionAndRecoversOriginalEventAfterConfigurationReturns() throws Exception {
        seed(0, "");reset(credentials);
        var available=new java.util.concurrent.atomic.AtomicBoolean(false);
        var route=new MerchantNotifyRoute(NotifyDeliveryChannel.BOCOM_OPENAPI,"A39","r1",
                "https://bank.invalid/api/trade/J_IEP_001_2104/v1","p","app","RSA","key","bank");
        var preparation=new MerchantNotifyPreparationService((org,url)->{
            if(!available.get())throw new IllegalStateException("missing bank protocol");return Optional.of(route);
        },new V1MerchantNotifyPayloadSigner(credentials),new V12RedemptionNotifyPayloadSigner(credentials),clock);
        var bankStore=new MybatisMinisoWriteOffStore(events,receiptMapper,
                new MybatisRechargeNotifyTaskAdapter(new SqlSessionTemplate(sqlSessionFactory()).getMapper(RechargeNotifyTaskMapper.class),new ObjectMapper(),clock),
                new V12RedemptionNotifyPayloadSigner(credentials),new DataSourceTransactionManager(jdbc.getDataSource()),clock,3,
                Duration.ofSeconds(60),5,MinisoWriteOffObserver.NOOP,preparation);
        bankStore.accept(notice());bankStore.processBatch(1,0,10,"worker");
        assertThat(count("recharge_miniso_writeoff_event")).isEqualTo(1);
        assertThat(status()).isZero();assertThat(count("recharge_notify_task")).isZero();
        assertThat(eventStatus()).isEqualTo("RETRY");
        available.set(true);clock.advance();bankStore.processBatch(1,0,10,"worker");
        assertThat(eventStatus()).isEqualTo("PROCESSED");assertThat(count("recharge_notify_task")).isEqualTo(1);
        verifyNoInteractions(credentials);
    }

    @Test
    void earlyCallbackRetriesThenAtomicallyGeneratesVerifiableNotification() throws Exception {
        store.accept(notice());
        assertThat(store.processBatch(1, 0, 100, "worker").retried()).isEqualTo(1);
        assertThat(eventStatus()).isEqualTo("RETRY");
        seed(0, "https://merchant.example/notify");
        clock.advance();
        assertThat(store.processBatch(1, 0, 100, "worker").processed()).isEqualTo(1);
        assertThat(eventStatus()).isEqualTo("PROCESSED");
        assertThat(status()).isEqualTo(1);
        assertThat(jdbc.queryForObject("select voucher_use_state from recharge_order_ext", String.class))
                .isEqualTo("2");
        assertThat(jdbc.queryForObject("select voucher_use_time from recharge_order_ext", java.sql.Timestamp.class)
                .toLocalDateTime()).isEqualTo(LocalDateTime.parse("2026-10-17T09:29:00"));
        assertThat(count("recharge_notify_task")).isEqualTo(1);
        var payload =
                new ObjectMapper()
                        .readValue(
                                jdbc.queryForObject(
                                        "select payload from recharge_notify_task", String.class),
                                new com.fasterxml.jackson.core.type.TypeReference<
                                        Map<String, Object>>() {});
        assertThat(payload)
                .containsEntry("use_time", "2026-10-17 09:29:00")
                .containsEntry("invalid_time", "")
                .containsEntry("state_type", "3")
                .containsEntry("use_state", "2")
                .containsEntry("keyId", "K1");
        assertThat(
                        V12MessageSignatures.verifyCallback(
                                V12MessageSignatures.Operation.WRITEOFF_CALLBACK,
                                payload,
                                "K1",
                                KEY))
                .isTrue();
        var original =
                jdbc.queryForObject("select payload from recharge_notify_task", String.class);
        when(credentials.findCurrent("A39")).thenReturn(Optional.empty());
        store.accept(notice());
        clock.advance();
        store.processBatch(1, 0, 100, "worker");
        assertThat(jdbc.queryForObject("select payload from recharge_notify_task", String.class))
                .isEqualTo(original);
    }

    @Test
    void blankUrlDoesNotReadSigningKey() {
        seed(0, "");
        store.accept(notice());
        store.processBatch(1, 0, 100, "w");
        assertThat(status()).isEqualTo(1);
        assertThat(eventStatus()).isEqualTo("PROCESSED");
        assertThat(count("recharge_notify_task")).isZero();
        verify(credentials, never()).findCurrent(anyString());
    }

    @Test
    void unavailableKeyRollsBackFactButPendingEventStopsVoidAndRelease() {
        seed(0, "https://merchant.example");
        when(credentials.findCurrent("A39")).thenReturn(Optional.empty());
        store.accept(notice());
        store.processBatch(1, 0, 100, "w");
        assertThat(status()).isZero();
        assertThat(eventStatus()).isEqualTo("RETRY");
        jdbc.update(
                "insert into recharge_dispatch_outbox(event_id,status) values('INV','PROCESSING')");
        jdbc.update("update mc_order set order_status=13");
        jdbc.update("update recharge_order_ext set dispatch_status='CANCEL_REQUESTED'");
        assertThat(protection.blockInvalidation("INV", "O1")).isTrue();
        assertThat(jdbc.queryForObject("select order_status from mc_order", Integer.class))
                .isEqualTo(12);
        assertThat(jdbc.queryForObject("select status from recharge_dispatch_outbox", String.class))
                .isEqualTo("MANUAL_REQUIRED");
        assertThatThrownBy(() -> tx.executeWithoutResult(s -> receipts.save(receipt(3))))
                .isInstanceOf(MinisoWriteOffException.class)
                .hasMessage("REDEEM_VOID_CONFLICT");
        assertThat(status()).isZero();
    }

    @Test
    void voidOrForeignOwnershipOrImpossibleUseTimeGoesToManual() {
        seed(3, "");
        store.accept(notice());
        assertThat(store.processBatch(1, 0, 100, "w").manual()).isEqualTo(1);
        assertThat(status()).isEqualTo(3);
        assertThat(eventStatus()).isEqualTo("MANUAL_REQUIRED");
    }

    @Test
    void foreignMerchantDoesNotUpdateReceipt() {
        seed(0, "");
        jdbc.update("update recharge_order_ext set org_code='OTHER'");
        store.accept(notice());
        store.processBatch(1, 0, 100, "w");
        assertThat(status()).isZero();
        assertThat(eventStatus()).isEqualTo("MANUAL_REQUIRED");
    }

    @Test
    void expiredAndAlreadyUsedReceiptsAcceptHistoricalUseTime() {
        seed(2, "");
        store.accept(notice());
        store.processBatch(1, 0, 100, "w");
        assertThat(status()).isEqualTo(1);
        assertThat(
                        jdbc.queryForObject(
                                "select use_time from recharge_miniso_receipt",
                                LocalDateTime.class))
                .isEqualTo(USED);
    }

    @Test
    void taskInsertFailureRollsBackEverythingExceptRetryRecord() {
        seed(0, "https://merchant.example");
        store.accept(notice());
        jdbc.execute("drop table recharge_notify_task");
        assertThat(store.processBatch(1, 0, 100, "w").retried()).isEqualTo(1);
        assertThat(status()).isZero();
        assertThat(
                        jdbc.queryForObject(
                                "select use_time from recharge_miniso_receipt",
                                LocalDateTime.class))
                .isNull();
        assertThat(eventStatus()).isEqualTo("RETRY");
    }

    @Test
    void retryLimitAndStaleFailureCannotOverwriteCompletion() {
        store.accept(notice());
        store.processBatch(1, 0, 100, "w");
        clock.advance();
        store.processBatch(1, 0, 100, "w");
        assertThat(eventStatus()).isEqualTo("MANUAL_REQUIRED");
        jdbc.update("update recharge_miniso_writeoff_event set status='PROCESSED',version=10");
        assertThat(
                        events.reschedule(
                                notice().eventId(), 0, 1, "RETRY", USED, "DB_UNAVAILABLE", USED))
                .isZero();
        assertThat(eventStatus()).isEqualTo("PROCESSED");
    }

    @Test
    void concurrentReceiversAndProcessorsCreateOneEventAndOneTask() throws Exception {
        seed(0, "https://merchant.example");
        try (var executor = Executors.newFixedThreadPool(4)) {
            var sends = new ArrayList<Callable<Void>>();
            for (int i = 0; i < 12; i++)
                sends.add(
                        () -> {
                            store.accept(notice());
                            return null;
                        });
            for (var result : executor.invokeAll(sends)) result.get();
            var jobs = new ArrayList<Callable<Void>>();
            for (int i = 0; i < 4; i++)
                jobs.add(
                        () -> {
                            store.processBatch(1, 0, 100, "w");
                            return null;
                        });
            for (var result : executor.invokeAll(jobs)) result.get();
        }
        assertThat(count("recharge_miniso_writeoff_event")).isEqualTo(1);
        assertThat(count("recharge_notify_task")).isEqualTo(1);
        assertThat(eventStatus()).isEqualTo("PROCESSED");
    }

    @Test
    void commitFailureIsSanitizedAndCannotBeAcknowledged() {
        var failingCommit =
                new org.springframework.transaction.PlatformTransactionManager() {
                    public org.springframework.transaction.TransactionStatus getTransaction(
                            org.springframework.transaction.TransactionDefinition definition) {
                        return tx.getTransactionManager().getTransaction(definition);
                    }

                    public void commit(org.springframework.transaction.TransactionStatus status) {
                        tx.getTransactionManager().rollback(status);
                        throw new org.springframework.transaction.TransactionSystemException(
                                "SQL contains sensitive C1");
                    }

                    public void rollback(org.springframework.transaction.TransactionStatus status) {
                        tx.getTransactionManager().rollback(status);
                    }
                };
        var receiver =
                new MybatisMinisoWriteOffStore(
                        events,
                        receiptMapper,
                        mock(RechargeNotifyTaskPort.class),
                        new V12RedemptionNotifyPayloadSigner(credentials),
                        failingCommit,
                        clock,
                        2,
                        Duration.ofSeconds(60),
                        5);
        assertThatThrownBy(() -> receiver.accept(notice()))
                .isInstanceOf(MinisoWriteOffException.class)
                .hasNoCause()
                .hasMessageNotContaining("C1");
        assertThat(count("recharge_miniso_writeoff_event")).isZero();
    }

    @Test
    void restartedEventReusesExistingTaskWithoutCurrentKeyOrResigning() {
        seed(1, "https://merchant.example");
        store.accept(notice());
        store.processBatch(1, 0, 100, "worker");
        var original =
                jdbc.queryForObject("select payload from recharge_notify_task", String.class);
        jdbc.update("update recharge_miniso_writeoff_event set status='RETRY',version=version+1");
        when(credentials.findCurrent("A39")).thenReturn(Optional.empty());
        clearInvocations(credentials);
        assertThat(store.processBatch(1, 0, 100, "worker").processed()).isEqualTo(1);
        assertThat(jdbc.queryForObject("select payload from recharge_notify_task", String.class))
                .isEqualTo(original);
        verify(credentials, never()).findCurrent(anyString());
    }

    @Test
    void outOfRangeUsageCannotCommitRedemptionFact() {
        seed(0, "https://merchant.example");
        store.accept(new MinisoWriteOffNotification("C1", USED.plusYears(1)));
        assertThat(store.processBatch(1, 0, 100, "worker").manual()).isEqualTo(1);
        assertThat(status()).isZero();
        assertThat(count("recharge_notify_task")).isZero();
    }

    @Test
    void conflictingExistingTaskLeavesFactUnchangedAndRequiresManualReview() {
        seed(0, "https://merchant.example");
        store.accept(notice());
        store.processBatch(1, 0, 100, "worker");
        jdbc.update("update recharge_miniso_receipt set coupon_status=0,use_time=null");
        jdbc.update("update recharge_miniso_writeoff_event set status='RETRY',version=version+1");
        jdbc.update("update recharge_notify_task set org_code='OTHER'");
        assertThat(store.processBatch(1, 0, 100, "worker").manual()).isEqualTo(1);
        assertThat(status()).isZero();
        assertThat(count("recharge_notify_task")).isEqualTo(1);
    }

    @Test
    void refundedOrderRemainsRefundedAfterLateRedemption() {
        seed(0, "");
        jdbc.update("update mc_order set order_status=11");
        store.accept(notice());
        assertThat(store.processBatch(1, 0, 100, "worker").manual()).isEqualTo(1);
        assertThat(status()).isZero();
        assertThat(jdbc.queryForObject("select order_status from mc_order", Integer.class))
                .isEqualTo(11);
    }

    @Test
    void pendingMetricsExcludeCompletedHistoryAndKeepManualEvidence() {
        seed(0, ""); store.accept(notice());
        assertThat(events.pendingStats().pending()).isEqualTo(1);
        store.processBatch(1, 0, 100, "worker");
        assertThat(events.pendingStats().pending()).isZero();
        assertThat(events.pendingStats().oldest()).isNull();
        jdbc.update("update recharge_miniso_writeoff_event set status='MANUAL_REQUIRED'");
        assertThat(events.pendingStats().manual()).isEqualTo(1);
        assertThat(events.pendingNotifications()).isZero();
    }

    @Test
    void shardFilterDoesNotProcessAnotherShardEvent() {
        seed(0, "");
        store.accept(notice());
        int shard = (int) (crc32(notice().eventId()) % 2);
        assertThat(store.processBatch(2, 1 - shard, 100, "worker").scanned()).isZero();
        assertThat(store.processBatch(2, shard, 100, "worker").processed()).isEqualTo(1);
    }

    void seed(int status, String url) {
        jdbc.update(
                "insert into"
                    + " mc_order(order_Id,org_code,out_orderId,account,num,supplier_channel,order_status,callback_url)"
                    + " values('O1','A39','OUT1','13800138000',1,'MINISO',12,?)",
                url);
        jdbc.update(
                "insert into"
                    + " recharge_order_ext(order_id,org_code,out_order_sn,supplier_batch_id,persist_status,dispatch_status)"
                    + " values('O1','A39','OUT1','00100023','DB_PERSISTED','SUCCESS')");
        tx.executeWithoutResult(s -> receipts.save(receipt(status)));
    }

    MinisoFulfillmentReceipt receipt(int status) {
        return new MinisoFulfillmentReceipt(
                "O1",
                "A39",
                100023,
                "C1",
                "13800138000",
                "2026-10-01 00:00:00",
                "2026-12-01 00:00:00",
                status);
    }

    MinisoWriteOffNotification notice() {
        return new MinisoWriteOffNotification("C1", USED);
    }

    int status() {
        return jdbc.queryForObject(
                "select coupon_status from recharge_miniso_receipt", Integer.class);
    }

    int count(String table) {
        return jdbc.queryForObject("select count(*) from " + table, Integer.class);
    }

    String eventStatus() {
        return jdbc.queryForObject(
                "select status from recharge_miniso_writeoff_event", String.class);
    }

    public static long crc32(String s) {
        var crc = new CRC32();
        crc.update(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return crc.getValue();
    }

    static final class MutableClock extends Clock {
        Instant now = Instant.parse("2026-10-17T01:30:00Z");

        void advance() {
            now = now.plusSeconds(61);
        }

        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        public Clock withZone(ZoneId zone) {
            return this;
        }

        public Instant instant() {
            return now;
        }
    }
}

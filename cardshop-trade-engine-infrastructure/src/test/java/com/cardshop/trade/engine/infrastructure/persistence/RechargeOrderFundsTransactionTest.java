package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.cardshop.trade.engine.infrastructure.redis.*;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.*;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** 真实SQL事务及核销竞争；Redis不确定回包由独立资金桩保留执行事实，真实Lua另有合同测试。 */
public class RechargeOrderFundsTransactionTest {
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-21T08:40:15Z"), ZoneOffset.UTC);
    private JdbcTemplate jdbc;
    private RechargeFulfillmentOrderPort orders;
    private MinisoWriteOffEventMapper events;
    private MinisoReceiptMapper receipts;
    private DataSourceTransactionManager tm;
    private SqlSessionTemplate session;
    private MybatisRechargeOrderFundsAdapter adapter;
    private final AtomicInteger credits = new AtomicInteger();
    private final AtomicBoolean refunded = new AtomicBoolean();
    private final AtomicReference<BalanceReservationReleaseCommand> command = new AtomicReference<>();
    private Runnable duringRedis = () -> {};
    private boolean failAfterClear;
    private boolean rejectRedis;

    @BeforeEach void setup() throws Exception {
        credits.set(0); refunded.set(false); command.set(null);
        duringRedis = () -> {}; failAfterClear = false; rejectRedis = false;
        var ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=2000");
        jdbc = new JdbcTemplate(ds);
        jdbc.execute("create table mc_order(order_Id varchar(64) primary key, org_code varchar(64),out_orderId varchar(64),account varchar(64),num int,supplier_channel varchar(32),order_status int,callback_url varchar(128))");
        jdbc.execute("create table recharge_order_ext(order_id varchar(64) primary key,org_code varchar(64),out_order_sn varchar(64),supplier_batch_id varchar(64),persist_status varchar(32),dispatch_status varchar(32),balance_reserved boolean)");
        jdbc.execute("create table recharge_miniso_receipt(order_id varchar(64) primary key,org_code varchar(64),batch_id int,coupon_code varchar(128) unique,phone varchar(64),begin_time varchar(19),end_time varchar(19),coupon_status int,use_time timestamp)");
        jdbc.execute("create table recharge_miniso_writeoff_event(event_id varchar(80) primary key,coupon_code varchar(128) unique,use_time timestamp,order_id varchar(64),org_code varchar(64),status varchar(24),retry_count int,next_retry_time timestamp,last_error varchar(128),version bigint,received_at timestamp,updated_at timestamp)");
        jdbc.execute("create table recharge_balance_delta_outbox(id bigint auto_increment primary key,event_id varchar(128) unique,org_code varchar(64),order_id varchar(64),out_order_sn varchar(64),delta_type varchar(32),delta_amount bigint,status varchar(32),retry_count int,max_retry_count int,next_retry_time timestamp,locked_by varchar(64),locked_until timestamp,version bigint,last_error varchar(128),created_at timestamp,updated_at timestamp)");
        jdbc.update("insert into mc_order values('O1','A35','OUT1','13800138000',1,'MINISO',12,'')");
        jdbc.update("insert into recharge_order_ext values('O1','A35','OUT1','1','DB_PERSISTED','CANCELLED',true)");
        jdbc.update("insert into recharge_miniso_receipt values('O1','A35',1,'C1','13800138000','2026-10-20 00:00:00','2026-10-23 23:59:59',3,null)");
        insertDelta("balance-flush-O1", "A35", "PRE_DEDUCT", -100);
        var cfg = new MybatisConfiguration();
        cfg.setMapUnderscoreToCamelCase(true);
        cfg.addMapper(MinisoReceiptMapper.class);
        cfg.addMapper(MinisoWriteOffEventMapper.class);
        cfg.addMapper(RechargeBalanceDeltaOutboxMapper.class);
        cfg.addMapper(McExtDepartMapper.class);
        var factory = new MybatisSqlSessionFactoryBean(); factory.setDataSource(ds); factory.setConfiguration(cfg);
        session = new SqlSessionTemplate(factory.getObject());
        receipts = session.getMapper(MinisoReceiptMapper.class);
        events = session.getMapper(MinisoWriteOffEventMapper.class);
        var deltas = session.getMapper(RechargeBalanceDeltaOutboxMapper.class);
        orders = mock(RechargeFulfillmentOrderPort.class);
        when(orders.findByOrderId("O1")).thenAnswer(inv -> Optional.of(current()));
        doAnswer(inv -> {
            jdbc.update("update recharge_order_ext set balance_reserved=false where order_id='O1'");
            if (failAfterClear) throw new IllegalStateException("injected failure after clear");
            return null;
        }).when(orders).markBalanceReleased("O1");
        BalanceReservationReleasePort redis = new BalanceReservationReleasePort() {
            @Override public BalanceReservationReleaseResult release(BalanceReservationReleaseCommand value) {
                command.set(value); duringRedis.run();
                if (rejectRedis) return BalanceReservationReleaseResult.failed("REDIS_UNAVAILABLE", "test");
                if (value.alreadyReleasedOnly() && !refunded.get())
                    return BalanceReservationReleaseResult.failed("RELEASE_EVIDENCE_MISSING", "test");
                if (refunded.compareAndSet(false, true)) { credits.incrementAndGet(); return BalanceReservationReleaseResult.releasedSuccess(); }
                return BalanceReservationReleaseResult.alreadyReleasedSuccess();
            }
            @Override public boolean confirm(RechargeFulfillmentOrder order) { duringRedis.run(); return true; }
        };
        tm = new DataSourceTransactionManager(ds);
        var protection = new MybatisMinisoWriteOffProtection(events, receipts, tm, CLOCK, 5);
        adapter = new MybatisRechargeOrderFundsAdapter(redis, orders,
                new MybatisBalanceDeltaOutboxWriteAdapter(deltas, CLOCK), deltas, receipts,
                () -> protection, tm, 5);
    }

    @Test void refundRunsWithOrderLocksAndCommitsDeltaAndFlagTogether() {
        duringRedis = () -> assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isTrue();
        assertThat(adapter.release(current(), "作废退款").succeeded()).isTrue();
        assertThat(releaseCount()).isEqualTo(1);
        assertThat(current().balanceReserved()).isFalse();
        assertThat(command.get().releaseEventId()).isEqualTo("balance-flush-release-O1");
        assertThat(command.get().expectedFundsStatus()).isEqualTo("CONSUMED");
        assertThat(command.get().allowLegacyCancelledRefund()).isTrue();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"INIT", "PROCESSING", "FAILED", "MANUAL_REQUIRED"})
    void unfinishedOriginalDebitDoesNotAuthorizeLegacyRefund(String status) {
        jdbc.update("update recharge_balance_delta_outbox set status=? where delta_type='PRE_DEDUCT'", status);
        assertThat(adapter.release(current(), "不能授权旧单兼容").succeeded()).isTrue();
        assertThat(command.get().allowLegacyCancelledRefund()).isFalse();
    }

    @Test void failedOrderNeverAuthorizesLegacyCancelledRefund() {
        jdbc.update("update mc_order set order_status=?",
                com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode.FAILED.code());
        jdbc.update("update recharge_order_ext set dispatch_status='FAILED'");
        assertThat(adapter.release(current(), "普通失败退款").succeeded()).isTrue();
        assertThat(command.get().allowLegacyCancelledRefund()).isFalse();
        assertThat(command.get().expectedFundsStatus()).isEqualTo("RESERVED");
    }

    @Test void mysqlRollbackAfterRedisSuccessIsRecoveredWithoutSecondCredit() {
        failAfterClear = true;
        assertThatThrownBy(() -> adapter.release(current(), "作废退款")).hasMessageContaining("injected failure");
        assertThat(releaseCount()).isZero();
        assertThat(current().balanceReserved()).isTrue();
        assertThat(credits.get()).isEqualTo(1);
        failAfterClear = false;
        assertThat(adapter.release(current(), "恢复收尾").succeeded()).isTrue();
        assertThat(releaseCount()).isEqualTo(1);
        assertThat(current().balanceReserved()).isFalse();
        assertThat(credits.get()).isEqualTo(1);
    }

    @Test void inconsistentExistingDeltaIsRejectedBeforeRedisCredit() {
        insertDelta("balance-flush-release-O1", "WRONG", "RELEASE", 100);
        assertThatThrownBy(() -> adapter.release(current(), "作废退款")).hasMessageContaining("delta");
        assertThat(credits.get()).isZero();
        assertThat(current().balanceReserved()).isTrue();
    }

    @Test void existingDeltaDoesNotAuthorizeSecondCreditWhenRedisReceiptIsMissing() {
        insertDelta("balance-flush-release-O1", "A35", "RELEASE", 100);
        assertThat(adapter.release(current(), "恢复收尾").succeeded()).isFalse();
        assertThat(credits.get()).isZero();
        assertThat(current().balanceReserved()).isTrue();
    }

    @Test void failedRedisDoesNotCreateDeltaOrClearFlag() {
        rejectRedis = true;
        assertThat(adapter.release(current(), "作废退款").succeeded()).isFalse();
        assertThat(releaseCount()).isZero(); assertThat(current().balanceReserved()).isTrue();
    }

    @Test void successWithoutCancellationCannotRequestRefund() {
        jdbc.update("update recharge_order_ext set dispatch_status='SUCCESS'");
        assertThatThrownBy(() -> adapter.release(current(), "非法退款")).hasMessageContaining("eligible");
        assertThat(credits.get()).isZero();
    }

    @Test void orderOwnershipMismatchCannotReachRedis() {
        jdbc.update("update recharge_order_ext set org_code='OTHER'");
        assertThatThrownBy(() -> adapter.release(current(), "非法退款")).hasMessageContaining("identity");
        assertThat(credits.get()).isZero();
    }

    @Test void acceptedRedemptionEvidenceBlocksCancelledRefund() {
        events.insertIfAbsent("redemption-C1", "C1", LocalDateTime.of(2026,10,21,16,0), "O1", "A35", LocalDateTime.now(CLOCK));
        assertThatThrownBy(() -> adapter.release(current(), "作废退款")).isInstanceOf(MinisoWriteOffException.class);
        assertThat(credits.get()).isZero(); assertThat(releaseCount()).isZero();
    }

    @Test void confirmationUsesTransactionAndRequiresSuccessfulUncancelledOrder() {
        jdbc.update("update recharge_order_ext set dispatch_status='SUCCESS'");
        duringRedis = () -> assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isTrue();
        assertThat(adapter.confirm(current())).isTrue();
        jdbc.update("update recharge_order_ext set dispatch_status='CANCELLED'");
        assertThatThrownBy(() -> adapter.confirm(current())).hasMessageContaining("eligible");
    }

    @Test void redemptionReceptionCannotEnterGapBetweenProtectionAndCredit() throws Exception {
        var entered = new CountDownLatch(1); var resume = new CountDownLatch(1);
        duringRedis = () -> {
            entered.countDown();
            try { assertThat(resume.await(3, TimeUnit.SECONDS)).isTrue(); }
            catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new IllegalStateException(ex); }
        };
        var store = new MybatisMinisoWriteOffStore(events, receipts, mock(RechargeNotifyTaskPort.class),
                mock(V12RedemptionNotifyPayloadSigner.class), tm, CLOCK, 10, Duration.ofSeconds(30), 5);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var refund = pool.submit(() -> adapter.release(current(), "作废退款"));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            var callback = pool.submit(() -> store.accept(new MinisoWriteOffNotification("C1", LocalDateTime.of(2026,10,21,16,0))));
            try { assertThatThrownBy(() -> callback.get(150, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class); }
            finally { resume.countDown(); }
            assertThat(refund.get(3, TimeUnit.SECONDS).succeeded()).isTrue();
            callback.get(3, TimeUnit.SECONDS);
            assertThat(credits.get()).isEqualTo(1);
        }
    }

    /** 真实Redis适配器、Lua、MySQL资格和刷账连用，避免只通过桩证明兼容授权。 */
    @Nested
    @org.junit.jupiter.api.condition.EnabledIfSystemProperty(named = "balance.localRedisTests", matches = "true")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class LegacyRedisIntegration {
        private Process process;
        private java.nio.file.Path log;
        private org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory factory;
        private org.springframework.data.redis.core.ReactiveStringRedisTemplate redis;
        private final RechargeRedisKeyBuilder keys = new RechargeRedisKeyBuilder("legacy:integration", 128);
        private final AtomicBoolean loseNextReply = new AtomicBoolean();

        @BeforeAll void startPrivateRedis() throws Exception {
            int port;
            try (var socket = new java.net.ServerSocket(0)) { port = socket.getLocalPort(); }
            log = java.nio.file.Files.createTempFile("legacy-refund-integration-", ".log");
            process = new ProcessBuilder("redis-server", "--bind", "127.0.0.1", "--port", Integer.toString(port),
                    "--save", "", "--appendonly", "no").redirectErrorStream(true).redirectOutput(log.toFile()).start();
            boolean ready = false;
            for (int attempt = 0; attempt < 100; attempt++) {
                try (var socket = new java.net.Socket("127.0.0.1", port)) { ready = true; break; }
                catch (java.io.IOException unavailable) { Thread.sleep(20); }
            }
            assertThat(ready).isTrue();
            factory = new org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory("127.0.0.1", port);
            factory.afterPropertiesSet(); factory.start();
            redis = new org.springframework.data.redis.core.ReactiveStringRedisTemplate(factory);
        }

        @AfterAll void stopPrivateRedis() throws Exception {
            if (factory != null) factory.destroy();
            if (process != null) {
                process.destroy();
                if (!process.waitFor(3, TimeUnit.SECONDS)) process.destroyForcibly();
            }
            if (log != null) java.nio.file.Files.deleteIfExists(log);
        }

        @BeforeEach void connectRealFundsAdapter() {
            // 仅连接本类随机loopback端口，旧结构与现场相同但全部身份和金额均为合成数据。
            try (var connection = factory.getConnection()) { connection.serverCommands().flushDb(); }
            loseNextReply.set(false);
            redis.opsForHash().putAll(keys.balanceKey("A35"), Map.of("available", "900", "frozen", "0")).block();
            redis.opsForHash().putAll(keys.reservationKey("A35", "O1"), Map.of("order_id", "O1",
                    "out_order_sn", "OUT1", "amount", "100", "status", "PENDING_MQ")).block();
            var sessions = new LettuceRechargeRedisSessionFactory(factory);
            RechargeRedisSessionFactory observed = () -> {
                var delegate = sessions.openSession();
                return new RechargeRedisSession() {
                    @Override public reactor.core.publisher.Mono<Object> execute(
                            org.springframework.data.redis.core.script.RedisScript<?> script, List<String> keyList,
                            List<String> arguments) {
                        // Lua真实完成后再丢回包；不能在执行前抛异常冒充“结果未知”。
                        return delegate.execute(script, keyList, arguments).flatMap(value ->
                                loseNextReply.getAndSet(false) ? reactor.core.publisher.Mono.never()
                                        : reactor.core.publisher.Mono.just(value));
                    }
                    @Override public reactor.core.publisher.Mono<Long> waitForReplication(int replicas, Duration timeout) {
                        throw new AssertionError("退款不调用WAIT");
                    }
                    @Override public void close() { delegate.close(); }
                };
            };
            var realRedis = new RedisBalanceReservationReleaseAdapter(observed, keys,
                    new RechargeRedisScriptLoader(), CLOCK, Duration.ofMillis(500));
            var deltas = session.getMapper(RechargeBalanceDeltaOutboxMapper.class);
            adapter = new MybatisRechargeOrderFundsAdapter(realRedis, orders,
                    new MybatisBalanceDeltaOutboxWriteAdapter(deltas, CLOCK), deltas, receipts,
                    () -> new MybatisMinisoWriteOffProtection(events, receipts, tm, CLOCK, 5), tm, 5);
        }

        @Test void legacyRefundReachesMysqlBalanceAndReplayDoesNotCreditTwice() {
            assertThat(adapter.release(current(), "旧单退款").succeeded()).isTrue();
            assertRefund("1000", false, 1);
            assertThat(adapter.release(current(), "重复恢复").alreadyReleased()).isTrue();
            assertRefund("1000", false, 1);

            jdbc.execute("create table mc_ext_depart(id bigint primary key,org_code varchar(64),balance bigint)");
            jdbc.update("insert into mc_ext_depart values(1,'A35',900)");
            jdbc.execute("CREATE ALIAS CRC32 FOR 'com.cardshop.trade.engine.infrastructure.persistence.RechargeOrderFundsTransactionTest.crc32'");
            var flush = new MybatisRechargeBalanceDeltaFlushAdapter(
                    session.getMapper(RechargeBalanceDeltaOutboxMapper.class), session.getMapper(McExtDepartMapper.class),
                    CLOCK, Duration.ofSeconds(30), List.of(Duration.ofSeconds(10)), 10, tm);
            assertThat(flush.flushDue(1, 0, 10, "local-legacy-refund").succeeded()).isEqualTo(1);
            assertThat(jdbc.queryForObject("select balance from mc_ext_depart where org_code='A35'", Long.class)).isEqualTo(1000L);
            assertThat(jdbc.queryForObject("select status from recharge_balance_delta_outbox where delta_type='RELEASE'", String.class))
                    .isEqualTo("SUCCESS");
            assertThat(flush.flushDue(1, 0, 10, "local-legacy-replay").succeeded()).isZero();
        }

        @Test void mysqlRollbackRecoversRealLegacyRedisRefundWithoutSecondCredit() {
            failAfterClear = true;
            assertThatThrownBy(() -> adapter.release(current(), "SQL回滚")).hasMessageContaining("injected failure");
            assertRefund("1000", true, 0);
            failAfterClear = false;
            assertThat(adapter.release(current(), "只补SQL").alreadyReleased()).isTrue();
            assertRefund("1000", false, 1);
        }

        @Test void lostReplyAfterRealLuaCommitRecoversWithoutSecondCredit() {
            loseNextReply.set(true);
            assertThat(adapter.release(current(), "丢失回包").succeeded()).isFalse();
            assertRefund("1000", true, 0);
            assertThat(adapter.release(current(), "恢复回包").alreadyReleased()).isTrue();
            assertRefund("1000", false, 1);
        }

        @Test void incompleteDebitCannotRefundLegacyReservation() {
            jdbc.update("update recharge_balance_delta_outbox set status='INIT' where delta_type='PRE_DEDUCT'");
            assertThat(adapter.release(current(), "原扣款未完成").failCode()).isEqualTo("RESERVATION_FUNDS_STATUS_MISSING");
            assertRefund("900", true, 0);
        }

        private void assertRefund(String available, boolean reserved, int expectedDeltas) {
            assertThat(redis.opsForHash().get(keys.balanceKey("A35"), "available").block()).isEqualTo(available);
            assertThat(redis.opsForHash().get(keys.balanceKey("A35"), "frozen").block()).isEqualTo("0");
            assertThat(current().balanceReserved()).isEqualTo(reserved);
            assertThat(releaseCount()).isEqualTo(expectedDeltas);
            assertThat(redis.hasKey(keys.orderKey("A35", "OUT1")).block()).isFalse();
        }
    }

    /** H2仅缺MySQL分片函数，金额/状态/增量更新仍执行生产Mapper SQL。 */
    public static long crc32(String value) {
        var crc = new java.util.zip.CRC32();
        crc.update(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return crc.getValue();
    }

    private RechargeFulfillmentOrder current() {
        String dispatch = jdbc.queryForObject("select dispatch_status from recharge_order_ext where order_id='O1'", String.class);
        return new RechargeFulfillmentOrder("O1","OUT1","A35","13800138000","P","SKU","MINISO",100,"",
                jdbc.queryForObject("select order_status from mc_order where order_Id='O1'", Integer.class),
                "DB_PERSISTED","PASSED",true,
                jdbc.queryForObject("select balance_reserved from recharge_order_ext where order_id='O1'", Boolean.class),
                dispatch, "CANCELLED".equals(dispatch) ? CLOCK.instant() : null);
    }
    private int releaseCount() { return jdbc.queryForObject("select count(*) from recharge_balance_delta_outbox where delta_type='RELEASE'", Integer.class); }
    private void insertDelta(String id, String org, String type, long amount) {
        jdbc.update("insert into recharge_balance_delta_outbox(event_id,org_code,order_id,out_order_sn,delta_type,delta_amount,status,version) values(?,?,'O1','OUT1',?,?,'SUCCESS',0)",id,org,type,amount);
    }
}

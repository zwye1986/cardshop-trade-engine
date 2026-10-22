package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxCommand;
import com.cardshop.trade.engine.application.recharge.SupplierQueryOutboxCommand;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeDispatchOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeDispatchOutboxMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 派单履约 outbox MyBatis 适配器测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」「9.3 作废设计」。
 * DB_PERSISTED 订单作废只写 {@code INVALID_ORDER} 可靠事件到 {@code recharge_dispatch_outbox}，
 * 接口线程不调用供应商。余额入队已拆到 {@link MybatisBalanceDeltaOutboxWriteAdapter}。
 */
class MybatisRechargeOutboxAdapterTest {

    @Test
    void enqueuesInvalidOrderOutboxWithEventIdShardKeyAndReasonPayload() {
        RecordingDispatchWriter writer = new RecordingDispatchWriter();
        MybatisRechargeDispatchOutboxAdapter adapter = new MybatisRechargeDispatchOutboxAdapter(
                writer,
                new ObjectMapper(),
                Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC));

        adapter.enqueueInvalidOrder(new InvalidRechargeOrderOutboxCommand(
                "A36", "ORDER-1", "OUT-1", "用户申请作废"));

        assertThat(writer.outbox.getEventId()).isEqualTo("invalid-ORDER-1");
        assertThat(writer.outbox.getEventType()).isEqualTo(OutboxEventType.INVALID_ORDER.name());
        assertThat(writer.outbox.getOrgCode()).isEqualTo("A36");
        assertThat(writer.outbox.getOrderId()).isEqualTo("ORDER-1");
        assertThat(writer.outbox.getOutOrderSn()).isEqualTo("OUT-1");
        assertThat(writer.outbox.getStatus()).isEqualTo(TaskStatus.INIT.name());
        assertThat(writer.outbox.getRetryCount()).isZero();
        assertThat(writer.outbox.getMaxRetryCount()).isEqualTo(10);
        assertThat(writer.outbox.getShardKey()).isEqualTo("invalid-ORDER-1");
        assertThat(writer.outbox.getNextRetryTime()).isEqualTo(LocalDateTime.of(2026, 7, 8, 11, 20));
        assertThat(writer.outbox.getPayload())
                .contains("\"reason\":\"用户申请作废\"")
                .contains("\"order_id\":\"ORDER-1\"");
    }

    @Test
    void enqueuesSupplierQueryOutboxWithStableEventIdAndOrderIdentityPayload() {
        RecordingDispatchWriter writer = new RecordingDispatchWriter();
        MybatisRechargeDispatchOutboxAdapter adapter = new MybatisRechargeDispatchOutboxAdapter(
                writer,
                new ObjectMapper(),
                Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC));

        adapter.enqueueSupplierQuery(new SupplierQueryOutboxCommand(
                "supplier-query-ORDER-1",
                "A36",
                "ORDER-1",
                "OUT-1",
                "13800138000",
                "FNK_CGS_BX",
                "{\"selected_product_id\":\"SKU-1\"}",
                Duration.ofSeconds(2),
                6));

        assertThat(writer.outbox.getEventId()).isEqualTo("supplier-query-ORDER-1");
        assertThat(writer.outbox.getEventType()).isEqualTo(OutboxEventType.SUPPLIER_QUERY.name());
        assertThat(writer.outbox.getOrgCode()).isEqualTo("A36");
        assertThat(writer.outbox.getOrderId()).isEqualTo("ORDER-1");
        assertThat(writer.outbox.getOutOrderSn()).isEqualTo("OUT-1");
        assertThat(writer.outbox.getStatus()).isEqualTo(TaskStatus.INIT.name());
        assertThat(writer.outbox.getMaxRetryCount()).isEqualTo(6);
        assertThat(writer.outbox.getNextRetryTime()).isEqualTo(LocalDateTime.of(2026, 7, 8, 11, 20, 2));
        assertThat(writer.outbox.getShardKey()).isEqualTo("supplier-query-ORDER-1");
        assertThat(writer.outbox.getPayload())
                .contains("\"account\":\"13800138000\"")
                .contains("\"supplier_channel\":\"FNK_CGS_BX\"")
                .contains("selected_product_id");
    }

    @Test
    void selectDueForShardUsesAdapterClockForScanDueTime() {
        RechargeDispatchOutboxMapper mapper = mock(RechargeDispatchOutboxMapper.class);
        LocalDateTime expectedNow = LocalDateTime.of(2026, 7, 8, 11, 20);
        when(mapper.selectDueForShardByEventTypes(
                8,
                3,
                20,
                java.util.List.of(OutboxEventType.SUPPLIER_DISPATCH.name()),
                expectedNow))
                .thenReturn(java.util.List.of());
        MybatisRechargeDispatchOutboxAdapter adapter = new MybatisRechargeDispatchOutboxAdapter(
                mapper,
                new ObjectMapper(),
                Clock.fixed(Instant.parse("2026-07-08T03:20:00Z"), ZoneOffset.UTC));

        assertThat(adapter.selectDueForShard(8, 3, 20)).isEmpty();

        verify(mapper).selectDueForShardByEventTypes(
                8,
                3,
                20,
                java.util.List.of(OutboxEventType.SUPPLIER_DISPATCH.name()),
                expectedNow);
    }

    @ParameterizedTest
    @EnumSource(value = OutboxEventType.class, names = {"SUPPLIER_DISPATCH", "SUPPLIER_QUERY", "INVALID_ORDER"})
    void persistedDueControlsRearmAndOnlyOneWorkerCanClaim(OutboxEventType eventType) throws Exception {
        var fixture = dispatchFixture(eventType);
        Clock early = Clock.fixed(Instant.parse("2026-10-20T08:26:32.815Z"), ZoneOffset.UTC);
        var adapter = new MybatisRechargeDispatchOutboxAdapter(fixture.mapper(), new ObjectMapper(), early);

        assertThat(adapter.findDueByEventId("E1", eventType)).isEmpty();
        assertThat(adapter.findPendingRetryDelay("E1", eventType)).contains(Duration.ofMillis(185));

        var atDue = new MybatisRechargeDispatchOutboxAdapter(
                fixture.mapper(), new ObjectMapper(), Clock.offset(early, Duration.ofMillis(185)));
        assertThat(atDue.findPendingRetryDelay("E1", eventType)).contains(Duration.ZERO);
        var event = atDue.findDueByEventId("E1", eventType).orElseThrow();
        assertThat(atDue.claim(event, "timer-A", Duration.ofSeconds(60))).isPresent();
        assertThat(atDue.claim(event, "timer-B", Duration.ofSeconds(60))).isEmpty();
        assertThat(atDue.findPendingRetryDelay("E1", eventType)).isEmpty();
        assertThat(fixture.jdbc().queryForObject(
                "select retry_count from recharge_dispatch_outbox where event_id='E1'", Integer.class)).isZero();
        assertThat(fixture.jdbc().queryForObject(
                "select locked_by from recharge_dispatch_outbox where event_id='E1'", String.class))
                .isEqualTo("timer-A");
    }

    @Test
    void onlyOwnedPendingEventsCanRearmAndOverdueReadReturnsZero() throws Exception {
        var fixture = dispatchFixture(OutboxEventType.SUPPLIER_QUERY);
        var adapter = new MybatisRechargeDispatchOutboxAdapter(fixture.mapper(), new ObjectMapper(),
                Clock.fixed(Instant.parse("2026-10-20T08:26:33.001Z"), ZoneOffset.UTC));
        assertThat(adapter.findPendingRetryDelay("E1", OutboxEventType.SUPPLIER_QUERY)).contains(Duration.ZERO);
        assertThat(adapter.findPendingRetryDelay("missing", OutboxEventType.SUPPLIER_QUERY)).isEmpty();
        assertThat(adapter.findPendingRetryDelay("E1", OutboxEventType.SUPPLIER_DISPATCH)).isEmpty();
        for (String state : new String[]{"SUCCESS", "MANUAL_REQUIRED", "PROCESSING"}) {
            fixture.jdbc().update("update recharge_dispatch_outbox set status=?", state);
            assertThat(adapter.findPendingRetryDelay("E1", OutboxEventType.SUPPLIER_QUERY)).isEmpty();
        }
        fixture.jdbc().update("update recharge_dispatch_outbox set status='FAILED'");
        assertThat(adapter.findPendingRetryDelay("E1", OutboxEventType.SUPPLIER_QUERY)).contains(Duration.ZERO);
        fixture.jdbc().update("update recharge_dispatch_outbox set retry_count=max_retry_count");
        assertThat(adapter.findPendingRetryDelay("E1", OutboxEventType.SUPPLIER_QUERY)).isEmpty();
        fixture.jdbc().update("update recharge_dispatch_outbox set retry_count=0");
        // 三个身份键和持久化哨兵缺一不可，不能只按 order_id 给旧订单续排。
        for (String column : new String[]{"org_code", "out_order_sn", "order_id", "persist_status"}) {
            String original = fixture.jdbc().queryForObject(
                    "select " + column + " from recharge_order_ext", String.class);
            fixture.jdbc().update("update recharge_order_ext set " + column + "='MISMATCH'");
            assertThat(adapter.findPendingRetryDelay("E1", OutboxEventType.SUPPLIER_QUERY)).isEmpty();
            fixture.jdbc().update("update recharge_order_ext set " + column + "=?", original);
        }
        fixture.jdbc().update("delete from recharge_order_ext");
        assertThat(adapter.findPendingRetryDelay("E1", OutboxEventType.SUPPLIER_QUERY)).isEmpty();
    }

    @Test
    void expiredLeaseIsReclaimedUsingInjectedShanghaiClockRatherThanSystemTime() throws Exception {
        var fixture = dispatchFixture(OutboxEventType.SUPPLIER_DISPATCH);
        fixture.jdbc().update("""
                update recharge_dispatch_outbox set status='PROCESSING',
                  locked_by='old-worker', locked_until='2099-01-01 16:00:00'
                """);
        var adapter = new MybatisRechargeDispatchOutboxAdapter(fixture.mapper(), new ObjectMapper(),
                Clock.fixed(Instant.parse("2099-01-01T08:00:01Z"), ZoneOffset.UTC));
        var event = adapter.findDueByEventId("E1", OutboxEventType.SUPPLIER_DISPATCH).orElseThrow();

        assertThat(adapter.claim(event, "recovery-worker", Duration.ofSeconds(60))).isPresent();
        assertThat(fixture.jdbc().queryForObject(
                "select locked_until from recharge_dispatch_outbox", LocalDateTime.class))
                .isEqualTo(LocalDateTime.of(2099, 1, 1, 16, 1, 1));
    }

    @Test
    void staleWorkerCannotFinishOrOverwriteAReclaimedLease() throws Exception {
        var fixture = dispatchFixture(OutboxEventType.INVALID_ORDER);
        var clock = Clock.fixed(Instant.parse("2026-10-20T08:26:33Z"), ZoneOffset.UTC);
        var old = new MybatisRechargeDispatchOutboxAdapter(fixture.mapper(), new ObjectMapper(), clock);
        var event = old.findDueByEventId("E1", OutboxEventType.INVALID_ORDER).orElseThrow();
        var first = old.claim(event, "old-worker", Duration.ofSeconds(1)).orElseThrow();
        var newer = new MybatisRechargeDispatchOutboxAdapter(fixture.mapper(), new ObjectMapper(), Clock.offset(clock, Duration.ofSeconds(2)));
        var due = newer.findDueByEventId("E1", OutboxEventType.INVALID_ORDER).orElseThrow();
        var second = newer.claim(due, "new-worker", Duration.ofSeconds(60)).orElseThrow();
        assertThatThrownBy(() -> old.markSuccess(first)).isInstanceOf(IllegalStateException.class);
        old.markFailed(first, "late failure", Duration.ofSeconds(30));
        old.markManualRequired(first, "late manual");
        assertThat(fixture.jdbc().queryForObject("select status from recharge_dispatch_outbox", String.class)).isEqualTo("PROCESSING");
        assertThat(fixture.jdbc().queryForObject("select locked_by from recharge_dispatch_outbox", String.class)).isEqualTo("new-worker");
        newer.markSuccess(second);
        assertThat(fixture.jdbc().queryForObject("select status from recharge_dispatch_outbox", String.class)).isEqualTo("SUCCESS");
    }

    @Test
    void expiredLeaseCannotBeCompletedEvenBeforeAnotherWorkerClaims() throws Exception {
        var fixture = dispatchFixture(OutboxEventType.INVALID_ORDER);
        var clock = Clock.fixed(Instant.parse("2026-10-20T08:26:33Z"), ZoneOffset.UTC);
        var owner = new MybatisRechargeDispatchOutboxAdapter(fixture.mapper(), new ObjectMapper(), clock);
        var claimed = owner.claim(owner.findDueByEventId("E1", OutboxEventType.INVALID_ORDER).orElseThrow(), "worker", Duration.ofSeconds(1)).orElseThrow();
        var expired = new MybatisRechargeDispatchOutboxAdapter(fixture.mapper(), new ObjectMapper(), Clock.offset(clock, Duration.ofSeconds(2)));
        assertThatThrownBy(() -> expired.markSuccess(claimed)).isInstanceOf(IllegalStateException.class);
        assertThat(fixture.jdbc().queryForObject("select status from recharge_dispatch_outbox", String.class)).isEqualTo("PROCESSING");
    }

    /**
     * 本地真实 MyBatis SQL 验证，不连接共享中间件；参见详细设计「9.1 派单 Outbox」。
     * 显式存入秒级 due，不把 H2 的舍入方式当作真实 MySQL 精度证据。
     */
    private static DispatchFixture dispatchFixture(OutboxEventType eventType) throws Exception {
        var dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        var jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("""
                create table recharge_order_ext(order_id varchar(64), org_code varchar(64),
                  out_order_sn varchar(64), persist_status varchar(32))
                """);
        jdbc.execute("""
                create table recharge_dispatch_outbox(id bigint primary key, event_id varchar(96) unique,
                  event_type varchar(32), org_code varchar(64), order_id varchar(64), out_order_sn varchar(64),
                  payload varchar(200), status varchar(32), retry_count int, max_retry_count int,
                  next_retry_time datetime(0), locked_by varchar(64), locked_until datetime(0), version bigint,
                  last_error varchar(200), shard_key varchar(96), created_at datetime(0), updated_at datetime(0))
                """);
        jdbc.update("insert into recharge_order_ext values('O1','A35','OUT1','DB_PERSISTED')");
        jdbc.update("""
                insert into recharge_dispatch_outbox(id,event_id,event_type,org_code,order_id,out_order_sn,
                  payload,status,retry_count,max_retry_count,next_retry_time,version,last_error,shard_key)
                values(1,'E1',?,'A35','O1','OUT1','{}','INIT',0,3,'2026-10-20 16:26:33',0,'','E1')
                """, eventType.name());
        var config = new com.baomidou.mybatisplus.core.MybatisConfiguration();
        config.setMapUnderscoreToCamelCase(true);
        config.addMapper(RechargeDispatchOutboxMapper.class);
        var factory = new com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean();
        factory.setDataSource(dataSource);
        factory.setConfiguration(config);
        var mapper = new SqlSessionTemplate(factory.getObject()).getMapper(RechargeDispatchOutboxMapper.class);
        return new DispatchFixture(jdbc, mapper);
    }

    private record DispatchFixture(JdbcTemplate jdbc, RechargeDispatchOutboxMapper mapper) { }

    private static final class RecordingDispatchWriter implements RechargeDispatchOutboxPersistWriter {

        private RechargeDispatchOutboxDO outbox;

        @Override
        public int insertIfAbsent(RechargeDispatchOutboxDO outbox) {
            this.outbox = outbox;
            return 1;
        }
    }
}

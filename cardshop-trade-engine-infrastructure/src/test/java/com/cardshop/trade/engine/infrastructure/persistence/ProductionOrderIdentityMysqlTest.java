package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.infrastructure.mq.RabbitPersistRechargeOrderBatchMessageListener;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.TransactionTemplate;

/** 本机可丢弃MySQL真实事务、唯一竞争与提交后恢复；不启动Nacos/MQ/供应商。参见详细设计8.2.1。 */
@EnabledIfSystemProperty(named = "production.compatibility.mysqlTests", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProductionOrderIdentityMysqlTest {
    private String schema;
    private DriverManagerDataSource source;
    private JdbcTemplate jdbc;
    private RechargeOrderPersistencePort single;
    private RechargeOrderBatchPersistencePort batch;
    private MybatisRechargePersistOutboxAsyncWriter writer;
    private MybatisPersistedOrderStateAdapter persisted;
    private MybatisRechargeOrderQueryAdapter queries;
    private TransactionTemplate tx;
    private final ObjectMapper json = new ObjectMapper();
    private final String restartStage = System.getProperty("production.compatibility.restartStage", "none");
    private final String collation = System.getProperty("production.compatibility.collation", "utf8mb3_general_ci");

    @BeforeAll
    void setup() throws Exception {
        var admin = dataSource("mysql");
        var adminJdbc = new JdbcTemplate(admin);
        assertThat(adminJdbc.queryForObject("select version()", String.class)).isEqualTo("8.0.36");
        assertThat(adminJdbc.queryForObject("select @@lower_case_table_names", Integer.class)).isEqualTo(1);
        assertThat(adminJdbc.queryForObject("select @@transaction_isolation", String.class)).isEqualTo("READ-COMMITTED");
        assertThat(restartStage).isIn("none", "facts", "recover");
        assertThat(collation).isIn("utf8mb3_general_ci", "utf8mb4_0900_ai_ci");
        String characterSet = collation.equals("utf8mb3_general_ci") ? "utf8mb3" : "utf8mb4";
        schema = restartStage.equals("none") ? "compat_identity_" + UUID.randomUUID().toString().replace("-", "")
                : System.getenv("COMPAT_RECOVERY_SCHEMA");
        assertThat(schema).matches("compat_identity_[a-f0-9]{32}");
        if (!restartStage.equals("recover")) {
            adminJdbc.execute("create database " + schema + " character set " + characterSet + " collate " + collation);
        }
        source = dataSource(schema);
        jdbc = new JdbcTemplate(source);
        if (!restartStage.equals("recover")) {
            try (var resource = getClass().getResourceAsStream("/miniso-bridge/legacy-baseline.sql")) {
                assertThat(resource).isNotNull();
                jdbc.execute(new String(resource.readAllBytes(), StandardCharsets.UTF_8));
            }
            jdbc.execute("alter table mc_order convert to character set " + characterSet + " collate " + collation);
            if (collation.equals("utf8mb4_0900_ai_ci")) {
                jdbc.execute("alter table mc_depart_record convert to character set utf8mb4 collate utf8mb4_0900_ai_ci");
            }
            jdbc.execute("alter table mc_order add index out_orderId_code_index(out_orderId,org_code)");
            try (var connection = source.getConnection(); var resources = new ClassLoaderResourceAccessor()) {
                var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
                try (var migration = new Liquibase("db/changelog/db.changelog-production-compatible.yaml", resources, database)) {
                    migration.update(new Contexts(), new LabelExpression());
                }
            }
            jdbc.execute("create table compat_fixture(run_id varchar(64) primary key)");
            jdbc.update("insert into compat_fixture(run_id) values(?)", schema);
        }
        assertThat(jdbc.queryForObject("select run_id from compat_fixture", String.class)).isEqualTo(schema);
        var factory = factory(source);
        var session = new SqlSessionTemplate(factory);
        var orders = session.getMapper(McOrderMapper.class);
        var ext = session.getMapper(RechargeOrderExtMapper.class);
        var tm = new DataSourceTransactionManager(source);
        tx = new TransactionTemplate(tm);
        single = transactional(new MybatisRechargeOrderPersistenceAdapter(orders, ext,
                session.getMapper(RechargeDispatchOutboxMapper.class), session.getMapper(RechargeBalanceDeltaOutboxMapper.class),
                session.getMapper(McDepartRecordMapper.class), json), tm, RechargeOrderPersistencePort.class);
        batch = transactional(new MybatisBatchRechargeOrderPersistenceAdapter(factory, json), tm,
                RechargeOrderBatchPersistencePort.class);
        // 物理独立数据源/SessionFactory/事务管理器，不能读到事实事务的未提交写入。
        var writerSource = dataSource(schema);
        writer = new MybatisRechargePersistOutboxAsyncWriter(factory(writerSource), json,
                TradeEngineObservabilityMetrics.noop(), Duration.ofSeconds(1), true,
                new DataSourceTransactionManager(writerSource));
        persisted = new MybatisPersistedOrderStateAdapter(orders, ext);
        queries = new MybatisRechargeOrderQueryAdapter(orders, ext);
    }

    @AfterAll
    void cleanup() {
        if (schema != null && !restartStage.equals("facts")) {
            new JdbcTemplate(dataSource("mysql")).execute("drop database " + schema);
        }
    }

    @BeforeEach
    void clearDisposableFacts() {
        if (restartStage.equals("recover")) return;
        for (String table : List.of("recharge_dispatch_outbox", "recharge_balance_delta_outbox", "mc_depart_record",
                "recharge_order_ext", "mc_order")) jdbc.execute("delete from " + table);
    }

    @Test
    void replayPreservesFactsButEveryImmutableIdentityChangeRollsBack() {
        var original = aggregate("O1", "OUT", "A35", 137, "hash", "v1", "SHA-256");
        single.persist(original);
        single.persist(original);
        for (var changed : List.of(
                aggregate("O2", "OUT", "A35", 137, "hash", "v1", "SHA-256"),
                aggregate("O1", "OTHER", "A35", 137, "hash", "v1", "SHA-256"),
                aggregate("O1", "OUT", "OTHER", 137, "hash", "v1", "SHA-256"),
                aggregate("O1", "OUT", "A35", 138, "hash", "v1", "SHA-256"),
                aggregate("O1", "OUT", "A35", 137, "other", "v1", "SHA-256"),
                aggregate("O1", "OUT", "A35", 137, "hash", "v2", "SHA-256"),
                aggregate("O1", "OUT", "A35", 137, "hash", "v1", "SM3"))) {
            assertThatThrownBy(() -> single.persist(changed)).isInstanceOf(RechargeOrderIdentityConflictException.class);
            assertCompleteFacts(1);
        }
        assertThat(jdbc.queryForObject("select order_price from mc_order", Long.class)).isEqualTo(137);
        assertThat(jdbc.queryForObject("select trade_engine_owned from mc_order", Integer.class)).isOne();
    }

    @Test
    void batchConflictRollsBackUnrelatedRowsAndFactsOnlyAlsoEnforcesIdentity() {
        for (boolean factsOnly : List.of(false, true)) {
            var aggregates = List.of(order("FIRST", "SEPARATE"), order("WIN", "SAME"), order("LOSE", "SAME"));
            assertThatThrownBy(() -> {
                if (factsOnly) batch.persistBatchFactsOnly(aggregates); else batch.persistBatchWithOutbox(aggregates);
            }).isInstanceOf(RechargeOrderIdentityConflictException.class);
            assertCompleteFacts(0);
        }
    }

    @Test
    void concurrentSinglesAndConcurrentBatchesProduceExactlyOneCompleteWinner() throws Exception {
        for (boolean batched : List.of(false, true)) {
            // 此用例只验新身份竞争；DELETE提交并不清除索引删除标记，不能串入下一场景。
            // 删除残留及其重投另有真实事务回归，参见详细设计8.3「并发验证细化」。
            for (String table : List.of("recharge_dispatch_outbox", "recharge_balance_delta_outbox", "mc_depart_record",
                    "recharge_order_ext", "mc_order")) jdbc.execute("truncate table " + table);
            var start = new CountDownLatch(1);
            try (var pool = Executors.newFixedThreadPool(2)) {
                var results = List.of(pool.submit(() -> compete("LEFT", batched, start)),
                        pool.submit(() -> compete("RIGHT", batched, start)));
                start.countDown();
                int successes = 0;
                for (var result : results) if (result.get(15, TimeUnit.SECONDS)) successes++;
                assertThat(successes).isOne();
                assertCompleteFacts(batched ? 2 : 1);
                var winners = jdbc.queryForList("select order_Id from mc_order", String.class);
                assertThat(jdbc.queryForList("select order_id from recharge_order_ext", String.class))
                        .containsExactlyInAnyOrderElementsOf(winners);
                assertThat(jdbc.queryForList("select order_id from recharge_dispatch_outbox", String.class))
                        .containsExactlyInAnyOrderElementsOf(winners);
                assertThat(jdbc.queryForList("select order_id from recharge_balance_delta_outbox", String.class))
                        .containsExactlyInAnyOrderElementsOf(winners);
                assertThat(jdbc.queryForList("select order_id from mc_depart_record", String.class))
                        .containsExactlyInAnyOrderElementsOf(winners);
            }
        }
    }

    private boolean compete(String id, boolean batched, CountDownLatch start) throws Exception {
        assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
        try {
            if (batched) batch.persistBatchWithOutbox(List.of(order(id, "SHARED"), order(id + "-OTHER", id)));
            else single.persist(order(id, "SHARED"));
            return true;
        } catch (RechargeOrderIdentityConflictException expected) {
            return false;
        }
    }

    /** 保留原删除残留触发条件，不能靠换号或吞1213掩盖回滚/身份裁决；参见详细设计8.3。 */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deletedIdentityContentionRollsBackLoserAndRedeliveryStillRejectsIt(boolean factsOnly) throws Exception {
        single.persist(order("RIGHT", "SHARED"));
        try (var oldView = source.getConnection(); var pool = Executors.newFixedThreadPool(2)) {
            oldView.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            oldView.setAutoCommit(false);
            try (var query = oldView.prepareStatement("select order_Id from mc_order where order_Id=?")) {
                query.setString(1, "RIGHT");
                try (var rows = query.executeQuery()) { assertThat(rows.next()).isTrue(); }
            }
            // 旧read view阻止purge移除删除标记，两个写事务仍按生产配置使用RC。
            clearDisposableFacts();
            var contenders = List.of(
                    List.of(order("LEFT", "SHARED"), aggregate("LEFT-OTHER", "LEFT", "A35", 211, "hash", "v1", "SHA-256")),
                    List.of(order("RIGHT", "SHARED"), aggregate("RIGHT-OTHER", "RIGHT", "A35", 211, "hash", "v1", "SHA-256")));
            var start = new CountDownLatch(1);
            var results = contenders.stream().map(orders -> pool.submit(() -> {
                assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                return catchThrowable(() -> {
                    if (factsOnly) batch.persistBatchFactsOnly(orders); else batch.persistBatchWithOutbox(orders);
                });
            })).toList();
            start.countDown();
            var leftFailure = results.getFirst().get(15, TimeUnit.SECONDS);
            var rightFailure = results.getLast().get(15, TimeUnit.SECONDS);
            assertThat((leftFailure == null ? 1 : 0) + (rightFailure == null ? 1 : 0)).isOne();
            var failure = leftFailure == null ? rightFailure : leftFailure;
            assertThat(failure).isInstanceOfAny(RechargeOrderIdentityConflictException.class, DeadlockLoserDataAccessException.class);
            if (failure instanceof DeadlockLoserDataAccessException deadlock) {
                assertThat(deadlock.getMostSpecificCause()).isInstanceOfSatisfying(SQLException.class, sql -> {
                    assertThat(sql.getErrorCode()).isEqualTo(1213);
                    assertThat(sql.getSQLState()).isEqualTo("40001");
                });
            }
            var winner = contenders.get(leftFailure == null ? 0 : 1);
            var loser = contenders.get(leftFailure == null ? 1 : 0);
            assertThat(jdbc.queryForList("select order_Id from mc_order", String.class))
                    .containsExactlyInAnyOrderElementsOf(winner.stream().map(a -> a.snapshot().orderId()).toList());
            var beforeRedelivery = durableState();
            // 等首批结局确定后才进行一次显式重投；不是在失败事务内重试，也不是循环直到变绿。
            assertThatThrownBy(() -> {
                if (factsOnly) batch.persistBatchFactsOnly(loser); else batch.persistBatchWithOutbox(loser);
            }).isInstanceOf(RechargeOrderIdentityConflictException.class);
            assertThat(durableState()).isEqualTo(beforeRedelivery);
            if (factsOnly) {
                assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox", Integer.class)).isZero();
                assertThat(jdbc.queryForObject("select count(*) from recharge_balance_delta_outbox", Integer.class)).isZero();
                writer.writeBatch(winner);
            }
            assertFinancialBatch(winner);
            System.out.println("COMPAT_DELETED_CONTENTION factsOnly=" + factsOnly + " loser="
                    + failure.getClass().getSimpleName() + " redelivery=IDENTITY_CONFLICT fiveTables=2 amount=348");
            oldView.rollback();
        }
    }

    /**
     * 确定性故障注入：在最后一张表制造真实锁环，确保不是靠偶发1213才覆盖恢复分支。
     * 只替换Redis和AMQP传输；Consumer、listener及MySQL事务均为真实实现。参见详细设计8.3。
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void mysqlDeadlockRollsBackWholeBatchAndMqRedeliveryRecovers(boolean outboxAsync) throws Exception {
        var existing = order("B-EXISTING", "B-OUT");
        single.persist(existing);
        var orders = List.of(aggregate("A-NEW", "A-OUT", "A35", 211, "hash", "v1", "SHA-256", 2),
                aggregate("B-EXISTING", "B-OUT", "A35", 137, "hash", "v1", "SHA-256", 2));
        var before = durableState();
        var reliability = mock(RechargeAcceptReliabilityPort.class);
        when(reliability.markPersistFailed(any(), any(), anyString(), anyString(), anyInt(), anyLong(), anyLong()))
                .thenAnswer(call -> {
                    assertThat(call.<String>getArgument(2)).isEqualTo("MYSQL_PERSIST_FAILED");
                    assertThat(call.<String>getArgument(3)).contains("Deadlock found");
                    assertThat(durableState()).as("all five tables rolled back before failure mark").isEqualTo(before);
                    return CompletableFuture.completedFuture(MarkPersistStateResult.success());
                });
        when(reliability.markDbPersisted(any(), any(), anyLong(), anyLong())).thenAnswer(call -> {
            assertFinancialBatch(orders);
            assertThat(jdbc.queryForList("select persisted_version from recharge_order_ext", Long.class)).containsOnly(2L);
            return CompletableFuture.completedFuture(MarkPersistStateResult.success());
        });
        var timer = mock(RechargeDispatchTimerPort.class);
        var consumer = new PersistRechargeOrderConsumerService(
                (org, out, id) -> orders.stream().map(OrderPersistAggregate::snapshot).filter(s -> s.orderId().equals(id)).findFirst(),
                single, batch, persisted, reliability, timer,
                Clock.fixed(existing.persistedAt(), ZoneOffset.UTC), Duration.ofSeconds(3), Duration.ofSeconds(1),
                outboxAsync ? writer : null, outboxAsync);
        var retryDeliveries = new LinkedBlockingQueue<RetryDelivery>();
        var transport = mock(RabbitTemplate.class);
        doAnswer(call -> {
            retryDeliveries.add(new RetryDelivery(call.getArgument(2), call.getArgument(3)));
            return null;
        }).when(transport).send(eq("fixture.retry"), eq("fixture.persist"), any(Message.class), any(CorrelationData.class));
        var listener = new RabbitPersistRechargeOrderBatchMessageListener(json, consumer, transport,
                "fixture.retry", "fixture.persist", 3);
        var channel = mock(Channel.class);
        var messages = new java.util.ArrayList<Message>();
        for (int i = 0; i < orders.size(); i++) {
            var snapshot = orders.get(i).snapshot();
            var payload = new PersistTradeOrderMessage(orders.get(i).persistMessageId(), snapshot.orgCode(), snapshot.orderId(),
                    snapshot.outOrderSn(), snapshot.requestHash(), snapshot.orderVersion(), Map.of("redis_order_written", true));
            var properties = new MessageProperties();
            properties.setDeliveryTag(i + 1);
            messages.add(new Message(json.writeValueAsBytes(payload), properties));
        }
        long deadlocksBefore = jdbc.queryForObject("select count from information_schema.innodb_metrics where name='lock_deadlocks'", Long.class);
        try (var pool = Executors.newSingleThreadExecutor(); var blocker = source.getConnection()) {
            blocker.setAutoCommit(false);
            try {
                // 64条临时夹具写入重于本例两单至多10条事实写入，使InnoDB稳定选择业务批为牺牲者。
                try (var weight = blocker.prepareStatement("insert into compat_fixture(run_id) values(?)")) {
                    for (int i = 0; i < 64; i++) {
                        weight.setString(1, "deadlock-weight-" + i);
                        weight.executeUpdate();
                    }
                }
                String lockSql = outboxAsync
                        ? "select id from mc_depart_record where order_id=? for update"
                        : "select id from recharge_balance_delta_outbox where order_id=? for update";
                try (var lock = blocker.prepareStatement(lockSql)) {
                    lock.setString(1, "B-EXISTING");
                    try (var rows = lock.executeQuery()) { assertThat(rows.next()).isTrue(); }
                }
                var handling = pool.submit(() -> { listener.onMessage(messages, channel); return null; });
                String blockedTable = outboxAsync ? "mc_depart_record" : "recharge_balance_delta_outbox";
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                int waiting = 0;
                while (System.nanoTime() < deadline) {
                    waiting = jdbc.queryForObject("""
                            select count(*) from performance_schema.data_lock_waits w
                            join performance_schema.data_locks l
                              on l.engine_lock_id=w.requesting_engine_lock_id and l.engine=w.engine
                            where l.object_schema=? and l.object_name=?
                            """, Integer.class, schema, blockedTable);
                    if (waiting > 0) break;
                    Thread.sleep(20);
                }
                assertThat(waiting).as("business batch reached final table before closing lock cycle").isPositive();
                try (var lock = blocker.prepareStatement("select order_Id from mc_order where order_Id=? for update")) {
                    lock.setString(1, "A-NEW");
                    try (var rows = lock.executeQuery()) { assertThat(rows.next()).as("victim's new row rolled back").isFalse(); }
                }
                blocker.rollback();
                var firstRetry = retryDeliveries.poll(5, TimeUnit.SECONDS);
                assertThat(firstRetry).isNotNull();
                assertThat(durableState()).isEqualTo(before);
                assertThat(jdbc.queryForObject("select count from information_schema.innodb_metrics where name='lock_deadlocks'", Long.class))
                        .isEqualTo(deadlocksBefore + 1);
                verify(reliability, times(2)).markPersistFailed(any(), any(), anyString(), anyString(), anyInt(), anyLong(), anyLong());
                verify(reliability, never()).markDbPersisted(any(), any(), anyLong(), anyLong());
                verifyNoInteractions(timer, channel);
                assertThat(handling.isDone()).as("retry publish not confirmed yet").isFalse();
                firstRetry.confirmation().getFuture().complete(new CorrelationData.Confirm(true, null));
                var secondRetry = retryDeliveries.poll(5, TimeUnit.SECONDS);
                assertThat(secondRetry).isNotNull();
                // 第一条已confirm可以单独ack，但未confirm的第二条不能被单条或multiple ack带走。
                verify(channel, never()).basicAck(2, false);
                verify(channel, never()).basicAck(anyLong(), eq(true));
                secondRetry.confirmation().getFuture().complete(new CorrelationData.Confirm(true, null));
                handling.get(5, TimeUnit.SECONDS);
                verify(channel).basicAck(1, false);
                verify(channel).basicAck(2, false);
                verifyNoMoreInteractions(channel);
                // 显式投递捕获的retry消息，保留正文/重试头；不重新构造一份不同业务输入。
                var redelivered = List.of(firstRetry.message(), secondRetry.message());
                for (int i = 0; i < redelivered.size(); i++) {
                    assertThat(redelivered.get(i).getBody()).isEqualTo(messages.get(i).getBody());
                    assertThat(redelivered.get(i).getMessageProperties().getHeaders()).containsEntry("x-retry-count", 1);
                    redelivered.get(i).getMessageProperties().setDeliveryTag(i + 11);
                }
                listener.onMessage(redelivered, channel);
                verify(channel).basicAck(12, true);
                verify(reliability, times(2)).markDbPersisted(any(), any(), eq(2L), anyLong());
                assertFinancialBatch(orders);
                var afterRecovery = durableState();
                listener.onMessage(redelivered, channel);
                assertThat(durableState()).as("redelivery after commit is idempotent").isEqualTo(afterRecovery);
                assertThat(retryDeliveries).isEmpty();
                System.out.println("COMPAT_DEADLOCK_RECOVERY outboxAsync=" + outboxAsync
                        + " mysqlDeadlocks=1 rollback=complete confirmBeforeAck=true redelivered=2 fiveTables=2 amount=348 versions=2");
            } finally {
                blocker.rollback();
            }
        }
    }

    private record RetryDelivery(Message message, CorrelationData confirmation) { }

    /** 每张表按同一订单集合核对金额/身份，不能只用总条数掩盖错单或重扣。 */
    private void assertFinancialBatch(List<OrderPersistAggregate> orders) {
        assertCompleteFacts(orders.size());
        for (var aggregate : orders) {
            var snapshot = aggregate.snapshot();
            String id = snapshot.orderId();
            assertThat(jdbc.queryForMap("select org_code,out_orderId,order_price from mc_order where order_Id=?", id))
                    .containsEntry("org_code", snapshot.orgCode()).containsEntry("out_orderId", snapshot.outOrderSn())
                    .containsEntry("order_price", snapshot.orderPrice());
            assertThat(jdbc.queryForMap("select org_code,out_order_sn from recharge_order_ext where order_id=?", id))
                    .containsEntry("org_code", snapshot.orgCode()).containsEntry("out_order_sn", snapshot.outOrderSn());
            assertThat(jdbc.queryForObject("select `value` from mc_depart_record where order_id=?", Long.class, id))
                    .isEqualTo(snapshot.orderPrice());
            assertThat(jdbc.queryForObject("select delta_amount from recharge_balance_delta_outbox where order_id=?", Long.class, id))
                    .isEqualTo(-snapshot.orderPrice());
            assertThat(jdbc.queryForObject("select event_id from recharge_dispatch_outbox where order_id=?", String.class, id))
                    .isEqualTo("dispatch-" + id);
        }
    }

    /** 同身份并发不是争出一个失败者：两个独立事务都应成功，副作用只能一份。参见详细设计8.2.1、8.3。 */
    @ParameterizedTest
    @EnumSource(PersistPath.class)
    void concurrentSameIdentityRedeliveryCommitsBothWithoutDuplicatingMoneyOrEvents(PersistPath path) throws Exception {
        var first = versioned(3, "first-message", PersistSource.REDIS_SNAPSHOT);
        var replay = versioned(3, "redelivered-message", PersistSource.MQ_SNAPSHOT_FALLBACK);
        try (var pool = Executors.newSingleThreadExecutor()) {
            var follower = tx.execute(status -> {
                writeFacts(path, first);
                var future = pool.submit(() -> persist(path, replay));
                // 不靠sleep猜线程重叠：首事务不提交，直到实际观察到第二事务等待本schema的主单锁。
                var observer = new JdbcTemplate(dataSource(schema));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                int waits = 0;
                while (System.nanoTime() < deadline) {
                    waits = observer.queryForObject("""
                            select count(*) from performance_schema.data_lock_waits w
                            join performance_schema.data_locks l
                              on l.engine_lock_id=w.requesting_engine_lock_id and l.engine=w.engine
                            where l.object_schema=? and l.object_name='mc_order'
                            """, Integer.class, schema);
                    if (waits > 0) break;
                    try { Thread.sleep(20); }
                    catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new IllegalStateException(ex); }
                }
                assertThat(waits).as("second transaction really waited on mc_order").isPositive();
                assertThat(future.isDone()).isFalse();
                System.out.println("COMPAT_SAME_REDELIVERY path=" + path + " observedMainLockWait=" + waits);
                return future;
            });
            // facts-only的writer必须在首事务提交后调用，不能借主事务看未提交数据。
            if (path == PersistPath.FACTS_WRITER) writer.writeBatch(List.of(first));
            follower.get(15, TimeUnit.SECONDS);
        }
        persist(path, replay);
        assertSingleFinancialIdentity();
        assertVersion(3);
        assertThat(jdbc.queryForObject("select persist_msg_id from recharge_order_ext", String.class)).isEqualTo("first-message");
        System.out.println("COMPAT_SAME_REDELIVERY path=" + path + " bothCommitted=true fiveTables=1 amount=137 delta=-137");
    }

    @ParameterizedTest
    @EnumSource(PersistPath.class)
    void versionsAdvanceAcrossGapsAndFutureWriterOrChangedIdentityCannotSkipFacts(PersistPath path) {
        for (long version : List.of(1L, 3L, 7L)) {
            var next = versioned(version, "message-" + version, version == 1
                    ? PersistSource.MQ_SNAPSHOT_FALLBACK : PersistSource.REDIS_SNAPSHOT);
            var probe = PersistedOrderProbe.from(next.snapshot());
            assertThat(persisted.findPersistedAtLeast(List.of(probe))).isEmpty();
            var before = durableState();
            assertThatThrownBy(() -> writer.writeBatch(List.of(next))).isInstanceOf(RechargeOrderIdentityConflictException.class);
            assertThat(durableState()).isEqualTo(before);
            persist(path, next);
            assertVersion(version);
            assertSingleFinancialIdentity();
            assertThat(persisted.findPersistedAtLeast(List.of(probe))).containsExactly(probe);
            assertThat(persisted.isPersistedAtLeast("A35", "OUT-VERSION", "VERSION", version)).isTrue();
            assertThat(persisted.isPersistedAtLeast("A35", "OUT-VERSION", "VERSION", version + 1)).isFalse();
        }
        assertThat(jdbc.queryForObject("select persist_source from recharge_order_ext", String.class)).isEqualTo("REDIS_SNAPSHOT");
        var covered = PersistedOrderProbe.from(versioned(3, "old-probe", PersistSource.REDIS_SNAPSHOT).snapshot());
        assertThat(persisted.findPersistedAtLeast(List.of(covered))).containsExactly(covered);
        var before = durableState();
        for (var changed : List.of(
                aggregate("VERSION", "OUT-VERSION", "A35", 138, "hash", "v1", "SHA-256", 8),
                aggregate("VERSION", "OUT-VERSION", "A35", 137, "different", "v1", "SHA-256", 8))) {
            assertThatThrownBy(() -> persist(path, changed)).isInstanceOf(RechargeOrderIdentityConflictException.class);
            assertThat(persisted.findPersistedAtLeast(List.of(PersistedOrderProbe.from(changed.snapshot())))).isEmpty();
            assertThat(durableState()).isEqualTo(before);
        }
        System.out.println("COMPAT_VERSION path=" + path + " advanced=1,3,7 futureWriterRejected=true identityConflictRolledBack=true");
    }

    @ParameterizedTest
    @EnumSource(PersistPath.class)
    void olderAndEqualVersionsPreserveFulfillmentAndCompletedEventProgress(PersistPath path) {
        persist(path, versioned(7, "current-message", PersistSource.REDIS_SNAPSHOT));
        // 仅模拟已完成的履约和刷账状态，不运行供应商/钱包；旧消息绝不能把它们改回初始状态。
        jdbc.execute("update mc_order set order_status=12,remark='synthetic fulfilled',end_time='2026-11-16 10:00:00'");
        jdbc.execute("update recharge_order_ext set dispatch_status='SUCCESS',balance_reserved=0,last_error='synthetic audit'");
        for (String table : List.of("recharge_dispatch_outbox", "recharge_balance_delta_outbox"))
            jdbc.execute("update " + table + " set status='SUCCESS',version=9,retry_count=2,last_error='synthetic completed'");
        var before = durableState();
        for (long version : List.of(3L, 1L, 7L)) {
            var old = versioned(version, "late-message-" + version, PersistSource.MQ_SNAPSHOT_FALLBACK);
            persist(path, old);
            writer.writeBatch(List.of(old));
            assertVersion(7);
            assertSingleFinancialIdentity();
            // 同版本允许清理last_error的既有合同；比较业务状态和事件进度，不把诊断时间当业务事实。
            assertThat(durableState()).isEqualTo(before);
            var probe = PersistedOrderProbe.from(old.snapshot());
            assertThat(persisted.findPersistedAtLeast(List.of(probe))).containsExactly(probe);
        }
        assertThatThrownBy(() -> writer.writeBatch(List.of(versioned(8, "future", PersistSource.REDIS_SNAPSHOT))))
                .isInstanceOf(RechargeOrderIdentityConflictException.class);
        assertThat(durableState()).isEqualTo(before);
        System.out.println("COMPAT_OLD_REPLAY path=" + path + " replayed=3,1,7 persisted=7 fulfillment=SUCCESS events=SUCCESS version=9");
    }

    private enum PersistPath { SINGLE, BATCH, FACTS_WRITER }

    private void writeFacts(PersistPath path, OrderPersistAggregate aggregate) {
        switch (path) {
            case SINGLE -> single.persist(aggregate);
            case BATCH -> batch.persistBatchWithOutbox(List.of(aggregate));
            case FACTS_WRITER -> batch.persistBatchFactsOnly(List.of(aggregate));
        }
    }

    private void persist(PersistPath path, OrderPersistAggregate aggregate) {
        writeFacts(path, aggregate);
        if (path == PersistPath.FACTS_WRITER) writer.writeBatch(List.of(aggregate));
    }

    private void assertVersion(long version) {
        assertThat(jdbc.queryForObject("select persisted_version from recharge_order_ext", Long.class)).isEqualTo(version);
        assertThat(jdbc.queryForObject("select redis_order_version from recharge_order_ext", Long.class)).isEqualTo(version);
        assertThat(jdbc.queryForObject("select request_hash_version from recharge_order_ext", String.class)).isEqualTo("v1");
    }

    private void assertSingleFinancialIdentity() {
        assertCompleteFacts(1);
        assertThat(jdbc.queryForObject("select order_Id from mc_order", String.class)).isEqualTo("VERSION");
        assertThat(jdbc.queryForObject("select order_price from mc_order", Long.class)).isEqualTo(137);
        assertThat(jdbc.queryForObject("select `value` from mc_depart_record", Long.class)).isEqualTo(137);
        assertThat(jdbc.queryForObject("select delta_amount from recharge_balance_delta_outbox", Long.class)).isEqualTo(-137);
        assertThat(jdbc.queryForObject("select event_id from recharge_dispatch_outbox", String.class)).isEqualTo("dispatch-VERSION");
        assertThat(jdbc.queryForObject("select event_id from recharge_balance_delta_outbox", String.class)).isEqualTo("balance-flush-VERSION");
    }

    /** 可观察业务事实快照，排除ext upsert允许刷新的诊断last_error/updated_at，避免锁定无关实现细节。 */
    private List<List<Map<String, Object>>> durableState() {
        return List.of(
                jdbc.queryForList("select order_Id,org_code,out_orderId,order_price,order_status,remark,create_time,edit_time,end_time,trade_engine_owned from mc_order order by id"),
                jdbc.queryForList("select order_id,org_code,out_order_sn,request_hash,request_hash_version,request_hash_alg,persist_msg_id,persist_status,persist_source,persisted_version,redis_order_version,dispatch_status,balance_reserved,created_at from recharge_order_ext order by order_id"),
                jdbc.queryForList("select order_id,type,`value`,create_time from mc_depart_record order by id"),
                jdbc.queryForList("select event_id,status,payload,version,retry_count,last_error,next_retry_time,created_at,updated_at from recharge_dispatch_outbox order by id"),
                jdbc.queryForList("select event_id,status,delta_amount,version,retry_count,last_error,next_retry_time,created_at,updated_at from recharge_balance_delta_outbox order by id"));
    }

    private static OrderPersistAggregate versioned(long version, String messageId, PersistSource source) {
        var aggregate = aggregate("VERSION", "OUT-VERSION", "A35", 137, "hash", "v1", "SHA-256", version);
        return new OrderPersistAggregate(messageId, aggregate.snapshot(), source, aggregate.persistedAt().plusSeconds(version));
    }

    @Test
    void committedFactsRecoverEventsWithoutRewritingFulfillmentState() {
        var aggregate = order("FACTS", "OUT");
        batch.persistBatchFactsOnly(List.of(aggregate));
        assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox", Integer.class)).isZero();
        assertThat(persisted.findPersistedAtLeast(List.of(PersistedOrderProbe.from(aggregate.snapshot())))).hasSize(1);
        jdbc.execute("update mc_order set order_status=12");
        jdbc.execute("update recharge_order_ext set dispatch_status='SUCCESS',balance_reserved=0");
        writer.writeBatch(List.of(aggregate));
        writer.writeBatch(List.of(aggregate));
        assertCompleteFacts(1);
        assertThat(jdbc.queryForObject("select dispatch_status from recharge_order_ext", String.class)).isEqualTo("SUCCESS");
        var wrong = aggregate("FACTS", "OUT", "A35", 138, "hash", "v1", "SHA-256");
        assertThat(persisted.findPersistedAtLeast(List.of(PersistedOrderProbe.from(wrong.snapshot())))).isEmpty();
        assertThatThrownBy(() -> writer.writeBatch(List.of(wrong))).isInstanceOf(RechargeOrderIdentityConflictException.class);
    }

    @Test
    void commitThenRecoverAcrossJvms() {
        var aggregate = order("RESTART", "OUT-RESTART");
        if (!restartStage.equals("recover")) {
            batch.persistBatchFactsOnly(List.of(aggregate));
            assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox", Integer.class)).isZero();
            if (restartStage.equals("facts")) {
                System.out.println("COMPAT_RESTART facts committed without events; JVM=" + ProcessHandle.current().pid());
                return;
            }
        }
        assertThat(jdbc.queryForObject("select count(*) from mc_order", Integer.class)).isOne();
        assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox", Integer.class)).isZero();
        var probe = PersistedOrderProbe.from(aggregate.snapshot());
        assertThat(persisted.findPersistedAtLeast(List.of(probe))).containsExactly(probe);
        writer.writeBatch(List.of(aggregate));
        writer.writeBatch(List.of(aggregate));
        assertCompleteFacts(1);
        System.out.println("COMPAT_RESTART recovered original events exactly once; JVM=" + ProcessHandle.current().pid());
    }

    @Test
    void writerRejectsMissingAndUncommittedFactsAndDoesNotCreateEvents() {
        var aggregate = order("UNCOMMITTED", "OUT");
        assertThatThrownBy(() -> writer.writeBatch(List.of(aggregate))).isInstanceOf(RechargeOrderIdentityConflictException.class);
        tx.executeWithoutResult(status -> {
            batch.persistBatchFactsOnly(List.of(aggregate));
            assertThatThrownBy(() -> writer.writeBatch(List.of(aggregate))).isInstanceOf(RechargeOrderIdentityConflictException.class);
            status.setRollbackOnly();
        });
        assertCompleteFacts(0);
    }

    @Test
    void existingOwnedMainWithoutExtensionCannotBeClaimedBySingleOrBatch() {
        jdbc.execute("insert into mc_order(order_Id,org_code,out_orderId,product_type,order_price,trade_engine_owned) "
                + "values('ORPHAN','A35','OUT',1,137,1)");
        var aggregate = order("ORPHAN", "OUT");
        assertThatThrownBy(() -> single.persist(aggregate)).isInstanceOf(RechargeOrderIdentityConflictException.class);
        assertThatThrownBy(() -> batch.persistBatchFactsOnly(List.of(aggregate))).isInstanceOf(RechargeOrderIdentityConflictException.class);
        assertThat(jdbc.queryForObject("select count(*) from recharge_order_ext", Integer.class)).isZero();
    }

    @Test
    void queryUsesExtensionAndNeverGuessesBetweenHistoricalCandidates() {
        jdbc.execute("insert into mc_order(order_Id,org_code,out_orderId,product_type,order_price,order_status) "
                + "values('OLD1','A35','OUT',1,137,12)");
        assertThat(queries.existsPersistedOrder("A35", "OUT")).isTrue();
        assertThat(queries.findByOrgCodeAndOutOrderSn("A35", "OUT").orElseThrow().orderId()).isEqualTo("OLD1");
        jdbc.execute("insert into mc_order(order_Id,org_code,out_orderId,product_type,order_price,order_status) "
                + "values('OLD2','A35','OUT',1,139,13)");
        assertThatThrownBy(() -> queries.findByOrgCodeAndOutOrderSn("A35", "OUT"))
                .isInstanceOf(RechargeOrderIdentityConflictException.class);
        single.persist(order("NEW", "OUT"));
        assertThat(queries.findByOrgCodeAndOutOrderSn("A35", "OUT").orElseThrow().orderId()).isEqualTo("NEW");
        jdbc.execute("delete from mc_order where order_Id='NEW'");
        assertThatThrownBy(() -> queries.findByOrgCodeAndOutOrderSn("A35", "OUT"))
                .isInstanceOf(RechargeOrderIdentityConflictException.class);
        assertThat(queries.existsPersistedOrder("A35", "OUT")).isTrue();
        assertThat(queries.existsPersistedOrder("A35", "ABSENT")).isFalse();
    }

    @Test
    void legacyQueryRejectsCaseInsensitiveSqlMatchesWithDifferentTextIdentity() {
        jdbc.execute("insert into mc_order(order_Id,org_code,out_orderId,product_type,order_price,order_status) "
                + "values('OLD','A35','OUT',1,137,12)");
        for (var identity : List.of(List.of("a35", "OUT"), List.of("A35", "out"))) {
            // utf8mb3_general_ci会匹配，但不能把不同原始文本的旧单当作本次精确身份展示。
            assertThat(queries.existsPersistedOrder(identity.getFirst(), identity.getLast())).isTrue();
            assertThatThrownBy(() -> queries.findByOrgCodeAndOutOrderSn(identity.getFirst(), identity.getLast()))
                    .isInstanceOf(RechargeOrderIdentityConflictException.class);
        }
        assertThat(queries.findByOrgCodeAndOutOrderSn("A35", "OUT").orElseThrow().orderId()).isEqualTo("OLD");
    }

    @Test
    void historicalOccupancyQueryUsesExistingIndex() {
        jdbc.execute("insert into mc_order(order_Id,org_code,out_orderId,product_type) values('OLD','A35','OUT',1)");
        var plan = jdbc.queryForMap("explain select order_Id from mc_order where out_orderId=? and org_code=? and product_type=1 limit 1", "OUT", "A35");
        assertThat(plan.get("key")).isEqualTo("out_orderId_code_index");
        assertThat(plan.get("type")).isNotEqualTo("ALL");
        System.out.println("COMPAT OCCUPANCY EXPLAIN: " + plan);
        var candidates = jdbc.queryForMap("explain select order_Id from mc_order where out_orderId=? and org_code=? and product_type=1 order by id limit 2", "OUT", "A35");
        assertThat(candidates.get("key")).isEqualTo("out_orderId_code_index");
        assertThat(candidates.get("type")).isNotEqualTo("ALL");
        single.persist(order("NEW", "OUT"));
        var ext = jdbc.queryForMap("explain select order_id from recharge_order_ext where org_code=? and out_order_sn=? limit 1", "A35", "OUT");
        assertThat(ext.get("key")).isEqualTo("uk_ext_org_out_order");
        assertThat(ext.get("type")).isNotEqualTo("ALL");
        System.out.println("COMPAT CANDIDATE EXPLAIN: " + candidates);
        System.out.println("COMPAT EXT EXPLAIN: " + ext);
    }

    private void assertCompleteFacts(int count) {
        for (String table : List.of("mc_order", "recharge_order_ext", "mc_depart_record", "recharge_dispatch_outbox", "recharge_balance_delta_outbox"))
            assertThat(jdbc.queryForObject("select count(*) from " + table, Integer.class)).as(table).isEqualTo(count);
    }

    private static OrderPersistAggregate order(String id, String out) {
        return aggregate(id, out, "A35", 137, "hash", "v1", "SHA-256");
    }

    private static OrderPersistAggregate aggregate(String id, String out, String org, long amount,
            String hash, String version, String algorithm) {
        return aggregate(id, out, org, amount, hash, version, algorithm, 1);
    }

    private static OrderPersistAggregate aggregate(String id, String out, String org, long amount,
            String hash, String version, String algorithm, long orderVersion) {
        return new OrderPersistAggregate("persist-" + id,
                new PersistOrderSnapshot(org, "fixture", id, out, "PARENT", "SKU", "10000000000", "", 1, amount,
                        "合成商品", 1000, 1, "codekey", "BOCOM", hash, version, algorithm, "ACCEPTED", "PASSED", "", "",
                        true, true, "PERSIST_QUEUED", orderVersion, 0, 1), PersistSource.MQ_SNAPSHOT_FALLBACK,
                Instant.parse("2026-11-16T01:00:00Z"));
    }

    private DriverManagerDataSource dataSource(String database) {
        String password = System.getenv("LEGACY_SCHEMA_TEST_PASSWORD");
        assertThat(password).isNotBlank();
        String port = System.getenv("LEGACY_SCHEMA_TEST_PORT");
        assertThat(port).matches("[0-9]{1,5}");
        assertThat(Integer.parseInt(port)).isBetween(1024, 65535).isNotEqualTo(3306);
        return new DriverManagerDataSource("jdbc:mysql://127.0.0.1:" + port + "/" + database
                + "?allowPublicKeyRetrieval=true&useSSL=false&allowMultiQueries=true&serverTimezone=Asia/Shanghai&socketTimeout=20000", "root", password);
    }

    private static SqlSessionFactory factory(DriverManagerDataSource source) throws Exception {
        var config = new MybatisConfiguration();
        config.setMapUnderscoreToCamelCase(true);
        for (var mapper : List.of(McOrderMapper.class, RechargeOrderExtMapper.class, McDepartRecordMapper.class,
                RechargeDispatchOutboxMapper.class, RechargeBalanceDeltaOutboxMapper.class)) config.addMapper(mapper);
        var factory = new MybatisSqlSessionFactoryBean();
        factory.setConfiguration(config);
        factory.setDataSource(source);
        return factory.getObject();
    }

    private static <T> T transactional(T target, DataSourceTransactionManager manager, Class<T> type) {
        var proxy = new ProxyFactory(target);
        proxy.addAdvice(new TransactionInterceptor(manager, new AnnotationTransactionAttributeSource()));
        return type.cast(proxy.getProxy());
    }
}

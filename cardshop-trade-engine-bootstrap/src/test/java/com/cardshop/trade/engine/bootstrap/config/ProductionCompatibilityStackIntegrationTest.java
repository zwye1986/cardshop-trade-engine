package com.cardshop.trade.engine.bootstrap.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.cardshop.trade.engine.api.v1.codec.V1RequestBodyCodec;
import com.cardshop.trade.engine.api.v1.controller.V1RechargeOrderController;
import com.cardshop.trade.engine.api.v1.protocol.*;
import com.cardshop.trade.engine.api.v1.routing.*;
import com.cardshop.trade.engine.api.v1.security.*;
import com.cardshop.trade.engine.api.v1.usecase.*;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.application.v1.*;
import com.cardshop.trade.engine.application.v1.crypto.*;
import com.cardshop.trade.engine.domain.service.RequestHashCanonicalizer;
import com.cardshop.trade.engine.infrastructure.config.*;
import com.cardshop.trade.engine.infrastructure.mq.*;
import com.cardshop.trade.engine.infrastructure.observability.TradeEngineObservabilityMetrics;
import com.cardshop.trade.engine.infrastructure.persistence.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.cardshop.trade.engine.infrastructure.redis.*;
import com.fasterxml.jackson.databind.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import liquibase.*;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.*;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareBatchMessageListener;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.context.annotation.*;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;
import reactor.core.publisher.Mono;

/**
 * 兼容改造的真实HTTP/Redis/MQ/MySQL链路及跨进程恢复验证，参见详细设计8.2.1、8.3。
 * 仅接受仓库脚本创建的随机回环夹具；认证/选品为合成数据，不启动履约、通知、Nacos或定时扫描。
 */
@EnabledIfSystemProperty(named = "production.compatibility.stackTests", matches = "true")
class ProductionCompatibilityStackIntegrationTest {
    private static final String ORG = "COMPAT";
    private static final String ORDER = "STACK-ORDER";
    private static final String OUT = "STACK-OUT";
    private static final long INITIAL_BALANCE_CENT = 10_000;
    private static final long ORDER_AMOUNT_CENT = 137;
    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void persistentOccupancyRejectsHttpWithoutRedisOrMqEffectsAndFreeNumberStillWorks() throws Exception {
        try (var f = new Fixture(null)) {
            f.jdbc.execute("insert into mc_order(order_Id,org_code,out_orderId,product_type,order_price,order_status) "
                    + "values('OLD1','COMPAT','OLD-ONE',1,137,12),('OLD2','COMPAT','OLD-MANY',1,139,12),"
                    + "('OLD3','COMPAT','OLD-MANY',1,141,13)");
            f.single.persist(aggregate("NEW", "NEW-OUT"));
            f.single.persist(aggregate("ORPHAN", "ORPHAN-OUT"));
            f.jdbc.update("delete from mc_order where order_Id='ORPHAN'");
            var beforeKeys = f.redis.keys(f.prefix + ":*").collectList().block(TIMEOUT);
            var beforeRows = f.jdbc.queryForList("select order_Id,order_price,order_status,trade_engine_owned from mc_order order by id");
            for (String out : List.of("OLD-ONE", "OLD-MANY", "NEW-OUT", "ORPHAN-OUT")) {
                assertThat(f.redis.hasKey(f.keys.idempotentKey(ORG, out)).block(TIMEOUT)).isFalse();
                f.post(out, 1, "505");
                // 非法数量本应发布失败事实，但号码已占用必须更早拒绝，不能污染资金或消息。
                f.post(out, 2, "505");
            }
            // 同一真实数据源暂时缺表，验证持久检查故障不会降级为不存在后受理。
            f.jdbc.execute("rename table recharge_order_ext to compat_unavailable_ext");
            try { f.post("DB-UNAVAILABLE", 1, "500"); }
            finally { f.jdbc.execute("rename table compat_unavailable_ext to recharge_order_ext"); }
            assertThat(f.redis.keys(f.prefix + ":*").collectList().block(TIMEOUT))
                    .containsExactlyInAnyOrderElementsOf(beforeKeys);
            assertThat(f.jdbc.queryForList("select order_Id,order_price,order_status,trade_engine_owned from mc_order order by id"))
                    .isEqualTo(beforeRows);
            f.assertBalance(INITIAL_BALANCE_CENT, 0);
            f.assertQueuesEmpty();
            System.out.println("COMPAT_STACK occupancy: HTTP505 x8 and real DB failure500, Redis keys/balance unchanged, main/retry/DLQ=0");

            f.post(OUT, 1, "200");
            f.assertBalance(INITIAL_BALANCE_CENT - ORDER_AMOUNT_CENT, ORDER_AMOUNT_CENT);
            var original = f.deliver(false, false);
            f.publish(original);
            f.deliver(false, false);
            f.assertCompleted();
            f.post(OUT, 1, "505");
            f.assertQueuesEmpty();
            System.out.println("COMPAT_STACK control/single replay: HTTP200 then505, debit=137 once, five tables=1 each, pending=0");
        }
    }

    @Test
    void realWriterRollbackRetainsPendingThenBrokerRetryRepairsEventsBeforeAck() throws Exception {
        try (var f = new Fixture(null)) {
            f.post(OUT, 1, "200");
            // writer先写dispatch再写delta；第二表报错必须回滚第一表，不能只检查异常类型。
            f.jdbc.execute("create trigger compat_writer_failure before insert on recharge_balance_delta_outbox "
                    + "for each row signal sqlstate '45000' set message_text='synthetic writer failure'");
            var original = f.deliver(true, false);
            f.assertCounts(1, 0);
            assertThat(f.hash("persist_status")).isEqualTo("PERSIST_FAILED");
            assertThat(f.redis.opsForZSet().score(f.keys.pendingKey(ORG), ORDER).block(TIMEOUT)).isNotNull();
            assertThat(f.ready(f.properties.getMq().getPersistQueue())).isZero();
            assertThat(f.ready(f.properties.getMq().getPersistRetryQueue())).isOne();
            f.assertBalance(INITIAL_BALANCE_CENT - ORDER_AMOUNT_CENT, ORDER_AMOUNT_CENT);
            System.out.println("COMPAT_STACK writer failure: facts=1 each, events=0, pending retained, retry=1, no extra debit");
            f.jdbc.execute("drop trigger compat_writer_failure");
            var retried = f.deliver(true, true);
            assertThat(retried).isEqualTo(original);
            f.assertCompleted();
            f.publish(original);
            f.deliver(true, false);
            f.assertCompleted();
            System.out.println("COMPAT_STACK retry: real broker TTL header=1, events repaired once, DB_PERSISTED, pending=0, acked");
        }
    }

    @Test
    void killedConsumerRecoversThroughRealBrokerRedeliveryInNewJvm() throws Exception {
        try (var f = new Fixture(null)) {
            f.post(OUT, 1, "200");
            Path crashLog = Files.createTempFile("compat-stack-crash-", ".log");
            Path recoverLog = Files.createTempFile("compat-stack-recover-", ".log");
            Process crashed = null;
            Process recovered = null;
            try {
                crashed = child(f.schema, "crash", crashLog);
                await(() -> read(crashLog).contains("COMPAT_STACK_BEFORE_WRITER"));
                f.assertCounts(1, 0);
                assertThat(f.hash("persist_status")).isEqualTo("PERSIST_QUEUED");
                assertThat(f.redis.opsForZSet().score(f.keys.pendingKey(ORG), ORDER).block(TIMEOUT)).isNotNull();
                await(() -> f.unacked() == 1);
                assertThat(f.ready(f.properties.getMq().getPersistQueue())).isZero();
                System.out.println("COMPAT_STACK pre-kill: committed facts=1, events=0, Redis=PERSIST_QUEUED, ready=0 unacked=1");
                crashed.destroyForcibly();
                assertThat(crashed.waitFor(10, TimeUnit.SECONDS)).isTrue();
                assertThat(crashed.exitValue()).isEqualTo(137);
                await(() -> f.ready(f.properties.getMq().getPersistQueue()) == 1);
                recovered = child(f.schema, "recover", recoverLog);
                assertThat(recovered.waitFor(40, TimeUnit.SECONDS)).as(read(recoverLog)).isTrue();
                assertThat(recovered.exitValue()).as(read(recoverLog)).isZero();
                assertThat(read(recoverLog)).contains("COMPAT_STACK_ACKED redelivered=true");
                f.assertCompleted();
                await(() -> f.unacked() == 0);
                // 新进程恢复后再次投当前完整快照，而非只检查一次重投恰好成功。
                var snapshot = new RedisPersistOrderSnapshotAdapter(f.redis, f.keys, TIMEOUT)
                        .findLatestSnapshot(ORG, OUT, ORDER).orElseThrow();
                Map<String, Object> replayFields = f.redis.opsForHash().entries(f.keys.orderKey(ORG, OUT))
                        .collectMap(entry -> entry.getKey().toString(), Map.Entry::getValue).block(TIMEOUT);
                var replay = new PersistTradeOrderMessage("replay-" + ORDER, ORG, ORDER, OUT,
                        snapshot.requestHash(), snapshot.orderVersion(), replayFields);
                f.publish(replay);
                f.deliver(true, false);
                f.assertCompleted();
                System.out.println("COMPAT_STACK SIGKILL/restart: killed=137 newJvm=0 redelivered=true, five tables=1 each, mark/ack complete");
            } finally {
                for (var p : Arrays.asList(crashed, recovered)) {
                    if (p != null && p.isAlive()) { p.destroyForcibly(); p.waitFor(10, TimeUnit.SECONDS); }
                }
                Files.deleteIfExists(crashLog);
                Files.deleteIfExists(recoverLog);
            }
        }
    }

    /** 真实落库在途时Redis前进，旧mark必须保留pending；旧MQ有/无Redis时都不能回退事实。参见详细设计8.3。 */
    @Test
    void oldMarkRetainsNewerPendingAndLateMqUsesLatestOrFallsBackWithoutRegression() throws Exception {
        try (var f = new Fixture(null)) {
            f.post(OUT, 1, "200");
            var original = f.deliver(true, false);
            f.assertCompleted();
            f.queueRedisVersion(3);
            var fields = new LinkedHashMap<>(original.initialOrderSnapshot());
            // Map经JSON回读时小整数为Integer；夹具保持相同类型，消息本身的订单版本仍为long。
            fields.put("order_version", 3);
            var thirdVersion = new PersistTradeOrderMessage("version-3", ORG, ORDER, OUT, original.requestHash(), 3, fields);
            f.publish(thirdVersion);
            Future<PersistTradeOrderMessage> inFlight;
            // 真锁制造snapshot读取与mark之间的版本变化，不mock快照端口或Lua结果。
            try (var connection = f.dataSource(f.schema).getConnection()) {
                connection.setAutoCommit(false);
                try (var lock = connection.prepareStatement("select order_Id from mc_order where order_Id=? for update")) {
                    lock.setString(1, ORDER);
                    try (var row = lock.executeQuery()) { assertThat(row.next()).isTrue(); }
                }
                inFlight = f.executor.submit(() -> f.deliver(true, false));
                await(() -> f.jdbc.queryForObject("""
                        select count(*) from performance_schema.data_lock_waits w
                        join performance_schema.data_locks l
                          on l.engine_lock_id=w.requesting_engine_lock_id and l.engine=w.engine
                        where l.object_schema=? and l.object_name='mc_order'
                        """, Integer.class, f.schema) > 0);
                assertThat(inFlight.isDone()).isFalse();
                f.queueRedisVersion(7);
                connection.commit();
            }
            assertThat(inFlight.get(15, TimeUnit.SECONDS)).isEqualTo(thirdVersion);
            assertThat(f.jdbc.queryForObject("select persisted_version from recharge_order_ext", Long.class)).isEqualTo(3);
            assertThat(f.hash("order_version")).isEqualTo("7");
            assertThat(f.hash("persisted_version")).isEqualTo("3");
            assertThat(f.hash("persist_status")).isEqualTo("PERSIST_QUEUED");
            assertThat(f.redis.opsForZSet().score(f.keys.pendingKey(ORG), ORDER).block(TIMEOUT)).isNotNull();
            assertThat(f.redis.opsForZSet().score(f.keys.pendingOrgIndexKey(f.keys.pendingOrgBucket(ORG)), ORG).block(TIMEOUT)).isNotNull();
            f.assertBalance(INITIAL_BALANCE_CENT - ORDER_AMOUNT_CENT, ORDER_AMOUNT_CENT);
            f.assertQueuesEmpty();
            System.out.println("COMPAT_STACK_VERSION staleMark: mysql=3 redisOrder=7 redisPersisted=3 pendingRetained=true acked=true");

            // 消息仍是3，Redis已7；必须按最新Redis快照推进，不能只按MQ旧版本跳过并清pending。
            f.publish(thirdVersion);
            f.deliver(true, false);
            assertThat(f.jdbc.queryForObject("select persisted_version from recharge_order_ext", Long.class)).isEqualTo(7);
            assertThat(f.jdbc.queryForObject("select redis_order_version from recharge_order_ext", Long.class)).isEqualTo(7);
            assertThat(f.hash("persisted_version")).isEqualTo("7");
            f.assertCompleted();

            f.jdbc.execute("update mc_order set order_status=12");
            f.jdbc.execute("update recharge_order_ext set dispatch_status='SUCCESS'");
            f.jdbc.execute("update recharge_dispatch_outbox set status='SUCCESS',version=9,retry_count=2");
            f.jdbc.execute("update recharge_balance_delta_outbox set status='SUCCESS',version=9,retry_count=2");
            var before = List.of(
                    f.jdbc.queryForMap("select order_Id,order_price,order_status from mc_order"),
                    f.jdbc.queryForMap("select persisted_version,redis_order_version,dispatch_status,persist_source,request_hash from recharge_order_ext"),
                    f.jdbc.queryForMap("select event_id,status,version,retry_count,payload from recharge_dispatch_outbox"),
                    f.jdbc.queryForMap("select event_id,status,version,retry_count,delta_amount from recharge_balance_delta_outbox"));
            f.redis.delete(f.keys.orderKey(ORG, OUT)).block(TIMEOUT);
            f.publish(thirdVersion);
            f.deliver(true, false);
            f.assertCounts(1, 1);
            assertThat(List.of(
                    f.jdbc.queryForMap("select order_Id,order_price,order_status from mc_order"),
                    f.jdbc.queryForMap("select persisted_version,redis_order_version,dispatch_status,persist_source,request_hash from recharge_order_ext"),
                    f.jdbc.queryForMap("select event_id,status,version,retry_count,payload from recharge_dispatch_outbox"),
                    f.jdbc.queryForMap("select event_id,status,version,retry_count,delta_amount from recharge_balance_delta_outbox"))).isEqualTo(before);
            assertThat(f.redis.hasKey(f.keys.orderKey(ORG, OUT)).block(TIMEOUT)).isFalse();
            f.assertBalance(INITIAL_BALANCE_CENT - ORDER_AMOUNT_CENT, ORDER_AMOUNT_CENT);
            f.assertQueuesEmpty();
            System.out.println("COMPAT_STACK_VERSION replay: mq=3 selectedRedis=7 thenMissingRedisFallback=3 mysqlKept=7 terminalEventsUnchanged=true acked=true");
        }
    }

    /** 子进程只运行真实批listener，故障闸门不改变生产writer或ack逻辑。 */
    public static void main(String[] args) throws Exception {
        assertThat(args).hasSize(2);
        try (var f = new Fixture(args[0])) {
            RechargePersistOutboxAsyncWriter writer = f.writer;
            if (args[1].equals("crash")) {
                writer = aggregates -> {
                    f.assertCounts(1, 0);
                    System.out.println("COMPAT_STACK_BEFORE_WRITER pid=" + ProcessHandle.current().pid());
                    System.out.flush();
                    try {
                        if (!new CountDownLatch(1).await(60, TimeUnit.SECONDS)) throw new AssertionError("parent did not kill child");
                    } catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new IllegalStateException(ex); }
                };
            }
            var listener = f.batchListener(f.consumer(writer, true));
            var done = new CountDownLatch(1);
            var failure = new AtomicReference<Throwable>();
            var container = new SimpleMessageListenerContainer(f.rabbit);
            container.setQueueNames(f.properties.getMq().getPersistQueue());
            container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
            container.setPrefetchCount(1);
            container.setConsumerBatchEnabled(true);
            container.setBatchSize(1);
            container.setReceiveTimeout(100);
            container.setMessageListener((ChannelAwareBatchMessageListener) (messages, channel) -> {
                try {
                    listener.onMessage(messages, channel);
                    f.assertCounts(1, 1);
                    assertThat(f.hash("persist_status")).isEqualTo("DB_PERSISTED");
                    System.out.println("COMPAT_STACK_ACKED redelivered=" + messages.getFirst().getMessageProperties().isRedelivered()
                            + " pid=" + ProcessHandle.current().pid());
                } catch (Throwable ex) { failure.set(ex); }
                finally { done.countDown(); }
            });
            try {
                container.start();
                assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
                assertThat(failure.get()).isNull();
            } finally { container.stop(); container.destroy(); }
        }
    }

    private static Process child(String schema, String mode, Path log) throws Exception {
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), ProductionCompatibilityStackIntegrationTest.class.getName(), schema, mode)
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

    private static String read(Path path) {
        try { return Files.readString(path); }
        catch (java.io.IOException ex) { throw new java.io.UncheckedIOException(ex); }
    }

    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(50);
        }
        throw new AssertionError("isolated stack condition timed out");
    }

    private static OrderPersistAggregate aggregate(String id, String out) {
        return new OrderPersistAggregate("persist-" + id,
                new PersistOrderSnapshot(ORG, "fixture", id, out, "P", "SKU", "10000000000", "", 1, ORDER_AMOUNT_CENT,
                        "合成商品", 1000, 1, "codekey", "BOCOM", "hash", "v1", "SHA-256", "ACCEPTED", "PASSED", "", "",
                        true, true, "PERSIST_QUEUED", 1, 0, 1), PersistSource.MQ_SNAPSHOT_FALLBACK, Instant.now());
    }

    /** 每个场景独占schema/prefix/topology；子JVM仅打开父进程已验证的夹具，不重新迁移或清理。 */
    private static final class Fixture implements AutoCloseable {
        private final String run = required("COMPAT_STACK_RUN");
        private final String password = required("COMPAT_STACK_PASSWORD");
        private final boolean owner;
        private final String schema;
        private final String prefix;
        private final JdbcTemplate jdbc;
        private final RechargeRedisKeyBuilder keys;
        private final LettuceConnectionFactory lettuce;
        private final ReactiveStringRedisTemplate redis;
        private final RedisSessionAcceptReliabilityAdapter reliability;
        private final TradeEngineProperties properties = new TradeEngineProperties();
        private final CachingConnectionFactory rabbit;
        private final RabbitAdmin admin;
        private final RabbitTemplate template;
        private final RabbitPersistRechargeOrderMessagePublisher publisher;
        private final RechargeOrderPersistencePort single;
        private final RechargeOrderBatchPersistencePort batch;
        private final MybatisPersistedOrderStateAdapter persisted;
        private final MybatisRechargeOrderQueryAdapter queries;
        private final MybatisRechargePersistOutboxAsyncWriter writer;
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        private final GenericApplicationContext brokerContext = new GenericApplicationContext();
        private AnnotationConfigApplicationContext context;
        private reactor.netty.DisposableServer server;
        private final Clock clock = Clock.systemUTC();
        private final V12SigningCredential credential = V12SigningCredential.generate(ORG);

        private Fixture(String existingSchema) throws Exception {
            assertThat(run).matches("[a-f0-9]{32}");
            assertThat(password).matches("[a-f0-9]{48}");
            owner = existingSchema == null;
            schema = owner ? "compat_stack_" + UUID.randomUUID().toString().replace("-", "") : existingSchema;
            assertThat(schema).matches("compat_stack_[a-f0-9]{32}");
            prefix = "compat:stack:" + run + ":" + schema;
            keys = new RechargeRedisKeyBuilder(prefix, 128);
            var source = dataSource(schema);
            var adminJdbc = new JdbcTemplate(dataSource("mysql"));
            assertThat(adminJdbc.queryForObject("select version()", String.class)).isEqualTo("8.0.36");
            assertThat(adminJdbc.queryForObject("select @@transaction_isolation", String.class)).isEqualTo("READ-COMMITTED");
            assertThat(adminJdbc.queryForObject("select @@lower_case_table_names", Integer.class)).isOne();
            if (owner) adminJdbc.execute("create database " + schema + " character set utf8mb3 collate utf8mb3_general_ci");
            jdbc = new JdbcTemplate(source);
            if (owner) {
                jdbc.execute(Files.readString(Path.of(required("COMPAT_STACK_BASELINE"))));
                jdbc.execute("alter table mc_order convert to character set utf8mb3 collate utf8mb3_general_ci");
                jdbc.execute("alter table mc_order add index out_orderId_code_index(out_orderId,org_code)");
                try (var c = source.getConnection(); var resources = new ClassLoaderResourceAccessor()) {
                    var db = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(c));
                    try (var migration = new Liquibase("db/changelog/db.changelog-production-compatible.yaml", resources, db)) {
                        migration.update(new Contexts(), new LabelExpression());
                    }
                }
                jdbc.execute("create table compat_stack_fixture(run_id varchar(32) primary key)");
                jdbc.update("insert into compat_stack_fixture(run_id) values(?)", run);
            }
            assertThat(jdbc.queryForObject("select run_id from compat_stack_fixture", String.class)).isEqualTo(run);
            var tm = new DataSourceTransactionManager(source);
            var factory = factory(source);
            var session = new SqlSessionTemplate(factory);
            var orders = session.getMapper(McOrderMapper.class);
            var ext = session.getMapper(RechargeOrderExtMapper.class);
            single = transactional(new MybatisRechargeOrderPersistenceAdapter(orders, ext,
                    session.getMapper(RechargeDispatchOutboxMapper.class), session.getMapper(RechargeBalanceDeltaOutboxMapper.class),
                    session.getMapper(McDepartRecordMapper.class), JSON), tm, RechargeOrderPersistencePort.class);
            batch = transactional(new MybatisBatchRechargeOrderPersistenceAdapter(factory, JSON), tm, RechargeOrderBatchPersistencePort.class);
            persisted = new MybatisPersistedOrderStateAdapter(orders, ext);
            queries = new MybatisRechargeOrderQueryAdapter(orders, ext);
            var writerSource = dataSource(schema);
            writer = new MybatisRechargePersistOutboxAsyncWriter(factory(writerSource), JSON, TradeEngineObservabilityMetrics.noop(),
                    Duration.ofSeconds(1), true, new DataSourceTransactionManager(writerSource));
            lettuce = new LettuceConnectionFactory("127.0.0.1", port("REDIS"));
            lettuce.afterPropertiesSet(); lettuce.start();
            redis = new ReactiveStringRedisTemplate(lettuce);
            assertThat(redis.opsForValue().get("compat-stack-fixture").block(TIMEOUT)).isEqualTo(run);
            properties.getRedis().setWriteEnabled(true);
            properties.getRedis().setWaitReplicas(0);
            reliability = new RedisSessionAcceptReliabilityAdapter(new LettuceRechargeRedisSessionFactory(lettuce), keys,
                    new RechargeRedisScriptLoader(), properties);
            var mq = properties.getMq();
            mq.setPersistQueue(schema + ".queue"); mq.setPersistExchange(schema + ".exchange"); mq.setPersistRoutingKey("persist");
            mq.setPersistRetryQueue(schema + ".retry.queue"); mq.setPersistRetryExchange(schema + ".retry.exchange"); mq.setPersistRetryRoutingKey("retry");
            mq.setPersistDeadLetterQueue(schema + ".dlq"); mq.setPersistDeadLetterExchange(schema + ".dlx"); mq.setPersistDeadLetterRoutingKey("dlq");
            mq.setConsumerRetryDelayMs(5000);
            rabbit = new CachingConnectionFactory("127.0.0.1", port("RABBIT"));
            rabbit.setUsername("compat_it"); rabbit.setPassword(password); rabbit.setVirtualHost("compat_" + run);
            rabbit.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
            rabbit.setPublisherReturns(true);
            // Spring关闭事件先于destroy，才能终止延迟关闭channel线程池；不能用System.exit掩盖资源泄漏。
            brokerContext.registerBean(CachingConnectionFactory.class, () -> rabbit);
            brokerContext.refresh();
            template = new RabbitTemplate(rabbit);
            admin = new RabbitAdmin(rabbit);
            if (owner) {
                for (Declarable item : new TradeEngineInfrastructureConfiguration().rechargePersistDeclarables(properties).getDeclarables()) {
                    if (item instanceof Exchange exchange) admin.declareExchange(exchange);
                    else if (item instanceof org.springframework.amqp.core.Queue queue) admin.declareQueue(queue);
                    else if (item instanceof Binding binding) admin.declareBinding(binding);
                }
                seedRedis();
            }
            publisher = new RabbitPersistRechargeOrderMessagePublisher(new RabbitTemplatePersistMessageOperations(template), JSON,
                    properties, clock, executor);
        }

        private void seedRedis() {
            redis.opsForHash().putAll(keys.balanceKey(ORG), Map.of("available", Long.toString(INITIAL_BALANCE_CENT), "frozen", "0", "version", "1")).block(TIMEOUT);
            redis.opsForHash().putAll(keys.merchantConfigKey(ORG), Map.of("status", "ENABLED", "version", "1")).block(TIMEOUT);
            var settings = new V1ProductCacheSettings(Duration.ofMinutes(25), Duration.ofMinutes(30), Duration.ofHours(1),
                    Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ofSeconds(10));
            var cache = new RedisV1ProductSelectionCacheAdapter(redis, keys, settings, TIMEOUT);
            var token = cache.tryAcquireRefreshLock(ORG, "P").orElseThrow();
            assertThat(cache.writeSnapshotIfLockOwned(ORG, "P", token, selection(), clock.instant())).isTrue();
        }

        private V1ProductSelection selection() {
            return V1ProductSelection.passed("P", "SKU", "BOCOM", "合成商品", "codekey", 1, ORDER_AMOUNT_CENT, 1000L, 100L, 1L);
        }

        /** 仅为本例模拟待落库版本前进，不再调用accept、不增加预扣。 */
        private void queueRedisVersion(long version) {
            for (String key : List.of(keys.orderKey(ORG, OUT), keys.idempotentKey(ORG, OUT))) {
                redis.opsForHash().putAll(key, Map.of("order_version", Long.toString(version), "persist_status", "PERSIST_QUEUED")).block(TIMEOUT);
            }
            long nextRetryEpochMillis = clock.millis() + Duration.ofMinutes(1).toMillis();
            redis.opsForZSet().add(keys.pendingKey(ORG), ORDER, nextRetryEpochMillis).block(TIMEOUT);
            redis.opsForZSet().add(keys.pendingOrgIndexKey(keys.pendingOrgBucket(ORG)), ORG, nextRetryEpochMillis).block(TIMEOUT);
        }

        private void startHttp() {
            var precheck = new V1ProtocolPrecheckService(new V1RequestBodyCodec(),
                    new V1TimestampValidator(new V1TimestampSettings(Duration.ofMinutes(5)), clock), new V12SignatureVerifier(), new V1ProtocolSettings(false));
            var useCase = new V1CreateRechargeOrderUseCaseService(precheck, new RequestHashCanonicalizer(),
                    (app, org) -> Optional.of(new V1MerchantProfile(app, org, "", true, 1L)),
                    (org, product, account) -> selection(), reliability, reliability, publisher, AcceptAnomalyRecordPort.noop(),
                    RedisHaGuardPort.noop(), V1ProductConfigPreheatPort.noop(), () -> ORDER, clock,
                    new V1RechargeAcceptSettings(Duration.ofMinutes(5)), executor, queries);
            // 本专项不验JWT，仅替换认证身份；请求HMAC-SM3验签、路由、HTTP及业务/中间件均是真实实现。
            var auth = new V1TokenAuthenticator(new V1TokenSettings("synthetic", "fixture", Duration.ofMinutes(1)), clock, Optional.empty()) {
                @Override public Optional<AuthenticatedMerchant> authenticate(String token) {
                    return "fixture".equals(token) ? Optional.of(new AuthenticatedMerchant("fixture", ORG, credential)) : Optional.empty();
                }
            };
            var controller = new V1RechargeOrderController(new V1RouteDecisionService(new V1RoutingSettings(true, "http://unused",
                    Duration.ofSeconds(3), List.of(ORG), List.of("fixture"), List.of("/v1/cardapi/order/createRechargeOrder"))),
                    auth, (request, body) -> Mono.error(new AssertionError("legacy route must not run")), useCase,
                    command -> { throw new AssertionError("query not in this fixture"); },
                    (command, invalidation) -> { throw new AssertionError("invalid not in this fixture"); });
            context = new AnnotationConfigApplicationContext();
            context.register(WebConfig.class);
            context.getBeanFactory().registerSingleton("compatController", controller);
            context.refresh();
            server = reactor.netty.http.server.HttpServer.create().host("127.0.0.1").port(0)
                    .handle(new ReactorHttpHandlerAdapter(WebHttpHandlerBuilder.applicationContext(context).build())).bindNow();
        }

        private void post(String out, int num, String expected) throws Exception {
            if (server == null) startHttp();
            var body = new LinkedHashMap<String, Object>();
            body.put("out_order_sn", out); body.put("product_id", "P"); body.put("account", "10000000000");
            body.put("num", num); body.put("notify_url", ""); body.put("timestamp", Long.toString(clock.millis()));
            body.put("nonce_str", UUID.randomUUID().toString().replace("-", ""));
            body.put("sign", V12MessageSignatures.sign(V12MessageSignatures.Operation.CREATE, body, credential.signKey()));
            try (var client = HttpClient.newHttpClient()) {
                var response = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port()
                                + "/v1/cardapi/order/createRechargeOrder")).timeout(TIMEOUT)
                        .header("Authorization", "fixture").header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body))).build(), HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).isEqualTo(200);
                assertThat(JSON.readTree(response.body()).path("code").asText()).as(response.body()).isEqualTo(expected);
            }
        }

        private PersistRechargeOrderConsumerService consumer(RechargePersistOutboxAsyncWriter outbox, boolean async) {
            return new PersistRechargeOrderConsumerService(new RedisPersistOrderSnapshotAdapter(redis, keys, TIMEOUT), single, batch,
                    persisted, reliability, RechargeDispatchTimerPort.noop(), clock, Duration.ofSeconds(3), Duration.ofSeconds(1), outbox, async);
        }

        private RabbitPersistRechargeOrderBatchMessageListener batchListener(PersistRechargeOrderConsumerService consumer) {
            return new RabbitPersistRechargeOrderBatchMessageListener(JSON, consumer, template, properties.getMq().getPersistRetryExchange(),
                    properties.getMq().getPersistRetryRoutingKey(), properties.getMq().getConsumerMaxRetry());
        }

        /** 从真实broker manual get后交生产listener；同步RPC作ack发送屏障，不用mock Channel推断ack。 */
        private PersistTradeOrderMessage deliver(boolean async, boolean expectRetry) throws Exception {
            try (var connection = rabbit.getRabbitConnectionFactory().newConnection(); var channel = connection.createChannel()) {
                var response = new AtomicReference<GetResponse>();
                await(() -> {
                    try { response.set(channel.basicGet(properties.getMq().getPersistQueue(), false)); return response.get() != null; }
                    catch (java.io.IOException ex) { throw new java.io.UncheckedIOException(ex); }
                });
                var delivery = response.get();
                var props = new DefaultMessagePropertiesConverter().toMessageProperties(delivery.getProps(), delivery.getEnvelope(), StandardCharsets.UTF_8.name());
                if (expectRetry) assertThat(((Number) props.getHeader("x-retry-count")).intValue()).isOne();
                var message = new Message(delivery.getBody(), props);
                if (async) batchListener(consumer(writer, true)).onMessage(List.of(message), channel);
                else new RabbitPersistRechargeOrderMessageListener(JSON, consumer(null, false), template,
                        properties.getMq().getPersistRetryExchange(), properties.getMq().getPersistRetryRoutingKey(),
                        properties.getMq().getConsumerMaxRetry()).onMessage(message, channel);
                channel.queueDeclarePassive(properties.getMq().getPersistQueue());
                return JSON.readValue(delivery.getBody(), PersistTradeOrderMessage.class);
            }
        }

        private void publish(PersistTradeOrderMessage message) throws Exception {
            assertThat(publisher.publish(message).toCompletableFuture().get(10, TimeUnit.SECONDS).confirmed()).isTrue();
        }

        private int ready(String queue) { return Objects.requireNonNull(admin.getQueueInfo(queue)).getMessageCount(); }

        private int unacked() {
            String url = "http://127.0.0.1:" + port("MANAGEMENT") + "/api/queues/"
                    + URLEncoder.encode("compat_" + run, StandardCharsets.UTF_8) + "/" + properties.getMq().getPersistQueue();
            try (var client = HttpClient.newHttpClient()) {
                var response = client.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(3))
                        .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(("compat_it:" + password).getBytes(StandardCharsets.UTF_8)))
                        .GET().build(), HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).isEqualTo(200);
                return JSON.readTree(response.body()).path("messages_unacknowledged").asInt(-1);
            } catch (java.io.IOException ex) { throw new java.io.UncheckedIOException(ex); }
            catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new IllegalStateException(ex); }
        }

        private Object hash(String field) { return redis.opsForHash().get(keys.orderKey(ORG, OUT), field).block(TIMEOUT); }

        private void assertBalance(long available, long frozen) {
            assertThat(redis.opsForHash().get(keys.balanceKey(ORG), "available").block(TIMEOUT)).isEqualTo(Long.toString(available));
            assertThat(redis.opsForHash().get(keys.balanceKey(ORG), "frozen").block(TIMEOUT)).isEqualTo(Long.toString(frozen));
        }

        private void assertCounts(int facts, int events) {
            assertThat(jdbc.queryForObject("select count(*) from mc_order where order_Id=?", Integer.class, ORDER)).isEqualTo(facts);
            for (String table : List.of("recharge_order_ext", "mc_depart_record", "recharge_dispatch_outbox", "recharge_balance_delta_outbox")) {
                assertThat(jdbc.queryForObject("select count(*) from " + table + " where order_id=?", Integer.class, ORDER))
                        .as(table).isEqualTo(table.endsWith("outbox") ? events : facts);
            }
        }

        private void assertQueuesEmpty() {
            for (String queue : List.of(properties.getMq().getPersistQueue(), properties.getMq().getPersistRetryQueue(), properties.getMq().getPersistDeadLetterQueue()))
                assertThat(ready(queue)).as(queue).isZero();
        }

        private void assertCompleted() {
            assertCounts(1, 1);
            assertThat(jdbc.queryForObject("select trade_engine_owned from mc_order where order_Id=?", Integer.class, ORDER)).isOne();
            assertThat(jdbc.queryForObject("select order_price from mc_order where order_Id=?", Long.class, ORDER)).isEqualTo(ORDER_AMOUNT_CENT);
            assertThat(jdbc.queryForObject("select `value` from mc_depart_record where order_id=?", Long.class, ORDER)).isEqualTo(ORDER_AMOUNT_CENT);
            assertThat(jdbc.queryForObject("select event_id from recharge_dispatch_outbox where order_id=?", String.class, ORDER)).isEqualTo("dispatch-" + ORDER);
            assertThat(jdbc.queryForObject("select event_id from recharge_balance_delta_outbox where order_id=?", String.class, ORDER)).isEqualTo("balance-flush-" + ORDER);
            assertThat(hash("persist_status")).isEqualTo("DB_PERSISTED");
            assertThat(redis.opsForZSet().size(keys.pendingKey(ORG)).block(TIMEOUT)).isZero();
            assertThat(redis.opsForZSet().score(keys.pendingOrgIndexKey(keys.pendingOrgBucket(ORG)), ORG).block(TIMEOUT)).isNull();
            assertBalance(INITIAL_BALANCE_CENT - ORDER_AMOUNT_CENT, ORDER_AMOUNT_CENT);
            assertQueuesEmpty();
        }

        private DriverManagerDataSource dataSource(String database) {
            return new DriverManagerDataSource("jdbc:mysql://127.0.0.1:" + port("MYSQL") + "/" + database
                    + "?allowPublicKeyRetrieval=true&useSSL=false&allowMultiQueries=true&serverTimezone=Asia/Shanghai&socketTimeout=15000", "root", password);
        }

        @Override public void close() {
            if (server != null) server.disposeNow();
            if (context != null) context.close();
            executor.close();
            if (owner) {
                redis.keys(prefix + ":*").flatMap(redis::delete).then().block(TIMEOUT);
                for (String queue : List.of(properties.getMq().getPersistQueue(), properties.getMq().getPersistRetryQueue(), properties.getMq().getPersistDeadLetterQueue())) admin.deleteQueue(queue);
                for (String exchange : List.of(properties.getMq().getPersistExchange(), properties.getMq().getPersistRetryExchange(), properties.getMq().getPersistDeadLetterExchange())) admin.deleteExchange(exchange);
                new JdbcTemplate(dataSource("mysql")).execute("drop database " + schema);
            }
            brokerContext.close(); lettuce.destroy();
        }
    }

    private static SqlSessionFactory factory(DriverManagerDataSource source) throws Exception {
        var config = new MybatisConfiguration(); config.setMapUnderscoreToCamelCase(true);
        for (var mapper : List.of(McOrderMapper.class, RechargeOrderExtMapper.class, McDepartRecordMapper.class,
                RechargeDispatchOutboxMapper.class, RechargeBalanceDeltaOutboxMapper.class)) config.addMapper(mapper);
        var factory = new MybatisSqlSessionFactoryBean(); factory.setDataSource(source); factory.setConfiguration(config);
        return factory.getObject();
    }

    private static <T> T transactional(T target, DataSourceTransactionManager manager, Class<T> type) {
        var proxy = new ProxyFactory(target);
        proxy.addAdvice(new TransactionInterceptor(manager, new AnnotationTransactionAttributeSource()));
        return type.cast(proxy.getProxy());
    }

    private static int port(String service) {
        String value = required("COMPAT_STACK_" + service + "_PORT");
        assertThat(value).matches("[0-9]{1,5}");
        int port = Integer.parseInt(value);
        assertThat(port).isBetween(1024, 65535).isNotIn(3306, 6379, 5672, 15672);
        return port;
    }

    private static String required(String key) { return Objects.requireNonNull(System.getenv(key), key); }
    @Configuration(proxyBeanMethods = false) @EnableWebFlux static class WebConfig {}
}

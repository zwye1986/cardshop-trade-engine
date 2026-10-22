package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.*;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Task17专用真实主库装配，只有脚本创建的随机schema才可使用。屏障独立连接，绝不写进业务事务。
 * 参见 docs/交行直充业务系统详细设计.md「9.8」。不启动Spring Boot、Nacos或供应商。
 */
final class MinisoMysqlArbitrationFixture {
    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-22T01:00:00Z"), ZoneOffset.UTC);
    final DataSource source;
    final JdbcTemplate jdbc;
    final TransactionTemplate tx;
    final SqlSessionTemplate session;
    final RechargeFulfillmentOrderPort orders;
    final MinisoReceiptStore receipts;
    final MybatisMinisoWriteOffStore writeOff;
    final MybatisMinisoWriteOffProtection protection;
    final AtomicReference<String> fault = new AtomicReference<>("");

    MinisoMysqlArbitrationFixture() throws Exception {
        assertThat(System.getProperty("java.specification.version")).isEqualTo("21");
        source = dataSource();
        jdbc = new JdbcTemplate(source);
        assertThat(jdbc.queryForObject("select run_id from miniso_bridge_fixture", String.class))
                .isEqualTo(env("MINISO_MYSQL_RUN_ID"));
        assertThat(jdbc.queryForObject("select version()", String.class)).isEqualTo("8.0.28");
        var config = new MybatisConfiguration();
        for (var type : new Class<?>[]{McOrderMapper.class, RechargeOrderExtMapper.class,
                McOrderSupplierFlowMapper.class, MinisoReceiptMapper.class, MinisoCouponGuardMapper.class,
                MinisoSharedCallbackInboxMapper.class, MinisoWriteOffEventMapper.class, McDepartRecordMapper.class,
                RechargeDispatchOutboxMapper.class, RechargeBalanceDeltaOutboxMapper.class, RechargeNotifyTaskMapper.class}) {
            config.addMapper(type);
        }
        config.addInterceptor(new CrashInterceptor(fault));
        var factory = new MybatisSqlSessionFactoryBean();
        factory.setDataSource(source); factory.setConfiguration(config);
        session = new SqlSessionTemplate(factory.getObject());
        var tm = new DataSourceTransactionManager(source);
        tx = new TransactionTemplate(tm);
        var receiptMapper = mapper(MinisoReceiptMapper.class);
        var guards = mapper(MinisoCouponGuardMapper.class);
        var events = mapper(MinisoWriteOffEventMapper.class);
        protection = new MybatisMinisoWriteOffProtection(events, receiptMapper, guards,
                mapper(MinisoSharedCallbackInboxMapper.class), tm, CLOCK, 10);
        receipts = new MinisoReceiptStore(receiptMapper, guards, CLOCK, () -> protection);
        var target = new MybatisRechargeFulfillmentOrderAdapter(mapper(McOrderMapper.class),
                mapper(RechargeOrderExtMapper.class), mapper(McOrderSupplierFlowMapper.class), new ObjectMapper(), CLOCK,
                () -> receipts, mapper(McDepartRecordMapper.class), mapper(RechargeDispatchOutboxMapper.class),
                mapper(RechargeBalanceDeltaOutboxMapper.class));
        var proxy = new ProxyFactory(target);
        proxy.addAdvice(new TransactionInterceptor(tm, new AnnotationTransactionAttributeSource()));
        orders = (RechargeFulfillmentOrderPort) proxy.getProxy();
        writeOff = new MybatisMinisoWriteOffStore(events, receiptMapper, guards,
                new MybatisRechargeNotifyTaskAdapter(mapper(RechargeNotifyTaskMapper.class), new ObjectMapper(), CLOCK),
                new V12RedemptionNotifyPayloadSigner(org.mockito.Mockito.mock(com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort.class)),
                tm, CLOCK, 10, Duration.ofSeconds(30), 10);
    }

    <T> T mapper(Class<T> type) { return session.getMapper(type); }

    static DataSource dataSource() {
        String run = env("MINISO_MYSQL_RUN_ID"), schema = env("MINISO_MYSQL_SCHEMA"), port = env("MINISO_MYSQL_PORT");
        assertThat(run).matches("[a-f0-9]{16}");
        assertThat(schema).isEqualTo("miniso_bridge_it_" + run);
        assertThat(port).matches("[0-9]{1,5}");
        int level = switch (env("MINISO_MYSQL_ISOLATION")) {
            case "READ-COMMITTED" -> Connection.TRANSACTION_READ_COMMITTED;
            case "REPEATABLE-READ" -> Connection.TRANSACTION_REPEATABLE_READ;
            default -> throw new AssertionError("Unsupported isolation");
        };
        // 每条连接显式设置并核对隔离级别；不能只依赖容器全局默认值。
        return new DriverManagerDataSource("jdbc:mysql://127.0.0.1:" + port + "/" + schema
                + "?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC&connectTimeout=5000&socketTimeout=65000&allowMultiQueries=true",
                "miniso_bridge_it", env("MINISO_MYSQL_PASSWORD")) {
            @Override public Connection getConnection() throws SQLException { return configure(super.getConnection()); }
            @Override public Connection getConnection(String user, String password) throws SQLException {
                return configure(super.getConnection(user, password));
            }
            private Connection configure(Connection c) throws SQLException {
                c.setTransactionIsolation(level);
                try (var statement = c.createStatement()) { statement.execute("set session innodb_lock_wait_timeout=10"); }
                assertThat(c.getTransactionIsolation()).isEqualTo(level);
                return c;
            }
        };
    }

    void seed(String id) {
        jdbc.update("""
                insert into mc_order(order_Id,org_code,out_orderId,product_type,num,supplier_channel,account,
                  order_status,order_price,callback_url,data_status,parent_id,produt_code)
                values (?,'TASK17',?,1,1,'MINISO','10000000000',12,137,'','PROCESSED','parent','sku')
                """, id, "out-" + id);
        jdbc.update("""
                insert into recharge_order_ext(order_id,org_code,out_order_sn,request_hash,request_hash_version,
                  request_hash_alg,persist_status,persist_source,validation_status,balance_reserved,dispatch_allowed,
                  dispatch_status,supplier_batch_id,voucher_use_state)
                values (?,'TASK17',?,'fixture','V1','SHA-256','DB_PERSISTED','FIXTURE','PASSED',1,1,'SUCCESS','100023','1')
                """, id, "out-" + id);
        jdbc.update("insert into mc_depart_record(order_id,org_code,type,`value`) values (?,'TASK17','1',137)", id);
        jdbc.update("""
                insert into recharge_dispatch_outbox(event_id,event_type,org_code,order_id,out_order_sn,payload,status,
                  retry_count,max_retry_count,next_retry_time,version,shard_key)
                values (?,'INVALID_ORDER','TASK17',? ,?,'{}','INIT',0,10,'2026-10-21',0,?)
                """, "source-" + id, id, "out-" + id, "source-" + id);
    }

    MinisoFulfillmentReceipt receipt(String id, String coupon, int status) {
        return new MinisoFulfillmentReceipt(id, "TASK17", 100023, coupon, "10000000000",
                "2026-10-01 00:00:00", "2026-12-01 00:00:00", status);
    }

    OrderFulfillmentStatusUpdate terminal(String id, String coupon) {
        return new OrderFulfillmentStatusUpdate(id, FulfillmentDispatchStatus.CANCELLED, 12, "PROCESSED", null,
                "", "", "fixture", receipt(id, coupon, 3), CLOCK.instant());
    }

    void late(String id, String coupon) {
        writeOff.accept(new MinisoWriteOffNotification(coupon, LocalDateTime.of(2026,10,22,9,0)));
        writeOff.processBatch(1,0,100,"task17");
        assertThat(jdbc.queryForObject("select status from recharge_miniso_writeoff_event where event_id=?",
                String.class, new MinisoWriteOffNotification(coupon, LocalDateTime.of(2026,10,22,9,0)).eventId())).isEqualTo("MANUAL_REQUIRED");
    }

    void signal(String name) {
        jdbc.update("insert into miniso_bridge_test_barrier(name,value) values (?,'1')", name);
        System.out.println("Task17 engine committed barrier " + name);
    }

    void await(String name) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            if (jdbc.queryForObject("select count(*) from miniso_bridge_test_barrier where name=?", Integer.class, name) == 1) return;
            java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
        }
        throw new AssertionError("Peer barrier timed out: " + name);
    }

    static String env(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new AssertionError("Required fixture env missing: " + name);
        return value;
    }

    /** 测试专用MyBatis语句拦截器，真实SQL执行前后注入崩溃；不向生产加入暂停开关。 */
    @Intercepts(@Signature(type=Executor.class, method="update", args={MappedStatement.class,Object.class}))
    static final class CrashInterceptor implements Interceptor {
        private final AtomicReference<String> fault;
        CrashInterceptor(AtomicReference<String> fault) { this.fault = fault; }
        @Override public Object intercept(Invocation invocation) throws Throwable {
            var statement = (MappedStatement) invocation.getArgs()[0];
            String sql = statement.getBoundSql(invocation.getArgs()[1]).getSql().toLowerCase(java.util.Locale.ROOT);
            if (sql.contains("insert into recharge_balance_delta_outbox") && fault.compareAndSet("before-delta", ""))
                throw new IllegalStateException("Task17 crash before delta insert");
            Object result = invocation.proceed();
            if (sql.contains("insert into recharge_balance_delta_outbox") && fault.compareAndSet("after-delta", ""))
                throw new IllegalStateException("Task17 crash after delta insert before release flag commit");
            if (sql.startsWith("update recharge_order_ext") && sql.contains("balance_release_authorized_at=")
                    && fault.compareAndSet("before-authorize-commit", ""))
                throw new IllegalStateException("Task17 crash after authorization SQL before commit");
            if (sql.startsWith("update recharge_order_ext") && sql.contains("balance_no_release_decided_at=")
                    && fault.compareAndSet("before-no-release-commit", ""))
                throw new IllegalStateException("No-release crash after decision SQL before commit");
            if (sql.startsWith("update recharge_order_ext") && sql.contains("balance_reserved=")
                    && fault.compareAndSet("after-flag", ""))
                throw new IllegalStateException("Task17 crash after delta and release flag SQL before commit");
            return result;
        }
    }
}

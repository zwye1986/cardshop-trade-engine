package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.cardshop.trade.engine.application.recharge.OrderPersistAggregate;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.PersistSource;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 新端真实迁移→原样旧候选Java8写入→重入核验，仅接收一次性脚本夹具。
 * 不加载旧应用配置或重新编译旧候选，参见详细设计8.2.1、9.8及集成测试记录。
 */
@EnabledIfSystemProperty(named = "production.compatibility.legacyWriterTests", matches = "true")
class ProductionLegacyWriterMysqlTest {
    @Test
    void originalJava8CandidateWritesAndRefundsWithoutBreakingScopedOwnership() throws Exception {
        verifyCandidate("verify");
    }

    @Test
    void activeXxlTasksRespectEngineOwnershipAndStillProcessLegacyOrders() throws Exception {
        verifyCandidate("xxl");
    }

    private void verifyCandidate(String stage) throws Exception {
        String schema = "compat_legacy_" + UUID.randomUUID().toString().replace("-", "");
        var admin = new JdbcTemplate(source("mysql"));
        assertThat(admin.queryForObject("select version()", String.class)).isEqualTo("8.0.36");
        admin.execute("create database " + schema + " character set utf8mb3 collate utf8mb3_general_ci");
        try {
            var source = source(schema);
            var jdbc = new JdbcTemplate(source);
            jdbc.execute("create table compat_fixture(run_id varchar(64) primary key)");
            jdbc.update("insert into compat_fixture(run_id) values(?)", schema);
            legacy(schema, "prepare");
            var historical = jdbc.queryForList("select order_Id,org_code,out_orderId,order_price,order_status,create_time,"
                    + "refund_order_price,product_type,num,account from mc_order order by id");
            assertThat(historical).hasSize(2);
            var oldLedger = jdbc.queryForList("select id,author,filename,md5sum,exectype from databasechangelog order by orderexecuted");
            migrate(source);
            assertThat(jdbc.queryForList("select order_Id,org_code,out_orderId,order_price,order_status,create_time,"
                    + "refund_order_price,product_type,num,account from mc_order order by id")).isEqualTo(historical);
            assertThat(jdbc.queryForList("select id,author,filename,md5sum,exectype from databasechangelog order by orderexecuted"))
                    .containsAll(oldLedger);

            var configuration = new MybatisConfiguration();
            for (var mapper : List.of(McOrderMapper.class, RechargeOrderExtMapper.class, McDepartRecordMapper.class,
                    RechargeDispatchOutboxMapper.class, RechargeBalanceDeltaOutboxMapper.class)) configuration.addMapper(mapper);
            var factory = new MybatisSqlSessionFactoryBean();
            factory.setDataSource(source);
            factory.setConfiguration(configuration);
            var session = new SqlSessionTemplate(factory.getObject());
            var adapter = new MybatisRechargeOrderPersistenceAdapter(session.getMapper(McOrderMapper.class),
                    session.getMapper(RechargeOrderExtMapper.class), session.getMapper(RechargeDispatchOutboxMapper.class),
                    session.getMapper(RechargeBalanceDeltaOutboxMapper.class), session.getMapper(McDepartRecordMapper.class), new ObjectMapper());
            var aggregate = new OrderPersistAggregate("compat-engine-message",
                    new PersistOrderSnapshot(stage.equals("xxl") ? "A35" : "LEGACY", "fixture", "ENGINE-COMPAT", "SHARED", "PARENT", "SKU", "synthetic-account", "",
                            1, 137L, "合成商品", 1000L, 1, "codekey", stage.equals("xxl") ? "MINISO" : "OTHER", "fixture-hash", "v1", "SHA-256",
                            "ACCEPTED", "PASSED", "", "", true, true, "PERSIST_QUEUED", 7, 0, 1),
                    PersistSource.MQ_SNAPSHOT_FALLBACK, Instant.parse("2026-11-16T01:00:00Z"));
            new TransactionTemplate(new DataSourceTransactionManager(source)).executeWithoutResult(status -> adapter.persist(aggregate));
            // 新单也置失败，确保旧退款候选/最终事务的排除确实由ext归属而非成功状态造成。
            jdbc.update("update mc_order set order_status='6' where order_Id='ENGINE-COMPAT'");
            var financialFacts = jdbc.queryForList("select order_id,type,`value` from mc_depart_record where order_id='ENGINE-COMPAT'");
            var dispatch = jdbc.queryForList("select event_id,status,payload,version from recharge_dispatch_outbox");
            var delta = jdbc.queryForList("select event_id,status,delta_amount,version from recharge_balance_delta_outbox");
            var ext = jdbc.queryForList("select order_id,org_code,request_hash,persisted_version,redis_order_version,balance_reserved,dispatch_status from recharge_order_ext");
            String output = legacy(schema, stage);
            assertThat(jdbc.queryForObject("select count(*) from mc_order where trade_engine_owned=1", Integer.class)).isOne();
            assertThat(jdbc.queryForObject("select count(*) from mc_order where trade_engine_owned=0 and direct_recharge_out_order_key is not null", Integer.class)).isZero();
            assertThat(jdbc.queryForList("select order_id,type,`value` from mc_depart_record where order_id='ENGINE-COMPAT'"))
                    .isEqualTo(financialFacts);
            assertThat(jdbc.queryForList("select event_id,status,payload,version from recharge_dispatch_outbox")).isEqualTo(dispatch);
            assertThat(jdbc.queryForList("select event_id,status,delta_amount,version from recharge_balance_delta_outbox")).isEqualTo(delta);
            assertThat(jdbc.queryForList("select order_id,org_code,request_hash,persisted_version,redis_order_version,balance_reserved,dispatch_status from recharge_order_ext"))
                    .isEqualTo(ext);
            var ledger = jdbc.queryForList("select id,author,filename,md5sum,exectype from databasechangelog order by orderexecuted");
            migrate(source);
            assertThat(jdbc.queryForList("select id,author,filename,md5sum,exectype from databasechangelog order by orderexecuted")).isEqualTo(ledger);
            System.out.println("LEGACY_PARENT_VERIFIED stage=" + stage + " engineMoneyExtOutboxesUnchanged=true ledgerStable=true");
            // 先核完资金/事件，再报告所有审计发现；不能因前面的已知红灯跳过后续独立证据。
            assertThat(output).doesNotContain("LEGACY_XXL_VIOLATION");
        } finally {
            admin.execute("drop database " + schema);
        }
    }

    private String legacy(String schema, String stage) throws Exception {
        Path log = Files.createTempFile(Path.of(System.getenv("COMPAT_LEGACY_WORK")), "legacy-" + stage, ".log");
        var command = new ProcessBuilder(System.getenv("COMPAT_LEGACY_JAVA"),
                "-Dlogback.configurationFile=" + System.getenv("COMPAT_LEGACY_LOGBACK"), "-cp",
                System.getenv("COMPAT_LEGACY_CLASSPATH"), "LegacyWriterCompatibilityProbe", stage);
        command.environment().put("COMPAT_LEGACY_SCHEMA", schema);
        Process process = command.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            boolean finished = process.waitFor(60, TimeUnit.SECONDS);
            if (!finished) { process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS); }
            String output = Files.readString(log);
            System.out.print(output);
            assertThat(finished).as("Java8 probe exits within timeout").isTrue();
            assertThat(process.exitValue()).as(output).isZero();
            if (stage.equals("verify")) assertThat(output).contains("LEGACY_WRITER_CASE schema-probe PASS",
                    "LEGACY_WRITER_CASE mapper-single-batch-plus-update PASS", "LEGACY_WRITER_CASE ledger PASS",
                    "LEGACY_WRITER_CASE refund PASS", "LEGACY_WRITER_CASE pricing PASS", "LEGACY_WRITER_CASE transfer-fulfillment PASS",
                    "LEGACY_WRITER_CASE admin-mutation-protection PASS");
            if (stage.equals("xxl")) assertThat(output).contains("LEGACY_XXL_CASE network-guard PASS",
                    "LEGACY_XXL_CASE updateOrderCode PASS", "LEGACY_XXL_CASE blOrderUpdate PASS",
                    "LEGACY_XXL_CASE orderRefund PASS", "LEGACY_XXL_CASE callbackByFulu PASS",
                    "LEGACY_XXL_CASE autoReissueExceptionOrders PASS", "LEGACY_XXL_CASE callback AUDIT",
                    "LEGACY_XXL_CASE unlockMovieOrder PASS", "LEGACY_XXL_CASE insertBatchOrder PASS",
                    "LEGACY_XXL_COMPLETED routes=8");
            return output;
        } finally {
            if (process.isAlive()) { process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS); }
            Files.deleteIfExists(log);
        }
    }

    private void migrate(DriverManagerDataSource source) throws Exception {
        try (var connection = source.getConnection(); var resources = new ClassLoaderResourceAccessor();
                var migration = new Liquibase("db/changelog/db.changelog-production-compatible.yaml", resources,
                        DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection)))) {
            migration.update(new Contexts(), new LabelExpression());
        }
    }

    private DriverManagerDataSource source(String schema) {
        String port = System.getenv("LEGACY_SCHEMA_TEST_PORT");
        String password = System.getenv("LEGACY_SCHEMA_TEST_PASSWORD");
        assertThat(port).matches("[0-9]{1,5}");
        assertThat(Integer.parseInt(port)).isBetween(1024, 65535).isNotEqualTo(3306);
        assertThat(password).isNotBlank();
        return new DriverManagerDataSource("jdbc:mysql://127.0.0.1:" + port + "/" + schema
                + "?allowPublicKeyRetrieval=true&useSSL=false&allowMultiQueries=true&serverTimezone=Asia/Shanghai", "root", password);
    }
}

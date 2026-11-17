package com.cardshop.trade.engine.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** 仅本机一次性MySQL的迁移回归，不读取应用配置；参见详细设计8.2.1。 */
@EnabledIfSystemProperty(named = "production.compatibility.mysqlTests", matches = "true")
class ProductionCompatibilityMigrationTest {
    private static final String ENTRY = "db/changelog/db.changelog-production-compatible.yaml";
    private String schema;
    private String password;
    private String port;

    @BeforeEach
    void createSchema() throws Exception {
        password = System.getenv("LEGACY_SCHEMA_TEST_PASSWORD");
        assertThat(password).isNotBlank();
        port = System.getenv("LEGACY_SCHEMA_TEST_PORT");
        assertThat(port).matches("[0-9]{1,5}");
        assertThat(Integer.parseInt(port)).isBetween(1024, 65535).isNotEqualTo(3306);
        schema = "compat_migration_" + UUID.randomUUID().toString().replace("-", "");
        try (var connection = connect("mysql")) {
            assertThat(query(connection, "SELECT VERSION()")).startsWith("8.0.36");
            assertThat(query(connection, "SELECT @@lower_case_table_names")).isEqualTo("1");
            assertThat(query(connection, "SELECT @@transaction_isolation")).isEqualTo("READ-COMMITTED");
            execute(connection, "CREATE DATABASE " + schema + " CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci");
        }
    }

    @AfterEach
    void dropSchema() throws Exception {
        if (schema != null) try (var connection = connect("mysql")) {
            execute(connection, "DROP DATABASE " + schema);
        }
    }

    @Test
    void originalMasterActuallyRejectsHistoricalDuplicates() throws Exception {
        oldTables();
        duplicates();
        assertThatThrownBy(() -> migrate("db/changelog/db.changelog-master.yaml"))
                .hasStackTraceContaining("0003-d2-check-direct-recharge-out-order-duplicates");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void preservesHistoricalFactsWithMissingOrProductionVariantGeneratedColumn(boolean generated) throws Exception {
        oldTables();
        duplicates();
        if (generated) sql("ALTER TABLE mc_order ADD direct_recharge_out_order_key varchar(191) GENERATED ALWAYS AS "
                + "(CASE WHEN product_type=1 AND org_code IS NOT NULL AND out_orderId IS NOT NULL AND out_orderId<>'' "
                + "THEN concat(org_code,':',out_orderId) ELSE NULL END) STORED COMMENT '生产旧注释'");
        var before = facts();
        migrate(ENTRY);
        assertThat(facts()).isEqualTo(before);
        assertThat(scalar("SELECT COUNT(*) FROM mc_order WHERE trade_engine_owned=0 AND direct_recharge_out_order_key IS NULL")).isEqualTo("6");
        // 旧仓McOrderMapper.xml#insertBachMcOrder原列清单/SQL结构，填合成参数，不加载旧端线上JAR。
        sql("INSERT INTO mc_order(org_code,create_time,order_Id,produt_code,parent_id,product_name,product_price,product_value,product_type,num,"
                + "supplier_channel,out_orderid,order_price,order_status,callback_url,account) VALUES"
                + "('OLD',NOW(),'old-again','sku','parent','fixture',101,97,1,2,'channel','split',137,'12','http://127.0.0.1/fixture','synthetic'),"
                + "('OLD',NOW(),'old-again-2','sku2','parent','fixture2',103,89,1,3,'channel','split',139,'13','http://127.0.0.1/fixture','synthetic2')");
        assertThat(scalar("SELECT trade_engine_owned FROM mc_order WHERE order_Id='old-again'")).isEqualTo("0");
        // 旧mapper#updateStatusByOrderId条件与增量退款语义，确认新增列不影响旧端按平台号写回。
        sql("UPDATE mc_order SET order_status='13',refund_order_price=refund_order_price+17 WHERE org_code='OLD' AND order_Id='old-again'");
        assertThat(scalar("SELECT CONCAT(order_status,':',refund_order_price,':',trade_engine_owned) FROM mc_order WHERE order_Id='old-again'"))
                .isEqualTo("13:17:0");
        assertThat(scalar("SELECT COUNT(*) FROM mc_order WHERE org_code='OLD' AND out_orderId='split'")).isEqualTo("4");
        var ledger = ledger();
        var ddl = scalar("SHOW CREATE TABLE mc_order", 2);
        migrate(ENTRY);
        assertThat(ledger()).isEqualTo(ledger);
        assertThat(scalar("SHOW CREATE TABLE mc_order", 2)).isEqualTo(ddl);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void upgradesPreparedSixTablesOrFullOldMasterAndKeepsOriginalLedger(boolean full) throws Exception {
        oldTables();
        if (full) migrate("db/changelog/db.changelog-master.yaml");
        else try (var connection = connect(schema); var resources = new ClassLoaderResourceAccessor()) {
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            try (var migration = new Liquibase(LegacySchemaPreparation.changelog(resources), resources, database)) {
                migration.update(new Contexts(), new LabelExpression());
            }
        }
        // 合成的其他旧端账本必须原样保留；不把现场总数或旧端checksum当作可写入的固定夹具。
        sql("INSERT INTO databasechangelog(ID,AUTHOR,FILENAME,DATEEXECUTED,ORDEREXECUTED,EXECTYPE,MD5SUM) "
                + "VALUES('legacy-other','legacy-fixture','legacy/other.xml','2020-01-01',100,'EXECUTED','9:synthetic-checksum')");
        if (!full) {
            // 组合覆盖实际部分升级分支：六表+其他旧账本+旧生成列无唯一索引+合法历史重复。
            duplicates();
            sql("ALTER TABLE mc_order ADD direct_recharge_out_order_key varchar(191) GENERATED ALWAYS AS "
                    + "(CASE WHEN product_type=1 AND org_code IS NOT NULL AND out_orderId IS NOT NULL AND out_orderId<>'' "
                    + "THEN concat(org_code,':',out_orderId) ELSE NULL END) STORED COMMENT '生产旧注释'");
        }
        sql("INSERT INTO mc_order(order_Id,org_code,out_orderId,product_type,update_time) VALUES('new-A','NEW','one',1,'2020-01-02 03:04:05')");
        sql("INSERT INTO recharge_order_ext(order_id,org_code,out_order_sn,request_hash,request_hash_version,request_hash_alg,"
                + "persist_status,persist_source,validation_status) VALUES('new-A','NEW','one','hash','v1','SHA256','PERSISTED','MQ','INVALID')");
        var before = facts();
        var ledger = ledger();
        migrate(ENTRY);
        assertThat(facts()).isEqualTo(before);
        assertThat(ledger()).containsAll(ledger);
        assertThat(scalar("SELECT trade_engine_owned FROM mc_order WHERE order_Id='new-A'")).isEqualTo("1");
        assertThatThrownBy(() -> sql("INSERT INTO mc_order(order_Id,org_code,out_orderId,product_type,trade_engine_owned) VALUES('conflict','NEW','one',1,1)"))
                .hasMessageContaining("Duplicate entry");
    }

    @Test
    void refusesEmptySchema() {
        assertThatThrownBy(() -> migrate(ENTRY)).hasStackTraceContaining("mc_order");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER TABLE mc_order ADD trade_engine_owned int NOT NULL DEFAULT 0",
            "ALTER TABLE mc_order ADD direct_recharge_out_order_key varchar(191)",
            "ALTER TABLE mc_order ADD INDEX uk_direct_recharge_out_order_key(org_code)",
            "ALTER TABLE mc_order ADD trade_engine_owned tinyint NOT NULL DEFAULT 0 COMMENT '新引擎订单归属标记：0旧系统，1新引擎'; UPDATE mc_order SET trade_engine_owned=2",
            "UPDATE recharge_order_ext SET order_id='orphan'",
            "UPDATE recharge_order_ext SET org_code='WRONG'",
            "UPDATE recharge_order_ext SET org_code='new'",
            "INSERT INTO mc_depart_record(id,order_id,type) VALUES(1,'a','1'),(2,'a','1')"})
    void refusesUnknownShapesOrIdentityAndFundConflicts(String mutation) throws Exception {
        oldTables();
        try (var connection = connect(schema); var resources = new ClassLoaderResourceAccessor()) {
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            try (var migration = new Liquibase(LegacySchemaPreparation.changelog(resources), resources, database)) {
                migration.update(new Contexts(), new LabelExpression());
            }
        }
        sql("INSERT INTO mc_order(order_Id,org_code,out_orderId,product_type) VALUES('new-A','NEW','one',1)");
        sql("INSERT INTO recharge_order_ext(order_id,org_code,out_order_sn,request_hash,request_hash_version,request_hash_alg,persist_status,persist_source,validation_status) "
                + "VALUES('new-A','NEW','one','hash','v1','SHA256','PERSISTED','MQ','VALID')");
        sql(mutation);
        String violation;
        if (mutation.contains("mc_depart_record")) violation = "0011-d7-check-mc-depart-record-trade-engine-key-duplicates";
        else if (mutation.contains("trade_engine_owned=2")) violation = "Invalid trade_engine_owned value";
        else if (mutation.contains("trade_engine_owned")) violation = "Unknown trade_engine_owned definition";
        else if (mutation.contains("ADD INDEX")) violation = "Unknown direct key index definition";
        else if (mutation.contains("direct_recharge_out_order_key")) violation = "Unknown generated direct key definition";
        else violation = "Orphan or mismatched recharge_order_ext identity";
        assertThatThrownBy(() -> migrate(ENTRY)).isInstanceOf(liquibase.exception.LiquibaseException.class)
                .hasStackTraceContaining(violation);
    }

    @Test
    void rejectsPreparedTableWhoseCouponUniqueConstraintWasLost() throws Exception {
        oldTables();
        try (var connection = connect(schema); var resources = new ClassLoaderResourceAccessor()) {
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            try (var migration = new Liquibase(LegacySchemaPreparation.changelog(resources), resources, database)) {
                migration.update(new Contexts(), new LabelExpression());
            }
        }
        sql("ALTER TABLE mc_miniso_callback_inbox DROP INDEX uk_miniso_inbox_coupon");
        assertThatThrownBy(() -> migrate(ENTRY)).hasStackTraceContaining("Unique key missing: mc_miniso_callback_inbox");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTER TABLE mc_order MODIFY trade_engine_owned tinyint NOT NULL DEFAULT 1 COMMENT '新引擎订单归属标记：0旧系统，1新引擎'",
            "ALTER TABLE mc_order DROP INDEX uk_direct_recharge_out_order_key",
            "ALTER TABLE mc_miniso_callback_inbox DROP INDEX uk_miniso_inbox_coupon",
            "ALTER TABLE recharge_order_ext DROP COLUMN balance_no_release_event_id",
            "ALTER TABLE recharge_order_ext DROP COLUMN persisted_version",
            "ALTER TABLE mc_order MODIFY direct_recharge_out_order_key varchar(191) GENERATED ALWAYS AS "
                    + "(CASE WHEN trade_engine_owned=1 AND product_type=1 AND org_code IS NOT NULL AND org_code<>'' "
                    + "AND out_orderId IS NOT NULL AND out_orderId<>'' THEN concat(org_code,': ',out_orderId) ELSE NULL END) "
                    + "STORED COMMENT '新引擎直充订单幂等唯一键'",
            "ALTER TABLE mc_order MODIFY direct_recharge_out_order_key varchar(191) GENERATED ALWAYS AS "
                    + "(CASE WHEN trade_engine_owned=1 AND product_type=1 AND org_code IS NOT NULL AND org_code<>'' "
                    + "AND out_orderId IS NOT NULL AND out_orderId<>'' THEN concat(org_code,'(:)',out_orderId) ELSE NULL END) "
                    + "STORED COMMENT '新引擎直充订单幂等唯一键'",
            "INSERT INTO mc_order(order_Id,org_code,out_orderId,product_type,trade_engine_owned) VALUES('orphan','NEW','one',1,1)"})
    void rechecksFullyRecordedTargetAndRejectsSubsequentDrift(String mutation) throws Exception {
        oldTables();
        migrate(ENTRY);
        var ledger = ledger();
        sql(mutation);
        assertThatThrownBy(() -> migrate(ENTRY)).hasStackTraceContaining("Preconditions Failed");
        assertThat(ledger()).isEqualTo(ledger);
    }

    @Test
    void reentersWhenSuccessfulCompatibilityDdlWasNotRecorded() throws Exception {
        oldTables();
        duplicates();
        migrate(ENTRY);
        var before = facts();
        sql("DELETE FROM databasechangelog WHERE ID LIKE '0026-compat-%'");
        migrate(ENTRY);
        assertThat(facts()).isEqualTo(before);
    }

    @Test
    void sqlPreviewRefusesDynamicMigrationWithoutMutatingExistingOwnership() throws Exception {
        oldTables();
        try (var connection = connect(schema); var resources = new ClassLoaderResourceAccessor()) {
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            try (var migration = new Liquibase(LegacySchemaPreparation.changelog(resources), resources, database)) {
                migration.update(new Contexts(), new LabelExpression());
            }
        }
        sql("ALTER TABLE mc_order ADD trade_engine_owned tinyint NOT NULL DEFAULT 0 COMMENT '新引擎订单归属标记：0旧系统，1新引擎'");
        sql("INSERT INTO mc_order(order_Id,org_code,out_orderId,product_type) VALUES('existing','NEW','one',1)");
        sql("INSERT INTO recharge_order_ext(order_id,org_code,out_order_sn,request_hash,request_hash_version,request_hash_alg,persist_status,persist_source,validation_status) "
                + "VALUES('existing','NEW','one','hash','v1','SHA256','PERSISTED','MQ','VALID')");
        var before = scalar("SHOW CREATE TABLE mc_order", 2);
        assertThatThrownBy(() -> {
            try (var connection = connect(schema); var resources = new ClassLoaderResourceAccessor()) {
                var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
                try (var migration = new Liquibase(ENTRY, resources, database)) {
                    migration.update(new Contexts(), new LabelExpression(), new java.io.StringWriter());
                }
            }
        }).hasStackTraceContaining("SQL preview is not supported");
        assertThat(scalar("SELECT trade_engine_owned FROM mc_order WHERE order_Id='existing'")).isEqualTo("0");
        assertThat(scalar("SHOW CREATE TABLE mc_order", 2)).isEqualTo(before);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {1, 2, 3, 4, 5, 6})
    void resumesAfterEachSuccessfulPreludeStep(int steps) throws Exception {
        oldTables();
        duplicates();
        var before = facts();
        try (var connection = connect(schema); var resources = new ClassLoaderResourceAccessor()) {
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            try (var migration = new Liquibase(ENTRY, resources, database)) {
                migration.update(steps, new Contexts(), new LabelExpression());
            }
        }
        migrate(ENTRY);
        assertThat(facts()).isEqualTo(before);
    }

    @Test
    void rejectsOwnedWithoutExtBeforeChangingGeneratedColumn() throws Exception {
        oldTables();
        sql("ALTER TABLE mc_order ADD trade_engine_owned tinyint NOT NULL DEFAULT 0 COMMENT '新引擎订单归属标记：0旧系统，1新引擎'");
        sql("INSERT INTO mc_order(order_Id,org_code,out_orderId,product_type,trade_engine_owned) VALUES('a','NEW','one',1,1),('b','NEW','one',1,1)");
        assertThatThrownBy(() -> migrate(ENTRY)).hasStackTraceContaining("Owned order has no recharge_order_ext");
        assertThat(scalar("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='mc_order' "
                + "AND COLUMN_NAME='direct_recharge_out_order_key'")).isEqualTo("0");
    }

    @Test
    void refusesOwnedDuplicatesUnderTargetCollationEvenWhenExtBinaryKeysDiffer() throws Exception {
        oldTables();
        try (var connection = connect(schema); var resources = new ClassLoaderResourceAccessor()) {
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            try (var migration = new Liquibase(LegacySchemaPreparation.changelog(resources), resources, database)) {
                migration.update(new Contexts(), new LabelExpression());
            }
        }
        sql("ALTER TABLE recharge_order_ext MODIFY out_order_sn varchar(100) COLLATE utf8mb4_bin NOT NULL");
        sql("INSERT INTO mc_order(order_Id,org_code,out_orderId,product_type) VALUES('a','NEW','one',1),('b','NEW','ONE',1)");
        sql("INSERT INTO recharge_order_ext(order_id,org_code,out_order_sn,request_hash,request_hash_version,request_hash_alg,persist_status,persist_source,validation_status) "
                + "VALUES('a','NEW','one','h','v1','SHA256','PERSISTED','MQ','VALID'),('b','NEW','ONE','h','v1','SHA256','PERSISTED','MQ','VALID')");
        var before = facts();
        assertThatThrownBy(() -> migrate(ENTRY)).hasStackTraceContaining("Duplicate owned direct identity");
        // 回填分批提交，后续重复守卫HALT不意味着回滚已提交的归属；业务时间和订单事实仍须不变。
        assertThat(scalar("SELECT COUNT(*) FROM mc_order WHERE trade_engine_owned=1")).isEqualTo("2");
        assertThat(facts()).isEqualTo(before);
        assertThat(scalar("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='mc_order' "
                + "AND COLUMN_NAME='direct_recharge_out_order_key'")).isEqualTo("0");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void permitsExistingColonProtocolButRefusesActualTupleCollision(boolean collision) throws Exception {
        oldTables();
        try (var connection = connect(schema); var resources = new ClassLoaderResourceAccessor()) {
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            try (var migration = new Liquibase(LegacySchemaPreparation.changelog(resources), resources, database)) {
                migration.update(new Contexts(), new LabelExpression());
            }
        }
        sql("INSERT INTO mc_order(order_Id,org_code,out_orderId,product_type) VALUES('a','A','B:C',1)");
        if (collision) sql("INSERT INTO mc_order(order_Id,org_code,out_orderId,product_type) VALUES('b','A:B','C',1)");
        sql("INSERT INTO recharge_order_ext(order_id,org_code,out_order_sn,request_hash,request_hash_version,request_hash_alg,persist_status,persist_source,validation_status) "
                + "SELECT order_Id,org_code,out_orderId,'hash','v1','SHA256','PERSISTED','MQ','VALID' FROM mc_order");
        if (collision) {
            assertThatThrownBy(() -> migrate(ENTRY)).hasStackTraceContaining("Duplicate owned direct identity");
        } else {
            migrate(ENTRY);
            assertThat(scalar("SELECT direct_recharge_out_order_key FROM mc_order")).isEqualTo("A:B:C");
            migrate(ENTRY);
        }
    }

    @Test
    void backfillsAcrossPrimaryKeyBatchesAndPreservesAutomaticBusinessTimestamps() throws Exception {
        oldTables();
        try (var connection = connect(schema); var resources = new ClassLoaderResourceAccessor()) {
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            try (var migration = new Liquibase(LegacySchemaPreparation.changelog(resources), resources, database)) {
                migration.update(new Contexts(), new LabelExpression());
            }
        }
        sql("INSERT INTO mc_order(order_Id,org_code,out_orderId,product_type,update_time,edit_time,end_time,success_time) "
                + "SELECT CONCAT('n',n),'NEW',CONCAT('out',n),1,'2020-01-02 03:04:05','2020-01-03','2020-01-04','2020-01-05' "
                + "FROM (SELECT a.n+10*b.n+100*c.n+1000*d.n AS n FROM "
                + "(SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a "
                + "CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b "
                + "CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c "
                + "CROSS JOIN (SELECT 0 n UNION ALL SELECT 1) d) numbers WHERE n<1003");
        sql("INSERT INTO recharge_order_ext(order_id,org_code,out_order_sn,request_hash,request_hash_version,request_hash_alg,persist_status,persist_source,validation_status) "
                + "SELECT order_Id,org_code,out_orderId,'hash','v1','SHA256','PERSISTED','MQ','VALID' FROM mc_order");
        var before = facts();
        migrate(ENTRY);
        assertThat(facts()).isEqualTo(before);
        assertThat(scalar("SELECT COUNT(*) FROM mc_order WHERE trade_engine_owned=1 AND direct_recharge_out_order_key IS NOT NULL")).isEqualTo("1003");
    }

    @Test
    void observesMetadataLockAndReportsSmallFixtureDdlTimingAndSpace() throws Exception {
        oldTables();
        duplicates();
        try (var connection = connect(schema); var resources = new ClassLoaderResourceAccessor()) {
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            try (var migration = new Liquibase(ENTRY, resources, database)) {
                migration.update(4, new Contexts(), new LabelExpression());
            }
        }
        long beforeBytes = Long.parseLong(scalar("SELECT DATA_LENGTH+INDEX_LENGTH FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='mc_order'"));
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var reader = connect(schema)) {
            reader.setAutoCommit(false);
            query(reader, "SELECT COUNT(*) FROM mc_order");
            long startNanos = System.nanoTime();
            var migration = executor.submit(() -> { migrate(ENTRY); return null; });
            try {
                org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10)).until(() ->
                        !scalar("SELECT COUNT(*) FROM performance_schema.metadata_locks WHERE OBJECT_SCHEMA=DATABASE() "
                                + "AND OBJECT_NAME='mc_order' AND LOCK_STATUS='PENDING'").equals("0"));
                assertThat(migration.isDone()).isFalse();
            } finally {
                reader.rollback();
            }
            migration.get(10, java.util.concurrent.TimeUnit.SECONDS);
            long elapsedMs = java.time.Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
            String afterBytes = scalar("SELECT DATA_LENGTH+INDEX_LENGTH FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='mc_order'");
            System.out.println("COMPAT_DDL_OBSERVATION rows=6 mdlPending=true elapsedMs=" + elapsedMs
                    + " allocatedBytesBefore=" + beforeBytes + " allocatedBytesAfter=" + afterBytes);
        } finally {
            executor.shutdownNow();
        }
    }

    private void oldTables() throws Exception {
        try (var resource = getClass().getResourceAsStream("/db/fixture/production-compatibility-legacy.sql")) {
            assertThat(resource).isNotNull();
            sql(new String(resource.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    private void duplicates() throws Exception {
        sql("INSERT INTO mc_order(order_Id,org_code,out_orderId,product_type,order_status) VALUES"
                + "('split-A','OLD','split',1,'12'),('split-B','OLD','split',1,'13'),"
                + "('retry-A','OLD','retry',1,'12'),('retry-B','OLD','retry',1,'13'),"
                + "('unknown-A','OLD','unknown',1,'12'),('unknown-B','OLD','unknown',1,'13')");
        // 非对称金额/退款/数量及履约内容，避免全NULL/相同值掩盖误更新或串行。
        sql("UPDATE mc_order SET order_price=id*137,refund_order_price=id*11,num=id+1,product_price=id*101,"
                + "product_value=id*97,produt_code=CONCAT('sku-',id),parent_id=CONCAT('parent-',id),"
                + "supplier_channel=CONCAT('channel-',id),flow_id=id*17,account=CONCAT('synthetic-',id),"
                + "data=CONCAT('payload-',id),data_status=CONCAT('state-',id),res_data=CONCAT('result-',id),"
                + "remark=CONCAT('remark-',id),out_order_no=CONCAT('supplier-',id),coupon_type='codekey',order_type='fixture',"
                + "callback_url='http://127.0.0.1/fixture',request_ip='127.0.0.1',product_name=CONCAT('product-',id),"
                + "edit_time='2020-01-03',end_time='2020-01-04',success_time='2020-01-05',update_time='2020-01-02 03:04:05'");
    }

    private void migrate(String path) throws Exception {
        try (var connection = connect(schema); var resources = new ClassLoaderResourceAccessor()) {
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            try (var migration = new Liquibase(path, resources, database)) {
                migration.update(new Contexts(), new LabelExpression());
            }
        }
    }

    private List<String> facts() throws Exception {
        // JSON_ARRAY保留NULL与空串差异及字段边界，仅排除本次有意改变的归属和生成列。
        return rows("SELECT JSON_ARRAY(id,order_Id,org_code,out_orderId,product_type,order_status,num,produt_code,"
                + "supplier_channel,account,data,data_status,remark,flow_id,create_time,edit_time,end_time,success_time,update_time,"
                + "product_name,product_price,product_value,out_order_no,order_price,refund_order_price,callback_url,request_ip,"
                + "res_data,parent_id,coupon_type,order_type) FROM mc_order ORDER BY order_Id");
    }

    private List<String> ledger() throws Exception {
        return rows("SELECT CONCAT_WS('|',ID,AUTHOR,FILENAME,MD5SUM,EXECTYPE) FROM databasechangelog ORDER BY ORDEREXECUTED");
    }

    private List<String> rows(String sql) throws Exception {
        try (var connection = connect(schema); var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            var values = new ArrayList<String>();
            while (result.next()) values.add(result.getString(1));
            return values;
        }
    }

    private String scalar(String sql) throws Exception { return scalar(sql, 1); }
    private String scalar(String sql, int column) throws Exception {
        try (var connection = connect(schema)) { return query(connection, sql, column); }
    }
    private static String query(Connection connection, String sql) throws Exception { return query(connection, sql, 1); }
    private static String query(Connection connection, String sql, int column) throws Exception {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertThat(result.next()).isTrue();
            return result.getString(column);
        }
    }
    private void sql(String sql) throws Exception { try (var connection = connect(schema)) { execute(connection, sql); } }
    private static void execute(Connection connection, String sql) throws Exception {
        try (var statement = connection.createStatement()) { statement.execute(sql); }
    }
    private Connection connect(String target) throws Exception {
        return DriverManager.getConnection("jdbc:mysql://127.0.0.1:" + port + "/" + target
                + "?allowPublicKeyRetrieval=true&useSSL=false&allowMultiQueries=true&serverTimezone=Asia/Shanghai", "root", password);
    }
}

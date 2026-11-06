package com.cardshop.trade.engine.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.UUID;
import liquibase.ChecksumVersion;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.changelog.ChangeLogParameters;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** 旧端前置入口只能执行原六组结构，不能复制或改写历史变更；参见详细设计9.8。 */
class LegacySchemaPreparationTest {
    @Test
    void selectsOnlyRequiredOriginalChangesAndPreservesTheirIdentityAndChecksums() throws Exception {
        try (var resources = new ClassLoaderResourceAccessor()) {
            var original = ChangeLogParserFactory.getInstance()
                    .getParser("db/changelog/db.changelog-master.yaml", resources)
                    .parse("db/changelog/db.changelog-master.yaml", new ChangeLogParameters(), resources);
            var selected = LegacySchemaPreparation.changelog(resources);
            assertThat(selected.getChangeSets()).extracting(change -> change.getId()).containsExactly(
                    "0005-d2-create-recharge-order-ext",
                    "0013-d9-add-recharge-order-ext-dispatch-status",
                    "0020-miniso-fulfillment-receipts",
                    "0021-miniso-writeoff-events",
                    "0022-bocom-voucher-state",
                    "0023-miniso-shared-callback-bridge");
            for (var change : selected.getChangeSets()) {
                var source = original.getChangeSets().stream()
                        .filter(candidate -> candidate.getId().equals(change.getId())).findFirst().orElseThrow();
                assertThat(List.of(change.getId(), change.getAuthor(), change.getFilePath(),
                        change.generateCheckSum(ChecksumVersion.latest()).toString()))
                        .containsExactly(source.getId(), source.getAuthor(), source.getFilePath(),
                                source.generateCheckSum(ChecksumVersion.latest()).toString());
            }
        }
    }

    @Test
    void previewNeedsNoCredentialsAndDoesNotIncludeOldTableAlterations() throws Exception {
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                "-cp", System.getProperty("surefire.test.class.path"), LegacySchemaPreparation.class.getName(), "preview")
                .redirectErrorStream(true);
        process.environment().remove("LEGACY_SCHEMA_JDBC_URL");
        process.environment().remove("LEGACY_SCHEMA_USER");
        process.environment().remove("LEGACY_SCHEMA_PASSWORD");
        var child = process.start();
        String output = new String(child.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        assertThat(child.waitFor()).as(output).isZero();
        assertThat(output.lines().filter(line -> line.startsWith("-- db/changelog/")).count()).isEqualTo(6);
        assertThat(output.toLowerCase()).contains("create table mc_miniso_coupon_guard")
                .doesNotContain("alter table mc_order", "alter table mc_depart_record", "mark_ran", "recharge_outbox");
    }

    /** 只允许显式启用、本机随机端口及本测试创建的schema，绝不读取Nacos/生产连接参数。 */
    @Nested
    @EnabledIfSystemProperty(named = "legacy.schema.mysqlTests", matches = "true")
    class Mysql {
        private String schema;
        private String port;
        private String password;

        @BeforeEach
        void createDisposableSchema() throws Exception {
            port = System.getenv("LEGACY_SCHEMA_TEST_PORT");
            password = System.getenv("LEGACY_SCHEMA_TEST_PASSWORD");
            assertThat(port).matches("[0-9]{1,5}");
            assertThat(Integer.parseInt(port)).isBetween(1024, 65535).isNotEqualTo(3306);
            assertThat(password).isNotBlank();
            schema = "legacy_schema_it_" + UUID.randomUUID().toString().replace("-", "");
            try (var connection = DriverManager.getConnection(url("mysql"), "root", password)) {
                assertThat(query(connection, "SELECT VERSION()")).startsWith("8.0.28");
                assertThat(query(connection, "SELECT @@lower_case_table_names")).isEqualTo("1");
                connection.createStatement().execute("CREATE DATABASE " + schema + " CHARACTER SET utf8 COLLATE utf8_general_ci");
            }
        }

        @Test
        void preparesWithLegalDuplicatesAndRepeatsWithoutChangingOldFactsOrLedger() throws Exception {
            try (var connection = connect()) {
                oldTables(connection, true);
                String before = query(connection, "SELECT GROUP_CONCAT(CONCAT(order_Id,':',order_status) ORDER BY order_Id) FROM mc_order");
                String ddl = query(connection, "SHOW CREATE TABLE mc_order", 2);
                assertThat(run("apply", schema, 0)).contains("APPLIED:");
                assertThat(query(connection, "SHOW CREATE TABLE mc_order", 2)).isEqualTo(ddl);
                assertThat(query(connection, "SELECT GROUP_CONCAT(CONCAT(order_Id,':',order_status) ORDER BY order_Id) FROM mc_order")).isEqualTo(before);
                assertThat(query(connection, "SELECT SUM(value) FROM mc_depart_record")).isEqualTo("147");
                assertThat(query(connection, "SELECT COUNT(*) FROM databasechangelog")).isEqualTo("6");
                for (String table : LegacySchemaPreparation.TABLES) {
                    assertThat(query(connection, "SELECT COUNT(*) FROM " + table)).isEqualTo("0");
                }
                String ledger = query(connection, "SELECT GROUP_CONCAT(CONCAT(ID,':',MD5SUM,':',EXECTYPE) ORDER BY ID) FROM databasechangelog");
                assertThat(run("apply", schema, 0)).contains("NOOP:");
                assertThat(run("verify", schema, 0)).contains("VERIFIED:");
                assertThat(query(connection, "SELECT GROUP_CONCAT(CONCAT(ID,':',MD5SUM,':',EXECTYPE) ORDER BY ID) FROM databasechangelog")).isEqualTo(ledger);
                try (var resources = new ClassLoaderResourceAccessor()) {
                    var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
                    try (var full = new Liquibase("db/changelog/db.changelog-master.yaml", resources, database)) {
                        full.validate();
                        assertThatThrownBy(() -> full.update(new Contexts(), new LabelExpression()))
                                .hasStackTraceContaining("0003-d2-check-direct-recharge-out-order-duplicates");
                        assertThat(query(connection, "SELECT COUNT(*) FROM databasechangelog WHERE ID LIKE '0003%' OR ID LIKE '0004%'")).isEqualTo("0");
                    }
                }
            }
        }

        @Test
        void laterFullMasterRecognizesPreparedChangesWithoutChecksumConflicts() throws Exception {
            try (var connection = connect(); var resources = new ClassLoaderResourceAccessor()) {
                oldTables(connection, false);
                run("apply", schema, 0);
                var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
                try (var full = new Liquibase("db/changelog/db.changelog-master.yaml", resources, database)) {
                    full.validate();
                    full.update(new Contexts(), new LabelExpression());
                    assertThat(query(connection, "SELECT COUNT(*) FROM databasechangelog WHERE ID='0023-miniso-shared-callback-bridge' AND EXECTYPE='EXECUTED'")).isEqualTo("1");
                    assertThat(query(connection, "SELECT COUNT(*) FROM databasechangelog WHERE ID='0025-miniso-no-release-decision'")).isEqualTo("1");
                    assertThat(query(connection, "SELECT COUNT(*) FROM recharge_order_ext")).isEqualTo("0");
                }
            }
        }

        @Test
        void rejectsPartialSchemaAndWrongDatabaseBeforeCreatingLedger() throws Exception {
            try (var connection = connect()) {
                assertThat(run("apply", "different_schema", 1)).contains("Connected schema differs");
                connection.createStatement().execute("CREATE TABLE mc_miniso_coupon_guard(coupon_code varchar(2))");
                assertThat(run("apply", schema, 1)).contains("Partial schema/ledger");
                assertThat(query(connection, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()")).isEqualTo("1");
            }
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "ALTER TABLE mc_miniso_coupon_guard MODIFY created_at datetime NOT NULL COMMENT '创建时间'",
                "ALTER TABLE mc_miniso_callback_inbox DROP INDEX uk_miniso_inbox_coupon",
                "ALTER TABLE recharge_order_ext DROP COLUMN voucher_use_time",
                "UPDATE databasechangelog SET EXECTYPE='MARK_RAN' WHERE ID='0023-miniso-shared-callback-bridge'",
                "UPDATE databasechangelog SET MD5SUM='9:incorrect' WHERE ID='0020-miniso-fulfillment-receipts'",
                "DELETE FROM databasechangelog WHERE ID='0013-d9-add-recharge-order-ext-dispatch-status'; "
                        + "INSERT INTO databasechangelog SELECT * FROM databasechangelog WHERE ID='0005-d2-create-recharge-order-ext'"})
        void rejectsDriftRatherThanSilentlyRepairingOrMarkingRan(String mutation) throws Exception {
            run("apply", schema, 0);
            try (var connection = connect()) {
                connection.createStatement().execute(mutation);
                assertThat(run("apply", schema, 1)).contains("differs");
                assertThat(query(connection, "SELECT COUNT(*) FROM databasechangelog")).isEqualTo("6");
            }
        }

        private Connection connect() throws Exception { return DriverManager.getConnection(url(schema), "root", password); }

        private String url(String target) {
            return "jdbc:mysql://127.0.0.1:" + port + "/" + target
                    + "?allowPublicKeyRetrieval=true&useSSL=false&allowMultiQueries=true&serverTimezone=Asia/Shanghai&connectTimeout=5000&socketTimeout=15000";
        }

        private String run(String mode, String expectedSchema, int expectedExit) throws Exception {
            var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                    "-cp", System.getProperty("surefire.test.class.path"), LegacySchemaPreparation.class.getName(), mode, expectedSchema)
                    .redirectErrorStream(true);
            process.environment().put("LEGACY_SCHEMA_JDBC_URL", url(schema));
            process.environment().put("LEGACY_SCHEMA_USER", "root");
            process.environment().put("LEGACY_SCHEMA_PASSWORD", password);
            var child = process.start();
            String output = new String(child.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            assertThat(child.waitFor()).as(output).isEqualTo(expectedExit);
            return output;
        }

        private String query(Connection connection, String sql) throws Exception { return query(connection, sql, 1); }

        private String query(Connection connection, String sql, int column) throws Exception {
            try (var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
                assertThat(rows.next()).isTrue(); return rows.getString(column);
            }
        }

        private void oldTables(Connection connection, boolean duplicates) throws Exception {
            try (var sql = connection.createStatement()) {
                sql.execute("CREATE TABLE mc_order(order_Id varchar(100) PRIMARY KEY,org_code varchar(64),out_orderId varchar(100),product_type int,order_status varchar(2)) ENGINE=InnoDB");
                sql.execute("INSERT INTO mc_order VALUES('split-A','OLD','parent-7',1,'12'),('split-B','OLD','"
                        + (duplicates ? "parent-7" : "parent-8") + "',1,'13')");
                sql.execute("CREATE TABLE mc_depart_record(id bigint PRIMARY KEY,order_id varchar(100),type varchar(1),value bigint,before_balance bigint NULL,balance bigint NULL) ENGINE=InnoDB");
                sql.execute("INSERT INTO mc_depart_record VALUES(1,'split-A','1',107,500,393),(2,'split-B','1',40,393,353)");
            }
        }
    }
}

package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

/**
 * 可丢弃MySQL结构合同；普通单测不联网，显式开启后缺配置或连接失败必须报错。
 * 只通过脚本创建的回环端口、随机schema及所有权标记访问本次容器，不接受外部JDBC URL。
 * 参见 docs/交行直充业务系统详细设计.md「9.8 名创单入口、共库核销分流」。
 * 本阶段只验证真实发布DDL，后续并发测试必须调用生产mapper/adapter和Spring事务代理。
 */
@EnabledIfSystemProperty(named = "miniso.mysqlTests", matches = "true")
class MinisoBridgeMysqlContractTest {
    private static final String MASTER = "db/changelog/db.changelog-master.yaml";
    private static final String BRIDGE_CHANGE = "0023-miniso-shared-callback-bridge";
    private static final List<String> BRIDGE_TABLES = List.of(
            "mc_miniso_callback_inbox", "mc_miniso_coupon_guard", "mc_miniso_legacy_notify_task");

    @Test
    void actualMasterMigratesHistoricalOrdersWithoutAuthorizationAndSecondRunPreservesEvidence()
            throws Exception {
        String runId = requiredEnv("MINISO_MYSQL_RUN_ID");
        String schema = requiredEnv("MINISO_MYSQL_SCHEMA");
        String port = requiredEnv("MINISO_MYSQL_PORT");
        String isolation = requiredEnv("MINISO_MYSQL_ISOLATION");
        assertThat(runId).matches("[a-f0-9]{16}");
        assertThat(schema).isEqualTo("miniso_bridge_it_" + runId);
        assertThat(port).matches("[0-9]{1,5}");
        assertThat(Integer.parseInt(port)).isBetween(1, 65535);
        assertThat(isolation).isIn("READ-COMMITTED", "REPEATABLE-READ");
        var dataSource = new DriverManagerDataSource(
                "jdbc:mysql://127.0.0.1:" + port + "/" + schema
                        + "?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC"
                        + "&connectTimeout=5000&socketTimeout=60000&allowMultiQueries=true",
                "miniso_bridge_it", requiredEnv("MINISO_MYSQL_PASSWORD"));
        var jdbc = new JdbcTemplate(dataSource);
        // 任何DDL/业务写入前先核验标记；禁止把任意本地转发的共享数据库当作夹具。
        assertThat(jdbc.queryForObject("SELECT run_id FROM miniso_bridge_fixture", String.class))
                .isEqualTo(runId);
        Map<String, Object> metadata = jdbc.queryForMap(
                "SELECT VERSION() AS version, DATABASE() AS db,"
                        + " @@GLOBAL.transaction_isolation AS global_isolation,"
                        + " @@SESSION.transaction_isolation AS session_isolation");
        assertThat(metadata).containsEntry("version", "8.0.28").containsEntry("db", schema)
                .containsEntry("global_isolation", isolation).containsEntry("session_isolation", isolation);
        System.out.println("MINISO fixture metadata: " + metadata);
        assertSchemaOnlyPrivileges(jdbc, schema);

        new ResourceDatabasePopulator(new ClassPathResource("miniso-bridge/legacy-baseline.sql"))
                .execute(dataSource);
        try (Connection connection = dataSource.getConnection();
                var resources = new ClassLoaderResourceAccessor();
                var liquibase = new Liquibase(MASTER, resources,
                        DatabaseFactory.getInstance().findCorrectDatabaseImplementation(
                                new JdbcConnection(connection)))) {
            var pending = liquibase.listUnrunChangeSets(new Contexts(), new LabelExpression());
            int beforeBridge = -1;
            for (int i = 0; i < pending.size(); i++) {
                if (BRIDGE_CHANGE.equals(pending.get(i).getId())) {
                    beforeBridge = i;
                    break;
                }
            }
            assertThat(beforeBridge).as("master必须包含独立桥接变更").isPositive();
            // 不复制或改写发布DDL：按真实master顺序先升级到0022，再植入两个历史终态。
            liquibase.update(beforeBridge, new Contexts(), new LabelExpression());
            for (String status : List.of("SUCCESS", "CANCELLED")) {
                jdbc.update("""
                        INSERT INTO mc_order (order_Id, org_code, out_orderId, product_type)
                        VALUES (?, 'FIXTURE', ?, 1)
                        """, "historical-" + status, "out-" + status);
                jdbc.update("""
                        INSERT INTO recharge_order_ext
                        (order_id, org_code, out_order_sn, request_hash, request_hash_version,
                         request_hash_alg, persist_status, persist_source, validation_status,
                         balance_reserved, dispatch_status)
                        VALUES (?, 'FIXTURE', ?, 'fixture-hash', 'V1', 'SHA-256',
                                'DB_PERSISTED', 'FIXTURE', 'VALID', 1, ?)
                        """, "historical-" + status, "out-" + status, status);
            }
            liquibase.update(new Contexts(), new LabelExpression());
            assertBridgeStructure(jdbc);
            assertHistoricalAuthorizationAbsent(jdbc);
            seedBridgeEvidence(jdbc);
            var checksums = checksums(jdbc);
            assertThat(checksums).hasSize(pending.size());
            assertThat(checksums).allSatisfy(row -> {
                assertThat(row.get("EXECTYPE")).isEqualTo("EXECUTED");
                assertThat(row.get("MD5SUM")).isNotNull();
            });
            var evidence = evidence(jdbc);
            System.out.println("MINISO migration checksums: " + checksums);

            liquibase.update(new Contexts(), new LabelExpression());
            assertThat(liquibase.listUnrunChangeSets(new Contexts(), new LabelExpression())).isEmpty();
            assertThat(checksums(jdbc)).isEqualTo(checksums);
            assertThat(evidence(jdbc)).isEqualTo(evidence);
            assertHistoricalAuthorizationAbsent(jdbc);
        }
    }

    private void assertSchemaOnlyPrivileges(JdbcTemplate jdbc, String schema) {
        // MySQL数据库授权中的下划线必须转义，不能把一次性schema前缀当作通配模式。
        assertThat(jdbc.queryForList("""
                SELECT DISTINCT TABLE_SCHEMA FROM information_schema.SCHEMA_PRIVILEGES
                """, String.class)).containsExactly(schema.replace("_", "\\_"));
        assertThat(jdbc.queryForList("""
                SELECT PRIVILEGE_TYPE FROM information_schema.USER_PRIVILEGES
                WHERE PRIVILEGE_TYPE <> 'USAGE'
                """, String.class)).isEmpty();
    }

    private void assertBridgeStructure(JdbcTemplate jdbc) {
        var indexes = Map.of(
                "mc_miniso_callback_inbox", List.of(
                        "PRIMARY:0:event_id", "uk_miniso_inbox_coupon:0:coupon_code",
                        "idx_miniso_inbox_due:1:status,next_retry_time",
                        "idx_miniso_inbox_lease:1:status,locked_until",
                        "idx_miniso_inbox_order:1:order_id", "idx_miniso_inbox_org:1:org_code"),
                "mc_miniso_coupon_guard", List.of("PRIMARY:0:coupon_code"),
                "mc_miniso_legacy_notify_task", List.of(
                        "PRIMARY:0:task_id", "uk_miniso_legacy_notify_event:0:event_id",
                        "idx_miniso_legacy_notify_due:1:status,next_retry_time",
                        "idx_miniso_legacy_notify_lease:1:status,locked_until"));
        for (String table : BRIDGE_TABLES) {
            assertThat(jdbc.queryForObject("""
                    SELECT ENGINE FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
                    """, String.class, table)).isEqualTo("InnoDB");
            assertThat(jdbc.queryForList("""
                    SELECT CONCAT(INDEX_NAME, ':', NON_UNIQUE, ':',
                                  GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX))
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
                    GROUP BY INDEX_NAME, NON_UNIQUE
                    """, String.class, table)).containsExactlyInAnyOrderElementsOf(indexes.get(table));
        }
        assertThat(jdbc.queryForList("""
                SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_DEFAULT FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'recharge_order_ext'
                  AND COLUMN_NAME IN ('balance_release_authorized_at', 'balance_release_event_id',
                                      'balance_release_amount')
                """)).hasSize(3).allSatisfy(column -> {
                    assertThat(column.get("IS_NULLABLE")).isEqualTo("YES");
                    assertThat(column.get("COLUMN_DEFAULT")).isNull();
                });
        assertThat(jdbc.queryForList("""
                SELECT COLLATION_NAME FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND COLUMN_NAME = 'coupon_code'
                  AND TABLE_NAME IN ('mc_miniso_callback_inbox', 'mc_miniso_coupon_guard')
                """, String.class)).containsExactlyInAnyOrder("utf8mb4_bin", "utf8mb4_bin");
    }

    private void assertHistoricalAuthorizationAbsent(JdbcTemplate jdbc) {
        var historical = jdbc.queryForList("""
                SELECT dispatch_status, balance_reserved, balance_release_authorized_at,
                       balance_release_event_id, balance_release_amount
                FROM recharge_order_ext WHERE org_code = 'FIXTURE' ORDER BY dispatch_status
                """);
        assertThat(historical).extracting(row -> row.get("dispatch_status"))
                .containsExactly("CANCELLED", "SUCCESS");
        assertThat(historical).allSatisfy(row -> {
                    assertThat(row.get("balance_reserved")).isEqualTo(true);
                    assertThat(row.get("balance_release_authorized_at")).isNull();
                    assertThat(row.get("balance_release_event_id")).isNull();
                    assertThat(row.get("balance_release_amount")).isNull();
                });
    }

    private void seedBridgeEvidence(JdbcTemplate jdbc) {
        var coupons = List.of("Fixture-Coupon", "fixture-coupon");
        for (int i = 0; i < coupons.size(); i++) {
            String coupon = coupons.get(i);
            jdbc.update("INSERT INTO mc_miniso_coupon_guard VALUES (?, CURRENT_TIMESTAMP(3))", coupon);
            jdbc.update("""
                    INSERT INTO mc_miniso_callback_inbox
                    (event_id, coupon_code, use_time, next_retry_time, received_at, updated_at)
                    VALUES (?, ?, '2026-10-21 01:02:03', CURRENT_TIMESTAMP(3),
                            CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))
                    """, "fixture-event-" + i, coupon);
        }
        assertThatThrownBy(() -> jdbc.update("""
                INSERT INTO mc_miniso_callback_inbox
                (event_id, coupon_code, use_time, next_retry_time, received_at, updated_at)
                VALUES ('different-event', 'Fixture-Coupon', '2026-10-21 01:02:04',
                        CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))
                """)).isInstanceOf(DuplicateKeyException.class);
        jdbc.update("""
                INSERT INTO mc_miniso_legacy_notify_task
                (task_id, event_id, order_id, org_code, notify_url, payload,
                 next_retry_time, created_at, updated_at)
                VALUES ('fixture-task', 'fixture-event-0', 'fixture-old-order', 'FIXTURE',
                        'https://fixture.invalid/notify', '{"frozen":"fixture"}',
                        CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))
                """);
        assertThatThrownBy(() -> jdbc.update("""
                INSERT INTO mc_miniso_legacy_notify_task
                SELECT 'different-task', event_id, order_id, org_code, notify_url, payload,
                       status, retry_count, max_retry_count, next_retry_time, lock_owner,
                       locked_until, version, last_error, created_at, updated_at
                FROM mc_miniso_legacy_notify_task WHERE task_id = 'fixture-task'
                """)).isInstanceOf(DuplicateKeyException.class);
    }

    private List<Map<String, Object>> checksums(JdbcTemplate jdbc) {
        return jdbc.queryForList("""
                SELECT ID, AUTHOR, FILENAME, MD5SUM, EXECTYPE, DATEEXECUTED, ORDEREXECUTED
                FROM DATABASECHANGELOG ORDER BY ORDEREXECUTED
                """);
    }

    private List<List<Map<String, Object>>> evidence(JdbcTemplate jdbc) {
        return BRIDGE_TABLES.stream()
                .map(table -> jdbc.queryForList("SELECT * FROM " + table + " ORDER BY 1"))
                .toList();
    }

    private String requiredEnv(String name) {
        String value = System.getenv(name);
        assertThat(value).as("显式开启MySQL测试必须通过可丢弃环境脚本提供%s", name).isNotBlank();
        return value;
    }
}

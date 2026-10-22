package com.cardshop.trade.engine.migration;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.LiquibaseException;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.resource.DirectoryResourceAccessor;
import org.junit.jupiter.api.Test;

/**
 * D2 数据库变更契约测试。
 *
 * <p>本测试覆盖 {@code docs/交行直充业务系统详细设计.md} 的「8.2 Liquibase 约束」
 * 「9.1 派单 Outbox」「9.2 余额 Delta 刷账 Outbox」，
 * 和 {@code docs/交行直充业务系统高可用方案设计.md} 的「6.2 `mc_order` 字段说明」「6.6 新增表」。
 * 这些断言用于防止后续迁移脚本遗漏共享旧表兼容字段、幂等生成列、拆分后的派单/余额刷账 outbox 或补偿任务扫描索引。
 */
class RechargeD2ChangelogContractTest {

    @Test
    void changelogDeclaresD2TablesAndMcOrderCompatibilityFields() throws IOException {
        String changelog = Files.readString(Path.of("src/main/resources/db/changelog/db.changelog-master.yaml"));

        assertThat(changelog)
                .contains("ADD COLUMN fail_code varchar(64) DEFAULT NULL COMMENT '失败码'")
                .contains("ADD COLUMN fail_reason varchar(500) DEFAULT NULL COMMENT '失败原因'")
                .contains("ADD COLUMN direct_recharge_out_order_key varchar(191)")
                .contains("GENERATED ALWAYS AS")
                .contains("0003-d2-check-direct-recharge-out-order-duplicates")
                .contains("0004-d2-add-direct-recharge-out-order-unique-key")
                .contains("direct_recharge_out_order_key, count(*) as cnt")
                .contains("uk_direct_recharge_out_order_key")
                .contains("trade_engine_record_key varchar(191)")
                .contains("uk_mc_depart_record_trade_engine_key")
                .contains("create table if not exists recharge_order_ext")
                .contains("create table if not exists recharge_outbox")
                .contains("create table if not exists recharge_dispatch_outbox")
                .contains("create table if not exists recharge_balance_delta_outbox")
                .contains("create table if not exists recharge_notify_task")
                .contains("create table if not exists recharge_accept_anomaly");
    }

    @Test
    void changelogKeepsTechnicalStateOutOfMcOrderAndIndexesRecoveryTables() throws IOException {
        String changelog = Files.readString(Path.of("src/main/resources/db/changelog/db.changelog-master.yaml"));

        assertThat(changelog)
                .contains("unique key uk_ext_order_id (order_id)")
                .contains("unique key uk_ext_org_out_order (org_code, out_order_sn)")
                .contains("key idx_ext_persist_status (persist_status, updated_at)")
                .contains("0016-d2-drop-recharge-order-ext-persist-status-index")
                .contains("DROP INDEX idx_ext_persist_status")
                .contains("unique key uk_outbox_event_id (event_id)")
                .contains("key idx_outbox_status_retry (status, next_retry_time)")
                .contains("key idx_outbox_locked_until (status, locked_until)")
                .contains("key idx_outbox_shard (shard_key)")
                .contains("unique key uk_dispatch_outbox_event_id (event_id)")
                .contains("unique key uk_balance_delta_event_id (event_id)")
                .contains("key idx_balance_delta_lease (status, locked_until, id)")
                .contains("unique key uk_notify_task_id (task_id)")
                .contains("unique key uk_accept_anomaly_id (anomaly_id)")
                .contains("key idx_accept_anomaly_org_out (org_code, out_order_sn)");

        String balanceCreateBlock = extractCreateTableBlock(changelog, "recharge_balance_delta_outbox");
        assertThat(balanceCreateBlock)
                .contains("delta_type")
                .contains("delta_amount")
                .doesNotContain("payload")
                .doesNotContain("event_type")
                .doesNotContain("shard_key");

        assertThat(changelog)
                .doesNotContain("ADD COLUMN request_hash")
                .doesNotContain("ADD COLUMN persist_status")
                .doesNotContain("ADD COLUMN persist_msg_id")
                .doesNotContain("ADD COLUMN redis_order_version");
    }

    @Test
    void changelogCanBeParsedByLiquibase() throws IOException, LiquibaseException {
        var resourceAccessor = new DirectoryResourceAccessor(Path.of("src/main/resources"));
        DatabaseChangeLog changeLog = ChangeLogParserFactory.getInstance()
                .getParser("db/changelog/db.changelog-master.yaml", resourceAccessor)
                .parse("db/changelog/db.changelog-master.yaml", new ChangeLogParameters(), resourceAccessor);

        assertThat(changeLog.getChangeSets())
                .extracting(changeSet -> changeSet.getId())
                .contains(
                        "0001-d2-mc-order-fail-code",
                        "0002a-d2-add-direct-recharge-out-order-key-column",
                        "0003-d2-check-direct-recharge-out-order-duplicates",
                        "0004-d2-add-direct-recharge-out-order-unique-key",
                        "0005-d2-create-recharge-order-ext",
                        "0006-d2-create-recharge-outbox",
                        "0007-d2-create-recharge-notify-task",
                        "0008-d2-create-recharge-accept-anomaly",
                        "0009-d2-verify-mc-depart-record-balance-nullable",
                        "0010-d7-add-mc-depart-record-trade-engine-key-column",
                        "0011-d7-check-mc-depart-record-trade-engine-key-duplicates",
                        "0012-d7-add-mc-depart-record-trade-engine-key-unique",
                        "0013-d9-add-recharge-order-ext-dispatch-status",
                        "0014-d2-create-recharge-dispatch-outbox",
                        "0015-d2-create-recharge-balance-delta-outbox",
                        "0016-d2-drop-recharge-order-ext-persist-status-index");
    }

    /** 桥接发布合同不可用缺表跳过或破坏性回滚替代，参见详细设计9.8。 */
    @Test
    void declaresBridgeEvidenceAndNullableReleaseAuthorization() throws Exception {
        try (var resources = new DirectoryResourceAccessor(Path.of("src/main/resources"))) {
            var changelog = ChangeLogParserFactory.getInstance()
                    .getParser("db/changelog/db.changelog-master.yaml", resources)
                    .parse("db/changelog/db.changelog-master.yaml", new ChangeLogParameters(), resources);
            var bridge = changelog.getChangeSets().stream()
                    .filter(change -> change.getId().equals("0023-miniso-shared-callback-bridge"))
                    .findFirst();
            assertThat(bridge).as("已批准的桥接结构必须由独立新迁移发布").isPresent();
            var changeSet = bridge.orElseThrow();
            String sql = changeSet.getChanges().stream()
                    .map(change -> ((liquibase.change.core.RawSQLChange) change).getSql())
                    .collect(java.util.stream.Collectors.joining("\n"));
            // 分表断言同名列，防止一张表的字段误替另一张表满足合同。
            String inbox = sql.substring(0, sql.indexOf("CREATE TABLE mc_miniso_legacy_notify_task"));
            String notification = sql.substring(sql.indexOf("CREATE TABLE mc_miniso_legacy_notify_task"),
                    sql.indexOf("CREATE TABLE mc_miniso_coupon_guard"));
            String guard = sql.substring(sql.indexOf("CREATE TABLE mc_miniso_coupon_guard"),
                    sql.indexOf("ALTER TABLE recharge_order_ext"));
            assertThat(inbox)
                    .contains("CREATE TABLE mc_miniso_callback_inbox (",
                            "event_id varchar(96) NOT NULL COMMENT '核销事件编号'",
                            "coupon_code varchar(128) COLLATE utf8mb4_bin NOT NULL COMMENT '名创券码'",
                            "use_time datetime NOT NULL COMMENT '核销时间'",
                            "status varchar(32) NOT NULL DEFAULT 'INIT' COMMENT '接收事件状态'",
                            "route_target varchar(16) NULL COMMENT '可信处理归属'",
                            "order_id varchar(100) COLLATE utf8mb4_bin NULL COMMENT '可信平台订单号'",
                            "org_code varchar(64) NULL COMMENT '可信商户编码'",
                            "retry_count int NOT NULL DEFAULT 0 COMMENT '失败处理次数'",
                            "next_retry_time datetime(3) NOT NULL COMMENT '下次处理时间'",
                            "version bigint NOT NULL DEFAULT 1 COMMENT '事件版本'",
                            "lock_owner varchar(128) NULL COMMENT '任务租约持有者'",
                            "locked_until datetime(3) NULL COMMENT '任务租约截止时间'",
                            "last_error varchar(500) NULL COMMENT '脱敏失败摘要'",
                            "received_at datetime(3) NOT NULL COMMENT '首次接收时间'",
                            "routed_at datetime(3) NULL COMMENT '归属处理完成时间'",
                            "updated_at datetime(3) NOT NULL COMMENT '更新时间'",
                            "PRIMARY KEY (event_id)", "UNIQUE KEY uk_miniso_inbox_coupon (coupon_code)",
                            "KEY idx_miniso_inbox_due (status, next_retry_time)",
                            "KEY idx_miniso_inbox_lease (status, locked_until)",
                            "KEY idx_miniso_inbox_order (order_id)", "KEY idx_miniso_inbox_org (org_code)",
                            "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='名创共享核销接收表'");
            assertThat(notification)
                    .contains("CREATE TABLE mc_miniso_legacy_notify_task (",
                            "task_id varchar(96) NOT NULL COMMENT '任务编号'",
                            "event_id varchar(96) NOT NULL COMMENT '核销事件编号'",
                            "order_id varchar(100) COLLATE utf8mb4_bin NOT NULL COMMENT '平台订单号'",
                            "org_code varchar(64) NOT NULL COMMENT '商户编码'",
                            "notify_url varchar(1024) NOT NULL COMMENT '核销通知地址'",
                            "payload longtext NOT NULL COMMENT '冻结通知报文'",
                            "status varchar(32) NOT NULL DEFAULT 'INIT' COMMENT '通知任务状态'",
                            "retry_count int NOT NULL DEFAULT 0 COMMENT '失败处理次数'",
                            "max_retry_count int NOT NULL DEFAULT 10 COMMENT '最大失败处理次数'",
                            "next_retry_time datetime(3) NOT NULL COMMENT '下次发送时间'",
                            "version bigint NOT NULL DEFAULT 1 COMMENT '任务版本'",
                            "lock_owner varchar(128) NULL COMMENT '任务租约持有者'",
                            "locked_until datetime(3) NULL COMMENT '任务租约截止时间'",
                            "last_error varchar(500) NULL COMMENT '脱敏失败摘要'",
                            "created_at datetime(3) NOT NULL COMMENT '创建时间'",
                            "updated_at datetime(3) NOT NULL COMMENT '更新时间'",
                            "PRIMARY KEY (task_id)", "UNIQUE KEY uk_miniso_legacy_notify_event (event_id)",
                            "KEY idx_miniso_legacy_notify_due (status, next_retry_time)",
                            "KEY idx_miniso_legacy_notify_lease (status, locked_until)",
                            "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='名创旧核销通知任务表'");
            assertThat(guard).contains("CREATE TABLE mc_miniso_coupon_guard (", "PRIMARY KEY (coupon_code)",
                    "coupon_code varchar(128) COLLATE utf8mb4_bin NOT NULL COMMENT '名创券码'",
                    "created_at datetime(3) NOT NULL COMMENT '创建时间'",
                    "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='名创券码事务互斥表'");
            assertThat(sql)
                    .contains("ALTER TABLE recharge_order_ext",
                            "ADD COLUMN balance_release_authorized_at datetime(3) NULL COMMENT '余额释放授权时间'",
                            "ADD COLUMN balance_release_event_id varchar(100) NULL COMMENT '余额释放授权事件编号'",
                            "ADD COLUMN balance_release_amount bigint NULL COMMENT '余额释放授权金额，单位分'")
                    .doesNotContain("IF NOT EXISTS", "UPDATE recharge_order_ext", "CURRENT_TIMESTAMP");
            // 已有同名但不兼容的结构必须失败，不能MARK_RAN后运行缺少保护的代码。
            assertThat(changeSet.getPreconditions().getOnFail().toString()).isEqualTo("HALT");
            assertThat(changeSet.getRollback().getChanges()).singleElement()
                    .satisfies(change -> assertThat(((liquibase.change.core.RawSQLChange) change).getSql())
                            .isEqualTo("SELECT 1"));
        }
    }

    /**
     * 截取 changelog 中指定表的 create table 块，便于断言余额刷账表不含通用 outbox 字段。
     *
     * <p>对应 {@code docs/交行直充业务系统详细设计.md}「8.2 Liquibase 约束」「9.2 余额 Delta 刷账 Outbox」。
     */
    private static String extractCreateTableBlock(String changelog, String tableName) {
        String marker = "create table if not exists " + tableName;
        int start = changelog.indexOf(marker);
        assertThat(start).as("missing create table for %s", tableName).isGreaterThanOrEqualTo(0);
        int end = changelog.indexOf(") engine=", start);
        assertThat(end).as("incomplete create table for %s", tableName).isGreaterThan(start);
        return changelog.substring(start, end);
    }
}

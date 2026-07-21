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
                        "0015-d2-create-recharge-balance-delta-outbox");
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

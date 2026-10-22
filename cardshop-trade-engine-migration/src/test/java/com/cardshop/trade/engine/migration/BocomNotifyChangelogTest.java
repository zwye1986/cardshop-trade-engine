package com.cardshop.trade.engine.migration;

import static org.assertj.core.api.Assertions.*;
import java.nio.file.*;
import liquibase.changelog.ChangeLogParameters;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.resource.DirectoryResourceAccessor;
import org.junit.jupiter.api.Test;

/** 新增迁移不得改历史changeset或删除可靠证据；参见详细设计2104实施细化。 */
class BocomNotifyChangelogTest {
    @Test void liquibaseAppliesNewColumnsAndPreservesHistoricalDirectTasks() throws Exception {
        try(var connection=java.sql.DriverManager.getConnection("jdbc:h2:mem:bocom-migration;MODE=MySQL;DATABASE_TO_LOWER=TRUE")) {
            connection.createStatement().execute("create table recharge_notify_task(task_id varchar(100) primary key,notify_url varchar(500) not null)");
            connection.createStatement().execute("insert into recharge_notify_task values('historical','https://merchant.invalid')");
            var database=liquibase.database.DatabaseFactory.getInstance().findCorrectDatabaseImplementation(
                    new liquibase.database.jvm.JdbcConnection(connection));
            try(var migration=new liquibase.Liquibase("db/changelog/0024-bocom-notify.yaml",
                    new DirectoryResourceAccessor(Path.of("src/main/resources")),database)) {
                migration.update(new liquibase.Contexts(),new liquibase.LabelExpression());
                var result=connection.createStatement().executeQuery("select delivery_channel,claim_version from recharge_notify_task");
                assertThat(result.next()).isTrue();assertThat(result.getString(1)).isEqualTo("DIRECT_JSON");
                assertThat(result.getLong(2)).isZero();
                connection.createStatement().execute("insert into recharge_notify_attempt(task_id,claim_version,msg_id,started_at,status) values('historical',1,'msg1',CURRENT_TIMESTAMP,'IN_FLIGHT')");
                assertThatThrownBy(()->connection.createStatement().execute("insert into recharge_notify_attempt(task_id,claim_version,msg_id,started_at,status) values('historical',2,'msg1',CURRENT_TIMESTAMP,'IN_FLIGHT')"))
                        .isInstanceOf(java.sql.SQLException.class);
            }
        }
    }
    @Test void appendsFrozenRouteLeaseAndAttemptSchemaAfterOriginalTaskTable() throws Exception {
        var root=Path.of("src/main/resources");
        var parser=ChangeLogParserFactory.getInstance().getParser("db/changelog/db.changelog-master.yaml",new DirectoryResourceAccessor(root));
        var changelog=parser.parse("db/changelog/db.changelog-master.yaml",new ChangeLogParameters(),new DirectoryResourceAccessor(root));
        assertThat(changelog.getChangeSets()).extracting(change -> change.getId())
                .containsSubsequence("0023-miniso-shared-callback-bridge", "0024-bocom-notify-route-and-attempt",
                        "0025-miniso-no-release-decision");
        String sql=Files.readString(root.resolve("db/changelog/0024-bocom-notify.yaml"));
        assertThat(sql).contains("DEFAULT 'DIRECT_JSON'", "claim_version bigint NOT NULL DEFAULT 0", "encoded_payload text CHARACTER SET utf8mb4",
                "CREATE TABLE recharge_notify_attempt", "UNIQUE KEY uk_notify_attempt_msg (msg_id)","idx_notify_attempt_task (task_id, started_at)");
        assertThat(sql).doesNotContain("DROP TABLE", "DELETE FROM");
    }
}

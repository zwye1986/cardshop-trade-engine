package com.cardshop.trade.engine.infrastructure.maintenance;

import static org.assertj.core.api.Assertions.*;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;

/** 历史卡密兼容数据修复的事务验收；参见详细设计「名创卡密导出兼容修复（2026-10-21）」。 */
class MinisoLegacyOrderDataRepairTest {
    @Test void dryRunDoesNotWriteAndApplyIsIdempotent() throws Exception {
        try (var f = fixture()) {
            assertThat(f.repair(false).repaired()).isEqualTo(2);
            assertThat(f.jdbc.queryForObject("select count(*) from mc_order where data is not null", Integer.class)).isZero();
            assertThat(f.repair(true).repaired()).isEqualTo(2);
            assertThat(f.jdbc.queryForObject("select count(*) from mc_order where data is not null", Integer.class)).isEqualTo(2);
            var rows = new com.fasterxml.jackson.databind.ObjectMapper().readTree(
                    f.jdbc.queryForObject("select data from mc_order where order_Id='O1'", String.class));
            assertThat(rows.size()).isOne();
            assertThat(rows.get(0).path("couponCode").asText()).isEqualTo("TEST-O1");
            assertThat(rows.get(0).path("couponKey").asText()).isEqualTo("TEST-O1");
            assertThat(rows.get(0).path("validTime").asText()).isEqualTo("2026-12-31 23:59:59");
            assertThat(f.repair(true).alreadyPresent()).isEqualTo(2);
            assertThat(f.jdbc.queryForObject("select sum(order_status) from mc_order", Integer.class)).isEqualTo(24);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "update mc_order set org_code='OTHER' where order_Id='O2'",
        "update mc_order set order_status=11 where order_Id='O2'",
        "update mc_order set supplier_channel='OTHER' where order_Id='O2'",
        "update mc_order set num=2 where order_Id='O2'",
        "update recharge_order_ext set out_order_sn='OTHER' where order_id='O2'",
        "update recharge_order_ext set persist_status='INIT' where order_id='O2'",
        "update recharge_order_ext set dispatch_status='PROCESSING' where order_id='O2'",
        "update recharge_order_ext set supplier_batch_id='106553' where order_id='O2'",
        "update recharge_miniso_receipt set phone='OTHER' where order_id='O2'",
        "update recharge_miniso_receipt set org_code='OTHER' where order_id='O2'",
        "delete from recharge_miniso_receipt where order_id='O2'",
        "update mc_order set data='[]' where order_Id='O2'"
    })
    void anyConflictingOrderRejectsWholeBatch(String sql) throws Exception {
        try (var f = fixture()) {
            f.jdbc.execute(sql);
            assertThatThrownBy(() -> f.repair(true)).hasMessageStartingWith("LEGACY_DATA_");
            assertThat(f.jdbc.queryForObject("select data from mc_order where order_Id='O1'", String.class)).isNull();
            assertThat(f.connection.getAutoCommit()).isTrue();
        }
    }

    @Test void secondWriteFailureRollsBackFirstOrder() throws Exception {
        try (var f = fixture()) {
            f.jdbc.execute("alter table mc_order add constraint reject_second check(order_Id<>'O2' or data is null)");
            assertThatThrownBy(() -> f.repair(true)).isInstanceOf(java.sql.SQLException.class);
            assertThat(f.jdbc.queryForObject("select count(*) from mc_order where data is not null", Integer.class)).isZero();
        }
    }

    @Test void wrongCatalogAndDuplicateScopeAreRejected() throws Exception {
        try (var f = fixture()) {
            assertThatThrownBy(() -> new MinisoLegacyOrderDataRepair().repair(f.connection, "wrong", "A35", List.of("O1"), true))
                    .hasMessage("LEGACY_DATA_DATABASE_MISMATCH");
            assertThatThrownBy(() -> new MinisoLegacyOrderDataRepair().repair(f.connection, f.connection.getCatalog(), "A35", List.of("O1", "O1"), true))
                    .hasMessage("LEGACY_DATA_SCOPE_INVALID");
        }
    }

    private Fixture fixture() throws Exception {
        var ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        var jdbc = new JdbcTemplate(ds);
        jdbc.execute("create table mc_order(order_Id varchar(100) primary key,org_code varchar(64),out_orderId varchar(100),account varchar(64),num int,supplier_channel varchar(32),order_status int,data varchar(4096))");
        jdbc.execute("create table recharge_order_ext(order_id varchar(100) primary key,org_code varchar(64),out_order_sn varchar(100),persist_status varchar(32),dispatch_status varchar(32),supplier_batch_id varchar(64))");
        jdbc.execute("create table recharge_miniso_receipt(order_id varchar(100) primary key,org_code varchar(64),batch_id int,coupon_code varchar(128),phone varchar(64),begin_time varchar(19),end_time varchar(19),coupon_status int)");
        for (String id : List.of("O1", "O2")) {
            jdbc.update("insert into mc_order values(?,'A35',?,'13800138000',1,'MINISO',12,null)", id, id);
            jdbc.update("insert into recharge_order_ext values(?,'A35',?,'DB_PERSISTED','SUCCESS','106552')", id, id);
            jdbc.update("insert into recharge_miniso_receipt values(?,'A35',106552,?,'13800138000','2026-10-20 17:51:30','2026-12-31 23:59:59',0)", id, "TEST-" + id);
        }
        return new Fixture(ds.getConnection(), jdbc);
    }

    private record Fixture(Connection connection, JdbcTemplate jdbc) implements AutoCloseable {
        MinisoLegacyOrderDataRepair.Result repair(boolean apply) throws Exception {
            return new MinisoLegacyOrderDataRepair().repair(connection, connection.getCatalog(), "A35", List.of("O2", "O1"), apply);
        }
        public void close() throws Exception { connection.close(); }
    }
}

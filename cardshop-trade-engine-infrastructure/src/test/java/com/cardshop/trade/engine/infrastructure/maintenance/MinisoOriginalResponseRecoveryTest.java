package com.cardshop.trade.engine.infrastructure.maintenance;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.util.Map;
import java.util.UUID;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;

/** 原始成功证据恢复的真实事务验收；参见详细设计「名创原始成功流水受控恢复」。 */
class MinisoOriginalResponseRecoveryTest {
    private final ObjectMapper json = new ObjectMapper();

    @Test
    void dryRunDoesNotWriteAndApplyCommitsAllFactsAndIsIdempotent() throws Exception {
        try (var f = fixture()) {
            assertThat(f.recover(false)).isEqualTo(MinisoOriginalResponseRecovery.Result.VERIFIED);
            assertUnchanged(f);
            assertThat(f.recover(true)).isEqualTo(MinisoOriginalResponseRecovery.Result.RECOVERED);
            assertThat(f.jdbc.queryForObject("select data from mc_order",String.class))
                    .contains("TEST-COUPON", "couponCode", "couponKey", "validTime");
            assertThat(f.jdbc.queryForObject("select count(*) from recharge_miniso_receipt", Integer.class)).isOne();
            assertThat(f.jdbc.queryForObject("select order_status from mc_order", Integer.class)).isEqualTo(12);
            assertThat(f.jdbc.queryForObject("select flow_id from mc_order", Long.class)).isEqualTo(10016L);
            assertThat(f.jdbc.queryForObject("select dispatch_status from recharge_order_ext", String.class)).isEqualTo("SUCCESS");
            assertThat(f.jdbc.queryForObject("select voucher_use_state from recharge_order_ext", String.class)).isEqualTo("1");
            assertThat(f.jdbc.queryForObject("select status from recharge_dispatch_outbox where event_type='SUPPLIER_QUERY'", String.class)).isEqualTo("SUCCESS");
            assertThat(f.jdbc.queryForObject("select retry_count from recharge_dispatch_outbox where event_type='SUPPLIER_QUERY'", Integer.class)).isEqualTo(6);
            assertThat(f.jdbc.queryForObject("select version from recharge_dispatch_outbox where event_type='SUPPLIER_QUERY'", Long.class)).isEqualTo(8L);
            assertThat(f.recover(true)).isEqualTo(MinisoOriginalResponseRecovery.Result.ALREADY_RECOVERED);
            assertThat(f.jdbc.queryForObject("select count(*) from recharge_miniso_receipt", Integer.class)).isOne();
            assertThat(f.jdbc.queryForObject("select delta_amount from recharge_balance_delta_outbox", Long.class)).isEqualTo(-100L);
            assertThat(f.jdbc.queryForObject("select count(*) from recharge_notify_task", Integer.class)).isZero();
        }
    }

    @Test
    void alreadyRecoveredOrderCanFillMissingLegacyDataWithoutRepeatingRecovery() throws Exception {
        try (var f = fixture()) {
            f.recover(true);
            String data = f.jdbc.queryForObject("select data from mc_order", String.class);
            f.jdbc.update("update mc_order set data=null");
            assertThat(f.recover(false)).isEqualTo(MinisoOriginalResponseRecovery.Result.ALREADY_RECOVERED);
            assertThat(f.jdbc.queryForObject("select data from mc_order", String.class)).isNull();
            assertThat(f.recover(true)).isEqualTo(MinisoOriginalResponseRecovery.Result.ALREADY_RECOVERED);
            assertThat(f.jdbc.queryForObject("select data from mc_order", String.class)).isEqualTo(data);
            assertThat(f.jdbc.queryForObject("select count(*) from recharge_miniso_receipt", Integer.class)).isOne();
            assertThat(f.jdbc.queryForObject("select version from recharge_dispatch_outbox where event_type='SUPPLIER_QUERY'", Long.class)).isEqualTo(8L);
        }
    }

    @Test
    void conflictingLegacyDataCannotBeOverwrittenByOriginalRecovery() throws Exception {
        try (var f = fixture()) {
            f.jdbc.update("update mc_order set data='[]'");
            assertThatThrownBy(() -> f.recover(true)).hasMessage("LEGACY_DATA_CONFLICT");
            assertThat(f.jdbc.queryForObject("select count(*) from recharge_miniso_receipt", Integer.class)).isZero();
            assertThat(f.jdbc.queryForObject("select order_status from mc_order", Integer.class)).isEqualTo(13);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"phone", "batchId", "couponCode", "beginTime", "endTime"})
    void incompleteOriginalResponseIsRejected(String field) throws Exception {
        try (var f = fixture()) {
            var wrapper = json.readTree(f.response);
            var raw = json.readTree(wrapper.path("raw_response").textValue());
            ((com.fasterxml.jackson.databind.node.ObjectNode) raw.path("data").get(0)).remove(field);
            ((com.fasterxml.jackson.databind.node.ObjectNode) wrapper).put("raw_response", json.writeValueAsString(raw));
            f.jdbc.update("update mc_order_supplier_flow set response_result=?", json.writeValueAsString(wrapper));
            assertThatThrownBy(() -> f.recover(true)).hasMessageStartingWith("RECOVERY_");
            assertUnchanged(f);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "update mc_order set account='other'",
        "update mc_order set order_price=101",
        "update mc_order set callback_url='https://example.invalid/notify'",
        "update mc_order set order_status=11",
        "update recharge_order_ext set org_code='OTHER'",
        "update recharge_order_ext set supplier_batch_id='106553'",
        "update recharge_order_ext set balance_reserved=false",
        "update recharge_dispatch_outbox set version=9 where event_type='SUPPLIER_QUERY'",
        "update recharge_dispatch_outbox set status='PROCESSING' where event_type='SUPPLIER_QUERY'",
        "update recharge_balance_delta_outbox set delta_type='REFUND'",
        "insert into recharge_miniso_writeoff_event values('O1','TEST-COUPON')",
        "insert into recharge_notify_task values('O1')",
        "insert into recharge_miniso_receipt values('OTHER','A35',106552,'TEST-COUPON','13800138000','2026-10-20 17:51:30','2026-12-31 23:59:59',0,null,null,null)"
    })
    void conflictingFactsCannotProduceSuccess(String sql) throws Exception {
        try (var f = fixture()) {
            f.jdbc.execute(sql);
            assertThatThrownBy(() -> f.recover(true)).hasMessageStartingWith("RECOVERY_");
            assertThat(f.jdbc.queryForObject("select count(*) from recharge_miniso_receipt where order_id='O1'", Integer.class)).isZero();
            assertThat(f.jdbc.queryForObject("select dispatch_status from recharge_order_ext", String.class)).isEqualTo("SUPPLIER_ACCEPTED");
        }
    }

    @Test
    void failureAfterReceiptAndMainOrderWriteRollsBackEntireTransaction() throws Exception {
        try (var f = fixture()) {
            f.jdbc.execute("alter table recharge_order_ext add constraint fail_success check(dispatch_status <> 'SUCCESS')");
            assertThatThrownBy(() -> f.recover(true)).isInstanceOf(java.sql.SQLException.class);
            assertUnchanged(f);
            assertThat(f.connection.getAutoCommit()).isTrue();
        }
    }

    @Test
    void wrongDatabaseAndChangedAuditMarkerFailClosed() throws Exception {
        try (var f = fixture()) {
            var wrong = new MinisoOriginalResponseRecovery.Request("A35", "O1", 10016, 7, "TEST-RECOVERY", "wrong_database");
            assertThatThrownBy(() -> new MinisoOriginalResponseRecovery(json).recover(f.connection, wrong, true))
                    .hasMessage("RECOVERY_DATABASE_MISMATCH");
            f.recover(true);
            f.jdbc.update("update recharge_dispatch_outbox set last_error='other' where event_type='SUPPLIER_QUERY'");
            assertThatThrownBy(() -> f.recover(true)).hasMessageStartingWith("RECOVERY_");
        }
    }

    @Test
    void concurrentOperatorsCommitOnlyOneRecovery() throws Exception {
        try (var f = fixture(); var second = java.sql.DriverManager.getConnection(f.connection.getMetaData().getURL());
             var workers = java.util.concurrent.Executors.newFixedThreadPool(2)) {
            var start = new java.util.concurrent.CountDownLatch(1);
            var firstResult = workers.submit(() -> { start.await(); return f.recover(true); });
            var secondResult = workers.submit(() -> {
                start.await();
                return new MinisoOriginalResponseRecovery(json).recover(second,
                        new MinisoOriginalResponseRecovery.Request("A35", "O1", 10016, 7, "TEST-RECOVERY", f.database), true);
            });
            start.countDown();
            assertThat(java.util.List.of(firstResult.get(15, java.util.concurrent.TimeUnit.SECONDS),
                    secondResult.get(15, java.util.concurrent.TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(MinisoOriginalResponseRecovery.Result.RECOVERED,
                            MinisoOriginalResponseRecovery.Result.ALREADY_RECOVERED);
            assertThat(f.jdbc.queryForObject("select count(*) from recharge_miniso_receipt", Integer.class)).isOne();
        }
    }

    @Test
    void duplicateJsonKeysCannotMasqueradeAsValidEvidence() throws Exception {
        try (var f = fixture()) {
            var wrapper = (com.fasterxml.jackson.databind.node.ObjectNode) json.readTree(f.response);
            wrapper.put("raw_response", "{\"code\":500,\"code\":200,\"data\":[]}");
            f.jdbc.update("update mc_order_supplier_flow set response_result=?", json.writeValueAsString(wrapper));
            assertThatThrownBy(() -> f.recover(true)).hasMessage("RECOVERY_EVIDENCE_JSON_INVALID");
            assertUnchanged(f);
        }
    }

    private static void assertUnchanged(Fixture f) {
        assertThat(f.jdbc.queryForObject("select count(*) from recharge_miniso_receipt", Integer.class)).isZero();
        assertThat(f.jdbc.queryForObject("select data from mc_order", String.class)).isNull();
        assertThat(f.jdbc.queryForObject("select order_status from mc_order", Integer.class)).isEqualTo(13);
        assertThat(f.jdbc.queryForObject("select dispatch_status from recharge_order_ext", String.class)).isEqualTo("SUPPLIER_ACCEPTED");
        assertThat(f.jdbc.queryForObject("select status from recharge_dispatch_outbox where event_type='SUPPLIER_QUERY'", String.class)).isEqualTo("MANUAL_REQUIRED");
    }

    private Fixture fixture() throws Exception {
        var ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        var jdbc = new JdbcTemplate(ds);
        jdbc.execute("create table mc_order(order_Id varchar(100) primary key,org_code varchar(64),out_orderId varchar(100),account varchar(64),num int,supplier_channel varchar(32),order_price bigint,parent_id varchar(64),produt_code varchar(64),callback_url varchar(1024),order_status int,data_status varchar(32),flow_id bigint,fail_code varchar(64),fail_reason varchar(512),remark varchar(512),edit_time timestamp,data varchar(4096))");
        jdbc.execute("create table recharge_order_ext(order_id varchar(100) primary key,org_code varchar(64),out_order_sn varchar(100),persist_status varchar(32),validation_status varchar(32),supplier_batch_id varchar(64),balance_reserved boolean,dispatch_status varchar(32),voucher_use_state varchar(8),voucher_use_time timestamp,voucher_invalid_time timestamp,last_error varchar(512),updated_at timestamp)");
        jdbc.execute("create table mc_order_supplier_flow(id bigint primary key,order_Id varchar(100),out_order_no varchar(100),channel varchar(32),status varchar(32),request_param clob,response_result clob)");
        jdbc.execute("create table recharge_miniso_receipt(order_id varchar(100) primary key,org_code varchar(64),batch_id int,coupon_code varchar(128) unique,phone varchar(64),begin_time varchar(19),end_time varchar(19),coupon_status int,use_time timestamp,created_at timestamp,updated_at timestamp)");
        jdbc.execute("create table recharge_miniso_writeoff_event(order_id varchar(100),coupon_code varchar(128))");
        jdbc.execute("create table recharge_dispatch_outbox(event_id varchar(100) primary key,event_type varchar(32),org_code varchar(64),order_id varchar(100),out_order_sn varchar(100),status varchar(32),retry_count int,max_retry_count int,locked_until timestamp,locked_by varchar(128),version bigint,last_error varchar(512),updated_at timestamp)");
        jdbc.execute("create table recharge_balance_delta_outbox(order_id varchar(100),org_code varchar(64),delta_type varchar(32),delta_amount bigint,status varchar(32))");
        jdbc.execute("create table recharge_notify_task(order_id varchar(100))");
        jdbc.execute("insert into mc_order values('O1','A35','OUT1','13800138000',1,'MINISO',100,'80100392','80100139','',13,'PROCESSING',10022,null,null,'',null,null)");
        jdbc.execute("insert into recharge_order_ext values('O1','A35','OUT1','DB_PERSISTED','PASSED','106552',true,'SUPPLIER_ACCEPTED','',null,null,'',null)");
        var raw = Map.of("code", 200, "data", java.util.List.of(Map.of("batchId", 106552, "couponCode", "TEST-COUPON", "phone", "13800138000", "beginTime", "2026-10-20 17:51:30", "endTime", "2026-12-31 23:59:59")));
        String response = json.writeValueAsString(Map.of("response_code", "200", "supplier_order_id", "O1", "raw_response", json.writeValueAsString(raw)));
        String request = json.writeValueAsString(Map.of("supplier_request_id", "dispatch-O1", "order_id", "O1", "org_code", "A35", "out_order_sn", "OUT1", "account", "13800138000", "supplier_channel", "MINISO", "order_price", 100, "product_id", "80100392", "selected_product_id", "80100139"));
        jdbc.update("insert into mc_order_supplier_flow values(10016,'O1','OUT1','MINISO','success',?,?)", request, response);
        jdbc.execute("insert into recharge_dispatch_outbox values('dispatch-O1','SUPPLIER_DISPATCH','A35','O1','OUT1','SUCCESS',0,3,null,'worker',1,'',null)");
        jdbc.execute("insert into recharge_dispatch_outbox values('supplier-query-O1','SUPPLIER_QUERY','A35','O1','OUT1','MANUAL_REQUIRED',6,6,null,'worker',7,'unknown',null)");
        jdbc.execute("insert into recharge_balance_delta_outbox values('O1','A35','PRE_DEDUCT',-100,'SUCCESS')");
        var c = ds.getConnection();
        return new Fixture(c, jdbc, response, c.getCatalog());
    }

    private record Fixture(Connection connection, JdbcTemplate jdbc, String response, String database) implements AutoCloseable {
        MinisoOriginalResponseRecovery.Result recover(boolean apply) throws Exception {
            return new MinisoOriginalResponseRecovery(new ObjectMapper()).recover(connection,
                    new MinisoOriginalResponseRecovery.Request("A35", "O1", 10016, 7, "TEST-RECOVERY", database), apply);
        }
        public void close() throws Exception { connection.close(); }
    }
}

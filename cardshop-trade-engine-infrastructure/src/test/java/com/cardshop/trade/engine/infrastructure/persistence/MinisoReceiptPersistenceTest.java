package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.*;
import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.MinisoReceiptMapper;
import java.util.UUID;
import org.apache.ibatis.session.Configuration;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** 本地H2事务验证名创凭据归属、唯一性与回滚；参见详细设计名创实施约束。 */
class MinisoReceiptPersistenceTest {
    JdbcTemplate jdbc;
    TransactionTemplate tx;
    MinisoReceiptStore store;
    @BeforeEach void setup() throws Exception {
        var ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        jdbc = new JdbcTemplate(ds);
        jdbc.execute(
                "create table mc_order(order_Id varchar(100) primary key,org_code"
                    + " varchar(64),out_orderId varchar(100),account varchar(64),num"
                    + " int,supplier_channel varchar(32),order_status int,data clob)");
        jdbc.execute(
                "create table recharge_order_ext(order_id varchar(100) primary key,org_code"
                    + " varchar(64),out_order_sn varchar(100),supplier_batch_id"
                    + " varchar(64),persist_status varchar(32))");
        jdbc.execute(
                "create table recharge_miniso_receipt(order_id varchar(100) primary key,org_code"
                    + " varchar(64),batch_id int,coupon_code varchar(128) unique,phone"
                    + " varchar(64),begin_time varchar(19),end_time varchar(19),coupon_status"
                    + " int,created_at timestamp default current_timestamp,updated_at timestamp"
                    + " default current_timestamp)");
        for (var id : new String[]{"O1", "O2"}) {
            jdbc.update("insert into mc_order values(?, 'A39', ?, '13800138000', 1, 'MINISO', 13, null)",id,id);
            jdbc.update("insert into recharge_order_ext values(?, 'A39', ?, '100023', 'DB_PERSISTED')",id,id);
        }
        var cfg = new Configuration();
        cfg.addMapper(MinisoReceiptMapper.class);
        var factory = new SqlSessionFactoryBean(); factory.setDataSource(ds); factory.setConfiguration(cfg);
        store = new MinisoReceiptStore(new SqlSessionTemplate(factory.getObject()).getMapper(MinisoReceiptMapper.class));
        tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
    }
    @Test void confirmedReceiptPopulatesLegacyExportDataAndReplayPreservesIt() throws Exception {
        tx.executeWithoutResult(s -> store.save(receipt("O1", "TEST-COUPON", 0)));
        String data = jdbc.queryForObject("select data from mc_order where order_Id='O1'", String.class);
        assertThat(data).isNotBlank();
        var rows = new com.fasterxml.jackson.databind.ObjectMapper().readTree(data);
        assertThat(rows.isArray()).isTrue();
        assertThat(rows.size()).isOne();
        assertThat(rows.get(0).path("couponCode").asText()).isEqualTo("TEST-COUPON");
        assertThat(rows.get(0).path("couponKey").asText()).isEqualTo("TEST-COUPON");
        assertThat(rows.get(0).path("validTime").asText()).isEqualTo("2026-12-01 00:00:00");
        tx.executeWithoutResult(s -> store.save(receipt("O1", "TEST-COUPON", 0)));
        assertThat(jdbc.queryForObject("select data from mc_order where order_Id='O1'", String.class)).isEqualTo(data);
        jdbc.update("update mc_order set data=null where order_Id='O1'");
        tx.executeWithoutResult(s -> store.save(receipt("O1", "TEST-COUPON", 0)));
        assertThat(jdbc.queryForObject("select data from mc_order where order_Id='O1'", String.class)).isEqualTo(data);
    }

    @Test void conflictingLegacyDataRejectsReceiptAndPreservesOriginal() {
        String conflicting = "[{\"couponCode\":\"FOREIGN-COUPON\"}]";
        jdbc.update("update mc_order set data=? where order_Id='O1'", conflicting);
        assertThatThrownBy(() -> tx.executeWithoutResult(s -> store.save(receipt("O1", "TEST-COUPON", 0))))
                .isInstanceOf(IllegalStateException.class).hasMessageNotContaining("TEST-COUPON").hasMessageNotContaining("FOREIGN-COUPON");
        assertThat(jdbc.queryForObject("select count(*) from recharge_miniso_receipt", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select data from mc_order where order_Id='O1'", String.class)).isEqualTo(conflicting);
    }

    @Test void legacyDataWriteFailureRollsBackReceipt() {
        jdbc.execute("alter table mc_order add constraint fail_legacy_data check(data is null)");
        assertThatThrownBy(() -> tx.executeWithoutResult(s -> store.save(receipt("O1", "TEST-COUPON", 0))))
                .isInstanceOf(IllegalStateException.class).hasNoCause().hasMessageNotContaining("TEST-COUPON");
        assertThat(jdbc.queryForObject("select count(*) from recharge_miniso_receipt", Integer.class)).isZero();
    }

    @Test void equivalentExistingLegacyDataIsPreservedAndInvalidJsonIsRejected() {
        String existing = "[{\"validTime\":\"2026-12-01 00:00:00\",\"couponKey\":\"C1\",\"couponCode\":\"C1\",\"productName\":\"原展示字段\"}]";
        jdbc.update("update mc_order set data=? where order_Id='O1'", existing);
        tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 0)));
        assertThat(jdbc.queryForObject("select data from mc_order where order_Id='O1'", String.class)).isEqualTo(existing);
        for (String invalid : java.util.List.of("not-json", "[]", "[{\"couponCode\":\"C1\",\"couponCode\":\"OTHER\"}]", existing + "[]")) {
            jdbc.update("update mc_order set data=? where order_Id='O1'", invalid);
            assertThatThrownBy(() -> tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 0))))
                    .isInstanceOf(IllegalStateException.class).hasNoCause().hasMessageNotContaining(invalid);
        }
    }

    @Test void receiptAndStatusParticipateInSameTransactionAndRollbackOnFailure() {
        assertThatThrownBy(() -> tx.executeWithoutResult(s -> {
            store.save(receipt("O1", "C1", 0));
            jdbc.update("update mc_order set order_status=12 where order_Id='O1'");
            throw new IllegalStateException("simulate status commit failure");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("select count(*) from recharge_miniso_receipt",Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select order_status from mc_order where order_Id='O1'",Integer.class)).isEqualTo(13);
        assertThat(jdbc.queryForObject("select data from mc_order where order_Id='O1'",String.class)).isNull();
    }
    @Test void duplicateRetryIsIdempotentButForeignCouponAndMerchantAreRejected() {
        tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 0)));
        tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 0)));
        assertThatThrownBy(() -> tx.executeWithoutResult(s -> store.save(receipt("O2", "C1", 0))))
                .isInstanceOf(IllegalStateException.class).hasNoCause().hasMessageNotContaining("C1");
        assertThatThrownBy(() -> tx.executeWithoutResult(s -> store.save(new MinisoFulfillmentReceipt("O1","OTHER",100023,"C1","13800138000","2026-10-01 00:00:00","2026-12-01 00:00:00",0)))).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("select count(*) from recharge_miniso_receipt",Integer.class)).isEqualTo(1);
    }
    @Test void actualFulfillmentPortProxyRollsBackReceiptAndMainOrderWhenExtWriteFails() {
        var config = new com.baomidou.mybatisplus.core.MybatisConfiguration();
        com.baomidou.mybatisplus.core.metadata.TableInfoHelper.initTableInfo(new org.apache.ibatis.builder.MapperBuilderAssistant(config,""),
                com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO.class);
        com.baomidou.mybatisplus.core.metadata.TableInfoHelper.initTableInfo(new org.apache.ibatis.builder.MapperBuilderAssistant(config,""),
                com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO.class);
        var orders = org.mockito.Mockito.mock(com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper.class);
        var ext = org.mockito.Mockito.mock(com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper.class);
        org.mockito.Mockito.when(orders.update(org.mockito.ArgumentMatchers.isNull(),org.mockito.ArgumentMatchers.any()))
                .thenAnswer(call -> jdbc.update("update mc_order set order_status=12 where order_Id='O1'"));
        org.mockito.Mockito.when(ext.update(org.mockito.ArgumentMatchers.isNull(),org.mockito.ArgumentMatchers.any()))
                .thenThrow(new IllegalStateException("simulate ext write failure"));
        var target = new MybatisRechargeFulfillmentOrderAdapter(orders,ext,
                org.mockito.Mockito.mock(com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderSupplierFlowMapper.class),
                new com.fasterxml.jackson.databind.ObjectMapper(),java.time.Clock.systemUTC(),store);
        var proxyFactory = new org.springframework.aop.framework.ProxyFactory(target);
        proxyFactory.addAdvice(new org.springframework.transaction.interceptor.TransactionInterceptor(
                tx.getTransactionManager(),new org.springframework.transaction.annotation.AnnotationTransactionAttributeSource()));
        var port = (com.cardshop.trade.engine.application.recharge.RechargeFulfillmentOrderPort)proxyFactory.getProxy();
        var update = new com.cardshop.trade.engine.application.recharge.OrderFulfillmentStatusUpdate("O1",
                com.cardshop.trade.engine.application.recharge.FulfillmentDispatchStatus.SUCCESS,12,"PROCESSED",null,null,null,"ok")
                .withMinisoReceipt(receipt("O1","C1",0));
        assertThatThrownBy(() -> port.updateFulfillmentStatus(update)).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("select count(*) from recharge_miniso_receipt",Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select order_status from mc_order where order_Id='O1'",Integer.class)).isEqualTo(13);
        assertThat(jdbc.queryForObject("select data from mc_order where order_Id='O1'",String.class)).isNull();
    }

    @Test void leadingZeroInFrozenNumericBatchDoesNotBreakConfirmedIssuance() {
        jdbc.update("update recharge_order_ext set supplier_batch_id='00100023' where order_id='O1'");
        tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 0)));
        assertThat(jdbc.queryForObject("select batch_id from recharge_miniso_receipt where order_id='O1'",Integer.class)).isEqualTo(100023);
    }
    @Test void lateQueryCannotDowngradeUsedOrVoidedReceipt() {
        tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 1)));
        tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 0)));
        assertThat(status()).isEqualTo(1);
        // 新核销合同禁止“已使用→作废”静默覆盖；独立未使用订单仍允许确认作废。
        assertThatThrownBy(() -> tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 3))))
                .isInstanceOf(
                        com.cardshop.trade.engine.application.recharge.MinisoWriteOffException
                                .class);
        assertThat(status()).isEqualTo(1);
        tx.executeWithoutResult(s -> store.save(receipt("O2", "C2", 3)));
        tx.executeWithoutResult(s -> store.save(receipt("O2", "C2", 1)));
        assertThat(
                        jdbc.queryForObject(
                                "select coupon_status from recharge_miniso_receipt where"
                                    + " order_id='O2'",
                                Integer.class)).isEqualTo(3);
        assertThatThrownBy(() -> store.save(receipt("O1","C1",0))).isInstanceOf(IllegalStateException.class);
    }
    int status() { return jdbc.queryForObject("select coupon_status from recharge_miniso_receipt where order_id='O1'",Integer.class); }
    MinisoFulfillmentReceipt receipt(String id,String coupon,int status) {
        return new MinisoFulfillmentReceipt(id,"A39",100023,coupon,"13800138000","2026-10-01 00:00:00","2026-12-01 00:00:00",status);
    }
}

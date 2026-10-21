package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.*;
import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
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
                    + " int,supplier_channel varchar(32),order_status int)");
        jdbc.execute(
                "create table recharge_order_ext(order_id varchar(100) primary key,org_code"
                    + " varchar(64),out_order_sn varchar(100),supplier_batch_id"
                    + " varchar(64),persist_status varchar(32),balance_release_authorized_at timestamp,"
                    + "balance_release_event_id varchar(100),balance_release_amount bigint)");
        jdbc.execute(
                "create table recharge_miniso_receipt(order_id varchar(100) primary key,org_code"
                    + " varchar(64),batch_id int,coupon_code varchar(128) unique,phone"
                    + " varchar(64),begin_time varchar(19),end_time varchar(19),coupon_status"
                    + " int,use_time timestamp,created_at timestamp default current_timestamp,updated_at timestamp"
                    + " default current_timestamp)");
        jdbc.execute("create table mc_miniso_coupon_guard(coupon_code varchar(128) primary key,"
                + "created_at timestamp not null)");
        jdbc.execute("create table mc_miniso_callback_inbox(event_id varchar(96) primary key,"
                + "coupon_code varchar(128) unique not null,use_time timestamp not null,"
                + "status varchar(32) not null,retry_count int not null,next_retry_time timestamp not null,"
                + "version bigint not null,received_at timestamp not null,updated_at timestamp not null)");
        jdbc.execute("create table recharge_miniso_writeoff_event(event_id varchar(96) primary key,"
                + "coupon_code varchar(128) unique,use_time timestamp,order_id varchar(100),org_code varchar(64),"
                + "status varchar(32),retry_count int,next_retry_time timestamp,last_error varchar(128),version bigint,"
                + "received_at timestamp,processed_at timestamp,updated_at timestamp)");
        for (var id : new String[]{"O1", "O2"}) {
            jdbc.update("insert into mc_order values(?, 'A39', ?, '13800138000', 1, 'MINISO', 13)",id,id);
            jdbc.update("insert into recharge_order_ext(order_id,org_code,out_order_sn,supplier_batch_id,persist_status) "
                    + "values(?, 'A39', ?, '100023', 'DB_PERSISTED')",id,id);
        }
        var cfg = new Configuration();
        cfg.addMapper(MinisoReceiptMapper.class);
        cfg.addMapper(MinisoWriteOffEventMapper.class);
        cfg.addMapper(MinisoCouponGuardMapper.class);
        cfg.addMapper(MinisoSharedCallbackInboxMapper.class);
        var factory = new SqlSessionFactoryBean(); factory.setDataSource(ds); factory.setConfiguration(cfg);
        var session = new SqlSessionTemplate(factory.getObject());
        var tm = new DataSourceTransactionManager(ds);
        tx = new TransactionTemplate(tm);
        var clock = java.time.Clock.systemUTC();
        var receipts = session.getMapper(MinisoReceiptMapper.class);
        var guards = session.getMapper(MinisoCouponGuardMapper.class);
        var protection = new MybatisMinisoWriteOffProtection(session.getMapper(MinisoWriteOffEventMapper.class),
                receipts, guards, session.getMapper(MinisoSharedCallbackInboxMapper.class), tm, clock, 5);
        store = new MinisoReceiptStore(receipts, guards, clock, () -> protection);
    }
    @Test void receiptAndStatusParticipateInSameTransactionAndRollbackOnFailure() {
        assertThatThrownBy(() -> tx.executeWithoutResult(s -> {
            store.save(receipt("O1", "C1", 0));
            jdbc.update("update mc_order set order_status=12 where order_Id='O1'");
            throw new IllegalStateException("simulate status commit failure");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("select count(*) from recharge_miniso_receipt",Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from mc_miniso_coupon_guard",Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select order_status from mc_order where order_Id='O1'",Integer.class)).isEqualTo(13);
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
    }

    @Test void leadingZeroInFrozenNumericBatchDoesNotBreakConfirmedIssuance() {
        jdbc.update("update recharge_order_ext set supplier_batch_id='00100023' where order_id='O1'");
        tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 0)));
        assertThat(jdbc.queryForObject("select batch_id from recharge_miniso_receipt where order_id='O1'",Integer.class)).isEqualTo(100023);
    }

    @Test void sharedEvidenceBeforeFirstReceiptAllowsPublicationButRejectsVoid() {
        var used = java.time.LocalDateTime.of(2026, 10, 21, 9, 30);
        jdbc.update("insert into mc_miniso_callback_inbox values(?,?,?,'MANUAL_REQUIRED',0,?,1,?,?)",
                "shared-C1", "C1", used, used, used, used);
        assertThatThrownBy(() -> tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 3))))
                .isInstanceOf(com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.class)
                .hasMessage("REDEEM_VOID_CONFLICT");
        assertThat(jdbc.queryForObject("select count(*) from recharge_miniso_receipt", Integer.class)).isZero();
        tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 0)));
        assertThat(status()).isZero();
        assertThat(jdbc.queryForObject("select count(*) from mc_miniso_coupon_guard where coupon_code='C1'",
                Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select status from mc_miniso_callback_inbox", String.class))
                .isEqualTo("MANUAL_REQUIRED");
    }

    @Test void missingGuardFailsClosedBeforeReceiptPublication() {
        jdbc.execute("drop table mc_miniso_coupon_guard");
        assertThatThrownBy(() -> tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 0))))
                .isInstanceOf(IllegalStateException.class).hasNoCause().hasMessageNotContaining("C1");
        assertThat(jdbc.queryForObject("select count(*) from recharge_miniso_receipt", Integer.class)).isZero();
    }

    @Test void useTimeWithoutEventOrUsedStatusStillBlocksVoid() {
        tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 0)));
        jdbc.update("update recharge_miniso_receipt set use_time=? where order_id='O1'",
                java.time.LocalDateTime.of(2026, 10, 21, 9, 30));
        assertThatThrownBy(() -> tx.executeWithoutResult(s -> store.save(receipt("O1", "C1", 3))))
                .isInstanceOf(com.cardshop.trade.engine.application.recharge.MinisoWriteOffException.class)
                .hasMessage("REDEEM_VOID_CONFLICT");
        assertThat(status()).isZero();
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

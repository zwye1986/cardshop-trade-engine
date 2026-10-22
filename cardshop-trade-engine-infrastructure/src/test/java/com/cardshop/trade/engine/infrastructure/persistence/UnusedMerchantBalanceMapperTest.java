package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.infrastructure.persistence.mapper.McExtDepartMapper;
import java.util.UUID;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

/** 验证安全初始化SQL排除已完成历史和孤立资金事件；参见详细设计9.2成功预扣收尾最小修订。 */
class UnusedMerchantBalanceMapperTest {
    @Test
    void onlyMerchantWithoutAnyEngineHistoryCanInitializeBalance() throws Exception {
        var ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
        var jdbc = new JdbcTemplate(ds);
        jdbc.execute("create table mc_ext_depart(id bigint,org_code varchar(64),balance bigint)");
        jdbc.execute("create table recharge_order_ext(org_code varchar(64),dispatch_status varchar(32))");
        jdbc.execute("create table recharge_balance_delta_outbox(org_code varchar(64),status varchar(32))");
        jdbc.execute("insert into mc_ext_depart values(56,'A35',700)");
        var config = new org.apache.ibatis.session.Configuration();
        config.addMapper(McExtDepartMapper.class);
        var factory = new SqlSessionFactoryBean(); factory.setDataSource(ds); factory.setConfiguration(config);
        var mapper = new SqlSessionTemplate(factory.getObject()).getMapper(McExtDepartMapper.class);
        assertThat(mapper.selectUnusedMerchantBalance("A35").getBalance()).isEqualTo(700);
        jdbc.execute("insert into recharge_order_ext values('A35','SUCCESS')");
        assertThat(mapper.selectUnusedMerchantBalance("A35")).isNull();
        jdbc.execute("delete from recharge_order_ext");
        jdbc.execute("insert into recharge_balance_delta_outbox values('A35','SUCCESS')");
        assertThat(mapper.selectUnusedMerchantBalance("A35")).isNull();
        assertThat(mapper.selectUnusedMerchantBalance("MISSING")).isNull();
    }
}

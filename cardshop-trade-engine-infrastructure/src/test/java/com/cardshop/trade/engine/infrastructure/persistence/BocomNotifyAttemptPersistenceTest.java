package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeNotifyAttemptMapper;
import org.apache.ibatis.session.Configuration;
import org.mybatis.spring.*;
import java.time.Duration;

/** 登记必须核验领取凭证，不能只写一条普通日志；参见详细设计2104实施细化。 */
class BocomNotifyAttemptPersistenceTest extends RechargeNotifyClaimCasTest {
    MybatisMerchantNotifyAttemptAdapter attempts;
    @BeforeEach @Override void setup() throws Exception {
        super.setup();
        var cfg = new Configuration(); cfg.addMapper(RechargeNotifyAttemptMapper.class);
        var factory = new SqlSessionFactoryBean(); factory.setConfiguration(cfg);
        factory.setDataSource(jdbc.getDataSource());
        attempts = new MybatisMerchantNotifyAttemptAdapter(new SqlSessionTemplate(factory.getObject()).getMapper(RechargeNotifyAttemptMapper.class),clock);
        jdbc.update("update recharge_notify_task set delivery_channel='BOCOM_OPENAPI'");
    }
    @Test void reliableAttemptStoreMustExist() {
        assertThatCode(() -> Class.forName("com.cardshop.trade.engine.infrastructure.persistence.MybatisMerchantNotifyAttemptAdapter"))
                .doesNotThrowAnyException();
    }
    @Test void registrationRequiresCurrentLeaseAndCompletionRetainsLateEvidence() {
        var scan=adapter.selectDueTasks(1).getFirst();
        var a=adapter.claim(scan,"A",Duration.ofSeconds(10)).orElseThrow();
        var request=MerchantNotifyRequest.from(a);
        assertThat(attempts.begin(request,"a".repeat(32))).isTrue();
        clock.advance();
        var b=adapter.claim(a,"B",Duration.ofSeconds(10)).orElseThrow();
        assertThat(attempts.begin(request,"b".repeat(32))).isFalse();
        attempts.finish(request,"a".repeat(32),new MerchantNotifyAttemptResult("SUCCESS",0,"S","ok","200","ref1"));
        assertThat(jdbc.queryForObject("select ref_msg_id from recharge_notify_attempt",String.class)).isEqualTo("ref1");
        assertThat(adapter.markSuccess(a)).isFalse();
        assertThat(adapter.markSuccess(b)).isTrue();
    }
}

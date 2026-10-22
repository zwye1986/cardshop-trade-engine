package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeNotifyAttemptMapper;
import java.time.*;
import java.util.Objects;

/** 银行请求尝试持久化；参见详细设计「交行 2104 通知实施细化」。SQL错误立即上抛由发送层脱敏记录。 */
public final class MybatisMerchantNotifyAttemptAdapter implements MerchantNotifyAttemptPort {
    private final RechargeNotifyAttemptMapper mapper;
    private final Clock clock;
    public MybatisMerchantNotifyAttemptAdapter(RechargeNotifyAttemptMapper mapper,Clock clock) {
        this.mapper=Objects.requireNonNull(mapper); this.clock=Objects.requireNonNull(clock);
    }
    private LocalDateTime now() { return LocalDateTime.ofInstant(clock.instant(),ZoneId.of("Asia/Shanghai")); }
    @Override public boolean begin(MerchantNotifyRequest request,String msgId) {
        return mapper.begin(request,msgId,now())==1;
    }
    @Override public void finish(MerchantNotifyRequest request,String msgId,MerchantNotifyAttemptResult result) {
        if (mapper.finish(request,msgId,result,now())!=1) throw new IllegalStateException("bank attempt completion lost");
    }
}

package com.cardshop.trade.engine.application.recharge;

import java.time.Duration;

/**
 * 同账号直充限频端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.9 派单 Outbox」。锁值必须是当前
 * orderId，终态释放时要比较 owner，避免误删同账号另一笔订单的锁。
 */
public interface DirectRechargeFrequencyPort {

    boolean acquire(String orgCode, String account, String orderId, Duration ttl);

    void releaseIfOwned(String orgCode, String account, String orderId, Duration minimumHold);
}

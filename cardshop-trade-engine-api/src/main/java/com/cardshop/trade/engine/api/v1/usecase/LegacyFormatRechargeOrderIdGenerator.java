package com.cardshop.trade.engine.api.v1.usecase;

import com.cardshop.trade.engine.application.v1.RechargeOrderIdGenerator;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 兼容旧卡管格式的平台订单号生成器。
 *
 * <p>旧卡管 `McOrderV2ServiceImpl.generateOrderSn()` 使用 {@code yyyyMMddHHmmss + 10位随机/Redis递增}
 * 生成订单号。这里先保持外部可见格式一致，后续需要接入 Redis 日维度递增序列以获得多节点严格唯一。
 */
@Component
public class LegacyFormatRechargeOrderIdGenerator implements RechargeOrderIdGenerator {

    private static final DateTimeFormatter ORDER_SECOND = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final SecureRandom RANDOM = new SecureRandom();

    private final Clock clock;

    public LegacyFormatRechargeOrderIdGenerator(Clock clock) {
        this.clock = clock;
    }

    @Override
    public String nextOrderId() {
        String prefix = LocalDateTime.now(clock).format(ORDER_SECOND);
        // TODO(D5): 接入 Redis `order:order_id:{yyyyMMdd}` 日维度递增序列，严格复刻旧卡管 generateOrderSn；参见 docs/交行直充业务系统详细设计.md「20.13.2 V1 请求 DTO 和字段来源」。
        return prefix + "%010d".formatted(Math.floorMod(RANDOM.nextLong(), 10_000_000_000L));
    }
}

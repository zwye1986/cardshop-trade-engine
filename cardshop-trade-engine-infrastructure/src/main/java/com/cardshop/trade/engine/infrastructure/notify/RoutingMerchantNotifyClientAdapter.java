package com.cardshop.trade.engine.infrastructure.notify;

import com.cardshop.trade.engine.application.recharge.*;

/** 只按任务冻结通道分流，配置变化不迁移历史任务；参见详细设计2104实施细化。 */
public final class RoutingMerchantNotifyClientAdapter implements MerchantNotifyClientPort {
    private final MerchantNotifyClientPort direct;
    private final MerchantNotifyClientPort bank;
    public RoutingMerchantNotifyClientAdapter(MerchantNotifyClientPort direct,MerchantNotifyClientPort bank) {
        this.direct=direct;this.bank=bank;
    }
    public MerchantNotifySendResult send(MerchantNotifyRequest request) {
        if (request.deliveryChannel()==null) return MerchantNotifySendResult.manual("UNKNOWN_NOTIFY_CHANNEL");
        return switch (request.deliveryChannel()) {
            case DIRECT_JSON -> direct.send(request);
            case BOCOM_OPENAPI -> bank.send(request);
        };
    }
}

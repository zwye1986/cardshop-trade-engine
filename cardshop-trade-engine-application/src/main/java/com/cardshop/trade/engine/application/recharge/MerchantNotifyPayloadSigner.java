package com.cardshop.trade.engine.application.recharge;

import java.time.Instant;
import java.util.Map;

/**
 * 商户回调载荷签名端口。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.12 回调设计」。实现必须把 Java null、
 * JSON null 和缺失字段规范化为空字符串后参与签名。
 */
public interface MerchantNotifyPayloadSigner {

    Map<String, String> signOrderStatus(RechargeFulfillmentOrder order, String orderStatus, Instant now);

    Map<String, String> signWriteOff(RechargeFulfillmentOrder order, Instant invalidTime);
}

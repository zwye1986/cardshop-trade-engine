package com.cardshop.trade.engine.application.recharge;

import java.util.List;
import java.util.Map;

/** 独立名创key2入站认证端口；参见docs/交行直充业务系统详细设计.md「9.5」，不使用商户token或signKey。 */
public interface MinisoWriteOffAuthenticator {
    MinisoWriteOffNotification authenticate(String rawBody, Map<String, List<String>> headers);
}

package com.cardshop.trade.engine.application.recharge;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;

/**
 * 名创直充履约凭据，不提供本地卡券库存能力。
 * 字段注释对应0020 DDL COMMENT；参见详细设计「名创 MINISO 专属协议实施约束（2026-10-17）」。
 * @param orderId 平台订单号
 * @param orgCode 商户编码
 * @param batchId 名创券批次号
 * @param couponCode 名创券码
 * @param phone 充值手机号
 * @param beginTime 开始使用时间
 * @param endTime 结束使用时间
 * @param couponStatus 名创券状态
 */
public record MinisoFulfillmentReceipt(String orderId, String orgCode, int batchId, String couponCode,
        String phone, String beginTime, String endTime, int couponStatus) {
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
            .withResolverStyle(ResolverStyle.STRICT);

    public MinisoFulfillmentReceipt {
        for (String value : new String[]{orderId, orgCode, couponCode, phone, beginTime, endTime}) {
            if (value == null || value.isBlank()) throw new IllegalArgumentException("名创履约凭据字段缺失");
        }
        if (batchId <= 0 || couponStatus < 0 || couponStatus > 3) {
            throw new IllegalArgumentException("名创履约凭据数值非法");
        }
        // 不把包含券码的输入或日期异常传入日志；只使用安全异常摘要。
        try {
            var begin = LocalDateTime.parse(beginTime, TIME);
            var end = LocalDateTime.parse(endTime, TIME);
            if (end.isBefore(begin)) throw new IllegalArgumentException("名创有效期顺序非法");
        } catch (java.time.format.DateTimeParseException ex) {
            throw new IllegalArgumentException("名创有效期格式非法");
        }
    }

    /** 日志和异常中不暴露券码或手机号。 */
    @Override public String toString() { return "MinisoFulfillmentReceipt[orderId=" + orderId + ",status=" + couponStatus + "]"; }
}

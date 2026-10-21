package com.cardshop.trade.engine.application.recharge;

import java.time.Instant;

/**
 * MINISO首次余额释放的持久许可，只能由锁定的主单、扩展归属及原预扣事实生成。
 * 参见 {@code docs/交行直充业务系统详细设计.md}「9.8」；重试必须复用冻结事件、金额及时间。
 *
 * @param orderId 平台订单号
 * @param orgCode 商户编码
 * @param outOrderSn 商户订单号
 * @param sourceEventId 余额释放授权事件编号
 * @param amount 余额释放授权金额，单位分
 * @param authorizedAt 余额释放授权时间
 */
public record MinisoBalanceReleaseAuthorization(
        String orderId, String orgCode, String outOrderSn,
        String sourceEventId, long amount, Instant authorizedAt) { }

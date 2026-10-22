package com.cardshop.trade.engine.application.recharge;

/**
 * 脱敏投递证据，只含有限代码，不保存响应原文；参见详细设计2104实施细化。
 * @param status 请求结果分类
 * @param httpStatus HTTP状态码（SDK未暴露时为0，不伪造200）
 * @param bizState 银行平台状态
 * @param rspCode 银行平台返回码
 * @param businessCode 银行业务返回码
 * @param refMsgId 银行关联消息ID
 */
public record MerchantNotifyAttemptResult(String status,int httpStatus,String bizState,String rspCode,
        String businessCode,String refMsgId) { }

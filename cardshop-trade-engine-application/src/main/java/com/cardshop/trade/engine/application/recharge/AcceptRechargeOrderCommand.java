package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.ValidationStatus;
import java.util.Objects;

/**
 * Redis-first 创建直充订单的应用层命令。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
 * 该命令把协议层已经确认的商户身份、Java 候选校验结果、商品候选快照、配置版本和 request_hash
 * 一次性传给 Redis 接单端口；Redis Lua 会基于这些字段做最终动态校验并原子写入订单快照、幂等、
 * pending 和事件索引。
 *
 * <p>订单不保存签名密钥，回调创建时使用商户当前密钥，参见详细设计3.1。
 *
 * @param orgCode 商户机构号，来自 token 对应的服务端商户资料
 * @param appId 商户应用 ID，来自 token claim 或商户资料
 * @param outOrderSn 商户订单号，对应 V1 请求 `out_order_sn`
 * @param orderId 新交易引擎生成的平台订单号
 * @param productId 调用方传入的父产品号，对应接口 {@code product_id}
 * @param selectedProductId Java 从父产品候选中选出的实际派单子商品号
 * @param account 充值账号
 * @param notifyUrl 商户回调地址，可为空；为空表示本订单不触发商户回调
 * @param nonceStr V1 请求防重放随机串
 * @param num 购买数量，首期直充只允许 1
 * @param orderAmountCent 订单扣款金额，单位分
 * @param productName 商品名称快照
 * @param productValueCent 商品面值，单位分
 * @param productType 商品类型，首期新链路只处理直充 1
 * @param couponType 商品券码类型兼容字段
 * @param supplierChannel 候选商品供应商渠道
 * @param requestHash request_hash v1 摘要值
 * @param requestHashVersion request_hash 规则版本
 * @param requestHashAlg request_hash 摘要算法
 * @param candidateValidationStatus Java 候选校验状态
 * @param candidateFailCode Java 候选校验失败码
 * @param candidateFailReason Java 候选校验失败原因
 * @param merchantConfigVersion Java 读取到的商户配置版本
 * @param productConfigVersion Java 读取到的商品配置版本
 * @param productSelectionFingerprint Java 完整选品快照稳定指纹
 * @param productSnapshotAvailable 是否已选出可供 Redis 父子快照校验的真实子商品
 * @param nowEpochMillis 接单请求当前时间戳，毫秒
 * @param nextRetryEpochMillis pending 首次补偿扫描时间，毫秒
 */
public record AcceptRechargeOrderCommand(
        String orgCode,
        String appId,
        String outOrderSn,
        String orderId,
        String productId,
        String selectedProductId,
        String account,
        String notifyUrl,
        String nonceStr,
        Integer num,
        Long orderAmountCent,
        String productName,
        Long productValueCent,
        Integer productType,
        String couponType,
        String supplierChannel,
        String requestHash,
        String requestHashVersion,
        String requestHashAlg,
        ValidationStatus candidateValidationStatus,
        String candidateFailCode,
        String candidateFailReason,
        Long merchantConfigVersion,
        Long productConfigVersion,
        String productSelectionFingerprint,
        boolean productSnapshotAvailable,
        Long nowEpochMillis,
        Long nextRetryEpochMillis) {

    public AcceptRechargeOrderCommand {
        orgCode = requireText(orgCode, "orgCode");
        appId = requireText(appId, "appId");
        outOrderSn = requireText(outOrderSn, "outOrderSn");
        orderId = requireText(orderId, "orderId");
        productId = requireText(productId, "productId");
        selectedProductId = requireText(selectedProductId, "selectedProductId");
        account = requireText(account, "account");
        notifyUrl = defaultString(notifyUrl);
        nonceStr = requireText(nonceStr, "nonceStr");
        requestHash = requireText(requestHash, "requestHash");
        requestHashVersion = requireText(requestHashVersion, "requestHashVersion");
        requestHashAlg = requireText(requestHashAlg, "requestHashAlg");
        candidateValidationStatus = Objects.requireNonNull(candidateValidationStatus, "candidateValidationStatus");
        num = requirePositive(num, "num");
        orderAmountCent = requireNonNegative(orderAmountCent, "orderAmountCent");
        productName = defaultString(productName);
        productValueCent = requireNonNegative(productValueCent, "productValueCent");
        productType = Objects.requireNonNull(productType, "productType");
        couponType = defaultString(couponType);
        supplierChannel = defaultString(supplierChannel);
        candidateFailCode = defaultString(candidateFailCode);
        candidateFailReason = defaultString(candidateFailReason);
        merchantConfigVersion = requirePositive(merchantConfigVersion, "merchantConfigVersion");
        productConfigVersion = requirePositive(productConfigVersion, "productConfigVersion");
        productSelectionFingerprint = requireText(productSelectionFingerprint, "productSelectionFingerprint");
        nowEpochMillis = requirePositive(nowEpochMillis, "nowEpochMillis");
        nextRetryEpochMillis = requirePositive(nextRetryEpochMillis, "nextRetryEpochMillis");
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }

    private static String defaultString(String value) {
        return value == null ? "" : value;
    }

    private static Integer requirePositive(Integer value, String fieldName) {
        if (value == null || value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
        return value;
    }

    private static Long requirePositive(Long value, String fieldName) {
        if (value == null || value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
        return value;
    }

    private static Long requireNonNegative(Long value, String fieldName) {
        if (value == null || value < 0) {
            throw new IllegalArgumentException(fieldName + " must not be negative");
        }
        return value;
    }
}

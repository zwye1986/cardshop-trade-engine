package com.cardshop.trade.engine.application.v1;

import com.cardshop.trade.engine.domain.order.ValidationStatus;

/**
 * V1 直充商品选择结果。
 *
 * <p>字段含义对应 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」。
 * 即使 Java 商品校验失败，也会保留请求商品、失败码和配置版本，让 Redis Lua 在验签通过后写入失败订单快照。
 *
 * @param productId 调用方传入的接口 {@code product_id}
 * @param selectedProductId 实际派单商品编码；按当前接口语义必须和 {@code productId} 相同
 * @param supplierChannel 供应商渠道
 * @param productName 商品名称快照
 * @param couponType 券码类型兼容字段
 * @param productType 商品类型，直充为 1
 * @param salePriceCent 商户售价，单位分
 * @param faceValueCent 商品面值，单位分
 * @param basePriceCent 商品底价，单位分
 * @param productConfigVersion 商品配置版本
 * @param validationStatus Java 候选校验结果
 * @param failCode Java 候选失败码
 * @param failReason Java 候选失败原因
 */
public record V1ProductSelection(
        String productId,
        String selectedProductId,
        String supplierChannel,
        String productName,
        String couponType,
        Integer productType,
        Long salePriceCent,
        Long faceValueCent,
        Long basePriceCent,
        Long productConfigVersion,
        ValidationStatus validationStatus,
        String failCode,
        String failReason) {

    public static V1ProductSelection passed(
            String productId,
            String selectedProductId,
            String supplierChannel,
            String productName,
            String couponType,
            Integer productType,
            Long salePriceCent,
            Long faceValueCent,
            Long basePriceCent,
            Long productConfigVersion) {
        return new V1ProductSelection(
                productId,
                selectedProductId,
                supplierChannel,
                productName,
                couponType,
                productType,
                salePriceCent,
                faceValueCent,
                basePriceCent,
                productConfigVersion,
                ValidationStatus.PASSED,
                "",
                "");
    }

    public static V1ProductSelection failed(
            String productId,
            String failCode,
            String failReason,
            Long productConfigVersion) {
        return new V1ProductSelection(
                productId,
                productId,
                "",
                "",
                "",
                1,
                0L,
                0L,
                0L,
                productConfigVersion,
                ValidationStatus.FAILED,
                failCode,
                failReason);
    }
}

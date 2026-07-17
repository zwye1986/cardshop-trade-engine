package com.cardshop.trade.engine.application.v1;

import com.cardshop.trade.engine.domain.order.ValidationStatus;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * V1 直充商品选择结果。
 *
 * <p>字段含义对应 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」。
 * 即使 Java 商品校验失败，也会保留请求商品、失败码和配置版本，让 Redis Lua 在验签通过后写入失败订单快照。
 *
 * @param productId 调用方传入的父产品号，对应接口 {@code product_id}
 * @param selectedProductId 实际派单子商品号；无候选失败时使用请求父产品号作为失败快照占位值
 * @param supplierChannel 供应商渠道
 * @param productName 商品名称快照
 * @param couponType 券码类型兼容字段
 * @param productType 商品类型，直充为 1
 * @param salePriceCent 商户售价，单位分
 * @param faceValueCent 商品面值，单位分
 * @param basePriceCent 商品底价，单位分
 * @param productConfigVersion 商品配置版本
 * @param productSnapshotAvailable 是否已经从 MySQL 选出可用于 Redis 补热的子商品快照；选品业务校验失败不影响该事实
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
        boolean productSnapshotAvailable,
        ValidationStatus validationStatus,
        String failCode,
        String failReason) {

    private static final String FINGERPRINT_ALGORITHM = "SHA-256";

    /**
     * 计算完整父商品选品快照的稳定指纹。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。指纹覆盖所有
     * 选品字段，使底价、渠道、名称或候选校验结果变化也能在接单 Lua 中阻止旧请求继续受理。
     */
    public String cacheFingerprint() {
        StringBuilder canonical = new StringBuilder(256);
        appendFingerprintField(canonical, productId);
        appendFingerprintField(canonical, selectedProductId);
        appendFingerprintField(canonical, supplierChannel);
        appendFingerprintField(canonical, productName);
        appendFingerprintField(canonical, couponType);
        appendFingerprintField(canonical, productType);
        appendFingerprintField(canonical, salePriceCent);
        appendFingerprintField(canonical, faceValueCent);
        appendFingerprintField(canonical, basePriceCent);
        appendFingerprintField(canonical, productConfigVersion);
        appendFingerprintField(canonical, productSnapshotAvailable);
        appendFingerprintField(canonical, validationStatus == null ? null : validationStatus.name());
        appendFingerprintField(canonical, failCode);
        appendFingerprintField(canonical, failReason);
        try {
            byte[] digest = MessageDigest.getInstance(FINGERPRINT_ALGORITHM)
                    .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 algorithm is unavailable", ex);
        }
    }

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
                true,
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
                false,
                ValidationStatus.FAILED,
                failCode,
                failReason);
    }

    private static void appendFingerprintField(StringBuilder canonical, Object value) {
        if (value == null) {
            canonical.append("-1:|");
            return;
        }
        String text = String.valueOf(value);
        canonical.append(text.length()).append(':').append(text).append('|');
    }
}

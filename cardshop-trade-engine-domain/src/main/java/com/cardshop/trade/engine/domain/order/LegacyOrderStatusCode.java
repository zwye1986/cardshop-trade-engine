package com.cardshop.trade.engine.domain.order;

import java.util.Arrays;

/**
 * 旧卡管 `mc_order.order_status` 数字状态码。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」和
 * 「3. API/V1 接入层」。新交易引擎的履约、查单、回调仍要兼容旧 V1 接口数字码，因此数字值只能在
 * 本枚举集中维护，避免 application、api、infrastructure 各自硬编码 2、6、11、12、13。
 */
public enum LegacyOrderStatusCode {

    /**
     * 订单已受理，等待落库或供应商派单。
     */
    ACCEPTED(2, "ACCEPTED"),

    /**
     * 订单失败或作废完成。
     */
    FAILED(6, "FAILED"),

    /**
     * 旧系统退款完成状态。
     */
    REFUNDED(11, "REFUNDED"),

    /**
     * 供应商充值成功。
     */
    SUCCESS(12, "SUCCESS"),

    /**
     * 供应商已受理，等待最终结果。
     */
    SUPPLIER_ACCEPTED(13, "SUPPLIER_ACCEPTED"),

    /**
     * 旧表未知或未覆盖状态，只用于兜底查询展示，不应写回旧表。
     */
    UNKNOWN(0, "UNKNOWN");

    private final int code;
    private final String queryStatus;

    LegacyOrderStatusCode(int code, String queryStatus) {
        this.code = code;
        this.queryStatus = queryStatus;
    }

    /**
     * 返回旧 V1 接口和旧表使用的数字状态码。
     */
    public int code() {
        return code;
    }

    /**
     * 返回 API 查单用例使用的内部状态名。
     */
    public String toQueryStatus() {
        return queryStatus;
    }

    /**
     * 按旧表数字状态码解析枚举，无法识别时返回 {@link #UNKNOWN}。
     */
    public static LegacyOrderStatusCode fromCode(int code) {
        return Arrays.stream(values())
                .filter(status -> status.code == code)
                .findFirst()
                .orElse(UNKNOWN);
    }
}

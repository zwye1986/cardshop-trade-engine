package com.cardshop.trade.engine.domain.notify;

/**
 * 商户通知对应的业务事实，不以 HTTP 目标或供应商名推断操作类型。
 * 参见 docs/交行直充业务系统详细设计.md「本次交行回调对接确认」。
 * 核销与作废即使共用 VOUCHER_STATE 任务分类，也必须分别映射为 2、3。
 */
public enum MerchantNotifyOperation {
    ORDER_STATUS("1"),
    REDEEMED("2"),
    CANCELLED("3");

    private final String code;

    MerchantNotifyOperation(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }
}

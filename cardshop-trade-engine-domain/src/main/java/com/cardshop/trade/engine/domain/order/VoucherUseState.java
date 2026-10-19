package com.cardshop.trade.engine.domain.order;

/**
 * 交行直充券使用状态，与充值订单状态分开；参见详细设计9.6节。
 * 供应商原生券状态必须经过业务映射，不能直接写入本状态。
 */
public enum VoucherUseState {
    UNUSED("1"), REDEEMED("2"), CANCELLED("3");

    public static final String STATE_TYPE = "2";
    public static final String MINISO_STATE_TYPE = "3";
    private static final String MINISO_CHANNEL = "MINISO";
    private final String code;

    VoucherUseState(String code) { this.code = code; }
    public String code() { return code; }

    /** 名创采用通用券分类3，其余直充按文档分类2；参见详细设计9.6。 */
    public static String stateTypeFor(String supplierChannel) {
        return MINISO_CHANNEL.equals(supplierChannel) ? MINISO_STATE_TYPE : STATE_TYPE;
    }

    public static boolean isSupportedStateType(String stateType) {
        return STATE_TYPE.equals(stateType) || MINISO_STATE_TYPE.equals(stateType);
    }
}

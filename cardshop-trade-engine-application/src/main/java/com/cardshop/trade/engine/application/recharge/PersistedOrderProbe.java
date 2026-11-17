package com.cardshop.trade.engine.application.recharge;

/**
 * MySQL 已落库二次确认探针。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。批消费在真正写 MySQL 前，
 * 会用本值对象表达“当前选中的订单快照版本是否已经被其它 MQ 副本落库”。这里必须使用选中快照版本，
 * 而不是原始 MQ 版本，避免 Redis 已有更新版本时误把旧消息当作可跳过。
 *
 * @param orgCode 商户编码
 * @param outOrderSn 商户订单号
 * @param orderId 平台订单号
 * @param orderVersion 本次选中快照版本
 * @param orderPrice 订单金额，单位分；旧调用未提供时不得跳过事实事务
 * @param requestHash 不可变请求摘要
 * @param requestHashVersion 请求摘要规范版本
 * @param requestHashAlg 请求摘要算法
 */
public record PersistedOrderProbe(
        String orgCode,
        String outOrderSn,
        String orderId,
        long orderVersion,
        Long orderPrice,
        String requestHash,
        String requestHashVersion,
        String requestHashAlg) {

    /** 旧调用缺少不可变请求身份时不能用于批消费跳过裁决；参见详细设计8.2.1。 */
    public PersistedOrderProbe(String orgCode, String outOrderSn, String orderId, long orderVersion) {
        this(orgCode, outOrderSn, orderId, orderVersion, null, null, null, null);
    }

    public static PersistedOrderProbe from(PersistOrderSnapshot snapshot) {
        return new PersistedOrderProbe(snapshot.orgCode(), snapshot.outOrderSn(), snapshot.orderId(),
                snapshot.orderVersion(), snapshot.orderPrice(), snapshot.requestHash(),
                snapshot.requestHashVersion(), snapshot.requestHashAlg());
    }

    public PersistedOrderProbe {
        orgCode = requireText(orgCode, "orgCode");
        outOrderSn = requireText(outOrderSn, "outOrderSn");
        orderId = requireText(orderId, "orderId");
        orderVersion = Math.max(1L, orderVersion);
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}

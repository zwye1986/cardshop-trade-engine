package com.cardshop.trade.engine.application.v1;

/**
 * V1 商户安全资料本地缓存低基数观测端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」和「12. 可观测性」。
 * 实现禁止把 appId、orgCode、aesKey 或订单号写入指标标签。
 */
public interface V1MerchantProfileCacheObserver {

    /**
     * 记录一次缓存查找结果。
     *
     * @param result {@code hit}（正缓存命中）、{@code negative}（负缓存命中）、{@code miss}（未命中并回源）
     * @param lookupType {@code app_org} 或 {@code org}
     */
    void recordLookup(String result, String lookupType);

    static V1MerchantProfileCacheObserver noop() {
        return (result, lookupType) -> {
        };
    }
}

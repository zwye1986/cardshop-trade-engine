package com.cardshop.trade.engine.application.v1;

/**
 * V1 直充商品配置比较修复端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
 * 商品选择器已经形成可信父商品选品快照；Redis 实际子商品配置缺失或字段漂移时，该端口先确认
 * 父快照仍与当前请求一致，再整体修复动态配置，随后接单 Lua 继续做版本、价格和类型的最终校验。
 */
@FunctionalInterface
public interface V1ProductConfigPreheatPort {

    void refreshIfMissingOrStale(String orgCode, V1ProductSelection selection);

    static V1ProductConfigPreheatPort noop() {
        return (orgCode, selection) -> {
        };
    }
}

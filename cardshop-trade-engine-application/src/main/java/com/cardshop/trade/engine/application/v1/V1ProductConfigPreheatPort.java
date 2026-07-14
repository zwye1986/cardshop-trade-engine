package com.cardshop.trade.engine.application.v1;

/**
 * V1 直充商品配置补热端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6. Redis 受理与缓存层」。
 * 商品选择器已经从旧卡管 MySQL 表形成可信商品快照；Redis 缺少商品配置时，由该端口按快照补写
 * 动态配置，随后接单 Lua 继续做版本、价格和类型的最终校验。
 */
@FunctionalInterface
public interface V1ProductConfigPreheatPort {

    void preheatIfMissing(String orgCode, V1ProductSelection selection);

    static V1ProductConfigPreheatPort noop() {
        return (orgCode, selection) -> {
        };
    }
}

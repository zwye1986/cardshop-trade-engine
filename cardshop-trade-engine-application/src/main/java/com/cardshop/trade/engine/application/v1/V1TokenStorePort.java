package com.cardshop.trade.engine.application.v1;

import java.time.Duration;
import java.util.Optional;

/**
 * V1 Token 共享存储端口。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。
 * 旧系统使用 Redis key {@code token:app_id:{appId}} 保存最新 JWT，新服务生成和校验 token
 * 必须复用同一 key 空间，保证灰度切流期间调用方无需重新适配。
 */
public interface V1TokenStorePort {

    /**
     * 保存本次签发的 JWT。
     *
     * @param appId 旧卡管商户应用 ID
     * @param token 裸 JWT，不包含 Bearer 前缀
     * @param ttl token Redis 缓存有效期
     * @return 写入成功返回 true；依赖不可用或写入失败返回 false
     */
    boolean storeIssuedToken(String appId, String token, Duration ttl);

    /**
     * 读取 appId 对应的当前有效 JWT。
     */
    Optional<String> findIssuedToken(String appId);
}

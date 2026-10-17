package com.cardshop.trade.engine.application.v1.crypto;

import java.util.Optional;

/**
 * 独立签名凭证的持久化端口；参见详细设计3.1及「V1.2 密钥存储实施约束」。
 * 存储失败必须抛异常，不能假装签发成功；版本有效性不使用token TTL。
 */
public interface V12SigningCredentialStorePort {
    /** 基础设施能力标识；不可用占位端口必须为false，不能被核销启用检查当作真实密钥存储。 */
    default boolean available() {
        return true;
    }

    /** 原子取得或首次初始化当前版本；撤销当前版本后必须失败，不得自动重建。 */
    V12SigningCredential getOrCreateCurrent(String orgCode);
    /** 业务请求与新回调只允许当前未撤销版本。 */
    Optional<V12SigningCredential> findCurrent(String orgCode);
    /** 已签名回调按原keyId重试，允许CURRENT及RETIRED，禁止REVOKED和跨商户查询。 */
    Optional<V12SigningCredential> findForCallback(String orgCode, String keyId);
    /** 仅供受控内部维护流程调用；调用方负责先协调暂停流量并完成安全同步。 */
    V12SigningCredential rotate(String orgCode);
    boolean revoke(String orgCode, String keyId);
    /** 缺少基础设施时失败关闭，禁止默认生成临时密钥；参见详细设计3.1。 */
    static V12SigningCredentialStorePort unavailable() {
        return new V12SigningCredentialStorePort() {
            public boolean available() {
                return false;
            }

            public V12SigningCredential getOrCreateCurrent(String org) { throw new IllegalStateException("V1.2 key store unavailable"); }
            public Optional<V12SigningCredential> findCurrent(String org) { return Optional.empty(); }
            public Optional<V12SigningCredential> findForCallback(String org,String id) { return Optional.empty(); }
            public V12SigningCredential rotate(String org) { throw new IllegalStateException("V1.2 key store unavailable"); }
            public boolean revoke(String org,String id) { throw new IllegalStateException("V1.2 key store unavailable"); }
        };
    }
}


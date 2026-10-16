package com.cardshop.trade.engine.application.v1.crypto;

import java.util.Optional;

/**
 * 独立签名凭证的持久化端口；参见详细设计3.1及「V1.2 密钥存储实施约束」。
 * 存储失败必须抛异常，不能假装签发成功；查询只返回未撤销记录，历史查询不得按token TTL过滤。
 */
public interface V12SigningCredentialStorePort {
    void save(V12SigningCredential credential);
    Optional<V12SigningCredential> findActive(String appId, String orgCode, String keyId);
    Optional<V12SigningCredential> findForOrder(String orgCode, String orderId);
    boolean revoke(String orgCode, String keyId);
    /** 缺少基础设施时失败关闭，禁止默认生成临时密钥；参见详细设计3.1。 */
    static V12SigningCredentialStorePort unavailable() {
        return new V12SigningCredentialStorePort() {
            public void save(V12SigningCredential credential) { throw new IllegalStateException("V1.2 key store unavailable"); }
            public Optional<V12SigningCredential> findActive(String app,String org,String id) { return Optional.empty(); }
            public Optional<V12SigningCredential> findForOrder(String org,String order) { return Optional.empty(); }
            public boolean revoke(String org,String id) { throw new IllegalStateException("V1.2 key store unavailable"); }
        };
    }
}


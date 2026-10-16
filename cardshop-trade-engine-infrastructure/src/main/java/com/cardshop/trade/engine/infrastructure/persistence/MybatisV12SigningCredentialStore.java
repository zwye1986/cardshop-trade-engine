package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredential;
import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.V12SigningKeyMapper;
import com.cardshop.trade.engine.infrastructure.security.V12KeyEnvelope;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * V1.2历史密钥的加密持久化；对应详细设计「V1.2 密钥存储实施约束」。
 * 不设置token TTL，不缓存撤销状态；数据库异常直接向调用边界传播，禁止以空密钥降级。
 */
public final class MybatisV12SigningCredentialStore implements V12SigningCredentialStorePort {
    private final V12SigningKeyMapper mapper;
    private final V12KeyEnvelope envelope;
    public MybatisV12SigningCredentialStore(V12SigningKeyMapper mapper, V12KeyEnvelope envelope) {
        this.mapper = mapper; this.envelope = envelope;
    }
    @Override public void save(V12SigningCredential credential) {
        String ciphertext = envelope.encrypt(credential.signKey(), credential.keyId(), credential.appId(), credential.orgCode());
        if (mapper.insert(credential.keyId(),credential.appId(),credential.orgCode(),ciphertext,
                credential.requestExpiresAt().toEpochMilli()) != 1) {
            throw new IllegalStateException("V1.2 signing credential was not persisted");
        }
    }
    @Override public Optional<V12SigningCredential> findActive(String appId,String orgCode,String keyId) {
        if (appId == null || orgCode == null || keyId == null) return Optional.empty();
        return decode(mapper.findActive(appId,orgCode,keyId));
    }
    @Override public Optional<V12SigningCredential> findForOrder(String orgCode,String orderId) {
        if (orgCode == null || orderId == null) return Optional.empty();
        String keyId = mapper.findOrderKeyId(orgCode, orderId);
        if (keyId == null || keyId.isBlank()) return Optional.empty();
        return decode(mapper.findActiveForOrg(orgCode, keyId));
    }
    @Override public boolean revoke(String orgCode,String keyId) {
        return mapper.revoke(orgCode,keyId) == 1;
    }
    private Optional<V12SigningCredential> decode(Map<String,Object> row) {
        if (row == null || row.isEmpty()) return Optional.empty();
        String id=(String) row.get("key_id"), app=(String) row.get("app_id"), org=(String) row.get("org_code");
        String key=envelope.decrypt((String) row.get("encrypted_sign_key"),id,app,org);
        return Optional.of(new V12SigningCredential(id,app,org,key,
                Instant.ofEpochMilli(((Number) row.get("request_expires_at_ms")).longValue())));
    }
}

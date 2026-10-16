package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredential;
import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredentialStorePort;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.V12SigningKeyMapper;
import com.cardshop.trade.engine.infrastructure.security.V12KeyEnvelope;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 商户级签名密钥仓储；参见 docs/交行直充业务系统详细设计.md「3.1」「V1.2 密钥存储实施约束」。
 * 商户指针行锁保证并发首次领取只生成一把密钥；密钥不受 token TTL、应用ID和订单生命周期影响。
 * 不缓存撤销状态，数据库/解密异常向调用边界传播，不以空密钥或自动重建降级。
 */
public final class MybatisV12SigningCredentialStore implements V12SigningCredentialStorePort {
    private final V12SigningKeyMapper mapper;
    private final V12KeyEnvelope envelope;
    private final TransactionTemplate transaction;

    public MybatisV12SigningCredentialStore(V12SigningKeyMapper mapper, V12KeyEnvelope envelope,
                                            PlatformTransactionManager transactionManager) {
        this.mapper = Objects.requireNonNull(mapper);
        this.envelope = Objects.requireNonNull(envelope);
        transaction = new TransactionTemplate(Objects.requireNonNull(transactionManager));
        // 领取密钥的提交不依赖外层业务事务；刷新 token 或下单失败不能丢失已经交付的密钥。
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Override
    public V12SigningCredential getOrCreateCurrent(String orgCode) {
        requireText(orgCode);
        return Objects.requireNonNull(transaction.execute(status -> {
            mapper.ensureMerchant(orgCode);
            String current = mapper.lockCurrentKeyId(orgCode);
            if (current == null) return createAndPoint(orgCode);
            // 保留被撤销的当前指针，禁止 getToken 隐式恢复已撤销商户。
            return decode(mapper.findCurrentVersion(orgCode, current))
                    .orElseThrow(() -> new IllegalStateException("merchant current signing key unavailable"));
        }));
    }

    @Override
    public Optional<V12SigningCredential> findCurrent(String orgCode) {
        if (blank(orgCode)) return Optional.empty();
        String current = mapper.currentKeyId(orgCode);
        return blank(current) ? Optional.empty() : decode(mapper.findCurrentVersion(orgCode, current));
    }

    @Override
    public Optional<V12SigningCredential> findForCallback(String orgCode, String keyId) {
        if (blank(orgCode) || blank(keyId)) return Optional.empty();
        // 已落库回调保留原签名，允许读取 RETIRED 历史版本，REVOKED 始终拒绝。
        return decode(mapper.findCallbackVersion(orgCode, keyId));
    }

    @Override
    public V12SigningCredential rotate(String orgCode) {
        requireText(orgCode);
        return Objects.requireNonNull(transaction.execute(status -> {
            mapper.ensureMerchant(orgCode);
            String previous = mapper.lockCurrentKeyId(orgCode);
            if (previous == null) throw new IllegalStateException("merchant signing key not initialized");
            mapper.retire(orgCode, previous); // 已撤销版本无需转为 RETIRED，显式轮换可恢复服务。
            return createAndPoint(orgCode);
        }));
    }

    @Override
    public boolean revoke(String orgCode, String keyId) {
        requireText(orgCode);
        requireText(keyId);
        return Boolean.TRUE.equals(transaction.execute(status -> {
            mapper.ensureMerchant(orgCode);
            mapper.lockCurrentKeyId(orgCode);
            return mapper.revoke(orgCode, keyId) == 1;
        }));
    }

    private V12SigningCredential createAndPoint(String orgCode) {
        V12SigningCredential credential = V12SigningCredential.generate(orgCode);
        String encrypted = envelope.encrypt(credential.signKey(), credential.keyId(), orgCode);
        if (mapper.insertVersion(credential.keyId(), orgCode, encrypted) != 1
                || mapper.setCurrent(orgCode, credential.keyId()) != 1) {
            throw new IllegalStateException("merchant signing key was not persisted");
        }
        return credential;
    }

    private Optional<V12SigningCredential> decode(Map<String, Object> row) {
        if (row == null || row.isEmpty()) return Optional.empty();
        String id = (String) row.get("key_id");
        String org = (String) row.get("org_code");
        return Optional.of(new V12SigningCredential(id, org,
                envelope.decrypt((String) row.get("encrypted_sign_key"), id, org)));
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static void requireText(String value) {
        if (blank(value)) throw new IllegalArgumentException("merchant identity and keyId must be nonblank");
    }
}

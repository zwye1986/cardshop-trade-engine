package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredential;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.V12SigningKeyMapper;
import com.cardshop.trade.engine.infrastructure.security.V12KeyEnvelope;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.apache.ibatis.session.Configuration;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.AdditionalAnswers.delegatesTo;

/** 用真实行锁和事务验证商户凭证生命周期；对应详细设计「V1.2 密钥存储实施约束」。 */
class MybatisV12SigningCredentialStoreTest {
    private static final String MASTER = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    private MybatisV12SigningCredentialStore store;
    private V12SigningKeyMapper mapper;
    private JdbcTemplate jdbc;
    private DataSourceTransactionManager transactions;

    @BeforeEach
    void initializeDatabase() throws Exception {
        var dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE");
        // 连接全部关闭后仍保留内存库，避免建表和后续事务之间数据库被销毁。
        dataSource.setURL(dataSource.getURL() + ";DB_CLOSE_DELAY=-1");
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("""
            CREATE TABLE recharge_v12_merchant_key (
                org_code varchar(64) PRIMARY KEY, current_key_id varchar(64),
                created_at timestamp DEFAULT CURRENT_TIMESTAMP)
            """);
        jdbc.execute("""
            CREATE TABLE recharge_v12_merchant_signing_key (
                key_id varchar(64) PRIMARY KEY, org_code varchar(64) NOT NULL,
                encrypted_sign_key varchar(256) NOT NULL, key_status varchar(16) NOT NULL,
                created_at timestamp DEFAULT CURRENT_TIMESTAMP, retired_at timestamp, revoked_at timestamp)
            """);
        Configuration configuration = new Configuration();
        configuration.addMapper(V12SigningKeyMapper.class);
        var factory = new SqlSessionFactoryBean();
        factory.setDataSource(dataSource);
        factory.setConfiguration(configuration);
        mapper = new SqlSessionTemplate(factory.getObject()).getMapper(V12SigningKeyMapper.class);
        transactions = new DataSourceTransactionManager(dataSource);
        store = new MybatisV12SigningCredentialStore(mapper, new V12KeyEnvelope(MASTER), transactions);
    }

    @Test
    void concurrentFirstIssuanceCreatesExactlyOneEncryptedMerchantKey() throws Exception {
        try (var executor = Executors.newFixedThreadPool(8)) {
            var calls = new ArrayList<Callable<V12SigningCredential>>();
            for (int i = 0; i < 24; i++) calls.add(() -> store.getOrCreateCurrent("A36"));
            var credentials = new ArrayList<V12SigningCredential>();
            for (var result : executor.invokeAll(calls)) credentials.add(result.get());
            assertThat(credentials).allMatch(credentials.getFirst()::equals);
            assertThat(rowCount()).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT encrypted_sign_key FROM recharge_v12_merchant_signing_key",
                    String.class)).doesNotContain(credentials.getFirst().signKey());
        }
    }

    @Test
    void tokenRefreshAndStoreRestartReuseKeyWhileRotationRetainsHistoricalCallbacks() {
        var original = store.getOrCreateCurrent("A36");
        var restarted = new MybatisV12SigningCredentialStore(mapper, new V12KeyEnvelope(MASTER), transactions);
        assertThat(restarted.getOrCreateCurrent("A36")).isEqualTo(original);
        var current = restarted.rotate("A36");
        assertThat(current.keyId()).isNotEqualTo(original.keyId());
        assertThat(current.signKey()).isNotEqualTo(original.signKey());
        assertThat(restarted.findCurrent("A36")).contains(current);
        assertThat(restarted.findForCallback("A36", original.keyId())).contains(original);
        assertThat(restarted.findForCallback("OTHER", original.keyId())).isEmpty();
        assertThat(jdbc.queryForObject("SELECT key_status FROM recharge_v12_merchant_signing_key WHERE key_id=?",
                String.class, original.keyId())).isEqualTo("RETIRED");
    }

    @Test
    void revocationCannotBeUndoneByGetTokenButExplicitRotationCanRestoreService() {
        var original = store.getOrCreateCurrent("A36");
        assertThat(store.revoke("OTHER", original.keyId())).isFalse();
        assertThat(store.revoke("A36", original.keyId())).isTrue();
        assertThat(store.revoke("A36", original.keyId())).isFalse();
        assertThat(store.findCurrent("A36")).isEmpty();
        assertThat(store.findForCallback("A36", original.keyId())).isEmpty();
        assertThatThrownBy(() -> store.getOrCreateCurrent("A36")).isInstanceOf(IllegalStateException.class);
        assertThat(rowCount()).isEqualTo(1);
        var restored = store.rotate("A36");
        assertThat(store.getOrCreateCurrent("A36")).isEqualTo(restored);
        assertThat(store.findForCallback("A36", original.keyId())).isEmpty();
    }

    @Test
    void retiringThenRevokingOldVersionDoesNotChangeCurrentKey() {
        var old = store.getOrCreateCurrent("A36");
        var current = store.rotate("A36");
        assertThat(store.revoke("A36", old.keyId())).isTrue();
        assertThat(store.findForCallback("A36", old.keyId())).isEmpty();
        assertThat(store.getOrCreateCurrent("A36")).isEqualTo(current);
        assertThat(store.getOrCreateCurrent("OTHER").keyId()).isNotEqualTo(current.keyId());
    }

    @Test
    void failedPointerUpdateRollsBackInsertedVersionAndRetirement() {
        var original = store.getOrCreateCurrent("A36");
        V12SigningKeyMapper failingMapper = mock(V12SigningKeyMapper.class, delegatesTo(mapper));
        doReturn(0).when(failingMapper).setCurrent(eq("A36"), anyString());
        var failing = new MybatisV12SigningCredentialStore(failingMapper, new V12KeyEnvelope(MASTER), transactions);
        assertThatThrownBy(() -> failing.rotate("A36")).isInstanceOf(IllegalStateException.class);
        assertThat(rowCount()).isEqualTo(1);
        assertThat(store.findCurrent("A36")).contains(original);
    }

    private int rowCount() {
        return jdbc.queryForObject("SELECT count(*) FROM recharge_v12_merchant_signing_key", Integer.class);
    }
}

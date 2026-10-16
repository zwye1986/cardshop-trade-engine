package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.v1.crypto.V12SigningCredential;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.V12SigningKeyMapper;
import com.cardshop.trade.engine.infrastructure.security.V12KeyEnvelope;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** 历史密钥隔离和加密落库契约，参见详细设计「V1.2 密钥存储实施约束」。 */
class MybatisV12SigningCredentialStoreTest {
    private static final String KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    @Test
    void persistsOnlyCiphertextAndReadsExpiredKeyUsingOrderBinding() {
        var mapper = mock(V12SigningKeyMapper.class);
        var store = new MybatisV12SigningCredentialStore(mapper, new V12KeyEnvelope(KEY));
        when(mapper.insert(anyString(), anyString(), anyString(), anyString(), anyLong())).thenReturn(1);
        var credential = new V12SigningCredential("old", "app", "A36", KEY, Instant.EPOCH);
        store.save(credential);
        var encrypted = ArgumentCaptor.forClass(String.class);
        verify(mapper).insert(eq("old"), eq("app"), eq("A36"), encrypted.capture(), eq(0L));
        assertThat(encrypted.getValue()).doesNotContain(KEY);
        when(mapper.findOrderKeyId("A36", "ORDER-1")).thenReturn("old");
        when(mapper.findActiveForOrg("A36", "old")).thenReturn(Map.of(
                "key_id", "old", "app_id", "app", "org_code", "A36", "encrypted_sign_key", encrypted.getValue(),
                "request_expires_at_ms", 0L));
        assertThat(store.findForOrder("A36", "ORDER-1")).contains(credential);
        assertThat(store.findForOrder("OTHER", "ORDER-1")).isEmpty();
        assertThat(store.findForOrder("A36", "UNKNOWN")).isEmpty();
        verify(mapper, never()).findActiveForOrg(eq("OTHER"), anyString());
        verify(mapper, never()).findActive(anyString(), anyString(), anyString());
    }
}

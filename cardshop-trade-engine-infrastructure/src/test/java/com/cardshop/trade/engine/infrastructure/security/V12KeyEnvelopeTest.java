package com.cardshop.trade.engine.infrastructure.security;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class V12KeyEnvelopeTest {
    private static final String KEY="AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    @Test void encryptsWithFreshIvAndAuthenticatesMerchantIdentity() {
        var envelope=new V12KeyEnvelope(KEY);
        String cipher=envelope.encrypt(KEY,"key-id","org");
        assertThat(cipher).isNotNull().doesNotContain(KEY);
        assertThat(envelope.decrypt(cipher,"key-id","org")).isEqualTo(KEY);
        assertThat(envelope.encrypt(KEY,"key-id","org")).isNotEqualTo(cipher);
        assertThatThrownBy(() -> envelope.decrypt(cipher,"key-id","other-org")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> envelope.decrypt(cipher,"other-id","org")).isInstanceOf(IllegalStateException.class);
    }
    @Test void rejectsMissingOrMalformedMasterKey() {
        assertThatThrownBy(() -> new V12KeyEnvelope("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new V12KeyEnvelope(KEY+"=")).isInstanceOf(IllegalArgumentException.class);
    }
}

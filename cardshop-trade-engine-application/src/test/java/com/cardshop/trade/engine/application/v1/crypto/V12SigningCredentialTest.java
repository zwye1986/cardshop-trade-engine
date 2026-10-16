package com.cardshop.trade.engine.application.v1.crypto;

import java.time.Instant;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** 对应详细设计3.1：签发完整随机密钥，且敏感凭证不得经toString泄露。 */
class V12SigningCredentialTest {
    private static final Instant EXPIRES = Instant.parse("2026-10-16T13:00:00Z");

    @Test void generatesIndependentFullLengthKeysAndIdsEvenAtSameTime() {
        V12SigningCredential first = V12SigningCredential.generate("app", "org", EXPIRES);
        V12SigningCredential second = V12SigningCredential.generate("app", "org", EXPIRES);
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(first.signKey()).hasSize(43).doesNotContain("=");
        assertThat(Base64.getUrlDecoder().decode(first.signKey())).hasSize(32);
        assertThat(first.signKey()).isNotEqualTo(second.signKey());
        assertThat(first.keyId()).isNotEqualTo(second.keyId());
        assertThat(first.appId()).isEqualTo("app");
        assertThat(first.orgCode()).isEqualTo("org");
        assertThat(first.requestExpiresAt()).isEqualTo(EXPIRES);
        assertThat(first.toString()).doesNotContain(first.signKey());
    }

    @Test void requestExpiryDoesNotDecideHistoricalCallbackValidity() {
        V12SigningCredential credential = V12SigningCredential.generate("app", "org", EXPIRES);
        assertThat(credential).isNotNull();
        assertThat(credential.validForRequestAt(EXPIRES.minusSeconds(1))).isTrue();
        assertThat(credential.validForRequestAt(EXPIRES)).isFalse();
        assertThat(credential.validForRequestAt(EXPIRES.plusSeconds(1))).isFalse();
        // 过期后密钥本身仍可读取，回调有效性由订单绑定和撤销状态决定。
        assertThat(credential.signKey()).hasSize(43);
    }
}

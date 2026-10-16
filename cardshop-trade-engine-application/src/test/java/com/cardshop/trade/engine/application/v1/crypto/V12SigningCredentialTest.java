package com.cardshop.trade.engine.application.v1.crypto;

import java.util.Base64;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** 密钥只有商户与版本归属，完整32字节，与token TTL和订单无关，参见详细设计3.1。 */
class V12SigningCredentialTest {
    @Test void generatesIndependentFullLengthKeysAndIdsForExplicitInitialization() {
        var first = V12SigningCredential.generate("org");
        var second = V12SigningCredential.generate("org");
        assertThat(Base64.getUrlDecoder().decode(first.signKey())).hasSize(32);
        assertThat(first.signKey()).hasSize(43).doesNotContain("=").isNotEqualTo(second.signKey());
        assertThat(first.keyId()).isNotEqualTo(second.keyId());
        assertThat(first.orgCode()).isEqualTo("org");
        assertThat(first.toString()).doesNotContain(first.signKey());
    }

    @Test void containsNoTokenExpiryApplicationOrOrderBinding() {
        assertThat(V12SigningCredential.class.getRecordComponents())
                .extracting(java.lang.reflect.RecordComponent::getName)
                .containsExactly("keyId", "orgCode", "signKey");
    }
}

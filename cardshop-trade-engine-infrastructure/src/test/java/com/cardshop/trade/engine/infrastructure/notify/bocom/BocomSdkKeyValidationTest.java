package com.cardshop.trade.engine.infrastructure.notify.bocom;

import static org.assertj.core.api.Assertions.*;
import java.security.KeyPairGenerator;
import java.util.Base64;
import org.junit.jupiter.api.Test;

/** 签名密钥格式错误应在登记物理请求之前暴露；参见详细设计2104实施细化。 */
class BocomSdkKeyValidationTest {
    @Test void malformedRsaKeyCannotProduceReadyToSendClient() throws Exception {
        var generator=KeyPairGenerator.getInstance("RSA");generator.initialize(2048);
        var pair=generator.generateKeyPair();
        String publicKey=Base64.getEncoder().encodeToString(pair.getPublic().getEncoded());
        String privateKey=Base64.getEncoder().encodeToString(pair.getPrivate().getEncoded());
        String certificate=BocomTestTlsFixture.trustedCertificate().toString();
        assertThatIllegalArgumentException().isThrownBy(()->BocomSdkClientFactory.create("app","x".repeat(200),publicKey,
                "RSA",certificate,1000,1000));
        assertThatIllegalArgumentException().isThrownBy(()->BocomSdkClientFactory.create("app",privateKey,"x".repeat(200),
                "RSA",certificate,1000,1000));
    }
}

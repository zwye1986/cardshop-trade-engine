package com.cardshop.trade.engine.infrastructure.notify.bocom;

import com.bocom.api.DefaultBocomClient;
import com.bocom.api.utils.BocomSignature;
import com.bocom.api.utils.enums.EncryptType;
import com.sun.net.httpserver.HttpsServer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** JVM 级 SDK 信任状态在一个场景内验证，不以反射重置信任绕过生产约束。 */
class BocomSdkClientFactoryTest {
    private final Path temporary = BocomTestTlsFixture.directory();

    @Test
    void requiresStrictHttpsTrustAndHostnameAndRejectsCertificateSwitches() throws Exception {
        var generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        var pair = generator.generateKeyPair();
        String privateKey = Base64.getEncoder().encodeToString(pair.getPrivate().getEncoded());
        String publicKey = Base64.getEncoder().encodeToString(pair.getPublic().getEncoded());
        assertThatThrownBy(() -> BocomSdkClientFactory.create("app", privateKey, publicKey,
                "RSA", "unused.cer", 0, 2000)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BocomSdkClientFactory.create("app", privateKey, publicKey,
                "AUTO", "unused.cer", 1000, 2000)).isInstanceOf(IllegalArgumentException.class);
        Path trusted = BocomTestTlsFixture.trustedCertificate();
        Path untrusted = createServerCertificate("untrusted");
        Path expired = createServerCertificate("expired", "2020/01/01 00:00:00");
        assertThatThrownBy(() -> BocomSdkClientFactory.create("app", privateKey, publicKey,
                "RSA", temporary.resolve("missing.cer").toString(), 1000, 2000)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> BocomSdkClientFactory.create("app", privateKey, publicKey,
                "RSA", expired.toString(), 1000, 2000)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> BocomSdkClientFactory.create("app", privateKey, publicKey,
                "SM2", trusted.toString(), 1000, 2000)).isInstanceOf(IllegalArgumentException.class);
        DefaultBocomClient client = BocomSdkClientFactory.create("app", privateKey, publicKey,
                "RSA", trusted.toString(), 1000, 2000);
        assertThat(client.getConnectTimeout()).isEqualTo(1000);
        assertThat(client.getReadTimeout()).isEqualTo(2000);
        assertThat(BocomSdkClientFactory.create("another-app", privateKey, publicKey,
                "RSA", trusted.toString(), 1000, 2000)).isNotNull();
        assertThatThrownBy(() -> BocomSdkClientFactory.create("app", privateKey, publicKey,
                "RSA", untrusted.toString(), 1000, 2000)).isInstanceOf(IllegalStateException.class);
        var sm2 = com.bocom.api.security.keygen.SM2KeyGen.generateKeyPair();
        assertThat(BocomSdkClientFactory.create("app", com.bocom.api.security.keygen.SM2KeyGen.getPriKey(sm2),
                com.bocom.api.security.keygen.SM2KeyGen.getPubKey(sm2), "SM2", trusted.toString(), 1000, 2000)).isNotNull();
        var trustedServer = startServer("trusted", privateKey);
        var untrustedServer = startServer("untrusted", privateKey);
        try {
            var request = request("https://localhost:" + trustedServer.getAddress().getPort());
            assertThat(client.execute(request).getBody().getCode()).isEqualTo("200");
            request.setServiceUrl("https://127.0.0.1:" + trustedServer.getAddress().getPort() + "/api/trade/J_IEP_001_2104/v1");
            assertThatThrownBy(() -> client.execute(request)).isInstanceOf(com.bocom.api.BocomApiException.class)
                    .hasStackTraceContaining("No subject alternative names matching IP address");
            request.setServiceUrl("https://localhost:" + untrustedServer.getAddress().getPort() + "/api/trade/J_IEP_001_2104/v1");
            assertThatThrownBy(() -> client.execute(request)).isInstanceOf(com.bocom.api.BocomApiException.class)
                    .hasStackTraceContaining("PKIX path");
            request.setServiceUrl("http://localhost:" + trustedServer.getAddress().getPort() + "/api/trade/J_IEP_001_2104/v1");
            assertThatThrownBy(() -> client.execute(request)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(client::ignoreSSLHostnameVerifier).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> client.openCertVerify(untrusted.toString())).isInstanceOf(UnsupportedOperationException.class);
        } finally {
            trustedServer.stop(0);
            untrustedServer.stop(0);
        }
    }

    private Bocom2104Request request(String url) {
        var request = new Bocom2104Request();
        request.setServiceUrl(url + "/api/trade/J_IEP_001_2104/v1");
        var content = new Bocom2104Request.Content();
        content.setHead(new Bocom2104Request.Head());
        var body = new Bocom2104Request.Body();
        body.setTrxDataImprt("order_id:offline,out_order_sn:offline,total_price:1");
        body.setProtocol("offline-test");
        content.setBody(body);
        request.setBizContent(content);
        return request;
    }

    private Path createServerCertificate(String name) throws Exception { return createServerCertificate(name, null); }
    private Path createServerCertificate(String name, String startDate) throws Exception {
        return BocomTestTlsFixture.createCertificate(name, startDate);
    }

    private HttpsServer startServer(String name, String privateKey) throws Exception {
        var server = BocomTestTlsFixture.server(name);
        String biz = "{\"rsp_code\":\"0000\",\"head\":{},\"body\":{\"code\":\"200\",\"msg\":\"ok\"}}";
        String signature = BocomSignature.sign(EncryptType.RSA_AND_AES, biz, privateKey, "UTF-8");
        byte[] response = ("{\"rsp_biz_content\":" + biz + ",\"sign\":\"" + signature + "\"}").getBytes(StandardCharsets.UTF_8);
        server.createContext("/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/json;charset=UTF-8");
            exchange.sendResponseHeaders(200, response.length);
            try (var body = exchange.getResponseBody()) { body.write(response); }
        });
        server.start();
        return server;
    }
}

package com.cardshop.trade.engine.infrastructure.notify.bocom;

import com.bocom.api.DefaultBocomClient;
import com.bocom.api.BocomApiException;
import com.bocom.api.security.keygen.SM2KeyGen;
import com.bocom.api.security.spi.impl.NormalPublicKeyApi;
import com.bocom.api.utils.BocomHashMap;
import com.bocom.api.utils.BocomSignature;
import com.bocom.api.utils.WebUtils;
import com.bocom.api.utils.enums.EncryptType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.KeyPairGenerator;
import java.util.Base64;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

/** 使用真实官方 SDK 与临时密钥验证协议，参见详细设计「交行 2104 通知实施细化」。 */
class BocomSdkCompatibilityTest {
    @ParameterizedTest
    @ValueSource(strings = {"RSA", "SM2"})
    void signsOfficialEnvelopeAndVerifiesBankResponseWithManagedDependencies(String algorithm) throws Exception {
        String privateKey;
        String publicKey;
        EncryptType type;
        if (algorithm.equals("RSA")) {
            var generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            var pair = generator.generateKeyPair();
            privateKey = Base64.getEncoder().encodeToString(pair.getPrivate().getEncoded());
            publicKey = Base64.getEncoder().encodeToString(pair.getPublic().getEncoded());
            type = EncryptType.RSA_AND_AES;
        } else {
            var pair = SM2KeyGen.generateKeyPair();
            privateKey = SM2KeyGen.getPriKey(pair);
            publicKey = SM2KeyGen.getPubKey(pair);
            type = EncryptType.SM2_AND_SM4;
        }
        var client = new ProbeClient(privateKey, publicKey, type);
        var request = new Bocom2104Request();
        request.setServiceUrl("https://localhost/api/trade/J_IEP_001_2104/v1");
        var body = new Bocom2104Request.Body();
        body.setTrxDataImprt("order_id:bank-order,out_order_sn:merchant-order,total_price:1950");
        body.setProtocol("FWS202604090265");
        var biz = new Bocom2104Request.Content();
        biz.setHead(new Bocom2104Request.Head());
        biz.setBody(body);
        request.setBizContent(biz);
        var params = client.envelope(request);
        assertThat(request.getMethod()).isEqualTo("POST");
        assertThat(request.isNeedEncrypt()).isFalse();
        assertThat(new ObjectMapper().readTree(params.get("biz_content")).at("/body/trx_data_imprt").asText())
                .isEqualTo(body.getTrxDataImprt());
        var unsigned = new java.util.HashMap<>(params);
        String requestSignature = unsigned.remove("sign");
        assertThat(new NormalPublicKeyApi(publicKey).verify(
                WebUtils.buildOrderedSignStr("/api/trade/J_IEP_001_2104/v1", unsigned), requestSignature)).isTrue();
        String responseBiz = "{\"biz_state\":\"S\",\"rsp_code\":\"0000\",\"head\":{},\"body\":{\"code\":\"200\",\"msg\":\"成功\"}}";
        String sign = BocomSignature.sign(type, responseBiz, privateKey, "UTF-8");
        String responseJson = "{\"rsp_biz_content\":" + responseBiz + ",\"sign\":\"" + sign + "\"}";
        var response = client.read(request, responseJson);
        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getBody().getCode()).isEqualTo("200");
        assertThat(response.getBody().getMsg()).isEqualTo("成功");
        assertThatThrownBy(() -> client.read(request, responseJson.replace("成功", "篡改")))
                .isInstanceOf(BocomApiException.class);
    }

    private static final class ProbeClient extends DefaultBocomClient {
        private final EncryptType type;
        ProbeClient(String privateKey, String publicKey, EncryptType type) {
            super("offline-test-app", privateKey, publicKey, type);
            this.type = type;
        }
        BocomHashMap envelope(Bocom2104Request request) throws Exception {
            return prepareParams(request, "offline-message", "", type, null, null);
        }
        Bocom2104Response read(Bocom2104Request request, String json) throws BocomApiException {
            return parse(request, json, type, null);
        }
    }
}

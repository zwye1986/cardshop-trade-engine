package com.cardshop.trade.engine.api.v1.codec;

import com.cardshop.trade.engine.api.v1.dto.V1RechargeCreateOrderRequest;
import com.cardshop.trade.engine.api.v1.dto.V1RechargeOrderQueryRequest;
import org.bouncycastle.crypto.engines.SM4Engine;
import org.bouncycastle.crypto.params.KeyParameter;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class V1RequestBodyCodecTest {

    private final V1RequestBodyCodec codec = new V1RequestBodyCodec();

    @Test
    void decodesPlainCreateRechargeOrderBodyWithSnakeCaseFields() {
        String body = """
                {
                  "product_id": "73301280",
                  "out_order_sn": "OUT-1",
                  "account": "18186222336",
                  "timestamp": "1783224958",
                  "nonce_str": "nonce-32",
                  "notify_url": "http://notify",
                  "num": 1,
                  "order_price": 5200,
                  "product_type": 1,
                  "sign": "abc"
                }
                """;

        V1RechargeCreateOrderRequest request = codec.decodePlain(body, V1RechargeCreateOrderRequest.class);

        assertThat(request.productId()).isEqualTo("73301280");
        assertThat(request.outOrderSn()).isEqualTo("OUT-1");
        assertThat(request.notifyUrl()).isEqualTo("http://notify");
        assertThat(request.nonceStr()).isEqualTo("nonce-32");
    }

    @Test
    void decodesPlainQueryBodyWithSnakeCaseFields() {
        String body = """
                {"out_order_sn":"OUT-1","nonce_str":"nonce-32","timestamp":"1783224958","sign":"abc"}
                """;

        V1RechargeOrderQueryRequest request = codec.decodePlain(body, V1RechargeOrderQueryRequest.class);

        assertThat(request.outOrderSn()).isEqualTo("OUT-1");
        assertThat(request.nonceStr()).isEqualTo("nonce-32");
    }

    @Test
    void rejectsBlankPlainBody() {
        assertThatThrownBy(() -> codec.decodePlain("  ", V1RechargeCreateOrderRequest.class))
                .isInstanceOf(V1RequestBodyDecodeException.class)
                .hasMessageContaining("blank");
    }

    @Test
    void decodesLegacySm4PayloadWithIvPrefix() {
        String body = """
                {"out_order_sn":"OUT-1","nonce_str":"nonce-32","timestamp":"1783224958","sign":"abc"}
                """;
        String encryptedBody = encryptLegacySm4(body, "1234567890abcdef");

        V1RechargeOrderQueryRequest request = codec.decodeSm4(
                encryptedBody,
                "1234567890abcdef",
                V1RechargeOrderQueryRequest.class
        );

        assertThat(request.outOrderSn()).isEqualTo("OUT-1");
        assertThat(request.nonceStr()).isEqualTo("nonce-32");
    }

    @Test
    void rejectsSm4KeyWithInvalidLength() {
        assertThatThrownBy(() -> codec.decodeSm4("invalid", "short", V1RechargeOrderQueryRequest.class))
                .isInstanceOf(V1RequestBodyDecodeException.class)
                .hasMessageContaining("16");
    }

    private static String encryptLegacySm4(String data, String key) {
        byte[] iv = "abcdefghijklmnop".getBytes(StandardCharsets.UTF_8);
        byte[] cipher = sm4Cbc(true, pkcs7(data.getBytes(StandardCharsets.UTF_8)), key.getBytes(StandardCharsets.UTF_8), iv);
        byte[] payload = new byte[iv.length + cipher.length];
        System.arraycopy(iv, 0, payload, 0, iv.length);
        System.arraycopy(cipher, 0, payload, iv.length, cipher.length);
        return Base64.getEncoder().encodeToString(payload);
    }

    private static byte[] sm4Cbc(boolean encrypt, byte[] data, byte[] key, byte[] iv) {
        SM4Engine engine = new SM4Engine();
        engine.init(encrypt, new KeyParameter(key));
        byte[] result = data.clone();
        byte[] currentIv = iv.clone();
        for (int offset = 0; offset < result.length; offset += 16) {
            for (int i = 0; i < 16; i++) {
                result[offset + i] = (byte) (result[offset + i] ^ currentIv[i]);
            }
            engine.processBlock(result, offset, result, offset);
            currentIv = Arrays.copyOfRange(result, offset, offset + 16);
        }
        return result;
    }

    private static byte[] pkcs7(byte[] data) {
        int padding = 16 - data.length % 16;
        byte[] padded = Arrays.copyOf(data, data.length + padding);
        Arrays.fill(padded, data.length, padded.length, (byte) padding);
        return padded;
    }
}

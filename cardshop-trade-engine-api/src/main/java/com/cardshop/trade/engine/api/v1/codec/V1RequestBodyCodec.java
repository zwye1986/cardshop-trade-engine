package com.cardshop.trade.engine.api.v1.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bouncycastle.crypto.engines.SM4Engine;
import org.bouncycastle.crypto.params.KeyParameter;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/**
 * `/v1/cardapi` 请求体解码器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「20.13.3 SM4 解密与 SM3 验签」。
 * 兼容旧系统 {@code SM4CryptoUtils}：密文格式为 {@code Base64(16字节IV + SM4-CBC密文)}，
 * 当前 aesKey 必须为 16 位 ASCII 字符串。
 */
@Component
public class V1RequestBodyCodec {

    private static final int BLOCK_SIZE = 16;

    private final ObjectMapper objectMapper;

    public V1RequestBodyCodec() {
        this(new ObjectMapper());
    }

    V1RequestBodyCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * 解码未加密的 V1 JSON 请求体。
     */
    public <T> T decodePlain(String rawBody, Class<T> targetType) {
        if (!StringUtils.hasText(rawBody)) {
            throw new V1RequestBodyDecodeException("request body is blank");
        }
        try {
            return objectMapper.readValue(rawBody, targetType);
        } catch (Exception ex) {
            throw new V1RequestBodyDecodeException("request body json decode failed", ex);
        }
    }

    /**
     * 解码 SM4 密文请求体。
     */
    public <T> T decodeSm4(String encryptedBody, String aesKey, Class<T> targetType) {
        // TODO(D13): 接入 current/previous key 轮换窗口和密钥命中审计；参见 docs/交行直充业务系统详细设计.md「20.13.3 SM4 解密与 SM3 验签」。
        byte[] plainBytes = decryptLegacySm4(encryptedBody, aesKey);
        return decodePlain(new String(plainBytes, StandardCharsets.UTF_8), targetType);
    }

    private static byte[] decryptLegacySm4(String encryptedBody, String aesKey) {
        byte[] keyBytes = keyBytes(aesKey);
        byte[] payload = payloadBytes(encryptedBody);
        if (payload.length <= BLOCK_SIZE || payload.length % BLOCK_SIZE != 0) {
            throw new V1RequestBodyDecodeException("SM4 payload is invalid");
        }

        byte[] iv = Arrays.copyOfRange(payload, 0, BLOCK_SIZE);
        byte[] cipherBytes = Arrays.copyOfRange(payload, BLOCK_SIZE, payload.length);
        byte[] paddedPlain = sm4CbcDecrypt(cipherBytes, keyBytes, iv);
        return removePkcs7Padding(paddedPlain);
    }

    private static byte[] keyBytes(String aesKey) {
        byte[] keyBytes = aesKey == null ? new byte[0] : aesKey.getBytes(StandardCharsets.UTF_8);
        if (keyBytes.length != BLOCK_SIZE) {
            throw new V1RequestBodyDecodeException("SM4 key must be 16 ASCII bytes");
        }
        return keyBytes;
    }

    private static byte[] payloadBytes(String encryptedBody) {
        if (!StringUtils.hasText(encryptedBody)) {
            throw new V1RequestBodyDecodeException("SM4 request body is blank");
        }
        try {
            return Base64.getDecoder().decode(encryptedBody.trim());
        } catch (IllegalArgumentException ex) {
            throw new V1RequestBodyDecodeException("SM4 payload base64 decode failed", ex);
        }
    }

    private static byte[] sm4CbcDecrypt(byte[] cipherBytes, byte[] keyBytes, byte[] iv) {
        try {
            SM4Engine engine = new SM4Engine();
            engine.init(false, new KeyParameter(keyBytes));
            byte[] plain = new byte[cipherBytes.length];
            byte[] currentIv = iv.clone();
            for (int offset = 0; offset < cipherBytes.length; offset += BLOCK_SIZE) {
                engine.processBlock(cipherBytes, offset, plain, offset);
                for (int i = 0; i < BLOCK_SIZE; i++) {
                    plain[offset + i] = (byte) (plain[offset + i] ^ currentIv[i]);
                }
                currentIv = Arrays.copyOfRange(cipherBytes, offset, offset + BLOCK_SIZE);
            }
            return plain;
        } catch (Exception ex) {
            throw new V1RequestBodyDecodeException("SM4 decrypt failed", ex);
        }
    }

    private static byte[] removePkcs7Padding(byte[] paddedPlain) {
        if (paddedPlain.length == 0) {
            throw new V1RequestBodyDecodeException("SM4 padding is invalid");
        }
        int padding = paddedPlain[paddedPlain.length - 1] & 0xFF;
        if (padding < 1 || padding > BLOCK_SIZE || padding > paddedPlain.length) {
            throw new V1RequestBodyDecodeException("SM4 padding is invalid");
        }
        for (int i = paddedPlain.length - padding; i < paddedPlain.length; i++) {
            if ((paddedPlain[i] & 0xFF) != padding) {
                throw new V1RequestBodyDecodeException("SM4 padding is invalid");
            }
        }
        return Arrays.copyOf(paddedPlain, paddedPlain.length - padding);
    }
}

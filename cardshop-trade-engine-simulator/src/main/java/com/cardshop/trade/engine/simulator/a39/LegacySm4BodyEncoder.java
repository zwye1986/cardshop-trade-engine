package com.cardshop.trade.engine.simulator.a39;

import org.bouncycastle.crypto.engines.SM4Engine;
import org.bouncycastle.crypto.params.KeyParameter;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

/**
 * 旧 V1/V3 请求体 SM4-CBC 编码器。
 *
 * <p>默认模拟器发送明文 JSON；只有目标环境打开请求体 SM4 开关时才启用该编码。格式兼容
 * {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」：{@code Base64(16字节IV + SM4-CBC密文)}。
 */
final class LegacySm4BodyEncoder {

    private static final int BLOCK_SIZE = 16;
    private static final SecureRandom RANDOM = new SecureRandom();

    String encode(String rawJson, String aesKey) {
        byte[] keyBytes = keyBytes(aesKey);
        byte[] iv = new byte[BLOCK_SIZE];
        RANDOM.nextBytes(iv);
        byte[] cipher = sm4CbcEncrypt(pkcs7(rawJson.getBytes(StandardCharsets.UTF_8)), keyBytes, iv);
        byte[] payload = new byte[iv.length + cipher.length];
        System.arraycopy(iv, 0, payload, 0, iv.length);
        System.arraycopy(cipher, 0, payload, iv.length, cipher.length);
        return Base64.getEncoder().encodeToString(payload);
    }

    private static byte[] keyBytes(String aesKey) {
        byte[] keyBytes = aesKey == null ? new byte[0] : aesKey.getBytes(StandardCharsets.UTF_8);
        if (keyBytes.length != BLOCK_SIZE) {
            throw new IllegalArgumentException("SM4 aesKey must be 16 UTF-8 bytes");
        }
        return keyBytes;
    }

    private static byte[] sm4CbcEncrypt(byte[] plain, byte[] keyBytes, byte[] iv) {
        SM4Engine engine = new SM4Engine();
        engine.init(true, new KeyParameter(keyBytes));
        byte[] result = plain.clone();
        byte[] currentIv = iv.clone();
        for (int offset = 0; offset < result.length; offset += BLOCK_SIZE) {
            for (int index = 0; index < BLOCK_SIZE; index++) {
                result[offset + index] = (byte) (result[offset + index] ^ currentIv[index]);
            }
            engine.processBlock(result, offset, result, offset);
            currentIv = Arrays.copyOfRange(result, offset, offset + BLOCK_SIZE);
        }
        return result;
    }

    private static byte[] pkcs7(byte[] data) {
        int padding = BLOCK_SIZE - data.length % BLOCK_SIZE;
        byte[] padded = Arrays.copyOf(data, data.length + padding);
        Arrays.fill(padded, data.length, padded.length, (byte) padding);
        return padded;
    }
}

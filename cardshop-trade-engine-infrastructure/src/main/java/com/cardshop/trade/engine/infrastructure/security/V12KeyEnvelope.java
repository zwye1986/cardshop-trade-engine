package com.cardshop.trade.engine.infrastructure.security;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * 签名密钥的AES-256-GCM存储信封，参见详细设计「V1.2 密钥存储实施约束」。
 * 主密钥必须由独立部署secret注入；每次加密使用随机96bit IV，AAD绑定keyId和orgCode。
 * 明文不写日志，密文损坏或身份被调换时失败关闭。主密钥轮换需要先迁移历史密文，不能直接替换配置。
 */
public final class V12KeyEnvelope {
    private static final int KEY_BYTES=32, IV_BYTES=12, TAG_BITS=128;
    private static final String ALGORITHM="AES/GCM/NoPadding";
    private static final SecureRandom RANDOM=new SecureRandom();
    private final SecretKeySpec masterKey;

    public V12KeyEnvelope(String encoded) {
        if (encoded == null || !encoded.matches("[A-Za-z0-9_-]{43}")) {
            throw new IllegalArgumentException("V1.2 key encryption key must be canonical Base64URL of 32 bytes");
        }
        byte[] bytes=Base64.getUrlDecoder().decode(encoded);
        try {
            if (bytes.length != KEY_BYTES || !Base64.getUrlEncoder().withoutPadding().encodeToString(bytes).equals(encoded)) {
                throw new IllegalArgumentException("invalid V1.2 key encryption key encoding");
            }
            masterKey=new SecretKeySpec(bytes,"AES");
        } finally { Arrays.fill(bytes,(byte)0); }
    }

    public String encrypt(String key,String keyId,String orgCode) {
        byte[] iv=new byte[IV_BYTES]; RANDOM.nextBytes(iv);
        byte[] plain=key.getBytes(StandardCharsets.US_ASCII);
        try {
            Cipher cipher=Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE,masterKey,new GCMParameterSpec(TAG_BITS,iv));
            cipher.updateAAD(aad(keyId,orgCode));
            byte[] encrypted=cipher.doFinal(plain);
            byte[] result=Arrays.copyOf(iv,iv.length+encrypted.length);
            System.arraycopy(encrypted,0,result,iv.length,encrypted.length);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(result);
        } catch (GeneralSecurityException ex) {
            throw new IllegalStateException("V1.2 signing key encryption failed",ex);
        } finally { Arrays.fill(plain,(byte)0); }
    }

    public String decrypt(String encoded,String keyId,String orgCode) {
        byte[] payload=Base64.getUrlDecoder().decode(encoded);
        if (payload.length <= IV_BYTES + TAG_BITS / 8) throw new IllegalStateException("invalid key envelope");
        byte[] plain=null;
        try {
            Cipher cipher=Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE,masterKey,new GCMParameterSpec(TAG_BITS,payload,0,IV_BYTES));
            cipher.updateAAD(aad(keyId,orgCode));
            plain=cipher.doFinal(payload,IV_BYTES,payload.length-IV_BYTES);
            return new String(plain,StandardCharsets.US_ASCII);
        } catch (GeneralSecurityException ex) {
            throw new IllegalStateException("V1.2 signing key authentication failed",ex);
        } finally { if (plain != null) Arrays.fill(plain,(byte)0); }
    }

    private static byte[] aad(String id,String org) {
        // 长度前缀避免身份字段中的分隔符造成AAD歧义。
        return (id.length()+":"+id+org.length()+":"+org).getBytes(StandardCharsets.UTF_8);
    }
}

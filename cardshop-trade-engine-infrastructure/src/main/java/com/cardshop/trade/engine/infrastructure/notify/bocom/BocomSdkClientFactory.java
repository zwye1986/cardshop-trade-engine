package com.cardshop.trade.engine.infrastructure.notify.bocom;

import com.bocom.api.BocomApiException;
import com.bocom.api.BocomRequest;
import com.bocom.api.BocomResponse;
import com.bocom.api.DefaultBocomClient;
import com.bocom.api.utils.enums.EncryptType;
import com.bocom.api.utils.WebUtils;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HexFormat;
import java.security.KeyStore;
import java.time.Duration;
import javax.net.ssl.*;

/**
 * 官方 SDK 的唯一生产创建入口：先完成全 JVM 信任初始化，再允许构造可发送的客户端。
 * SDK 默认信任管理器不验链，openCertVerify 又使用静态全局状态，故不可按商户切换证书。
 * 参见 docs/交行直充业务系统详细设计.md「交行 2104 通知实施细化」。
 */
public final class BocomSdkClientFactory {
    private static final String RSA = "RSA";
    private static final String SM2 = "SM2";
    private static final String HTTPS = "https";
    // 官方 NormalLevelSignApi 以私钥字符串长度大于 100 区分 RSA；显式配置须与此规则一致。
    private static final int SDK_RSA_PRIVATE_KEY_LENGTH_BOUNDARY = 100;
    private static String trustedCertificateDigest;
    private static SSLContext trustedContext;

    private BocomSdkClientFactory() { }

    /**
     * TLS 证书是 X.509 信任锚，与 bankPublicKey 报文验签公钥严格分离。
     * 相同证书可复用；证书轮换必须停发并重启 JVM，禁止覆盖已经初始化的全局信任。
     */
    public static DefaultBocomClient create(String appId, String privateKey, String bankPublicKey,
                                           String algorithm, String tlsCertificatePath,
                                           int connectTimeoutMs, int readTimeoutMs) {
        requireText(appId, "appId");
        requireText(privateKey, "privateKey");
        requireText(bankPublicKey, "bankPublicKey");
        requireText(tlsCertificatePath, "tlsCertificatePath");
        if (connectTimeoutMs <= 0 || readTimeoutMs <= 0) {
            throw new IllegalArgumentException("SDK connect/read timeouts must be positive");
        }
        EncryptType type = switch (algorithm == null ? "" : algorithm) {
            case RSA -> {
                if (bankPublicKey.startsWith("04") || privateKey.length() <= SDK_RSA_PRIVATE_KEY_LENGTH_BOUNDARY) {
                    throw new IllegalArgumentException("RSA configuration contains SM2 key material");
                }
                validateRsaKeys(privateKey,bankPublicKey);
                yield EncryptType.RSA_AND_AES;
            }
            case SM2 -> {
                if (!bankPublicKey.matches("04[0-9a-fA-F]{128}") || !isSm2PrivateKey(privateKey)) {
                    throw new IllegalArgumentException("SM2 configuration requires raw hexadecimal SM2 keys");
                }
                var point=org.bouncycastle.asn1.gm.GMNamedCurves.getByName("sm2p256v1").getCurve()
                        .decodePoint(HexFormat.of().parseHex(bankPublicKey));
                if (point.isInfinity() || !point.isValid()) throw new IllegalArgumentException("invalid SM2 public point");
                yield EncryptType.SM2_AND_SM4;
            }
            default -> throw new IllegalArgumentException("SDK algorithm must explicitly be RSA or SM2");
        };
        var client = new StrictHttpsClient(appId, privateKey, bankPublicKey, type);
        client.configureTransport(initializeTrust(client, Path.of(tlsCertificatePath)),connectTimeoutMs,readTimeoutMs);
        client.setConnectTimeout(connectTimeoutMs);
        client.setReadTimeout(readTimeoutMs);
        return client;
    }

    /** 串行保护 SDK 静态 socketFactory，避免两个商户并发完成不同信任初始化。 */
    private static synchronized SSLContext initializeTrust(StrictHttpsClient client, Path certificatePath) {
        try {
            byte[] bytes = Files.readAllBytes(certificatePath);
            var certificate = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(bytes));
            certificate.checkValidity();
            byte[] encoded = certificate.getEncoded();
            String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(encoded));
            if (trustedCertificateDigest != null) {
                if (!trustedCertificateDigest.equals(digest)) {
                    throw new IllegalStateException("Conflicting JVM-wide Bocom TLS certificate; restart required");
                }
                return trustedContext;
            }
            // 将已验证的单个证书快照交给官方方法，防止校验后路径内容被替换而加载另一证书。
            Path snapshot = Files.createTempFile("bocom-tls-trust-", ".cer");
            try {
                Files.write(snapshot, encoded);
                client.initializeOfficialTrust(snapshot.toString());
                // SDK HTTP会跨目标递归重定向，因此为替代传输创建同一信任锚的严格上下文。
                var keyStore=KeyStore.getInstance(KeyStore.getDefaultType());
                keyStore.load(null,null);keyStore.setCertificateEntry("bocom-trust",certificate);
                var trust=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());trust.init(keyStore);
                var context=SSLContext.getInstance("TLSv1.2");context.init(null,trust.getTrustManagers(),null);
                trustedContext=context;
                trustedCertificateDigest = digest;
            } finally {
                Files.deleteIfExists(snapshot);
            }
            return trustedContext;
        } catch (Exception exception) {
            // 直接包装抛出，由调用层记录一次失败；绝不返回默认不验链客户端。
            throw new IllegalStateException("Bocom TLS trust initialization rejected", exception);
        }
    }

    /** 官方 SM2KeyGen 使用 BigInteger 补码，须兼容 00 前缀及不足 32 字节的合法标量。 */
    private static boolean isSm2PrivateKey(String privateKey) {
        if (!privateKey.matches("[0-9a-fA-F]{2,66}") || privateKey.length() % 2 != 0) {
            return false;
        }
        var scalar = new java.math.BigInteger(privateKey, 16);
        var order = org.bouncycastle.asn1.gm.GMNamedCurves.getByName("sm2p256v1").getN();
        return scalar.signum() > 0 && scalar.compareTo(order) < 0;
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
    private static void validateRsaKeys(String privateKey,String publicKey) {
        try {
            var keys=java.security.KeyFactory.getInstance("RSA");
            keys.generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(java.util.Base64.getDecoder().decode(privateKey)));
            keys.generatePublic(new java.security.spec.X509EncodedKeySpec(java.util.Base64.getDecoder().decode(publicKey)));
        } catch (Exception ex) {
            // 格式错误属于配置等待，不登记伪物理请求；不回显密钥及原始异常正文。
            throw new IllegalArgumentException("Bocom RSA key encoding invalid");
        }
    }

    /** 防止调用方修改 SDK 静态信任或放宽主机名校验，所有 execute 重载均汇入本方法。 */
    private static final class StrictHttpsClient extends DefaultBocomClient {
        private final EncryptType type;
        private HttpClient http;
        private Duration totalTimeout;
        private StrictHttpsClient(String appId, String privateKey, String bankPublicKey, EncryptType type) {
            super(appId, privateKey, bankPublicKey, type);
            this.type=type;
        }
        private void configureTransport(SSLContext context,int connectMs,int readMs) {
            var parameters=new SSLParameters();parameters.setProtocols(new String[]{"TLSv1.2"});
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            http=HttpClient.newBuilder().connectTimeout(Duration.ofMillis(connectMs)).sslContext(context)
                    .sslParameters(parameters).followRedirects(HttpClient.Redirect.NEVER).build();
            totalTimeout=Duration.ofMillis((long)connectMs+readMs);
        }
        private void initializeOfficialTrust(String path) throws Exception { super.openCertVerify(path); }
        @Override public void ignoreSSLHostnameVerifier() {
            throw new UnsupportedOperationException("Bocom hostname verification cannot be disabled");
        }
        @Override public void openCertVerify(String path) {
            throw new UnsupportedOperationException("Bocom TLS trust is initialized only by the factory");
        }
        @Override
        public <T extends BocomResponse> T execute(BocomRequest<T> request, String messageId, String authToken)
                throws BocomApiException {
            URI uri = URI.create(request.getServiceUrl());
            if (!HTTPS.equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null || uri.getUserInfo() != null) {
                throw new IllegalArgumentException("Bocom production requests require HTTPS with a valid host");
            }
            if (!"POST".equals(request.getMethod()) || request.isNeedEncrypt())
                throw new IllegalArgumentException("Bocom notification transport supports unencrypted POST only");
            try {
                // 官方SDK负责原生参数构造/签名、form编码和响应验签；只替换会越界重定向的HTTP步骤。
                var params=prepareParams(request,messageId,authToken,type,null,null);
                String form=WebUtils.buildQuery(params,StandardCharsets.UTF_8.name());
                var httpRequest=HttpRequest.newBuilder(uri).timeout(totalTimeout)
                        .header("Content-Type","application/x-www-form-urlencoded;charset=UTF-8")
                        .POST(HttpRequest.BodyPublishers.ofString(form,StandardCharsets.UTF_8)).build();
                // 总期限同时包住连接、TLS、响应头与正文，防止慢正文跨租约；超时主动取消网络请求。
                var pending=http.sendAsync(httpRequest,HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                HttpResponse<String> response;
                try { response=pending.get(totalTimeout.toMillis(),java.util.concurrent.TimeUnit.MILLISECONDS); }
                catch (java.util.concurrent.TimeoutException ex) {
                    pending.cancel(true);
                    throw new HttpTimeoutException("Bocom total request timeout");
                } catch (InterruptedException ex) {
                    pending.cancel(true);
                    throw ex;
                }
                if (response.statusCode()<200 || response.statusCode()>=300)
                    throw new BocomTransportException(response.statusCode(),false,null);
                T parsed;
                try { parsed=parse(request,response.body(),type,null); }
                catch (BocomApiException ex) {
                    throw new BocomTransportException(response.statusCode(),ex.getMessage()!=null && ex.getMessage().contains("sign verify"),ex);
                }
                if (parsed instanceof Bocom2104Response result) result.setTransportHttpStatus(response.statusCode());
                return parsed;
            } catch (BocomApiException ex) {
                throw ex;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new BocomApiException("Bocom request interrupted",ex);
            } catch (Exception ex) {
                // 直接上抛，由适配器统一脱敏及限频记录，禁止输出form或原始响应。
                throw new BocomApiException("Bocom request failed",ex);
            }
        }
    }
}

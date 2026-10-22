package com.cardshop.trade.engine.infrastructure.notify.bocom;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 银行发送开关与可信配置；参见详细设计「交行 2104 通知实施细化」。
 * 密钥仅由环境/部署秘密注入credentials，任务只保存版本引用，不能持久化该配置对象。
 */
@ConfigurationProperties("trade.engine.notify.bocom")
public class BocomNotifyProperties {
    private boolean enabled;
    private String tlsTrustCertificatePath = "";
    private int connectTimeoutMs = 3000;
    private int readTimeoutMs = 3000;
    private Map<String, Merchant> merchants = new LinkedHashMap<>();
    private Map<String, Credential> credentials = new LinkedHashMap<>();
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean v) { enabled = v; }
    public String getTlsTrustCertificatePath() { return tlsTrustCertificatePath; }
    public void setTlsTrustCertificatePath(String v) { tlsTrustCertificatePath = v; }
    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public void setConnectTimeoutMs(int v) { connectTimeoutMs = v; }
    public int getReadTimeoutMs() { return readTimeoutMs; }
    public void setReadTimeoutMs(int v) { readTimeoutMs = v; }
    public Map<String, Merchant> getMerchants() { return merchants; }
    public void setMerchants(Map<String, Merchant> v) { merchants = v == null ? new LinkedHashMap<>() : v; }
    public Map<String, Credential> getCredentials() { return credentials; }
    public void setCredentials(Map<String, Credential> v) { credentials = v == null ? new LinkedHashMap<>() : v; }

    /** 每商户显式路由；注册存在但不完整时仍属于银行通道，不能退回调用方URL。 */
    public static class Merchant {
        private String routeVersion = "";
        private String gatewayUrl = "";
        private String protocol = "";
        private String appId = "";
        private String signatureAlgorithm = "RSA";
        private String privateKeyRef = "";
        private String bankPublicKeyRef = "";
        public String getRouteVersion() { return routeVersion; }
        public void setRouteVersion(String v) { routeVersion = v; }
        public String getGatewayUrl() { return gatewayUrl; }
        public void setGatewayUrl(String v) { gatewayUrl = v; }
        public String getProtocol() { return protocol; }
        public void setProtocol(String v) { protocol = v; }
        public String getAppId() { return appId; }
        public void setAppId(String v) { appId = v; }
        public String getSignatureAlgorithm() { return signatureAlgorithm; }
        public void setSignatureAlgorithm(String v) { signatureAlgorithm = v; }
        public String getPrivateKeyRef() { return privateKeyRef; }
        public void setPrivateKeyRef(String v) { privateKeyRef = v; }
        public String getBankPublicKeyRef() { return bankPublicKeyRef; }
        public void setBankPublicKeyRef(String v) { bankPublicKeyRef = v; }
    }
    /** 版本化凭据绑定商户/appId/算法，禁止历史任务借用别的应用或商户私钥。 */
    public static class Credential {
        private String orgCode = "";
        private String appId = "";
        private String signatureAlgorithm = "RSA";
        private String privateKey = "";
        private String bankPublicKey = "";
        public String getOrgCode() { return orgCode; }
        public void setOrgCode(String v) { orgCode = v; }
        public String getAppId() { return appId; }
        public void setAppId(String v) { appId = v; }
        public String getSignatureAlgorithm() { return signatureAlgorithm; }
        public void setSignatureAlgorithm(String v) { signatureAlgorithm = v; }
        public String getPrivateKey() { return privateKey; }
        public void setPrivateKey(String v) { privateKey = v; }
        public String getBankPublicKey() { return bankPublicKey; }
        public void setBankPublicKey(String v) { bankPublicKey = v; }
        @Override public String toString() { return "BocomCredential[redacted]"; }
    }
}

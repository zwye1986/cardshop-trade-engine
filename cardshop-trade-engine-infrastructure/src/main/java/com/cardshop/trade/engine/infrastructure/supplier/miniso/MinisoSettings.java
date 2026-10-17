package com.cardshop.trade.engine.infrastructure.supplier.miniso;

import java.net.URI;
import java.time.Duration;
import java.util.Set;

/**
 * 名创协议客户端配置，密钥只参与签名，禁止通过记录的默认字符串表示暴露。
 *
 * <p>见 {@code docs/交行直充业务系统详细设计.md}「名创 MINISO 专属协议实施约束（2026-10-17）」。
 * 外部地址强制 HTTPS；仅为本地 fake server 允许显式回环 HTTP 地址，禁止按 DNS 解析结果放宽限制。
 */
public record MinisoSettings(URI baseUri, String tenant, String tripartite, String version,
                             String key, Duration timeout) {

    private static final String REQUIRED_TENANT = "MINISO";
    private static final String REQUIRED_VERSION = "tripartite1.0";
    private static final Set<String> LOCAL_TEST_HOSTS = Set.of("localhost", "127.0.0.1", "[::1]", "::1");

    public MinisoSettings {
        if (!REQUIRED_TENANT.equals(tenant) || !REQUIRED_VERSION.equals(version)) {
            throw new IllegalArgumentException("Invalid MINISO protocol tenant or version");
        }
        if (tripartite == null || tripartite.isBlank() || !isHeaderValue(tripartite)) {
            throw new IllegalArgumentException("MINISO tripartite must be a nonblank HTTP header value");
        }
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("MINISO key is required");
        }
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("MINISO timeout must be positive");
        }
        if (baseUri == null || baseUri.getHost() == null || baseUri.getUserInfo() != null
                || baseUri.getRawQuery() != null || baseUri.getRawFragment() != null) {
            throw new IllegalArgumentException("Invalid MINISO base URI");
        }
        boolean secure = "https".equalsIgnoreCase(baseUri.getScheme());
        boolean localHttp = "http".equalsIgnoreCase(baseUri.getScheme())
                && LOCAL_TEST_HOSTS.stream().anyMatch(host -> host.equalsIgnoreCase(baseUri.getHost()));
        if (!secure && !localHttp) {
            throw new IllegalArgumentException("MINISO base URI must use HTTPS or explicit local test HTTP");
        }
    }

    private static boolean isHeaderValue(String value) {
        return value.chars().allMatch(character -> character >= 32 && character <= 126);
    }

    @Override
    public String toString() {
        return "MinisoSettings[tenant=" + tenant + ", version=" + version + ", credentials=redacted]";
    }
}

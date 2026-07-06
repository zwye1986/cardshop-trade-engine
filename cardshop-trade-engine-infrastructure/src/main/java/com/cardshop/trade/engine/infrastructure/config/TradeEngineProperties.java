package com.cardshop.trade.engine.infrastructure.config;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 交易引擎统一配置绑定对象。
 *
 * <p>配置前缀为 {@code trade.engine}，字段含义对应
 * {@code docs/交行直充业务系统详细设计.md} 的「7.4 核心配置项」和「20.10 Nacos 配置全量模板」。
 * 该类是本地 YAML、Nacos 配置和运行时代码之间的合同：新增字段时必须同步更新设计文档、Nacos 模板和相关测试。
 */
@ConfigurationProperties(prefix = "trade.engine")
public class TradeEngineProperties {

    /**
     * 当前运行环境标识，取值通常为 dev、test、prod；参见详细设计「7.1 环境划分」。
     */
    private String environment = "dev";

    /**
     * Redis 高可用、Lua 执行和写入保护配置；参见详细设计「9. Redis 设计落地」。
     */
    private final Redis redis = new Redis();

    /**
     * RabbitMQ 持久化消息和 Confirm 保护配置；参见详细设计「10. RabbitMQ 设计落地」。
     */
    private final Mq mq = new Mq();

    /**
     * 订单缓存 TTL、请求幂等哈希和配置变更重试配置；参见详细设计「12. 核心业务流程」。
     */
    private final Order order = new Order();

    /**
     * `/v1/cardapi` 灰度路由和旧系统代理配置；参见详细设计「3.1 `/v1/cardapi` 无侵入路由切流」。
     */
    private final Routing routing = new Routing();

    /**
     * 交行 V1 协议的加解密、验签、Token 和防重放配置；参见详细设计「20.13 `/v1/cardapi` 小粒度详细设计」。
     */
    private final V1 v1 = new V1();

    /**
     * 余额归属和对账容忍阈值配置；参见高可用设计「14.7 `mc_ext_depart.balance` 刷账协议」。
     */
    private final Balance balance = new Balance();

    /**
     * 数据库、MQ Confirm、供应商 I/O 线程池隔离配置；参见高可用设计「3.7 线程池与隔离」。
     */
    private final ThreadPool threadPool = new ThreadPool();

    /**
     * 测试环境故障注入开关；参见详细设计「15. 测试设计」。
     */
    private final FaultInjection faultInjection = new FaultInjection();

    /**
     * 健康检查扩展开关；参见详细设计「16.2 健康检查」。
     */
    private final Health health = new Health();

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public Redis getRedis() {
        return redis;
    }

    public Mq getMq() {
        return mq;
    }

    public Order getOrder() {
        return order;
    }

    public Routing getRouting() {
        return routing;
    }

    public V1 getV1() {
        return v1;
    }

    public Balance getBalance() {
        return balance;
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    public FaultInjection getFaultInjection() {
        return faultInjection;
    }

    public Health getHealth() {
        return health;
    }

    /**
     * Redis 运行参数。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「9.2 Key 规范」、
     * 「9.3 Redis HA Guard」和 {@code docs/交行直充业务系统高可用方案设计.md} 的「15. Redis 高可用设计」。
     */
    public static class Redis {

        /**
         * Redis 写入总开关；生产默认关闭，完成灰度和高可用验证后才能开启。
         */
        private boolean writeEnabled = true;

        /**
         * Redis WAIT 期望副本数，用于降低主从切换时已确认写丢失风险。
         */
        private int waitReplicas = 1;

        /**
         * Redis WAIT 最大等待毫秒数，避免下单链路被复制确认长期阻塞。
         */
        private int waitTimeoutMs = 100;

        /**
         * 待持久化订单按机构分桶数量，用于分片补偿任务扫描。
         */
        private int pendingOrgBucketCount = 128;

        /**
         * Redis 不可用时是否 fail-closed；直充下单链路默认拒绝写入以保护资金一致性。
         */
        private boolean unavailableFailClosed = true;

        /**
         * Lua 脚本执行慢日志阈值，超过该值应记录业务上下文用于排查热点 Key 或 Redis 抖动。
         */
        private int luaWarnThresholdMs = 30;

        /**
         * Redis Key 统一前缀，后续 Key 必须继续使用 hash tag 规则保证 Cluster 兼容。
         */
        private String keyPrefix = "cardshop:trade";

        public boolean isWriteEnabled() {
            return writeEnabled;
        }

        public void setWriteEnabled(boolean writeEnabled) {
            this.writeEnabled = writeEnabled;
        }

        public int getWaitReplicas() {
            return waitReplicas;
        }

        public void setWaitReplicas(int waitReplicas) {
            this.waitReplicas = waitReplicas;
        }

        public int getWaitTimeoutMs() {
            return waitTimeoutMs;
        }

        public void setWaitTimeoutMs(int waitTimeoutMs) {
            this.waitTimeoutMs = waitTimeoutMs;
        }

        public int getPendingOrgBucketCount() {
            return pendingOrgBucketCount;
        }

        public void setPendingOrgBucketCount(int pendingOrgBucketCount) {
            this.pendingOrgBucketCount = pendingOrgBucketCount;
        }

        public boolean isUnavailableFailClosed() {
            return unavailableFailClosed;
        }

        public void setUnavailableFailClosed(boolean unavailableFailClosed) {
            this.unavailableFailClosed = unavailableFailClosed;
        }

        public int getLuaWarnThresholdMs() {
            return luaWarnThresholdMs;
        }

        public void setLuaWarnThresholdMs(int luaWarnThresholdMs) {
            this.luaWarnThresholdMs = luaWarnThresholdMs;
        }

        public String getKeyPrefix() {
            return keyPrefix;
        }

        public void setKeyPrefix(String keyPrefix) {
            this.keyPrefix = keyPrefix;
        }
    }

    /**
     * RabbitMQ 持久化与 Confirm 参数。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「10. RabbitMQ 设计落地」和「20.9 RabbitMQ 声明契约」。
     */
    public static class Mq {

        /**
         * Redis 受理成功后投递异步落库消息的 Exchange。
         */
        private String persistExchange = "recharge.persist.exchange";

        /**
         * 异步落库消息 Routing Key。
         */
        private String persistRoutingKey = "recharge.persist.mysql";

        /**
         * Publisher Confirm 等待超时时间，超时后进入补偿或重试路径。
         */
        private int publishConfirmTimeoutMs = 500;

        /**
         * 发布失败同步重试次数，耗尽后依赖补投任务修复。
         */
        private int publishRetryTimes = 3;

        /**
         * Confirm 工作队列容量，用于保护应用内存并暴露积压风险。
         */
        private int confirmWorkerQueueCapacity = 10000;

        /**
         * MQ 发布熔断参数。
         */
        private final Breaker breaker = new Breaker();

        public String getPersistExchange() {
            return persistExchange;
        }

        public void setPersistExchange(String persistExchange) {
            this.persistExchange = persistExchange;
        }

        public String getPersistRoutingKey() {
            return persistRoutingKey;
        }

        public void setPersistRoutingKey(String persistRoutingKey) {
            this.persistRoutingKey = persistRoutingKey;
        }

        public int getPublishConfirmTimeoutMs() {
            return publishConfirmTimeoutMs;
        }

        public void setPublishConfirmTimeoutMs(int publishConfirmTimeoutMs) {
            this.publishConfirmTimeoutMs = publishConfirmTimeoutMs;
        }

        public int getPublishRetryTimes() {
            return publishRetryTimes;
        }

        public void setPublishRetryTimes(int publishRetryTimes) {
            this.publishRetryTimes = publishRetryTimes;
        }

        public int getConfirmWorkerQueueCapacity() {
            return confirmWorkerQueueCapacity;
        }

        public void setConfirmWorkerQueueCapacity(int confirmWorkerQueueCapacity) {
            this.confirmWorkerQueueCapacity = confirmWorkerQueueCapacity;
        }

        public Breaker getBreaker() {
            return breaker;
        }

        /**
         * MQ 发布熔断器配置。
         *
         * <p>参见高可用设计「7.2 可靠性要求」和「14.3 Redis 成功但 MQ confirm 失败协议」。
         */
        public static class Breaker {

            /**
             * 是否启用 MQ 发布熔断保护。
             */
            private boolean enabled = true;

            /**
             * 滚动窗口内触发熔断的失败次数阈值。
             */
            private int failureThreshold = 20;

            /**
             * 失败计数滚动窗口秒数。
             */
            private int rollingWindowSeconds = 30;

            /**
             * 熔断打开后的持续秒数。
             */
            private int openSeconds = 30;

            /**
             * 半开状态允许探测的请求数。
             */
            private int halfOpenPermits = 3;

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public int getFailureThreshold() {
                return failureThreshold;
            }

            public void setFailureThreshold(int failureThreshold) {
                this.failureThreshold = failureThreshold;
            }

            public int getRollingWindowSeconds() {
                return rollingWindowSeconds;
            }

            public void setRollingWindowSeconds(int rollingWindowSeconds) {
                this.rollingWindowSeconds = rollingWindowSeconds;
            }

            public int getOpenSeconds() {
                return openSeconds;
            }

            public void setOpenSeconds(int openSeconds) {
                this.openSeconds = openSeconds;
            }

            public int getHalfOpenPermits() {
                return halfOpenPermits;
            }

            public void setHalfOpenPermits(int halfOpenPermits) {
                this.halfOpenPermits = halfOpenPermits;
            }
        }
    }

    /**
     * 订单生命周期与幂等控制配置。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「12. 核心业务流程」和
     * {@code docs/交行直充业务系统高可用方案设计.md} 的「14.8 幂等协议」。
     */
    public static class Order {

        /**
         * Redis 待完成订单 TTL 天数。
         */
        private int pendingTtlDays = 7;

        /**
         * Redis 终态订单保留小时数。
         */
        private int terminalTtlHours = 72;

        /**
         * 新受理订单保护期秒数，保护期内补偿任务不应误处理仍在确认中的订单。
         */
        private int pendingProtectSeconds = 300;

        /**
         * 请求幂等哈希版本，用于未来算法升级兼容。
         */
        private String requestHashVersion = "v1";

        /**
         * 请求幂等哈希算法。
         */
        private String requestHashAlg = "SHA-256";

        /**
         * 配置变更导致商品或路由失效时，最小重试等待毫秒数。
         */
        private int configChangedRetryMinMs = 500;

        /**
         * 配置变更导致商品或路由失效时，最大重试等待毫秒数。
         */
        private int configChangedRetryMaxMs = 2000;

        public int getPendingTtlDays() {
            return pendingTtlDays;
        }

        public void setPendingTtlDays(int pendingTtlDays) {
            this.pendingTtlDays = pendingTtlDays;
        }

        public int getTerminalTtlHours() {
            return terminalTtlHours;
        }

        public void setTerminalTtlHours(int terminalTtlHours) {
            this.terminalTtlHours = terminalTtlHours;
        }

        public int getPendingProtectSeconds() {
            return pendingProtectSeconds;
        }

        public void setPendingProtectSeconds(int pendingProtectSeconds) {
            this.pendingProtectSeconds = pendingProtectSeconds;
        }

        public String getRequestHashVersion() {
            return requestHashVersion;
        }

        public void setRequestHashVersion(String requestHashVersion) {
            this.requestHashVersion = requestHashVersion;
        }

        public String getRequestHashAlg() {
            return requestHashAlg;
        }

        public void setRequestHashAlg(String requestHashAlg) {
            this.requestHashAlg = requestHashAlg;
        }

        public int getConfigChangedRetryMinMs() {
            return configChangedRetryMinMs;
        }

        public void setConfigChangedRetryMinMs(int configChangedRetryMinMs) {
            this.configChangedRetryMinMs = configChangedRetryMinMs;
        }

        public int getConfigChangedRetryMaxMs() {
            return configChangedRetryMaxMs;
        }

        public void setConfigChangedRetryMaxMs(int configChangedRetryMaxMs) {
            this.configChangedRetryMaxMs = configChangedRetryMaxMs;
        }
    }

    /**
     * `/v1/cardapi` 路由与灰度配置。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「3.1 `/v1/cardapi` 无侵入路由切流」和
     * 「20.13.1.1 `/v1/cardapi` 路由兼容设计」。
     */
    public static class Routing {

        /**
         * 路由开关；关闭时应回退旧链路或拒绝切流。
         */
        private boolean enabled = true;

        /**
         * 旧系统基础地址。该值属于环境配置，日志中只能记录是否已配置，不能输出明文。
         */
        private String legacyBaseUrl = "";

        /**
         * 代理旧系统请求的超时时间毫秒数。
         */
        private int proxyTimeoutMs = 3000;

        /**
         * 按机构编码灰度切流的白名单。
         */
        private List<String> grayOrgCodes = new ArrayList<>();

        /**
         * 按应用 ID 灰度切流的白名单。
         */
        private List<String> grayAppIds = new ArrayList<>();

        /**
         * 新服务支持接管的 `/v1/cardapi` 路径清单。
         */
        private List<String> supportedPaths = new ArrayList<>(List.of(
                "/v1/cardapi/auth/getToken",
                "/v1/cardapi/order/createRechargeOrder",
                "/v1/cardapi/order/getRechargeOrder",
                "/v1/cardapi/order/invalid"));

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getLegacyBaseUrl() {
            return legacyBaseUrl;
        }

        public void setLegacyBaseUrl(String legacyBaseUrl) {
            this.legacyBaseUrl = legacyBaseUrl;
        }

        public int getProxyTimeoutMs() {
            return proxyTimeoutMs;
        }

        public void setProxyTimeoutMs(int proxyTimeoutMs) {
            this.proxyTimeoutMs = proxyTimeoutMs;
        }

        public List<String> getGrayOrgCodes() {
            return grayOrgCodes;
        }

        public void setGrayOrgCodes(List<String> grayOrgCodes) {
            this.grayOrgCodes = grayOrgCodes;
        }

        public List<String> getGrayAppIds() {
            return grayAppIds;
        }

        public void setGrayAppIds(List<String> grayAppIds) {
            this.grayAppIds = grayAppIds;
        }

        public List<String> getSupportedPaths() {
            return supportedPaths;
        }

        public void setSupportedPaths(List<String> supportedPaths) {
            this.supportedPaths = supportedPaths;
        }
    }

    /**
     * 交行 V1 接口协议参数。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「20.13.2 V1 请求 DTO 和字段来源」、
     * 「20.13.3 SM4 解密与 SM3 验签」和「20.13.10 查单设计」。
     */
    public static class V1 {

        /**
         * 是否启用请求 SM4 解密。
         */
        private boolean requestSm4Enabled = true;

        /**
         * 请求时间戳允许的前后偏差秒数。
         */
        private int timestampAllowedSkewSeconds = 300;

        /**
         * nonce 防重放缓存 TTL 秒数。
         */
        private int nonceReplayTtlSeconds = 600;

        /**
         * Token 校验允许的时钟偏差秒数。
         */
        private int tokenClockSkewSeconds = 30;

        /**
         * 请求验签算法。
         */
        private String signAlg = "SM3";

        /**
         * V1 Token JWT 密钥。该值为敏感配置，禁止写入日志。
         */
        private String jwtSecret = "";

        /**
         * Token Redis Key 前缀。
         */
        private String tokenRedisKeyPrefix = "token:app_id:";

        /**
         * Token 有效期秒数，默认与旧系统 3 小时一致。
         */
        private int tokenTtlSeconds = 3 * 60 * 60;

        /**
         * Token claim 中订单 token key 的前缀。
         */
        private String tokenClaimKey = "order_token_key:";

        /**
         * 加解密和验签密钥来源配置。
         */
        private final Crypto crypto = new Crypto();

        public boolean isRequestSm4Enabled() {
            return requestSm4Enabled;
        }

        public void setRequestSm4Enabled(boolean requestSm4Enabled) {
            this.requestSm4Enabled = requestSm4Enabled;
        }

        public int getTimestampAllowedSkewSeconds() {
            return timestampAllowedSkewSeconds;
        }

        public void setTimestampAllowedSkewSeconds(int timestampAllowedSkewSeconds) {
            this.timestampAllowedSkewSeconds = timestampAllowedSkewSeconds;
        }

        public int getNonceReplayTtlSeconds() {
            return nonceReplayTtlSeconds;
        }

        public void setNonceReplayTtlSeconds(int nonceReplayTtlSeconds) {
            this.nonceReplayTtlSeconds = nonceReplayTtlSeconds;
        }

        public int getTokenClockSkewSeconds() {
            return tokenClockSkewSeconds;
        }

        public void setTokenClockSkewSeconds(int tokenClockSkewSeconds) {
            this.tokenClockSkewSeconds = tokenClockSkewSeconds;
        }

        public String getSignAlg() {
            return signAlg;
        }

        public void setSignAlg(String signAlg) {
            this.signAlg = signAlg;
        }

        public String getJwtSecret() {
            return jwtSecret;
        }

        public void setJwtSecret(String jwtSecret) {
            this.jwtSecret = jwtSecret;
        }

        public String getTokenRedisKeyPrefix() {
            return tokenRedisKeyPrefix;
        }

        public void setTokenRedisKeyPrefix(String tokenRedisKeyPrefix) {
            this.tokenRedisKeyPrefix = tokenRedisKeyPrefix;
        }

        public int getTokenTtlSeconds() {
            return tokenTtlSeconds;
        }

        public void setTokenTtlSeconds(int tokenTtlSeconds) {
            this.tokenTtlSeconds = tokenTtlSeconds;
        }

        public String getTokenClaimKey() {
            return tokenClaimKey;
        }

        public void setTokenClaimKey(String tokenClaimKey) {
            this.tokenClaimKey = tokenClaimKey;
        }

        public Crypto getCrypto() {
            return crypto;
        }

        /**
         * V1 加密验签密钥来源。
         *
         * <p>参见详细设计「20.13.3 SM4 解密与 SM3 验签」。
         */
        public static class Crypto {

            /**
             * SM4 密钥来源，默认使用商户密钥派生。
             */
            private String sm4KeySource = "MERCHANT_SECRET";

            /**
             * SM3 验签密钥来源，默认使用商户密钥派生。
             */
            private String signKeySource = "MERCHANT_SECRET";

            /**
             * 是否启用上一版本密钥兼容，用于密钥轮换灰度。
             */
            private boolean keyRotationPreviousEnabled = true;

            public String getSm4KeySource() {
                return sm4KeySource;
            }

            public void setSm4KeySource(String sm4KeySource) {
                this.sm4KeySource = sm4KeySource;
            }

            public String getSignKeySource() {
                return signKeySource;
            }

            public void setSignKeySource(String signKeySource) {
                this.signKeySource = signKeySource;
            }

            public boolean isKeyRotationPreviousEnabled() {
                return keyRotationPreviousEnabled;
            }

            public void setKeyRotationPreviousEnabled(boolean keyRotationPreviousEnabled) {
                this.keyRotationPreviousEnabled = keyRotationPreviousEnabled;
            }
        }
    }

    /**
     * 余额归属和对账配置。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md} 的「14.7 `mc_ext_depart.balance` 刷账协议」和
     * 「17.5 余额对账任务」。
     */
    public static class Balance {

        /**
         * 余额所有权模式；NEW 表示新交易引擎负责 Redis 预占和后续一致性维护。
         */
        private String ownerMode = "NEW";

        /**
         * 余额对账差异阈值，单位为分。
         */
        private int reconcileDiffThresholdCent = 100;

        public String getOwnerMode() {
            return ownerMode;
        }

        public void setOwnerMode(String ownerMode) {
            this.ownerMode = ownerMode;
        }

        public int getReconcileDiffThresholdCent() {
            return reconcileDiffThresholdCent;
        }

        public void setReconcileDiffThresholdCent(int reconcileDiffThresholdCent) {
            this.reconcileDiffThresholdCent = reconcileDiffThresholdCent;
        }
    }

    /**
     * 线程池隔离配置。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md} 的「3.7 线程池与隔离」。
     */
    public static class ThreadPool {

        /**
         * 数据库落库核心线程数。
         */
        private int dbCoreSize = 16;

        /**
         * 数据库落库最大线程数。
         */
        private int dbMaxSize = 32;

        /**
         * MQ Confirm 处理线程数。
         */
        private int mqConfirmWorkerSize = 8;

        /**
         * 供应商 I/O 线程数。
         */
        private int supplierIoSize = 32;

        /**
         * 通用任务队列容量。
         */
        private int queueCapacity = 10000;

        public int getDbCoreSize() {
            return dbCoreSize;
        }

        public void setDbCoreSize(int dbCoreSize) {
            this.dbCoreSize = dbCoreSize;
        }

        public int getDbMaxSize() {
            return dbMaxSize;
        }

        public void setDbMaxSize(int dbMaxSize) {
            this.dbMaxSize = dbMaxSize;
        }

        public int getMqConfirmWorkerSize() {
            return mqConfirmWorkerSize;
        }

        public void setMqConfirmWorkerSize(int mqConfirmWorkerSize) {
            this.mqConfirmWorkerSize = mqConfirmWorkerSize;
        }

        public int getSupplierIoSize() {
            return supplierIoSize;
        }

        public void setSupplierIoSize(int supplierIoSize) {
            this.supplierIoSize = supplierIoSize;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }
    }

    /**
     * 故障注入配置。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「15.3 压测」和验收前故障演练要求。
     */
    public static class FaultInjection {

        /**
         * 是否启用故障注入。生产环境必须保持关闭。
         */
        private boolean enabled = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    /**
     * 健康检查配置。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「16.2 健康检查」。
     */
    public static class Health {

        /**
         * 是否在 readiness 中检查外部依赖。D1 阶段默认关闭，避免本地无中间件时无法启动。
         */
        private boolean externalCheckEnabled = false;

        public boolean isExternalCheckEnabled() {
            return externalCheckEnabled;
        }

        public void setExternalCheckEnabled(boolean externalCheckEnabled) {
            this.externalCheckEnabled = externalCheckEnabled;
        }
    }
}

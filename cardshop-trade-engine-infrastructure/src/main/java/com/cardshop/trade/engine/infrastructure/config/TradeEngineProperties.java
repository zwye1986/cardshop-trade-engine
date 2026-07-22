package com.cardshop.trade.engine.infrastructure.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 交易引擎统一配置绑定对象。
 *
 * <p>配置前缀为 {@code trade.engine}，字段含义对应
 * {@code docs/交行直充业务系统详细设计.md} 的「11. 配置中心、部署与健康检查」和「11. 配置中心、部署与健康检查」。
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
     * RabbitMQ 持久化消息和 Confirm 保护配置；参见详细设计「7. RabbitMQ 持久化消息层」。
     */
    private final Mq mq = new Mq();

    /**
     * 订单缓存 TTL、请求幂等哈希和配置变更重试配置；参见详细设计「4. Application 用例编排层」。
     */
    private final Order order = new Order();

    /**
     * `/v1/cardapi` 新链路接管和旧系统代理配置；参见详细设计「3. API/V1 接入层」。
     */
    private final Routing routing = new Routing();

    /**
     * 交行 V1 协议的加解密、验签、Token 和防重放配置；参见详细设计「3. API/V1 接入层」。
     */
    private final V1 v1 = new V1();

    /**
     * 余额归属和对账容忍阈值配置；参见高可用设计「14.7 `mc_ext_depart.balance` 刷账协议」。
     */
    private final Balance balance = new Balance();

    /**
     * D9/D10 outbox 履约和限频参数；参见详细设计「9.1 派单 Outbox」。
     */
    private final Fulfillment fulfillment = new Fulfillment();

    /**
     * 余额 Delta 刷账 outbox 运行参数；参见详细设计「9.2 余额 Delta 刷账 Outbox」
     * 与「11. 配置中心、部署与健康检查」中 {@code trade.engine.outbox.*}。
     *
     * <p>派单履约租约/退避仍使用 {@link #fulfillment}；本配置族只服务
     * {@code recharge_balance_delta_outbox} 刷账任务，不提供旧混合表兼容开关。
     */
    private final Outbox outbox = new Outbox();

    /**
     * D10 商户回调任务参数；参见详细设计「9.3 回调设计」。
     */
    private final Notify notify = new Notify();

    /**
     * 供应商 Gateway 配置；参见详细设计「9.1 派单 Outbox」。
     */
    private final Supplier supplier = new Supplier();

    /**
     * 数据库、MQ Confirm、供应商 I/O 线程池隔离配置；参见高可用设计「3.7 线程池与隔离」。
     */
    private final ThreadPool threadPool = new ThreadPool();

    /**
     * 测试环境故障注入开关；参见详细设计「13. 测试与验收标准」。
     */
    private final FaultInjection faultInjection = new FaultInjection();

    /**
     * 健康检查扩展开关；参见详细设计「11. 配置中心、部署与健康检查」。
     */
    private final Health health = new Health();

    /**
     * dev/test 集成联调入口配置；参见详细设计「10.1 XXL-Job Handler 契约」。
     */
    private final Integration integration = new Integration();

    /**
     * XXL-Job 执行器配置；参见详细设计「10.1 XXL-Job Handler 契约」和高可用设计「17. 分片补偿与对账任务」。
     */
    private final XxlJob xxlJob = new XxlJob();

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

    public Fulfillment getFulfillment() {
        return fulfillment;
    }

    public Outbox getOutbox() {
        return outbox;
    }

    public Notify getNotify() {
        return notify;
    }

    public Supplier getSupplier() {
        return supplier;
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

    public Integration getIntegration() {
        return integration;
    }

    public XxlJob getXxlJob() {
        return xxlJob;
    }

    /**
     * Redis 运行参数。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「6.1 Redis Lua 入参与 Key 构造」、
     * 「6.3 Redis HA Guard 与状态未知窗口」和 {@code docs/交行直充业务系统高可用方案设计.md} 的「15. Redis 高可用设计」。
     */
    public static class Redis {

        /**
         * Redis 写入总开关；生产默认关闭，完成灰度和高可用验证后才能开启。
         */
        private boolean writeEnabled = false;

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
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「7. RabbitMQ 持久化消息层」和「7. RabbitMQ 持久化消息层」。
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
         * 异步落库 Consumer 订阅的持久化队列名称。
         *
         * <p>参见 {@code docs/交行直充业务系统详细设计.md}「7. RabbitMQ 持久化消息层」。
         * 该值需要和 broker 上声明的 Quorum Queue、Exchange Binding 保持一致；生产环境变更队列名时必须同步
         * Nacos、RabbitMQ 声明脚本和补投任务配置，避免消息发布成功但 Consumer 订阅旧队列。
         */
        private String persistQueue = "recharge.persist.mysql.queue";

        /**
         * 异步落库主队列的死信 Exchange。
         *
         * <p>参见 {@code docs/交行直充业务系统详细设计.md}「7. RabbitMQ 持久化消息层」。
         * Consumer 反序列化失败或 broker 重试策略耗尽后，消息应进入该死信交换机，避免毒消息阻塞主队列。
         */
        private String persistDeadLetterExchange = "recharge.persist.dlx";

        /**
         * 异步落库死信队列名称。
         *
         * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「12.5 故障模式与处理」。
         * DLQ 中的消息由后续 MySQL 落库补偿任务或人工排查处理，不能在正常 Consumer 中静默丢弃。
         */
        private String persistDeadLetterQueue = "recharge.persist.mysql.dlq";

        /**
         * 异步落库死信 Routing Key。
         *
         * <p>该值同时用于主队列 `x-dead-letter-routing-key` 和 DLQ 绑定，必须与 RabbitMQ broker 声明保持一致。
         */
        private String persistDeadLetterRoutingKey = "recharge.persist.mysql.dlq";

        /**
         * 异步落库 Consumer 端业务失败/异常后的最大重试次数。
         *
         * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「12.5 故障模式与处理」。
         * listener 通过 {@code x-retry-count} 消息头累计重试次数；达到该阈值后 {@code basicReject(requeue=false)}
         * 让消息进入 DLQ，避免 broker 立即 requeue 形成热重投死循环。首次投递不计入重试次数，
         * 因此实际最大处理次数为 {@code consumerMaxRetry + 1}。
         */
        private int consumerMaxRetry = 3;

        /**
         * 异步落库失败后的退避重试 Exchange。
         *
         * <p>主队列消费失败但尚未达到 {@link #consumerMaxRetry} 时，listener 会先把消息发布到该
         * retry exchange，并在 RabbitMQ confirm ack 后再 ack 原消息；retry queue 通过 TTL 和 DLX
         * 把消息投回主 exchange，避免立即热重投，同时避免“先 ack 原消息、后发布 retry 失败”的丢消息窗口。
         */
        private String persistRetryExchange = "recharge.persist.retry.exchange";

        /**
         * 异步落库失败后的退避重试队列。
         */
        private String persistRetryQueue = "recharge.persist.mysql.retry.queue";

        /**
         * 异步落库失败后进入 retry queue 的 Routing Key。
         */
        private String persistRetryRoutingKey = "recharge.persist.mysql.retry";

        /**
         * retry queue 消息退避毫秒数。
         */
        private int consumerRetryDelayMs = 30000;

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
         * 是否启用 Redis/MQ 到 MySQL 的批消费落库路径。
         *
         * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。该开关只控制业务
         * listener 互斥注册：默认 {@code false} 时保留单条消息一事务路径；开启时必须同步设置
         * {@code spring.rabbitmq.listener.simple.consumer-batch-enabled=true}，让 Spring Rabbit 容器实际传入
         * {@code List<Message>}。
         */
        private boolean persistBatchEnabled = false;

        /**
         * 批消费业务批大小。
         *
         * <p>默认 10，阶段 1 不建议超过 10。该值必须小于等于
         * {@code spring.rabbitmq.listener.simple.prefetch}，并应与
         * {@code spring.rabbitmq.listener.simple.batch-size} 保持一致，避免容器聚合批大小和业务验收口径漂移。
         */
        private int persistBatchSize = 10;

        /**
         * 批消费攒批等待上限，单位毫秒。
         *
         * <p>默认 200ms。对应 Spring AMQP {@code batchReceiveTimeout}：从收到本批第一条消息起，
         * 容器在两次 {@code nextMessage} 之间检查是否超过该上限；超时后提交当前 partial batch。
         * 该值越大越容易攒满批次，但低流量订单落库延迟也越大。不要把它和 {@link #persistBatchPollTimeoutMs}
         * 混用：盲目放大 poll timeout 会把低流量 partial batch 一起拖长。
         */
        private int persistBatchReceiveTimeoutMs = 200;

        /**
         * 批消费单次从 broker 拉取消息的等待上限，单位毫秒。
         *
         * <p>默认 200ms。对应 Spring AMQP {@code receiveTimeout}：每一轮 {@code nextMessage(receiveTimeout)}
         * 的阻塞上限。阶段 1 保持与攒批上限同量级即可；若单独放大该值，低流量时每一轮空等都会拉长
         * partial batch 的提交时间。
         */
        private int persistBatchPollTimeoutMs = 200;

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

        public String getPersistQueue() {
            return persistQueue;
        }

        public void setPersistQueue(String persistQueue) {
            this.persistQueue = persistQueue;
        }

        public String getPersistDeadLetterExchange() {
            return persistDeadLetterExchange;
        }

        public void setPersistDeadLetterExchange(String persistDeadLetterExchange) {
            this.persistDeadLetterExchange = persistDeadLetterExchange;
        }

        public String getPersistDeadLetterQueue() {
            return persistDeadLetterQueue;
        }

        public void setPersistDeadLetterQueue(String persistDeadLetterQueue) {
            this.persistDeadLetterQueue = persistDeadLetterQueue;
        }

        public String getPersistDeadLetterRoutingKey() {
            return persistDeadLetterRoutingKey;
        }

        public void setPersistDeadLetterRoutingKey(String persistDeadLetterRoutingKey) {
            this.persistDeadLetterRoutingKey = persistDeadLetterRoutingKey;
        }

        public int getConsumerMaxRetry() {
            return consumerMaxRetry;
        }

        public void setConsumerMaxRetry(int consumerMaxRetry) {
            this.consumerMaxRetry = consumerMaxRetry;
        }

        public String getPersistRetryExchange() {
            return persistRetryExchange;
        }

        public void setPersistRetryExchange(String persistRetryExchange) {
            this.persistRetryExchange = persistRetryExchange;
        }

        public String getPersistRetryQueue() {
            return persistRetryQueue;
        }

        public void setPersistRetryQueue(String persistRetryQueue) {
            this.persistRetryQueue = persistRetryQueue;
        }

        public String getPersistRetryRoutingKey() {
            return persistRetryRoutingKey;
        }

        public void setPersistRetryRoutingKey(String persistRetryRoutingKey) {
            this.persistRetryRoutingKey = persistRetryRoutingKey;
        }

        public int getConsumerRetryDelayMs() {
            return consumerRetryDelayMs;
        }

        public void setConsumerRetryDelayMs(int consumerRetryDelayMs) {
            this.consumerRetryDelayMs = consumerRetryDelayMs;
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

        public boolean isPersistBatchEnabled() {
            return persistBatchEnabled;
        }

        public void setPersistBatchEnabled(boolean persistBatchEnabled) {
            this.persistBatchEnabled = persistBatchEnabled;
        }

        public int getPersistBatchSize() {
            return persistBatchSize;
        }

        public void setPersistBatchSize(int persistBatchSize) {
            this.persistBatchSize = persistBatchSize;
        }

        public int getPersistBatchReceiveTimeoutMs() {
            return persistBatchReceiveTimeoutMs;
        }

        public void setPersistBatchReceiveTimeoutMs(int persistBatchReceiveTimeoutMs) {
            this.persistBatchReceiveTimeoutMs = persistBatchReceiveTimeoutMs;
        }

        public int getPersistBatchPollTimeoutMs() {
            return persistBatchPollTimeoutMs;
        }

        public void setPersistBatchPollTimeoutMs(int persistBatchPollTimeoutMs) {
            this.persistBatchPollTimeoutMs = persistBatchPollTimeoutMs;
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
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「4. Application 用例编排层」和
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
     * `/v1/cardapi` 路由与旧链路代理配置。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
     * 当前 dev/test 只接入 A39 交行机构，暂时不按灰度名单切流；灰度字段保留用于后续恢复灰度或生产切流。
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
         * 按机构编码灰度切流的白名单；当前临时接入口径不参与路由判定，仅保留配置兼容。
         */
        private List<String> grayOrgCodes = new ArrayList<>();

        /**
         * 按应用 ID 灰度切流的白名单；当前临时接入口径不参与路由判定，仅保留配置兼容。
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
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「14.1 V1 DTO 字段」、
     * 「3. API/V1 接入层」和「3. API/V1 接入层」。
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
         * 父商品选品快照逻辑过期、物理 TTL 和单飞刷新参数。
         */
        private final ProductCache productCache = new ProductCache();

        /**
         * 商户安全资料进程内短 TTL 本地缓存参数。
         */
        private final MerchantProfileCache merchantProfileCache = new MerchantProfileCache();

        /**
         * 商户配置启动白名单预热参数。
         */
        private final MerchantConfigPreheat merchantConfigPreheat = new MerchantConfigPreheat();

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

        public ProductCache getProductCache() {
            return productCache;
        }

        public MerchantProfileCache getMerchantProfileCache() {
            return merchantProfileCache;
        }

        public MerchantConfigPreheat getMerchantConfigPreheat() {
            return merchantConfigPreheat;
        }

        /**
         * V1 商品选品缓存参数。
         *
         * <p>参见详细设计「6. Redis 受理与缓存层」。默认 25 分钟软过期、30 分钟硬过期，
         * 满足运营确认的改价最长 30 分钟生效窗口。
         */
        public static class ProductCache {

            private Duration refreshAfter = Duration.ofMinutes(25);
            private Duration maxStaleAge = Duration.ofMinutes(30);
            private Duration physicalTtl = Duration.ofMinutes(60);
            private Duration refreshLockTtl = Duration.ofSeconds(5);
            private Duration refreshWaitTimeout = Duration.ofSeconds(1);
            private Duration negativeCacheTtl = Duration.ofMinutes(1);

            public Duration getRefreshAfter() {
                return refreshAfter;
            }

            public void setRefreshAfter(Duration refreshAfter) {
                this.refreshAfter = refreshAfter;
            }

            public Duration getMaxStaleAge() {
                return maxStaleAge;
            }

            public void setMaxStaleAge(Duration maxStaleAge) {
                this.maxStaleAge = maxStaleAge;
            }

            public Duration getPhysicalTtl() {
                return physicalTtl;
            }

            public void setPhysicalTtl(Duration physicalTtl) {
                this.physicalTtl = physicalTtl;
            }

            public Duration getRefreshLockTtl() {
                return refreshLockTtl;
            }

            public void setRefreshLockTtl(Duration refreshLockTtl) {
                this.refreshLockTtl = refreshLockTtl;
            }

            public Duration getRefreshWaitTimeout() {
                return refreshWaitTimeout;
            }

            public void setRefreshWaitTimeout(Duration refreshWaitTimeout) {
                this.refreshWaitTimeout = refreshWaitTimeout;
            }

            public Duration getNegativeCacheTtl() {
                return negativeCacheTtl;
            }

            public void setNegativeCacheTtl(Duration negativeCacheTtl) {
                this.negativeCacheTtl = negativeCacheTtl;
            }
        }

        /**
         * V1 商户安全资料进程内短 TTL 缓存参数。
         *
         * <p>参见详细设计「3. API/V1 接入层」。默认开启，用于降低下单 prepare 对
         * {@code mc_ext_depart} 的每单直读；不把 aesKey 写入 Redis。
         */
        public static class MerchantProfileCache {

            private boolean enabled = true;
            private Duration ttl = Duration.ofSeconds(60);
            private Duration negativeTtl = Duration.ofSeconds(10);
            private int maximumSize = 1024;

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public Duration getTtl() {
                return ttl;
            }

            public void setTtl(Duration ttl) {
                this.ttl = ttl;
            }

            public Duration getNegativeTtl() {
                return negativeTtl;
            }

            public void setNegativeTtl(Duration negativeTtl) {
                this.negativeTtl = negativeTtl;
            }

            public int getMaximumSize() {
                return maximumSize;
            }

            public void setMaximumSize(int maximumSize) {
                this.maximumSize = maximumSize;
            }
        }

        /**
         * V1 商户配置启动预热参数。
         *
         * <p>参见详细设计「6. Redis 受理与缓存层」。默认关闭，只允许按显式白名单写入
         * {@code merchant:config}，避免应用启动时意外扫描全量旧库或写入未灰度商户。
         */
        public static class MerchantConfigPreheat {

            /**
             * 是否在应用启动时执行商户配置预热。
             */
            private boolean enabled = false;

            /**
             * 需要预热的商户机构编码白名单。
             */
            private List<String> orgCodes = new ArrayList<>();

            /**
             * 预热失败是否阻止应用启动。
             */
            private boolean failOnError = true;

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public List<String> getOrgCodes() {
                return orgCodes;
            }

            public void setOrgCodes(List<String> orgCodes) {
                this.orgCodes = orgCodes == null ? new ArrayList<>() : new ArrayList<>(orgCodes);
            }

            public boolean isFailOnError() {
                return failOnError;
            }

            public void setFailOnError(boolean failOnError) {
                this.failOnError = failOnError;
            }
        }

        /**
         * V1 加密验签密钥来源。
         *
         * <p>参见详细设计「3. API/V1 接入层」。
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
     * 派单履约 outbox 与限频配置。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。
     * 仅作用于 {@code recharge_dispatch_outbox}；余额刷账见 {@link Outbox}。
     */
    public static class Fulfillment {

        private int outboxClaimLeaseSeconds = 120;
        private int outboxRetryBackoffSeconds = 5;
        private int frequencyRetryDelaySeconds = 30;
        private int frequencyLockTtlMinutes = 30;
        private int frequencySuccessMinimumHoldSeconds = 5;
        /**
         * 已 claim 履约事件的并行度。参见
         * {@code docs/superpowers/specs/2026-07-22-outbox-dispatch-parallel-fair-design.md}。
         */
        private int outboxParallelism = 8;
        /**
         * due 扫描相对 limit 的放大倍数，用于公平过滤后仍凑满 limit。
         */
        private int outboxScanOverfetchFactor = 3;

        public int getOutboxClaimLeaseSeconds() {
            return outboxClaimLeaseSeconds;
        }

        public void setOutboxClaimLeaseSeconds(int outboxClaimLeaseSeconds) {
            this.outboxClaimLeaseSeconds = outboxClaimLeaseSeconds;
        }

        public int getOutboxRetryBackoffSeconds() {
            return outboxRetryBackoffSeconds;
        }

        public void setOutboxRetryBackoffSeconds(int outboxRetryBackoffSeconds) {
            this.outboxRetryBackoffSeconds = outboxRetryBackoffSeconds;
        }

        public int getFrequencyRetryDelaySeconds() {
            return frequencyRetryDelaySeconds;
        }

        public void setFrequencyRetryDelaySeconds(int frequencyRetryDelaySeconds) {
            this.frequencyRetryDelaySeconds = frequencyRetryDelaySeconds;
        }

        public int getFrequencyLockTtlMinutes() {
            return frequencyLockTtlMinutes;
        }

        public void setFrequencyLockTtlMinutes(int frequencyLockTtlMinutes) {
            this.frequencyLockTtlMinutes = frequencyLockTtlMinutes;
        }

        public int getFrequencySuccessMinimumHoldSeconds() {
            return frequencySuccessMinimumHoldSeconds;
        }

        public void setFrequencySuccessMinimumHoldSeconds(int frequencySuccessMinimumHoldSeconds) {
            this.frequencySuccessMinimumHoldSeconds = frequencySuccessMinimumHoldSeconds;
        }

        public int getOutboxParallelism() {
            return outboxParallelism;
        }

        public void setOutboxParallelism(int outboxParallelism) {
            this.outboxParallelism = outboxParallelism;
        }

        public int getOutboxScanOverfetchFactor() {
            return outboxScanOverfetchFactor;
        }

        public void setOutboxScanOverfetchFactor(int outboxScanOverfetchFactor) {
            this.outboxScanOverfetchFactor = outboxScanOverfetchFactor;
        }
    }

    /**
     * 余额 Delta 刷账 outbox 运行参数。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.2 余额 Delta 刷账 Outbox」与
     * 「11. 配置中心、部署与健康检查」{@code trade.engine.outbox.*}；租约与退避约束见
     * {@code docs/交行直充业务系统高可用方案设计.md}「14.7 {@code mc_ext_depart.balance} 刷账协议」。
     *
     * <p>拆表是上线前目标模型，不提供旧 {@code recharge_outbox} 双写/读取开关。默认值必须保持低风险，
     * 仅允许通过 Nacos、环境变量或部署密钥覆盖。
     */
    public static class Outbox {

        private static final Logger log = LoggerFactory.getLogger(Outbox.class);

        /**
         * 详细设计默认退避序列 {@code 5,30,120,600}（秒）。
         */
        private static final String DEFAULT_BALANCE_DELTA_RETRY_BACKOFF_SECONDS = "5,30,120,600";

        private static final List<Duration> DEFAULT_BALANCE_DELTA_RETRY_BACKOFF_DURATIONS = List.of(
                Duration.ofSeconds(5),
                Duration.ofSeconds(30),
                Duration.ofSeconds(120),
                Duration.ofSeconds(600));

        /**
         * 余额 delta 刷账单轮抢占上限；对应
         * {@code trade.engine.outbox.balance-delta-flush-batch-size}，默认 500。
         */
        private int balanceDeltaFlushBatchSize = 500;

        /**
         * 余额刷账 CAS 租约秒数；对应
         * {@code trade.engine.outbox.balance-delta-claim-lease-seconds}，默认 120。
         * 必须覆盖单轮刷账事务 P99。
         */
        private int balanceDeltaClaimLeaseSeconds = 120;

        /**
         * 余额刷账失败退避序列（秒，逗号分隔）；对应
         * {@code trade.engine.outbox.balance-delta-retry-backoff-seconds}，默认 {@code 5,30,120,600}。
         */
        private String balanceDeltaRetryBackoffSeconds = DEFAULT_BALANCE_DELTA_RETRY_BACKOFF_SECONDS;

        public int getBalanceDeltaFlushBatchSize() {
            return balanceDeltaFlushBatchSize;
        }

        public void setBalanceDeltaFlushBatchSize(int balanceDeltaFlushBatchSize) {
            this.balanceDeltaFlushBatchSize = balanceDeltaFlushBatchSize;
        }

        public int getBalanceDeltaClaimLeaseSeconds() {
            return balanceDeltaClaimLeaseSeconds;
        }

        public void setBalanceDeltaClaimLeaseSeconds(int balanceDeltaClaimLeaseSeconds) {
            this.balanceDeltaClaimLeaseSeconds = balanceDeltaClaimLeaseSeconds;
        }

        public String getBalanceDeltaRetryBackoffSeconds() {
            return balanceDeltaRetryBackoffSeconds;
        }

        public void setBalanceDeltaRetryBackoffSeconds(String balanceDeltaRetryBackoffSeconds) {
            this.balanceDeltaRetryBackoffSeconds = balanceDeltaRetryBackoffSeconds;
        }

        /**
         * 解析逗号分隔退避秒数为 {@link Duration} 列表。
         *
         * <p>参见详细设计「9.2 余额 Delta 刷账 Outbox」「11. 配置中心」。空白配置回退设计默认序列；
         * 非空但含非法片段（非正整数）时必须 fail-fast，禁止静默降级掩盖 Nacos 配错。
         * 解析失败路径写 {@code error} 日志并抛出 {@link IllegalArgumentException}，符合生产 catch 留证规范。
         */
        public List<Duration> balanceDeltaRetryBackoffDurations() {
            if (balanceDeltaRetryBackoffSeconds == null || balanceDeltaRetryBackoffSeconds.isBlank()) {
                return DEFAULT_BALANCE_DELTA_RETRY_BACKOFF_DURATIONS;
            }
            List<Duration> parsed = new ArrayList<>();
            List<String> invalidTokens = new ArrayList<>();
            for (String token : balanceDeltaRetryBackoffSeconds.split(",")) {
                String trimmed = token == null ? "" : token.strip();
                if (trimmed.isEmpty()) {
                    continue;
                }
                try {
                    long seconds = Long.parseLong(trimmed);
                    if (seconds > 0L) {
                        parsed.add(Duration.ofSeconds(seconds));
                    } else {
                        invalidTokens.add(trimmed);
                    }
                } catch (NumberFormatException ex) {
                    invalidTokens.add(trimmed);
                    log.error(
                            "解析 trade.engine.outbox.balance-delta-retry-backoff-seconds 片段失败 rawConfig={}, token={}",
                            balanceDeltaRetryBackoffSeconds,
                            trimmed,
                            ex);
                }
            }
            if (parsed.isEmpty() && invalidTokens.isEmpty()) {
                // 仅逗号/空白，等价于未配置，回退设计默认序列。
                return DEFAULT_BALANCE_DELTA_RETRY_BACKOFF_DURATIONS;
            }
            if (!invalidTokens.isEmpty() || parsed.isEmpty()) {
                log.error(
                        "trade.engine.outbox.balance-delta-retry-backoff-seconds 非法，拒绝静默降级 rawConfig={}, invalidTokens={}",
                        balanceDeltaRetryBackoffSeconds,
                        invalidTokens);
                throw new IllegalArgumentException(
                        "trade.engine.outbox.balance-delta-retry-backoff-seconds must be a comma-separated list of positive seconds, raw="
                                + balanceDeltaRetryBackoffSeconds
                                + ", invalidTokens="
                                + invalidTokens);
            }
            return List.copyOf(parsed);
        }
    }

    /**
     * 商户回调任务配置。
     */
    public static class Notify {

        private int claimLeaseSeconds = 60;
        private int retryBackoffSeconds = 30;
        private int httpTimeoutMs = 3000;

        public int getClaimLeaseSeconds() {
            return claimLeaseSeconds;
        }

        public void setClaimLeaseSeconds(int claimLeaseSeconds) {
            this.claimLeaseSeconds = claimLeaseSeconds;
        }

        public int getRetryBackoffSeconds() {
            return retryBackoffSeconds;
        }

        public void setRetryBackoffSeconds(int retryBackoffSeconds) {
            this.retryBackoffSeconds = retryBackoffSeconds;
        }

        public int getHttpTimeoutMs() {
            return httpTimeoutMs;
        }

        public void setHttpTimeoutMs(int httpTimeoutMs) {
            this.httpTimeoutMs = httpTimeoutMs;
        }
    }

    /**
     * 供应商 Gateway 配置。
     */
    public static class Supplier {

        private boolean httpEnabled = false;
        private int defaultTimeoutMs = 3000;
        private Map<String, Channel> channels = new LinkedHashMap<>();

        public boolean isHttpEnabled() {
            return httpEnabled;
        }

        public void setHttpEnabled(boolean httpEnabled) {
            this.httpEnabled = httpEnabled;
        }

        public int getDefaultTimeoutMs() {
            return defaultTimeoutMs;
        }

        public void setDefaultTimeoutMs(int defaultTimeoutMs) {
            this.defaultTimeoutMs = defaultTimeoutMs;
        }

        public Map<String, Channel> getChannels() {
            return channels;
        }

        public void setChannels(Map<String, Channel> channels) {
            this.channels = channels == null ? new LinkedHashMap<>() : channels;
        }

        public static class Channel {

            private String dispatchUrl = "";
            private String queryUrl = "";
            private String invalidUrl = "";
            private Integer timeoutMs;

            public String getDispatchUrl() {
                return dispatchUrl;
            }

            public void setDispatchUrl(String dispatchUrl) {
                this.dispatchUrl = dispatchUrl;
            }

            public String getQueryUrl() {
                return queryUrl;
            }

            public void setQueryUrl(String queryUrl) {
                this.queryUrl = queryUrl;
            }

            public String getInvalidUrl() {
                return invalidUrl;
            }

            public void setInvalidUrl(String invalidUrl) {
                this.invalidUrl = invalidUrl;
            }

            public Integer getTimeoutMs() {
                return timeoutMs;
            }

            public void setTimeoutMs(Integer timeoutMs) {
                this.timeoutMs = timeoutMs;
            }
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
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「13. 测试与验收标准」和验收前故障演练要求。
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
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「11. 配置中心、部署与健康检查」。
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

    /**
     * 集成测试专用开关和保护阈值。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」和
     * {@code docs/集成测试方案.md}「8.5 Redis pending 补投联调设计」。这些配置只服务 dev/test 联调入口，
     * 生产环境默认不开启人工入口。
     */
    public static class Integration {

        /**
         * 人工触发 Redis pending 补投任务的 HTTP 响应超时，单位毫秒。
         */
        private int manualJobTimeoutMs = 30000;

        public int getManualJobTimeoutMs() {
            return manualJobTimeoutMs;
        }

        public void setManualJobTimeoutMs(int manualJobTimeoutMs) {
            this.manualJobTimeoutMs = manualJobTimeoutMs;
        }
    }

    /**
     * XXL-Job 执行器配置。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」。真实 handler
     * Bean 可以常驻应用上下文，便于 SDK 扫描；执行器网络注册必须由 {@link #enabled} 显式打开，避免 dev/test
     * 未配置调度中心时应用启动去绑定端口或注册错误地址。
     */
    public static class XxlJob {

        /**
         * 是否启动 XXL-Job executor。默认 false，dev/test 完成调度中心配置后再打开。
         */
        private boolean enabled = false;

        /**
         * XXL-Job admin 地址，多个地址用英文逗号分隔。
         */
        private String adminAddresses = "";

        /**
         * 调度中心访问 token。该值属于密钥，必须来自环境变量或 Nacos，不能提交明文。
         */
        private String accessToken = "";

        /**
         * 执行器网络与日志配置。
         */
        private final Executor executor = new Executor();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getAdminAddresses() {
            return adminAddresses;
        }

        public void setAdminAddresses(String adminAddresses) {
            this.adminAddresses = adminAddresses;
        }

        public String getAccessToken() {
            return accessToken;
        }

        public void setAccessToken(String accessToken) {
            this.accessToken = accessToken;
        }

        public Executor getExecutor() {
            return executor;
        }

        public static class Executor {

            /**
             * XXL-Job 执行器 AppName，需要和调度中心执行器配置一致。
             */
            private String appname = "cardshop-trade-engine";

            /**
             * 外部可访问地址。为空时由 XXL-Job SDK 根据 ip/port 生成。
             */
            private String address = "";

            /**
             * 执行器绑定 IP。为空时由 XXL-Job SDK 自动探测。
             */
            private String ip = "";

            /**
             * 执行器内嵌 RPC 端口。
             */
            private int port = 19999;

            /**
             * XXL-Job 执行日志目录。
             */
            private String logpath = "/data/logs/cardshop-trade-engine/xxl-job";

            /**
             * 执行日志保留天数。
             */
            private int logRetentionDays = 30;

            public String getAppname() {
                return appname;
            }

            public void setAppname(String appname) {
                this.appname = appname;
            }

            public String getAddress() {
                return address;
            }

            public void setAddress(String address) {
                this.address = address;
            }

            public String getIp() {
                return ip;
            }

            public void setIp(String ip) {
                this.ip = ip;
            }

            public int getPort() {
                return port;
            }

            public void setPort(int port) {
                this.port = port;
            }

            public String getLogpath() {
                return logpath;
            }

            public void setLogpath(String logpath) {
                this.logpath = logpath;
            }

            public int getLogRetentionDays() {
                return logRetentionDays;
            }

            public void setLogRetentionDays(int logRetentionDays) {
                this.logRetentionDays = logRetentionDays;
            }
        }
    }
}

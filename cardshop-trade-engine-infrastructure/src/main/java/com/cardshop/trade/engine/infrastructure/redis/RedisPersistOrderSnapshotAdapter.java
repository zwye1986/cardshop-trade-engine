package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshot;
import com.cardshop.trade.engine.application.recharge.PersistOrderSnapshotPort;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.util.StringUtils;

/**
 * Persist Consumer 使用的 Redis 完整订单快照读取适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。
 * 查单适配器只映射接口响应字段；Consumer 写 `mc_order` 需要商品、账号、回调、request_hash 元数据等完整字段，
 * 因此这里单独实现完整快照端口，避免复用查单快照导致 MySQL 落库信息缺失。
 */
public class RedisPersistOrderSnapshotAdapter implements PersistOrderSnapshotPort {

    private static final Logger log = LoggerFactory.getLogger(RedisPersistOrderSnapshotAdapter.class);

    private final RedisRechargeOrderQueryOperations queryOperations;
    private final RechargeRedisKeyBuilder keyBuilder;

    public RedisPersistOrderSnapshotAdapter(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            Duration timeout) {
        this(new ReactiveRedisPersistOrderSnapshotOperations(redisTemplate, timeout), keyBuilder);
    }

    RedisPersistOrderSnapshotAdapter(
            RedisRechargeOrderQueryOperations queryOperations,
            RechargeRedisKeyBuilder keyBuilder) {
        this.queryOperations = Objects.requireNonNull(queryOperations, "queryOperations must not be null");
        this.keyBuilder = Objects.requireNonNull(keyBuilder, "keyBuilder must not be null");
    }

    @Override
    public Optional<PersistOrderSnapshot> findLatestSnapshot(String orgCode, String outOrderSn, String orderId) {
        String key = keyBuilder.orderKey(orgCode, outOrderSn);
        Map<String, String> fields;
        try {
            fields = queryOperations.entries(key);
        } catch (RuntimeException ex) {
            log.error("Redis读取落库完整订单快照失败 orgCode={}, outOrderSn={}, orderId={}", orgCode, outOrderSn, orderId, ex);
            return Optional.empty();
        }
        if (fields.isEmpty() || !StringUtils.hasText(fields.get("order_id"))) {
            return Optional.empty();
        }
        if (StringUtils.hasText(orderId) && !orderId.equals(fields.get("order_id"))) {
            log.error("Redis落库快照订单号不匹配 orgCode={}, outOrderSn={}, expectOrderId={}, actualOrderId={}",
                    orgCode,
                    outOrderSn,
                    orderId,
                    fields.get("order_id"));
            return Optional.empty();
        }
        return Optional.of(new PersistOrderSnapshot(
                text(fields, "org_code", orgCode),
                text(fields, "app_id", ""),
                text(fields, "order_id", orderId),
                text(fields, "out_order_sn", outOrderSn),
                text(fields, "product_id", ""),
                text(fields, "selected_product_id", ""),
                text(fields, "account", ""),
                text(fields, "notify_url", ""),
                intValue(fields, "num", 1),
                longValue(fields, "order_price", 0L),
                text(fields, "product_name", ""),
                longValue(fields, "product_value", 0L),
                intValue(fields, "product_type", 1),
                text(fields, "coupon_type", ""),
                text(fields, "supplier_channel", ""),
                text(fields, "request_hash", ""),
                text(fields, "request_hash_version", "v1"),
                text(fields, "request_hash_alg", "SHA-256"),
                text(fields, "order_status", ""),
                text(fields, "validation_status", ""),
                text(fields, "fail_code", ""),
                text(fields, "fail_reason", ""),
                bool(fields, "dispatch_allowed", false),
                bool(fields, "balance_reserved", false),
                text(fields, "persist_status", ""),
                longValue(fields, "order_version", 1L),
                longValue(fields, "persisted_version", 0L),
                intValue(fields, "mq_publish_count", 0)));
    }

    private static String text(Map<String, String> fields, String key, String fallback) {
        String value = fields.get(key);
        return StringUtils.hasText(value) ? value : fallback;
    }

    private static long longValue(Map<String, String> fields, String key, long fallback) {
        String value = fields.get(key);
        return StringUtils.hasText(value) ? Long.parseLong(value) : fallback;
    }

    private static int intValue(Map<String, String> fields, String key, int fallback) {
        String value = fields.get(key);
        return StringUtils.hasText(value) ? Integer.parseInt(value) : fallback;
    }

    private static boolean bool(Map<String, String> fields, String key, boolean fallback) {
        String value = fields.get(key);
        if (!StringUtils.hasText(value)) {
            return fallback;
        }
        return "1".equals(value) || "true".equalsIgnoreCase(value);
    }

    private static final class ReactiveRedisPersistOrderSnapshotOperations implements RedisRechargeOrderQueryOperations {

        private final ReactiveStringRedisTemplate redisTemplate;
        private final Duration timeout;

        private ReactiveRedisPersistOrderSnapshotOperations(
                ReactiveStringRedisTemplate redisTemplate,
                Duration timeout) {
            this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
            this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
        }

        @Override
        public Map<String, String> entries(String key) {
            /*
             * Persist Consumer 是后台线程，不在 Netty 事件循环上执行；这里使用短超时阻塞读取 Redis hash，
             * 以便后续 Rabbit listener 维持清晰的同步 ack/nack 流程。
             */
            return redisTemplate.opsForHash()
                    .entries(key)
                    .collectMap(
                            entry -> String.valueOf(entry.getKey()),
                            entry -> String.valueOf(entry.getValue()))
                    .block(timeout);
        }
    }
}

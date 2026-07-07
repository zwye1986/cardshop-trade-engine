package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.util.StringUtils;

/**
 * Redis 订单快照查询适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.10 查单设计」。D4 阶段 Redis 是异步落库前
 * 的短期事实来源，查单和作废入口都先读取该 hash；MySQL 兜底在 D7/D8 落库闭环完成后补充。
 */
public class RedisRechargeOrderQueryAdapter implements RechargeOrderQueryPort {

    private static final Logger log = LoggerFactory.getLogger(RedisRechargeOrderQueryAdapter.class);

    private final RedisRechargeOrderQueryOperations queryOperations;
    private final RechargeRedisKeyBuilder keyBuilder;

    public RedisRechargeOrderQueryAdapter(
            ReactiveStringRedisTemplate redisTemplate,
            RechargeRedisKeyBuilder keyBuilder,
            Duration timeout) {
        this(new ReactiveRedisRechargeOrderQueryOperations(redisTemplate, timeout), keyBuilder);
    }

    RedisRechargeOrderQueryAdapter(
            RedisRechargeOrderQueryOperations queryOperations,
            RechargeRedisKeyBuilder keyBuilder) {
        this.queryOperations = queryOperations;
        this.keyBuilder = keyBuilder;
    }

    @Override
    public Optional<RechargeOrderSnapshot> findByOrgCodeAndOutOrderSn(String orgCode, String outOrderSn) {
        String key = keyBuilder.orderKey(orgCode, outOrderSn);
        Map<String, String> fields;
        try {
            fields = queryOperations.entries(key);
        } catch (RuntimeException ex) {
            log.error("Redis查单读取订单快照失败 orgCode={}, outOrderSn={}", orgCode, outOrderSn, ex);
            return Optional.empty();
        }
        if (fields.isEmpty() || !StringUtils.hasText(fields.get("order_id"))) {
            return Optional.empty();
        }
        return Optional.of(new RechargeOrderSnapshot(
                fields.getOrDefault("order_id", ""),
                fields.getOrDefault("out_order_sn", outOrderSn),
                fields.getOrDefault("order_status", ""),
                fields.getOrDefault("persist_status", ""),
                fields.getOrDefault("validation_status", ""),
                fields.getOrDefault("fail_code", ""),
                fields.getOrDefault("fail_reason", ""),
                "1".equals(fields.getOrDefault("dispatch_allowed", "0"))));
    }

    private static final class ReactiveRedisRechargeOrderQueryOperations implements RedisRechargeOrderQueryOperations {

        private final ReactiveStringRedisTemplate redisTemplate;
        private final Duration timeout;

        private ReactiveRedisRechargeOrderQueryOperations(
                ReactiveStringRedisTemplate redisTemplate,
                Duration timeout) {
            this.redisTemplate = redisTemplate;
            this.timeout = timeout;
        }

        @Override
        public Map<String, String> entries(String key) {
            /*
             * V1 查单/作废 use case 仍是同步 application 端口，controller 会把调用隔离到 boundedElastic。
             * 这里保留短超时 block 兼容现有接口形态；后续若 application 端口整体改成 Reactor，再移除此处阻塞。
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

interface RedisRechargeOrderQueryOperations {

    Map<String, String> entries(String key);
}

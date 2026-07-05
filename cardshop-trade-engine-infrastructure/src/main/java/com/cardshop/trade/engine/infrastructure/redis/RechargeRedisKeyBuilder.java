package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32;

/**
 * 直充 Redis Key 构造器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.6 Redis Lua 入参与 Key 构造」和
 * {@code docs/交行直充业务系统高可用方案设计.md}「5. 关键 Redis Key 设计」。下单 Lua 使用固定 KEYS
 * 位置语义，因此所有 key 必须由该类集中构造，避免 Java 适配器、补偿任务和 Lua 脚本分别拼接造成顺序漂移。
 */
public final class RechargeRedisKeyBuilder {

    private final String keyPrefix;
    private final int pendingOrgBucketCount;

    public RechargeRedisKeyBuilder(String keyPrefix, int pendingOrgBucketCount) {
        if (keyPrefix == null || keyPrefix.isBlank()) {
            throw new IllegalArgumentException("keyPrefix must not be blank");
        }
        if (pendingOrgBucketCount <= 0) {
            throw new IllegalArgumentException("pendingOrgBucketCount must be positive");
        }
        this.keyPrefix = trimTrailingColon(keyPrefix.strip());
        this.pendingOrgBucketCount = pendingOrgBucketCount;
    }

    /**
     * 构造 `accept_recharge_order.lua` 所需 KEYS，顺序必须与详细设计「20.6」保持一致。
     */
    public List<String> acceptKeys(AcceptRechargeOrderCommand command) {
        String orgTag = orgTag(command.orgCode());
        int bucket = pendingOrgBucket(command.orgCode());
        return List.of(
                key("recharge:" + orgTag + ":balance"),
                key("recharge:" + orgTag + ":idem:" + command.outOrderSn()),
                key("recharge:" + orgTag + ":order:" + command.outOrderSn()),
                key("recharge:" + orgTag + ":reservation:" + command.orderId()),
                key("recharge:" + orgTag + ":persist:pending"),
                key("recharge:" + orgTag + ":order:event_index:" + command.orderId()),
                key("recharge:" + orgTag + ":order:event:" + command.orderId() + ":1"),
                key("recharge:" + orgTag + ":nonce:" + command.nonceStr()),
                key("recharge:" + orgTag + ":merchant:config"),
                key("recharge:" + orgTag + ":product:" + command.productId() + ":config"),
                key("recharge:" + orgTag + ":balance:repairing_lock"),
                key("recharge:persist:pending:org_index:" + bucket));
    }

    /**
     * 按 CRC32 对商户 pending 索引分桶，保证 XXL-Job 后续可通过 org_index 分片扫描。
     */
    public int pendingOrgBucket(String orgCode) {
        if (orgCode == null || orgCode.isBlank()) {
            throw new IllegalArgumentException("orgCode must not be blank");
        }
        CRC32 crc32 = new CRC32();
        crc32.update(orgCode.getBytes(StandardCharsets.UTF_8));
        return Math.floorMod((int) crc32.getValue(), pendingOrgBucketCount);
    }

    private String key(String suffix) {
        return keyPrefix + ":" + suffix;
    }

    private static String orgTag(String orgCode) {
        return "{" + orgCode + "}";
    }

    private static String trimTrailingColon(String value) {
        String result = value;
        while (result.endsWith(":")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }
}

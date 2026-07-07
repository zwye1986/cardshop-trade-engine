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
     * 构造 `cancel_unconfirmed_accept.lua` 所需 KEYS。
     *
     * <p>参见详细设计「20.5 下单服务实现细节」第 8-10 步。WAIT 失败回滚必须和 pending/org_index
     * 维护在同一个 Lua 中完成，避免 Java 先删 pending 再删 org_index 时丢失新写入商户索引。
     */
    public List<String> cancelUnconfirmedAcceptKeys(AcceptRechargeOrderCommand command) {
        String orgTag = orgTag(command.orgCode());
        int bucket = pendingOrgBucket(command.orgCode());
        return List.of(
                key("recharge:" + orgTag + ":balance"),
                key("recharge:" + orgTag + ":idem:" + command.outOrderSn()),
                key("recharge:" + orgTag + ":order:" + command.outOrderSn()),
                key("recharge:" + orgTag + ":reservation:" + command.orderId()),
                key("recharge:" + orgTag + ":persist:pending"),
                key("recharge:" + orgTag + ":order:event_index:" + command.orderId()),
                key("recharge:" + orgTag + ":order:event:" + command.orderId() + ":2"),
                key("recharge:persist:pending:org_index:" + bucket));
    }

    /**
     * 构造 `mark_persist_queued.lua` 所需 KEYS。
     */
    public List<String> markPersistQueuedKeys(AcceptRechargeOrderCommand command) {
        String orgTag = orgTag(command.orgCode());
        return List.of(
                key("recharge:" + orgTag + ":order:" + command.outOrderSn()),
                key("recharge:" + orgTag + ":idem:" + command.outOrderSn()),
                key("recharge:" + orgTag + ":order:event_index:" + command.orderId()),
                key("recharge:" + orgTag + ":order:event:" + command.orderId() + ":2"));
    }

    /**
     * 构造 `mark_db_persisted.lua` 所需 KEYS。
     *
     * <p>参见详细设计「20.13.8 MySQL 异步落库」。Consumer 事务提交后必须原子推进 Redis
     * `persist_status=DB_PERSISTED`，并同时清理 pending 与 org_index，避免补投任务重复投递已经落库的订单。
     */
    public List<String> markDbPersistedKeys(AcceptRechargeOrderCommand command) {
        return persistCompletionKeys(command);
    }

    /**
     * 构造 `mark_persist_failed.lua` 所需 KEYS。
     *
     * <p>参见详细设计「20.13.8 MySQL 异步落库」。落库失败时不能删除 pending，只能刷新失败原因和下次重试时间，
     * 让补偿任务继续基于 pending/org_index 扫描。
     */
    public List<String> markPersistFailedKeys(AcceptRechargeOrderCommand command) {
        return persistCompletionKeys(command);
    }

    /**
     * 构造查单/作废入口读取的 Redis 订单快照 key。
     *
     * <p>参见详细设计「20.13.10 查单设计」。接口层只知道商户订单号，因此 Redis 查询必须基于
     * `orgCode + outOrderSn` 定位订单 hash；后续 MySQL 兜底也应沿用同一个业务主键。
     */
    public String orderKey(String orgCode, String outOrderSn) {
        return key("recharge:" + orgTag(orgCode) + ":order:" + outOrderSn);
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

    private List<String> persistCompletionKeys(AcceptRechargeOrderCommand command) {
        String orgTag = orgTag(command.orgCode());
        int bucket = pendingOrgBucket(command.orgCode());
        return List.of(
                key("recharge:" + orgTag + ":order:" + command.outOrderSn()),
                key("recharge:" + orgTag + ":idem:" + command.outOrderSn()),
                key("recharge:" + orgTag + ":persist:pending"),
                key("recharge:" + orgTag + ":order:event_index:" + command.orderId()),
                key("recharge:" + orgTag + ":order:event:" + command.orderId() + ":3"),
                key("recharge:persist:pending:org_index:" + bucket));
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

package com.cardshop.trade.engine.infrastructure.persistence;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxEvent;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxPort;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxCommand;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxPort;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 基于 MyBatis 的 outbox 扫描、抢占和作废事件写入适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.9 派单 Outbox」和「20.13.11 作废设计」。
 * outbox 的 {@code shard_key} 固定使用 {@code event_id}，避免不同任务混用分片语义。
 */
public class MybatisRechargeOutboxAdapter implements RechargeOutboxPort, InvalidRechargeOrderOutboxPort {

    private static final int DEFAULT_MAX_RETRY_COUNT = 10;
    private static final ZoneId LEGACY_CARDSHOP_DB_ZONE = ZoneId.of("Asia/Shanghai");

    private final RechargeOutboxMapper mapper;
    private final RechargeOutboxPersistWriter writer;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public MybatisRechargeOutboxAdapter(RechargeOutboxMapper mapper, ObjectMapper objectMapper, Clock clock) {
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        this.writer = mapper::insertIfAbsent;
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    MybatisRechargeOutboxAdapter(
            RechargeOutboxPersistWriter writer,
            ObjectMapper objectMapper,
            Clock clock) {
        this.mapper = null;
        this.writer = Objects.requireNonNull(writer, "writer must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public List<RechargeOutboxEvent> selectDueForShard(int shardTotal, int shardIndex, int limit) {
        return requireMapper().selectDueForShard(shardTotal, shardIndex, limit)
                .stream()
                .map(this::toEvent)
                .toList();
    }

    @Override
    public java.util.Optional<RechargeOutboxEvent> claim(RechargeOutboxEvent event, String workerId, Duration lease) {
        LocalDateTime lockedUntil = now().plusNanos(lease.toNanos());
        int updated = requireMapper().claimProcessing(event.eventId(), event.version(), workerId, lockedUntil);
        return updated == 1 ? java.util.Optional.of(event.claimedCopy()) : java.util.Optional.empty();
    }

    @Override
    public void markSuccess(RechargeOutboxEvent event) {
        requireMapper().update(null, new LambdaUpdateWrapper<RechargeOutboxDO>()
                .eq(RechargeOutboxDO::getEventId, event.eventId())
                .eq(RechargeOutboxDO::getVersion, event.version())
                .set(RechargeOutboxDO::getStatus, TaskStatus.SUCCESS.name())
                .set(RechargeOutboxDO::getLockedUntil, null)
                .set(RechargeOutboxDO::getLastError, "")
                .set(RechargeOutboxDO::getUpdatedAt, now())
                .setSql("version = version + 1"));
    }

    @Override
    public void markFailed(RechargeOutboxEvent event, String errorMessage, Duration nextRetryDelay) {
        requireMapper().update(null, new LambdaUpdateWrapper<RechargeOutboxDO>()
                .eq(RechargeOutboxDO::getEventId, event.eventId())
                .eq(RechargeOutboxDO::getVersion, event.version())
                .set(RechargeOutboxDO::getStatus, TaskStatus.FAILED.name())
                .set(RechargeOutboxDO::getNextRetryTime, now().plusNanos(nextRetryDelay.toNanos()))
                .set(RechargeOutboxDO::getLockedUntil, null)
                .set(RechargeOutboxDO::getLastError, truncate(errorMessage, 1000))
                .set(RechargeOutboxDO::getUpdatedAt, now())
                .setSql("retry_count = retry_count + 1")
                .setSql("version = version + 1"));
    }

    @Override
    public void enqueueInvalidOrder(InvalidRechargeOrderOutboxCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        LocalDateTime now = now();
        String eventId = "invalid-" + command.orderId();
        RechargeOutboxDO outbox = new RechargeOutboxDO();
        outbox.setEventId(eventId);
        outbox.setEventType(OutboxEventType.INVALID_ORDER.name());
        outbox.setOrgCode(command.orgCode());
        outbox.setOrderId(command.orderId());
        outbox.setOutOrderSn(command.outOrderSn());
        outbox.setPayload(invalidPayload(command));
        outbox.setStatus(TaskStatus.INIT.name());
        outbox.setRetryCount(0);
        outbox.setMaxRetryCount(DEFAULT_MAX_RETRY_COUNT);
        outbox.setNextRetryTime(now);
        outbox.setLockedBy(null);
        outbox.setLockedUntil(null);
        outbox.setVersion(0L);
        outbox.setLastError("");
        outbox.setShardKey(eventId);
        outbox.setCreatedAt(now);
        outbox.setUpdatedAt(now);
        writer.insertIfAbsent(outbox);
    }

    private RechargeOutboxEvent toEvent(RechargeOutboxDO outbox) {
        return new RechargeOutboxEvent(
                outbox.getEventId(),
                OutboxEventType.valueOf(outbox.getEventType()),
                outbox.getOrgCode(),
                outbox.getOrderId(),
                outbox.getOutOrderSn(),
                outbox.getPayload(),
                valueOrZero(outbox.getRetryCount()),
                outbox.getMaxRetryCount() == null ? DEFAULT_MAX_RETRY_COUNT : outbox.getMaxRetryCount(),
                outbox.getVersion() == null ? 0L : outbox.getVersion(),
                outbox.getNextRetryTime(),
                outbox.getLastError());
    }

    private String invalidPayload(InvalidRechargeOrderOutboxCommand command) {
        try {
            return objectMapper.writeValueAsString(Map.of(
                    "org_code", command.orgCode(),
                    "order_id", command.orderId(),
                    "out_order_sn", command.outOrderSn(),
                    "reason", command.reason() == null ? "" : command.reason()));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("serialize invalid order outbox payload failed", ex);
        }
    }

    private LocalDateTime now() {
        return LocalDateTime.ofInstant(clock.instant(), LEGACY_CARDSHOP_DB_ZONE);
    }

    private RechargeOutboxMapper requireMapper() {
        if (mapper == null) {
            throw new IllegalStateException("RechargeOutboxMapper is required for this operation");
        }
        return mapper;
    }

    private static int valueOrZero(Integer value) {
        return value == null ? 0 : value;
    }

    private static String truncate(String value, int maxLength) {
        if (value == null) {
            return "";
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}

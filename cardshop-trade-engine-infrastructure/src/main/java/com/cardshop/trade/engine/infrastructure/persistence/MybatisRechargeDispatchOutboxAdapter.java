package com.cardshop.trade.engine.infrastructure.persistence;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxEvent;
import com.cardshop.trade.engine.application.recharge.RechargeOutboxPort;
import com.cardshop.trade.engine.application.recharge.SupplierQueryOutboxCommand;
import com.cardshop.trade.engine.application.recharge.SupplierQueryOutboxPort;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxCommand;
import com.cardshop.trade.engine.application.v1.InvalidRechargeOrderOutboxPort;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeDispatchOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeDispatchOutboxMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 基于 {@code recharge_dispatch_outbox} 的派单履约 outbox 适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」「8.3 异步落库 Consumer」以及
 * 「9.3 作废设计」中 {@code INVALID_ORDER} 入队路径。本适配器只服务供应商履约生命周期
 * （{@code SUPPLIER_DISPATCH}/{@code SUPPLIER_QUERY}/{@code INVALID_ORDER}），不写入余额刷账事件；
 * 余额 delta 入队见 {@link MybatisBalanceDeltaOutboxWriteAdapter}
 * （详细设计「9.2 余额 Delta 刷账 Outbox」）。
 *
 * <p>分片键固定使用 {@code event_id}，与高可用方案 {@code docs/交行直充业务系统高可用方案设计.md}
 * 「17.2 分片规则」一致，避免不同任务混用分片语义。
 */
public class MybatisRechargeDispatchOutboxAdapter
        implements RechargeOutboxPort, SupplierQueryOutboxPort, InvalidRechargeOrderOutboxPort {

    private static final int DEFAULT_MAX_RETRY_COUNT = 10;
    private static final ZoneId LEGACY_CARDSHOP_DB_ZONE = ZoneId.of("Asia/Shanghai");

    private final RechargeDispatchOutboxMapper mapper;
    private final RechargeDispatchOutboxPersistWriter writer;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public MybatisRechargeDispatchOutboxAdapter(
            RechargeDispatchOutboxMapper mapper,
            ObjectMapper objectMapper,
            Clock clock) {
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        this.writer = mapper::insertIfAbsent;
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * 测试用构造：只注入写入桩，扫描/抢占路径仍要求真实 Mapper。
     */
    MybatisRechargeDispatchOutboxAdapter(
            RechargeDispatchOutboxPersistWriter writer,
            ObjectMapper objectMapper,
            Clock clock) {
        this.mapper = null;
        this.writer = Objects.requireNonNull(writer, "writer must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public List<RechargeOutboxEvent> selectDueForShard(int shardTotal, int shardIndex, int limit) {
        return selectDueForShard(shardTotal, shardIndex, limit, List.of(OutboxEventType.SUPPLIER_DISPATCH));
    }

    @Override
    public List<RechargeOutboxEvent> selectDueForShard(
            int shardTotal, int shardIndex, int limit, List<OutboxEventType> eventTypes) {
        List<String> types = eventTypes == null || eventTypes.isEmpty()
                ? List.of(OutboxEventType.SUPPLIER_DISPATCH.name())
                : eventTypes.stream().map(Enum::name).toList();
        return requireMapper().selectDueForShardByEventTypes(shardTotal, shardIndex, limit, types, now())
                .stream()
                .map(this::toEvent)
                .toList();
    }

    @Override
    public Optional<RechargeOutboxEvent> findDueByEventId(String eventId, OutboxEventType eventType) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        return Optional.ofNullable(requireMapper().selectDueByEventId(eventId, eventType.name(), now()))
                .map(this::toEvent);
    }

    @Override
    public Optional<RechargeOutboxEvent> claim(RechargeOutboxEvent event, String workerId, Duration lease) {
        LocalDateTime lockedUntil = now().plusNanos(lease.toNanos());
        int updated = requireMapper().claimProcessing(event.eventId(), event.version(), workerId, lockedUntil);
        return updated == 1 ? Optional.of(event.claimedCopy()) : Optional.empty();
    }

    @Override
    public void markSuccess(RechargeOutboxEvent event) {
        requireMapper().update(null, new LambdaUpdateWrapper<RechargeDispatchOutboxDO>()
                .eq(RechargeDispatchOutboxDO::getEventId, event.eventId())
                .eq(RechargeDispatchOutboxDO::getVersion, event.version())
                .set(RechargeDispatchOutboxDO::getStatus, TaskStatus.SUCCESS.name())
                .set(RechargeDispatchOutboxDO::getLockedUntil, null)
                .set(RechargeDispatchOutboxDO::getLastError, "")
                .set(RechargeDispatchOutboxDO::getUpdatedAt, now())
                .setSql("version = version + 1"));
    }

    @Override
    public void markFailed(RechargeOutboxEvent event, String errorMessage, Duration nextRetryDelay) {
        if (event.nextFailureReachesMaxRetry()) {
            markManualRequired(event, errorMessage);
            return;
        }
        requireMapper().update(null, new LambdaUpdateWrapper<RechargeDispatchOutboxDO>()
                .eq(RechargeDispatchOutboxDO::getEventId, event.eventId())
                .eq(RechargeDispatchOutboxDO::getVersion, event.version())
                .set(RechargeDispatchOutboxDO::getStatus, TaskStatus.FAILED.name())
                .set(RechargeDispatchOutboxDO::getNextRetryTime, now().plusNanos(nextRetryDelay.toNanos()))
                .set(RechargeDispatchOutboxDO::getLockedUntil, null)
                .set(RechargeDispatchOutboxDO::getLastError, truncate(errorMessage, 1000))
                .set(RechargeDispatchOutboxDO::getUpdatedAt, now())
                .setSql("retry_count = retry_count + 1")
                .setSql("version = version + 1"));
    }

    @Override
    public void markManualRequired(RechargeOutboxEvent event, String errorMessage) {
        LambdaUpdateWrapper<RechargeDispatchOutboxDO> wrapper = new LambdaUpdateWrapper<RechargeDispatchOutboxDO>()
                .eq(RechargeDispatchOutboxDO::getEventId, event.eventId())
                .eq(RechargeDispatchOutboxDO::getVersion, event.version())
                .set(RechargeDispatchOutboxDO::getStatus, TaskStatus.MANUAL_REQUIRED.name())
                .set(RechargeDispatchOutboxDO::getLockedUntil, null)
                .set(RechargeDispatchOutboxDO::getLastError, truncate(errorMessage, 1000))
                .set(RechargeDispatchOutboxDO::getUpdatedAt, now())
                .setSql("version = version + 1");
        if (!event.retryExhausted()) {
            wrapper.setSql("retry_count = retry_count + 1");
        }
        requireMapper().update(null, wrapper);
    }

    /**
     * 写入 {@code INVALID_ORDER} 可靠事件，供派单 Worker 异步调用供应商作废。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 作废设计」「9.1 派单 Outbox」。
     * {@code event_id} 固定为 {@code invalid-{orderId}}，{@code shard_key} 与之相同以保证分片稳定。
     */
    @Override
    public void enqueueInvalidOrder(InvalidRechargeOrderOutboxCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        LocalDateTime now = now();
        String eventId = "invalid-" + command.orderId();
        RechargeDispatchOutboxDO outbox = new RechargeDispatchOutboxDO();
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

    /**
     * 写入 {@code SUPPLIER_QUERY} 可靠事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。
     * {@code event_id} 唯一键保证 dispatch 重试或 Worker 宕机恢复时不会重复入查单事件。
     */
    @Override
    public void enqueueSupplierQuery(SupplierQueryOutboxCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        LocalDateTime now = now();
        RechargeDispatchOutboxDO outbox = new RechargeDispatchOutboxDO();
        outbox.setEventId(command.eventId());
        outbox.setEventType(OutboxEventType.SUPPLIER_QUERY.name());
        outbox.setOrgCode(command.orgCode());
        outbox.setOrderId(command.orderId());
        outbox.setOutOrderSn(command.outOrderSn());
        outbox.setPayload(supplierQueryPayload(command));
        outbox.setStatus(TaskStatus.INIT.name());
        outbox.setRetryCount(0);
        outbox.setMaxRetryCount(command.maxRetryCount());
        outbox.setNextRetryTime(now.plusNanos(command.initialDelay().toNanos()));
        outbox.setLockedBy(null);
        outbox.setLockedUntil(null);
        outbox.setVersion(0L);
        outbox.setLastError("");
        outbox.setShardKey(command.eventId());
        outbox.setCreatedAt(now);
        outbox.setUpdatedAt(now);
        writer.insertIfAbsent(outbox);
    }

    private RechargeOutboxEvent toEvent(RechargeDispatchOutboxDO outbox) {
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

    private String supplierQueryPayload(SupplierQueryOutboxCommand command) {
        try {
            return objectMapper.writeValueAsString(Map.of(
                    "org_code", command.orgCode(),
                    "order_id", command.orderId(),
                    "out_order_sn", command.outOrderSn(),
                    "account", command.account(),
                    "supplier_channel", command.supplierChannel(),
                    "source_payload", command.payload() == null ? "{}" : command.payload()));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("serialize supplier query outbox payload failed", ex);
        }
    }

    private LocalDateTime now() {
        return LocalDateTime.ofInstant(clock.instant(), LEGACY_CARDSHOP_DB_ZONE);
    }

    private RechargeDispatchOutboxMapper requireMapper() {
        if (mapper == null) {
            throw new IllegalStateException("RechargeDispatchOutboxMapper is required for this operation");
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

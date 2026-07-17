package com.cardshop.trade.engine.infrastructure.persistence;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyTask;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyTaskDraft;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyTaskPort;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerTaskPort;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeNotifyTaskDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeNotifyTaskMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 MyBatis 的商户回调任务适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.3 回调设计」。创建任务按 task_id
 * 幂等，发送前通过状态和租约抢占，失败按退避时间重试。
 */
public class MybatisRechargeNotifyTaskAdapter implements RechargeNotifyTaskPort, RechargeNotifyWorkerTaskPort {

    private static final Logger log = LoggerFactory.getLogger(MybatisRechargeNotifyTaskAdapter.class);
    private static final ZoneId LEGACY_CARDSHOP_DB_ZONE = ZoneId.of("Asia/Shanghai");

    private final RechargeNotifyTaskMapper mapper;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public MybatisRechargeNotifyTaskAdapter(RechargeNotifyTaskMapper mapper, ObjectMapper objectMapper, Clock clock) {
        this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public void insertIfAbsent(RechargeNotifyTaskDraft task) {
        /*
         * 订单 notify_url 为空时不落 recharge_notify_task，与详细设计「9.3 回调设计」一致。
         * Outbox Worker 已有前置跳过；此处再拦截一层，避免防御缺口写入空 URL 任务。
         */
        if (task.notifyUrl() == null || task.notifyUrl().isBlank()) {
            log.warn("跳过空notify_url回调任务写入 taskId={}, orderId={}, notifyType={}",
                    task.taskId(), task.orderId(), task.notifyType());
            return;
        }
        RechargeNotifyTaskDO entity = new RechargeNotifyTaskDO();
        entity.setTaskId(task.taskId());
        entity.setNotifyType(task.notifyType());
        entity.setOrgCode(task.orgCode());
        entity.setOrderId(task.orderId());
        entity.setOutOrderSn(task.outOrderSn());
        entity.setNotifyUrl(task.notifyUrl());
        entity.setBizStatus(task.bizStatus());
        entity.setPayload(json(task.payload()));
        entity.setStatus(TaskStatus.INIT.name());
        entity.setRetryCount(0);
        entity.setMaxRetryCount(task.maxRetryCount());
        entity.setNextRetryTime(task.nextRetryTime());
        entity.setLockedBy(null);
        entity.setLockedUntil(null);
        entity.setLastError("");
        entity.setCreatedAt(now());
        entity.setUpdatedAt(now());
        mapper.insertIfAbsent(entity);
    }

    @Override
    public List<RechargeNotifyTask> selectDueTasks(int limit) {
        return mapper.selectDueTasks(limit).stream().map(this::toTask).toList();
    }

    @Override
    public int closeBlankNotifyUrlTasks() {
        LocalDateTime now = now();
        return mapper.update(null, new LambdaUpdateWrapper<RechargeNotifyTaskDO>()
                .and(blank -> blank
                        .isNull(RechargeNotifyTaskDO::getNotifyUrl)
                        .or()
                        .eq(RechargeNotifyTaskDO::getNotifyUrl, ""))
                .in(RechargeNotifyTaskDO::getStatus,
                        TaskStatus.INIT.name(),
                        TaskStatus.FAILED.name(),
                        TaskStatus.PROCESSING.name(),
                        TaskStatus.MANUAL_REQUIRED.name())
                .set(RechargeNotifyTaskDO::getStatus, TaskStatus.SUCCESS.name())
                .set(RechargeNotifyTaskDO::getLockedBy, null)
                .set(RechargeNotifyTaskDO::getLockedUntil, null)
                .set(RechargeNotifyTaskDO::getLastError, "")
                .set(RechargeNotifyTaskDO::getUpdatedAt, now));
    }

    @Override
    public Optional<RechargeNotifyTask> claim(RechargeNotifyTask task, String workerId, Duration lease) {
        LocalDateTime now = now();
        int updated = mapper.update(null, new LambdaUpdateWrapper<RechargeNotifyTaskDO>()
                .eq(RechargeNotifyTaskDO::getTaskId, task.taskId())
                .and(wrapper -> wrapper
                        .and(due -> due
                                .in(RechargeNotifyTaskDO::getStatus, TaskStatus.INIT.name(), TaskStatus.FAILED.name())
                                .le(RechargeNotifyTaskDO::getNextRetryTime, now))
                        .or(zombie -> zombie
                                .eq(RechargeNotifyTaskDO::getStatus, TaskStatus.PROCESSING.name())
                                .and(lock -> lock
                                        .lt(RechargeNotifyTaskDO::getLockedUntil, now)
                                        .or()
                                        .isNull(RechargeNotifyTaskDO::getLockedUntil))))
                .set(RechargeNotifyTaskDO::getStatus, TaskStatus.PROCESSING.name())
                .set(RechargeNotifyTaskDO::getLockedBy, workerId)
                .set(RechargeNotifyTaskDO::getLockedUntil, now.plusNanos(lease.toNanos()))
                .set(RechargeNotifyTaskDO::getUpdatedAt, now));
        return updated == 1 ? Optional.of(task) : Optional.empty();
    }

    @Override
    public void markSuccess(RechargeNotifyTask task) {
        mapper.update(null, new LambdaUpdateWrapper<RechargeNotifyTaskDO>()
                .eq(RechargeNotifyTaskDO::getTaskId, task.taskId())
                .set(RechargeNotifyTaskDO::getStatus, TaskStatus.SUCCESS.name())
                .set(RechargeNotifyTaskDO::getLockedUntil, null)
                .set(RechargeNotifyTaskDO::getLastError, "")
                .set(RechargeNotifyTaskDO::getUpdatedAt, now()));
    }

    @Override
    public void markFailed(RechargeNotifyTask task, String errorMessage, Duration nextRetryDelay) {
        mapper.update(null, new LambdaUpdateWrapper<RechargeNotifyTaskDO>()
                .eq(RechargeNotifyTaskDO::getTaskId, task.taskId())
                .set(RechargeNotifyTaskDO::getStatus, TaskStatus.FAILED.name())
                .set(RechargeNotifyTaskDO::getNextRetryTime, now().plusNanos(nextRetryDelay.toNanos()))
                .set(RechargeNotifyTaskDO::getLockedUntil, null)
                .set(RechargeNotifyTaskDO::getLastError, truncate(errorMessage, 1000))
                .set(RechargeNotifyTaskDO::getUpdatedAt, now())
                .setSql("retry_count = retry_count + 1"));
    }

    private RechargeNotifyTask toTask(RechargeNotifyTaskDO entity) {
        return new RechargeNotifyTask(
                entity.getTaskId(),
                entity.getNotifyType(),
                entity.getOrgCode(),
                entity.getOrderId(),
                entity.getOutOrderSn(),
                entity.getNotifyUrl(),
                entity.getBizStatus(),
                entity.getPayload(),
                entity.getRetryCount() == null ? 0 : entity.getRetryCount(),
                entity.getMaxRetryCount() == null ? 10 : entity.getMaxRetryCount(),
                entity.getNextRetryTime(),
                entity.getLastError());
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("serialize notify task payload failed", ex);
        }
    }

    private LocalDateTime now() {
        return LocalDateTime.ofInstant(clock.instant(), LEGACY_CARDSHOP_DB_ZONE);
    }

    private static String truncate(String value, int maxLength) {
        if (value == null) {
            return "";
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}

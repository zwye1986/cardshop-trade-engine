package com.cardshop.trade.engine.infrastructure.timer;

import com.cardshop.trade.engine.application.recharge.RechargeDispatchTimerPort;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 {@link ScheduledThreadPoolExecutor} 的供应商履约 timer。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。本类参考 JAIN-SIP timer 的
 * executor 调度模型，但在业务侧增加 {@code eventType + eventId} 维度的 future 替换：同一事件被重新计算下一次
 * 唤醒时间时，只保留最新 future，避免压测下旧 timer 句柄滞留。timer 线程只负责到点投递，真正可能发生供应商
 * HTTP I/O 的 {@code processEvent} 会进入独立有界 worker executor；幂等、租约和多节点互斥仍由 MySQL outbox CAS 承担。
 */
public class ScheduledExecutorRechargeDispatchTimer implements RechargeDispatchTimerPort, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ScheduledExecutorRechargeDispatchTimer.class);

    private static final char KEY_SEPARATOR = '\0';
    private static final int MAX_POOL_SIZE = 64;
    /**
     * 默认本地待触发任务上限。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。timer 不保存业务事实，
     * 达到该上限时应拒绝新增内存 future，并依赖持久 outbox 的恢复扫描补漏。
     */
    public static final int DEFAULT_MAX_PENDING_TASKS = 200_000;
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;
    private static final long ERROR_LOG_SAMPLE_INTERVAL = 100L;

    private final TimerWorker worker;
    private final String workerId;
    private final int maxPendingTasks;
    private final ScheduledThreadPoolExecutor executor;
    private final ThreadPoolExecutor workerExecutor;
    private final ConcurrentMap<String, TimerHandle> futures = new ConcurrentHashMap<>();
    private final AtomicLong executionFailures = new AtomicLong();
    private final AtomicLong rejectedSchedules = new AtomicLong();
    private final AtomicLong rejectedWorkerExecutions = new AtomicLong();

    public ScheduledExecutorRechargeDispatchTimer(TimerWorker worker, String workerId, int poolSize) {
        this(worker, workerId, poolSize, DEFAULT_MAX_PENDING_TASKS);
    }

    public ScheduledExecutorRechargeDispatchTimer(
            TimerWorker worker,
            String workerId,
            int poolSize,
            int maxPendingTasks) {
        this.worker = Objects.requireNonNull(worker, "worker must not be null");
        this.workerId = requireText(workerId, "workerId");
        this.maxPendingTasks = requirePositive(maxPendingTasks, "maxPendingTasks");
        int normalizedPoolSize = normalizePoolSize(poolSize);
        this.executor = new ScheduledThreadPoolExecutor(
                normalizedPoolSize,
                daemonThreadFactory("recharge-dispatch-timer"));
        this.executor.setRemoveOnCancelPolicy(true);
        this.executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        this.executor.prestartAllCoreThreads();
        this.workerExecutor = new ThreadPoolExecutor(
                normalizedPoolSize,
                normalizedPoolSize,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(normalizedPoolSize),
                daemonThreadFactory("recharge-dispatch-worker"),
                new ThreadPoolExecutor.AbortPolicy());
        this.workerExecutor.prestartAllCoreThreads();
    }

    /**
     * 调度一个指定 outbox 事件；同一事件重复调度时，新 future 会替换旧 future。
     *
     * <p>参见详细设计「9.1 派单 Outbox」：timer 是热路径唤醒，不保存业务事实。因此替换旧 future 不会丢单；
     * 若进程在唤醒前退出，低频 XXL-Job 扫描会按 {@code next_retry_time} 从 outbox 恢复。
     */
    @Override
    public void schedule(String eventId, OutboxEventType eventType, Duration delay) {
        String normalizedEventId = requireText(eventId, "eventId");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Duration normalizedDelay = requireNonNegative(delay, "delay");
        if (executor.isShutdown()) {
            throw new IllegalStateException("recharge dispatch timer has been stopped");
        }

        String key = timerKey(eventType, normalizedEventId);
        TimerHandle handle = new TimerHandle();
        TimerHandle previous;
        synchronized (futures) {
            previous = futures.get(key);
            if (previous == null && futures.size() >= maxPendingTasks) {
                rejectedSchedules.incrementAndGet();
                throw new RejectedExecutionException(
                        "recharge dispatch timer max pending tasks reached, max pending=" + maxPendingTasks);
            }
            previous = futures.put(key, handle);
        }
        if (previous != null) {
            previous.cancel();
        }

        ScheduledFuture<?> future;
        try {
            future = executor.schedule(
                    () -> runScheduled(key, handle, normalizedEventId, eventType),
                    // 参见详细设计「9.1 派单 Outbox」：保留剩余延迟精度，避免截断到毫秒后再次早醒。
                    normalizedDelay.toNanos(),
                    TimeUnit.NANOSECONDS);
        } catch (RuntimeException ex) {
            futures.remove(key, handle);
            throw ex;
        }
        handle.future = future;
        if (futures.get(key) != handle) {
            future.cancel(false);
        }
    }

    public int pendingCount() {
        return futures.size();
    }

    public int maxPendingTasks() {
        return maxPendingTasks;
    }

    public long rejectedScheduleCount() {
        return rejectedSchedules.get();
    }

    public long rejectedWorkerExecutionCount() {
        return rejectedWorkerExecutions.get();
    }

    @Override
    public void close() {
        futures.values().forEach(TimerHandle::cancel);
        futures.clear();
        executor.shutdown();
        workerExecutor.shutdown();
        try {
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
            if (!workerExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                workerExecutor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
            workerExecutor.shutdownNow();
            log.warn("供应商履约 timer 关闭被中断 workerId={}", workerId, ex);
        }
    }

    private void runScheduled(String key, TimerHandle handle, String eventId, OutboxEventType eventType) {
        if (!futures.remove(key, handle)) {
            return;
        }
        try {
            /*
             * 参见 docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」。
             * timer 线程只做唤醒投递，供应商 HTTP/DB 状态机放到独立 worker executor，避免慢供应商占住
             * ScheduledThreadPoolExecutor 后让其它 due 事件长期留在 pending。
             */
            workerExecutor.execute(() -> processWorkerEvent(eventId, eventType));
        } catch (RejectedExecutionException ex) {
            logWorkerRejection(eventId, eventType, ex);
        } catch (Throwable ex) {
            logExecutionFailure(eventId, eventType, ex);
            if (ex instanceof Error error) {
                throw error;
            }
        }
    }

    private void processWorkerEvent(String eventId, OutboxEventType eventType) {
        try {
            worker.processEvent(eventId, eventType, workerId);
        } catch (Throwable ex) {
            logExecutionFailure(eventId, eventType, ex);
            if (ex instanceof Error error) {
                throw error;
            }
        }
    }

    private void logExecutionFailure(String eventId, OutboxEventType eventType, Throwable ex) {
        long failureCount = executionFailures.incrementAndGet();
        if (failureCount <= 10 || failureCount % ERROR_LOG_SAMPLE_INTERVAL == 0) {
            log.error(
                    "供应商履约 timer 执行失败 workerId={}, eventId={}, eventType={}, failureCount={}",
                    workerId,
                    eventId,
                    eventType,
                    failureCount,
                    ex);
            return;
        }
        // timer 是高频热路径；重复异常按采样 error 保留堆栈，非采样日志降为 warn，避免故障时日志放大数据库/IO 压力。
        log.warn(
                "供应商履约 timer 执行失败已采样抑制 workerId={}, eventId={}, eventType={}, failureCount={}",
                workerId,
                eventId,
                eventType,
                failureCount);
    }

    private void logWorkerRejection(String eventId, OutboxEventType eventType, RejectedExecutionException ex) {
        long rejectionCount = rejectedWorkerExecutions.incrementAndGet();
        if (rejectionCount <= 10 || rejectionCount % ERROR_LOG_SAMPLE_INTERVAL == 0) {
            log.error(
                    "供应商履约 worker executor 拒绝执行，保留outbox due等待恢复扫描 eventId={}, eventType={}, rejectionCount={}",
                    eventId,
                    eventType,
                    rejectionCount,
                    ex);
            return;
        }
        log.warn(
                "供应商履约 worker executor 拒绝执行已采样抑制 eventId={}, eventType={}, rejectionCount={}",
                eventId,
                eventType,
                rejectionCount);
    }

    private static String timerKey(OutboxEventType eventType, String eventId) {
        return eventType.name() + KEY_SEPARATOR + eventId;
    }

    private static int normalizePoolSize(int poolSize) {
        if (poolSize < 1 || poolSize > MAX_POOL_SIZE) {
            throw new IllegalArgumentException("poolSize must be between 1 and " + MAX_POOL_SIZE);
        }
        return poolSize;
    }

    private static int requirePositive(int value, String fieldName) {
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
        return value;
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }

    private static Duration requireNonNegative(Duration value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.isNegative()) {
            throw new IllegalArgumentException(fieldName + " must not be negative");
        }
        return value;
    }

    private static ThreadFactory daemonThreadFactory(String namePrefix) {
        AtomicInteger sequence = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, namePrefix + "-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    @FunctionalInterface
    public interface TimerWorker {

        /**
         * 处理被 timer 唤醒的单个 outbox 事件。
         */
        void processEvent(String eventId, OutboxEventType eventType, String workerId);
    }

    private static final class TimerHandle {

        private volatile ScheduledFuture<?> future;

        private void cancel() {
            ScheduledFuture<?> scheduledFuture = future;
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
        }
    }
}

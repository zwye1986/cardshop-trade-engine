package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.BalanceDeltaFlushOutboxCommand;
import com.cardshop.trade.engine.application.recharge.BalanceDeltaFlushOutboxPort;
import com.cardshop.trade.engine.domain.order.TaskStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeBalanceDeltaOutboxDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeBalanceDeltaOutboxMapper;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * 余额 Delta 刷账 outbox 写入适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.2 余额 Delta 刷账 Outbox」「8.3 异步落库 Consumer」。
 * 履约 Worker 在供应商失败/作废释放预扣后，通过 {@link BalanceDeltaFlushOutboxPort} 写入
 * {@code recharge_balance_delta_outbox}；预扣路径由 Persist Consumer 直接写同表，不经过本端口。
 *
 * <p>稳定 {@code event_id} 规则（与 Persist 落库一致）：
 * <ul>
 *   <li>{@code PRE_DEDUCT} → {@code balance-flush-{orderId}}</li>
 *   <li>{@code RELEASE}（含空白默认）→ {@code balance-flush-release-{orderId}}</li>
 * </ul>
 * 写入结构化 {@code delta_type}/{@code delta_amount}，不再写 JSON payload / event_type / shard_key。
 * 刷账协议见 {@code docs/交行直充业务系统高可用方案设计.md}
 * 「14.7 {@code mc_ext_depart.balance} 刷账协议」。
 */
public class MybatisBalanceDeltaOutboxWriteAdapter implements BalanceDeltaFlushOutboxPort {

    private static final int DEFAULT_MAX_RETRY_COUNT = 10;
    private static final ZoneId LEGACY_CARDSHOP_DB_ZONE = ZoneId.of("Asia/Shanghai");
    private static final String DELTA_TYPE_PRE_DEDUCT = "PRE_DEDUCT";
    private static final String DELTA_TYPE_RELEASE = "RELEASE";
    private static final Set<String> ALLOWED_DELTA_TYPES = Set.of(DELTA_TYPE_PRE_DEDUCT, DELTA_TYPE_RELEASE);

    private final RechargeBalanceDeltaOutboxPersistWriter writer;
    private final Clock clock;

    public MybatisBalanceDeltaOutboxWriteAdapter(RechargeBalanceDeltaOutboxMapper mapper, Clock clock) {
        this(Objects.requireNonNull(mapper, "mapper must not be null")::insertIfAbsent, clock);
    }

    /**
     * 测试用构造：注入写入桩，避免启动 MyBatis。
     */
    MybatisBalanceDeltaOutboxWriteAdapter(RechargeBalanceDeltaOutboxPersistWriter writer, Clock clock) {
        this.writer = Objects.requireNonNull(writer, "writer must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public void enqueueBalanceDeltaFlush(BalanceDeltaFlushOutboxCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        LocalDateTime now = now();
        String normalizedDeltaType = normalizeDeltaType(command.deltaType());
        String eventId = eventIdFor(normalizedDeltaType, command.orderId());
        RechargeBalanceDeltaOutboxDO outbox = new RechargeBalanceDeltaOutboxDO();
        outbox.setEventId(eventId);
        outbox.setOrgCode(command.orgCode());
        outbox.setOrderId(command.orderId());
        outbox.setOutOrderSn(command.outOrderSn());
        outbox.setDeltaType(normalizedDeltaType);
        outbox.setDeltaAmount(command.deltaAmount());
        outbox.setStatus(TaskStatus.INIT.name());
        outbox.setRetryCount(0);
        outbox.setMaxRetryCount(DEFAULT_MAX_RETRY_COUNT);
        outbox.setNextRetryTime(now);
        outbox.setLockedBy(null);
        outbox.setLockedUntil(null);
        outbox.setVersion(0L);
        outbox.setLastError("");
        outbox.setCreatedAt(now);
        outbox.setUpdatedAt(now);
        writer.insertIfAbsent(outbox);
    }

    /**
     * 归一化到封闭集合 {@code PRE_DEDUCT}/{@code RELEASE}；空白默认 {@code RELEASE}。
     *
     * <p>参见详细设计「9.2 余额 Delta 刷账 Outbox」事件 ID 与 delta_type 对照表。
     */
    static String normalizeDeltaType(String deltaType) {
        if (deltaType == null || deltaType.isBlank()) {
            return DELTA_TYPE_RELEASE;
        }
        String normalized = deltaType.strip().toUpperCase(Locale.ROOT);
        if (!ALLOWED_DELTA_TYPES.contains(normalized)) {
            throw new IllegalArgumentException("unsupported balance delta_type=" + deltaType);
        }
        return normalized;
    }

    /**
     * 按 delta_type 生成稳定 event_id，禁止再使用旧的 {@code balance-flush-{type}-{orderId}} 泛化规则。
     */
    static String eventIdFor(String normalizedDeltaType, String orderId) {
        if (DELTA_TYPE_PRE_DEDUCT.equals(normalizedDeltaType)) {
            return "balance-flush-" + orderId;
        }
        return "balance-flush-release-" + orderId;
    }

    private LocalDateTime now() {
        return LocalDateTime.ofInstant(clock.instant(), LEGACY_CARDSHOP_DB_ZONE);
    }
}

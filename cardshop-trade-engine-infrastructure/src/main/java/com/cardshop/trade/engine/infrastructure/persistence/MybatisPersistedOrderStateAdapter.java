package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.recharge.PersistedOrderStatePort;
import com.cardshop.trade.engine.application.recharge.PersistedOrderProbe;
import com.cardshop.trade.engine.domain.order.PersistStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * 基于 `recharge_order_ext` 的 MySQL 已落库状态确认适配器。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」。Redis pending
 * 补投任务在重发 MQ 前必须先查 MySQL，若 `persist_status=DB_PERSISTED` 且 `persisted_version`
 * 已覆盖 Redis 订单版本，则只修复 Redis pending 状态，不再向 RabbitMQ 投递重复消息。
 */
public class MybatisPersistedOrderStateAdapter implements PersistedOrderStatePort {

    private static final Logger log = LoggerFactory.getLogger(MybatisPersistedOrderStateAdapter.class);
    private static final String LOG_EVENT_STATE_LOOKUP_FAILED = "persisted-order-state.lookup.failed";
    private static final String LOG_EVENT_BATCH_STATE_LOOKUP_FAILED = "persisted-order-state.batch-lookup.failed";

    private final RechargeOrderExtMapper extMapper;
    private final LogStormGuard logStormGuard = LogStormGuard.defaultGuard();

    public MybatisPersistedOrderStateAdapter(RechargeOrderExtMapper extMapper) {
        this.extMapper = Objects.requireNonNull(extMapper, "extMapper must not be null");
    }

    @Override
    public boolean isPersistedAtLeast(String orgCode, String outOrderSn, String orderId, long orderVersion) {
        try {
            RechargeOrderExtDO ext = extMapper.selectByOrderId(orderId);
            return matchesPersistedAtLeast(ext, orgCode, outOrderSn, orderId, orderVersion);
        } catch (RuntimeException ex) {
            /*
             * 这是后台补投前置二次确认。查询失败时不能把订单当作已落库，否则会误删 Redis pending；
             * 因此记录 error 并返回 false，让补投按 MQ 幂等链路继续恢复。
             */
            LogStormGuard.LogDecision decision = logStormGuard.shouldLog(LOG_EVENT_STATE_LOOKUP_FAILED);
            if (decision.shouldLog()) {
                log.error(
                        "MySQL落库状态二次确认失败 orgCode={}, outOrderSn={}, orderId={}, orderVersion={}, occurrence={}, sampled={}",
                        orgCode,
                        outOrderSn,
                        orderId,
                        orderVersion,
                        decision.occurrence(),
                        decision.sampledOccurrence(),
                        ex);
            }
            return false;
        }
    }

    @Override
    public Set<PersistedOrderProbe> findPersistedAtLeast(Collection<PersistedOrderProbe> probes) {
        Objects.requireNonNull(probes, "probes must not be null");
        List<PersistedOrderProbe> requested = probes.stream()
                .map(probe -> Objects.requireNonNull(probe, "probes must not contain null"))
                .toList();
        if (requested.isEmpty()) {
            return Set.of();
        }

        try {
            List<String> orderIds = requested.stream()
                    .map(PersistedOrderProbe::orderId)
                    .distinct()
                    .toList();
            List<RechargeOrderExtDO> rows = extMapper.selectByOrderIds(orderIds);
            Map<String, RechargeOrderExtDO> rowsByOrderId = new LinkedHashMap<>();
            if (rows != null) {
                for (RechargeOrderExtDO row : rows) {
                    if (row != null && StringUtils.hasText(row.getOrderId())) {
                        rowsByOrderId.putIfAbsent(row.getOrderId(), row);
                    }
                }
            }

            Set<PersistedOrderProbe> persisted = new LinkedHashSet<>();
            for (PersistedOrderProbe probe : requested) {
                RechargeOrderExtDO ext = rowsByOrderId.get(probe.orderId());
                if (matchesPersistedAtLeast(
                        ext,
                        probe.orgCode(),
                        probe.outOrderSn(),
                        probe.orderId(),
                        probe.orderVersion())) {
                    persisted.add(probe);
                }
            }
            return persisted;
        } catch (RuntimeException ex) {
            /*
             * 参见 docs/交行直充业务系统详细设计.md「8.3 异步落库 Consumer」。批量二次确认只用于
             * 跳过 RabbitMQ 已落库重复副本；查询失败时必须保守返回空集合，让后续批写幂等兜底。
             */
            LogStormGuard.LogDecision decision = logStormGuard.shouldLog(LOG_EVENT_BATCH_STATE_LOOKUP_FAILED);
            if (decision.shouldLog()) {
                log.error(
                        "MySQL批量落库状态二次确认失败 probeCount={}, occurrence={}, sampled={}",
                        requested.size(),
                        decision.occurrence(),
                        decision.sampledOccurrence(),
                        ex);
            }
            return Set.of();
        }
    }

    private static boolean matchesPersistedAtLeast(
            RechargeOrderExtDO ext,
            String orgCode,
            String outOrderSn,
            String orderId,
            long orderVersion) {
        if (ext == null || !StringUtils.hasText(ext.getOrderId()) || !StringUtils.hasText(orderId)) {
            return false;
        }
        if (!orderId.equals(ext.getOrderId())) {
            return false;
        }
        if (StringUtils.hasText(orgCode) && !orgCode.equals(ext.getOrgCode())) {
            return false;
        }
        if (StringUtils.hasText(outOrderSn) && !outOrderSn.equals(ext.getOutOrderSn())) {
            return false;
        }
        long persistedVersion = ext.getPersistedVersion() == null ? 0L : ext.getPersistedVersion();
        return PersistStatus.DB_PERSISTED.name().equals(ext.getPersistStatus())
                && persistedVersion >= Math.max(1L, orderVersion);
    }
}

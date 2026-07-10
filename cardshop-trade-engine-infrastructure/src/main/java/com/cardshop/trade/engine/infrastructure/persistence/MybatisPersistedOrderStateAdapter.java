package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.PersistedOrderStatePort;
import com.cardshop.trade.engine.domain.order.PersistStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import java.util.Objects;
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

    private final RechargeOrderExtMapper extMapper;

    public MybatisPersistedOrderStateAdapter(RechargeOrderExtMapper extMapper) {
        this.extMapper = Objects.requireNonNull(extMapper, "extMapper must not be null");
    }

    @Override
    public boolean isPersistedAtLeast(String orgCode, String outOrderSn, String orderId, long orderVersion) {
        try {
            RechargeOrderExtDO ext = extMapper.selectByOrderId(orderId);
            if (ext == null || !StringUtils.hasText(ext.getOrderId())) {
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
        } catch (RuntimeException ex) {
            /*
             * 这是后台补投前置二次确认。查询失败时不能把订单当作已落库，否则会误删 Redis pending；
             * 因此记录 error 并返回 false，让补投按 MQ 幂等链路继续恢复。
             */
            log.error("MySQL落库状态二次确认失败 orgCode={}, outOrderSn={}, orderId={}, orderVersion={}",
                    orgCode, outOrderSn, orderId, orderVersion, ex);
            return false;
        }
    }
}

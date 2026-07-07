package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * 基于 MyBatis 的 MySQL 查单兜底适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.10 查单设计」。该适配器只读取
 * `mc_order` 和 `recharge_order_ext`，不调用供应商、不推进订单状态；供应商状态推进必须由 outbox/XXL-Job
 * 异步完成，避免高频查单放大供应商压力。
 */
public class MybatisRechargeOrderQueryAdapter implements RechargeOrderQueryPort {

    private static final Logger log = LoggerFactory.getLogger(MybatisRechargeOrderQueryAdapter.class);
    private static final String PERSIST_STATUS_DB_PERSISTED = "DB_PERSISTED";
    private static final String VALIDATION_STATUS_PASSED = "PASSED";
    private static final String VALIDATION_STATUS_FAILED = "FAILED";

    private final McOrderQueryReader orderReader;
    private final RechargeOrderExtQueryReader extReader;

    public MybatisRechargeOrderQueryAdapter(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper) {
        this(orderMapper::selectByOrgAndOutOrder, extMapper::selectByOrgAndOutOrder);
    }

    MybatisRechargeOrderQueryAdapter(
            McOrderQueryReader orderReader,
            RechargeOrderExtQueryReader extReader) {
        this.orderReader = Objects.requireNonNull(orderReader, "orderReader must not be null");
        this.extReader = Objects.requireNonNull(extReader, "extReader must not be null");
    }

    @Override
    public Optional<RechargeOrderSnapshot> findByOrgCodeAndOutOrderSn(String orgCode, String outOrderSn) {
        McOrderDO order = orderReader.selectByOrgAndOutOrder(orgCode, outOrderSn);
        if (order == null || !StringUtils.hasText(order.getOrderId())) {
            return Optional.empty();
        }
        RechargeOrderExtDO ext = extReader.selectByOrgAndOutOrder(orgCode, outOrderSn);
        if (ext != null
                && StringUtils.hasText(ext.getOrderId())
                && !order.getOrderId().equals(ext.getOrderId())) {
            /*
             * 查询条件相同但平台订单号不同，说明扩展表和旧订单事实存在不一致。查单仍以 mc_order 事实为准，
             * 但保守丢弃 ext 状态，避免错误的 persist_status 影响作废入口判断。
             */
            log.warn("MySQL查单发现mc_order与recharge_order_ext订单号不一致 orgCode={}, outOrderSn={}, mcOrderId={}, extOrderId={}",
                    orgCode,
                    outOrderSn,
                    order.getOrderId(),
                    ext.getOrderId());
            ext = null;
        }
        return Optional.of(toSnapshot(order, ext, outOrderSn));
    }

    private static RechargeOrderSnapshot toSnapshot(
            McOrderDO order,
            RechargeOrderExtDO ext,
            String requestedOutOrderSn) {
        int mysqlOrderStatus = order.getOrderStatus() == null ? 0 : order.getOrderStatus();
        String persistStatus = ext != null && StringUtils.hasText(ext.getPersistStatus())
                ? ext.getPersistStatus()
                : PERSIST_STATUS_DB_PERSISTED;
        String validationStatus = ext != null && StringUtils.hasText(ext.getValidationStatus())
                ? ext.getValidationStatus()
                : validationStatusFromMysql(mysqlOrderStatus);
        boolean dispatchAllowed = ext != null && ext.getDispatchAllowed() != null
                ? ext.getDispatchAllowed()
                : mysqlOrderStatus != 6;
        return new RechargeOrderSnapshot(
                order.getOrderId(),
                StringUtils.hasText(order.getOutOrderId()) ? order.getOutOrderId() : requestedOutOrderSn,
                orderStatusFromMysql(mysqlOrderStatus),
                persistStatus,
                validationStatus,
                defaultText(order.getFailCode()),
                defaultText(order.getFailReason()),
                dispatchAllowed);
    }

    /**
     * 将旧表数字状态归一成 API 层已使用的内部状态名，再由 V1 查单用例统一映射为协议 order_status。
     */
    private static String orderStatusFromMysql(int orderStatus) {
        return switch (orderStatus) {
            case 12 -> "SUCCESS";
            case 13 -> "SUPPLIER_ACCEPTED";
            case 11 -> "REFUNDED";
            case 6 -> "FAILED";
            case 2 -> "ACCEPTED";
            default -> "UNKNOWN";
        };
    }

    private static String validationStatusFromMysql(int orderStatus) {
        return orderStatus == 6 ? VALIDATION_STATUS_FAILED : VALIDATION_STATUS_PASSED;
    }

    private static String defaultText(String value) {
        return value == null ? "" : value;
    }
}

/**
 * 旧订单事实读取抽象，便于单元测试不启动 MyBatis 容器。
 */
interface McOrderQueryReader {

    McOrderDO selectByOrgAndOutOrder(String orgCode, String outOrderSn);
}

/**
 * 新链路扩展状态读取抽象，便于单元测试覆盖 ext 缺失和不一致场景。
 */
interface RechargeOrderExtQueryReader {

    RechargeOrderExtDO selectByOrgAndOutOrder(String orgCode, String outOrderSn);
}

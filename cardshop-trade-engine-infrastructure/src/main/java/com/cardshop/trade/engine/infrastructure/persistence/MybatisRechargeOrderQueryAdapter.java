package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderSnapshot;
import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.recharge.FulfillmentDispatchStatus;
import com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode;
import com.cardshop.trade.engine.domain.order.PersistStatus;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import com.cardshop.trade.engine.domain.order.VoucherUseState;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import java.util.Objects;
import java.util.Optional;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * 基于 MyBatis 的 MySQL 查单兜底适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「3. API/V1 接入层」。该适配器只读取
 * `mc_order` 和 `recharge_order_ext`，不调用供应商、不推进订单状态；供应商状态推进必须由 outbox/XXL-Job
 * 异步完成，避免高频查单放大供应商压力。
 */
public class MybatisRechargeOrderQueryAdapter implements RechargeOrderQueryPort {

    private static final Logger log = LoggerFactory.getLogger(MybatisRechargeOrderQueryAdapter.class);
    private static final DateTimeFormatter VOUCHER_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final LogStormGuard HISTORICAL_CANCEL_LOG_GUARD = LogStormGuard.defaultGuard();

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
        if (ext != null && !order.getOrderId().equals(ext.getOrderId())) {
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
        boolean historicalCancelled = mysqlOrderStatus == LegacyOrderStatusCode.FAILED.code()
                && ext != null && FulfillmentDispatchStatus.CANCELLED.name().equals(ext.getDispatchStatus());
        /*
         * 详细设计「9.6 交行对外券状态修订」：旧实现用订单6承载作废，不能再将其解释为发券失败。
         * 此处只规范查询视图为12，不修旧库；缺乏可信券状态及作废时间时保留空券字段并提示人工核查。
         */
        if (historicalCancelled) {
            mysqlOrderStatus = LegacyOrderStatusCode.SUCCESS.code();
        }
        String persistStatus = ext != null && StringUtils.hasText(ext.getPersistStatus())
                ? ext.getPersistStatus()
                : PersistStatus.DB_PERSISTED.name();
        String validationStatus = historicalCancelled ? ValidationStatus.PASSED.name()
                : ext != null && StringUtils.hasText(ext.getValidationStatus())
                ? ext.getValidationStatus()
                : validationStatusFromMysql(mysqlOrderStatus);
        boolean dispatchAllowed = ext != null && ext.getDispatchAllowed() != null
                ? ext.getDispatchAllowed()
                : mysqlOrderStatus != LegacyOrderStatusCode.FAILED.code();
        String voucherUseState = ext == null ? "" : defaultText(ext.getVoucherUseState());
        String candidateVoucherState = voucherUseState;
        if (Arrays.stream(VoucherUseState.values()).noneMatch(state -> state.code().equals(candidateVoucherState))) {
            voucherUseState = "";
        }
        if (historicalCancelled && (!VoucherUseState.CANCELLED.code().equals(voucherUseState)
                || ext.getVoucherInvalidTime() == null)) {
            voucherUseState = "";
            // 高频查单只采样warn，避免待核查历史记录导致日志风暴；参见详细设计9.6。
            var decision = HISTORICAL_CANCEL_LOG_GUARD.shouldLog("query.historical-cancelled-voucher-missing");
            if (decision.shouldLog()) {
                log.warn("历史作废订单缺可信券事实，查询保留发券成功但券状态需核查 orderId={}, outOrderSn={}, occurrence={}",
                        order.getOrderId(), requestedOutOrderSn, decision.occurrence());
            }
        }
        boolean hasVoucherState = StringUtils.hasText(voucherUseState);
        return new RechargeOrderSnapshot(
                order.getOrderId(),
                StringUtils.hasText(order.getOutOrderId()) ? order.getOutOrderId() : requestedOutOrderSn,
                LegacyOrderStatusCode.fromCode(mysqlOrderStatus).toQueryStatus(),
                persistStatus,
                validationStatus,
                historicalCancelled ? "" : defaultText(order.getFailCode()),
                historicalCancelled ? "" : defaultText(order.getFailReason()),
                dispatchAllowed,
                hasVoucherState ? VoucherUseState.stateTypeFor(order.getSupplierChannel()) : "",
                voucherUseState,
                hasVoucherState ? formatVoucherTime(ext.getVoucherUseTime()) : "",
                hasVoucherState ? formatVoucherTime(ext.getVoucherInvalidTime()) : "");
    }

    /** 持久化时间已是上海本地时间，输出协议秒级格式，不推断或补造时间；参见详细设计9.6。 */
    private static String formatVoucherTime(LocalDateTime time) {
        return time == null ? "" : VOUCHER_TIME.format(time);
    }

    private static String validationStatusFromMysql(int orderStatus) {
        return orderStatus == LegacyOrderStatusCode.FAILED.code()
                ? ValidationStatus.FAILED.name()
                : ValidationStatus.PASSED.name();
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

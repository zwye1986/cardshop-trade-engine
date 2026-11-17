package com.cardshop.trade.engine.infrastructure.persistence;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cardshop.trade.engine.application.recharge.RechargeOrderIdentityConflictException;
import com.cardshop.trade.engine.application.v1.RechargeOrderExistencePort;
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
public class MybatisRechargeOrderQueryAdapter implements RechargeOrderQueryPort, RechargeOrderExistencePort {

    private static final Logger log = LoggerFactory.getLogger(MybatisRechargeOrderQueryAdapter.class);
    private static final DateTimeFormatter VOUCHER_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final LogStormGuard HISTORICAL_CANCEL_LOG_GUARD = LogStormGuard.defaultGuard();
    private static final int MAX_LEGACY_CANDIDATES = 2;

    private final McOrderMapper orderMapper;
    private final RechargeOrderExtMapper extMapper;

    public MybatisRechargeOrderQueryAdapter(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper) {
        this.orderMapper = Objects.requireNonNull(orderMapper, "orderMapper must not be null");
        this.extMapper = Objects.requireNonNull(extMapper, "extMapper must not be null");
    }

    /** 只判断持久占用，历史歧义和孤儿 ext 同样占用；参见详细设计3章、8.2.1。 */
    @Override
    public boolean existsPersistedOrder(String orgCode, String outOrderSn) {
        return extMapper.selectByOrgAndOutOrder(orgCode, outOrderSn) != null
                || !orderMapper.selectList(directRechargeIdentity(orgCode, outOrderSn)
                        .select(McOrderDO::getOrderId).last("limit 1")).isEmpty();
    }

    /** 优先按 ext 平台号定位新单，无 ext 仅允许唯一归属0的旧直充单只读展示；参见详细设计8.2.1。 */
    @Override
    public Optional<RechargeOrderSnapshot> findByOrgCodeAndOutOrderSn(String orgCode, String outOrderSn) {
        RechargeOrderExtDO ext = extMapper.selectByOrgAndOutOrder(orgCode, outOrderSn);
        if (ext != null) {
            McOrderDO order = orderMapper.selectByOrderId(ext.getOrderId());
            if (order == null
                    || !StringUtils.hasText(ext.getOrderId())
                    || !Objects.equals(ext.getOrderId(), order.getOrderId())
                    || !Objects.equals(orgCode, ext.getOrgCode())
                    || !Objects.equals(outOrderSn, ext.getOutOrderSn())
                    || !Objects.equals(orgCode, order.getOrgCode())
                    || !Objects.equals(outOrderSn, order.getOutOrderId())
                    || !Objects.equals(McOrderDO.DIRECT_RECHARGE_PRODUCT_TYPE, order.getProductType())
                    || !Objects.equals(McOrderDO.TRADE_ENGINE_OWNED, order.getTradeEngineOwned())) {
                throw new RechargeOrderIdentityConflictException("MySQL订单身份冲突，需人工核查 orgCode=" + orgCode
                        + ", outOrderSn=" + outOrderSn + ", extOrderId=" + ext.getOrderId()
                        + ", mcOrderId=" + (order == null ? "" : order.getOrderId()));
            }
            return Optional.of(toSnapshot(order, ext, outOrderSn));
        }
        var candidates = orderMapper.selectList(directRechargeIdentity(orgCode, outOrderSn)
                .select(McOrderDO::getOrderId, McOrderDO::getOrgCode, McOrderDO::getOutOrderId,
                        McOrderDO::getTradeEngineOwned, McOrderDO::getProductType, McOrderDO::getOrderStatus,
                        McOrderDO::getFailCode, McOrderDO::getFailReason, McOrderDO::getSupplierChannel)
                .orderByAsc(McOrderDO::getId).last("limit " + MAX_LEGACY_CANDIDATES));
        if (candidates.isEmpty()) return Optional.empty();
        McOrderDO order = candidates.getFirst();
        if (candidates.size() > 1 || !Objects.equals(0, order.getTradeEngineOwned())
                || !Objects.equals(orgCode, order.getOrgCode())
                || !Objects.equals(outOrderSn, order.getOutOrderId())
                || !StringUtils.hasText(order.getOrderId())) {
            throw new RechargeOrderIdentityConflictException("MySQL旧单身份歧义、文本错配或新单缺失ext，需人工核查 orgCode="
                    + orgCode + ", outOrderSn=" + outOrderSn + ", orderId=" + order.getOrderId()
                    + ", candidateCount=" + candidates.size());
        }
        return Optional.of(toSnapshot(order, null, outOrderSn));
    }

    private static LambdaQueryWrapper<McOrderDO> directRechargeIdentity(String orgCode, String outOrderSn) {
        // 使用旧表现有(out_orderId,org_code)索引，不依赖收窄后旧单为NULL的生成列；参见详细设计8.2.1。
        return new LambdaQueryWrapper<McOrderDO>()
                .eq(McOrderDO::getOutOrderId, outOrderSn)
                .eq(McOrderDO::getOrgCode, orgCode)
                .eq(McOrderDO::getProductType, McOrderDO.DIRECT_RECHARGE_PRODUCT_TYPE);
    }

    static RechargeOrderSnapshot toSnapshot(
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

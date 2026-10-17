package com.cardshop.trade.engine.infrastructure.redis;

import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import java.time.Duration;
import java.util.List;

/**
 * `accept_recharge_order.lua` 的 KEYS 和 ARGV 映射对象。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「6.1 Redis Lua 入参与 Key 构造」。
 * Lua 入参是位置协议，字段扩展只能在该类和脚本中同步完成，不能在适配器内临时拼接字符串列表。
 */
record AcceptRechargeOrderLuaArguments(List<String> keys, List<String> args) {

    static AcceptRechargeOrderLuaArguments from(
            AcceptRechargeOrderCommand command,
            RechargeRedisKeyBuilder keyBuilder,
            Duration pendingTtl,
            Duration nonceTtl) {
        return new AcceptRechargeOrderLuaArguments(
                keyBuilder.acceptKeys(command),
                List.of(
                        command.orgCode(),
                        command.appId(),
                        command.outOrderSn(),
                        command.orderId(),
                        command.productId(),
                        command.selectedProductId(),
                        command.account(),
                        command.notifyUrl(),
                        command.nonceStr(),
                        String.valueOf(command.num()),
                        String.valueOf(command.orderAmountCent()),
                        command.productName(),
                        String.valueOf(command.productValueCent()),
                        String.valueOf(command.productType()),
                        command.couponType(),
                        command.supplierChannel(),
                        command.requestHash(),
                        command.requestHashVersion(),
                        command.requestHashAlg(),
                        command.candidateValidationStatus().name(),
                        command.candidateFailCode(),
                        command.candidateFailReason(),
                        String.valueOf(command.merchantConfigVersion()),
                        String.valueOf(command.productConfigVersion()),
                        command.productSelectionFingerprint(),
                        String.valueOf(command.productSnapshotAvailable()),
                        String.valueOf(command.nowEpochMillis()),
                        String.valueOf(command.nextRetryEpochMillis()),
                        String.valueOf(pendingTtl.toMillis()),
                        String.valueOf(nonceTtl.toMillis()),
                        command.supplierBatchId()));
    }
}

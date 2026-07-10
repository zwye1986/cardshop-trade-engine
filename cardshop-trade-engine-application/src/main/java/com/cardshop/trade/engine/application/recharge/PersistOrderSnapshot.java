package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.PersistStatus;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import java.util.Map;
import java.util.Objects;

/**
 * Persist Consumer 写 MySQL 所需的完整订单快照。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「7. RabbitMQ 持久化消息层」和
 * 「8.3 异步落库 Consumer」。`RechargeOrderSnapshot` 只服务查单/作废入口，字段不足以写
 * `mc_order`；本对象专门承载落库字段，避免 Consumer 从多个协议 DTO 或 Redis hash 中临时拼字段。
 *
 * @param orgCode 商户编码，对应旧表 {@code mc_order.org_code}
 * @param appId 商户 appId，用于追踪 V1 请求来源
 * @param orderId 平台订单号，对应旧表 {@code mc_order.order_Id}
 * @param outOrderSn 商户外部订单号，对应旧表 {@code mc_order.out_orderId}
 * @param productId 外部请求中的父商品编码，对应旧表 {@code mc_order.parent_id}
 * @param selectedProductId 实际落库子商品编码，对应旧表 {@code mc_order.produt_code}
 * @param account 直充账号，对应旧表 {@code mc_order.account}
 * @param notifyUrl 商户回调地址，对应旧表 {@code mc_order.callback_url}
 * @param num 下单数量，对应旧表 {@code mc_order.num}
 * @param orderPrice 订单金额，单位分，对应旧表 {@code mc_order.order_price}
 * @param productName 商品名称，对应旧表 {@code mc_order.product_name}
 * @param productValue 商品面值，单位分，对应旧表 {@code mc_order.product_value}
 * @param productType 商品类型，对应旧表 {@code mc_order.product_type}
 * @param couponType 券码类型，对应旧表 {@code mc_order.coupon_type}
 * @param supplierChannel 供应商渠道编码，对应旧表 {@code mc_order.url}
 * @param requestHash 请求摘要，对应 {@code recharge_order_ext.request_hash}
 * @param requestHashVersion 请求摘要版本，对应 {@code recharge_order_ext.request_hash_version}
 * @param requestHashAlg 请求摘要算法，对应 {@code recharge_order_ext.request_hash_alg}
 * @param orderStatus Redis 接单阶段订单状态，对应 {@code recharge_order_ext.order_status}
 * @param validationStatus 业务校验状态，对应 {@code recharge_order_ext.validation_status}
 * @param failCode 校验或落库失败码，对应旧表 {@code mc_order.fail_code}
 * @param failReason 校验或落库失败原因，对应旧表 {@code mc_order.fail_reason}
 * @param dispatchAllowed 是否允许供应商派单，对应 {@code recharge_order_ext.dispatch_allowed}
 * @param balanceReserved 是否已完成 Redis 预扣，对应 {@code recharge_order_ext.balance_reserved}
 * @param persistStatus Redis 到 MySQL 持久化状态，对应 {@code recharge_order_ext.persist_status}
 * @param orderVersion Redis 订单版本，对应 {@code recharge_order_ext.redis_order_version}
 * @param persistedVersion MySQL 已持久化版本，对应 {@code recharge_order_ext.persisted_version}
 * @param mqPublishCount MQ 发布次数，对应 {@code recharge_order_ext.mq_publish_count}
 */
public record PersistOrderSnapshot(
        String orgCode,
        String appId,
        String orderId,
        String outOrderSn,
        String productId,
        String selectedProductId,
        String account,
        String notifyUrl,
        int num,
        long orderPrice,
        String productName,
        long productValue,
        int productType,
        String couponType,
        String supplierChannel,
        String requestHash,
        String requestHashVersion,
        String requestHashAlg,
        String orderStatus,
        String validationStatus,
        String failCode,
        String failReason,
        boolean dispatchAllowed,
        boolean balanceReserved,
        String persistStatus,
        long orderVersion,
        long persistedVersion,
        int mqPublishCount) {

    private static final String REDIS_TRUE = "1";
    private static final String TEXT_TRUE = "true";

    public PersistOrderSnapshot {
        orgCode = requireText(orgCode, "orgCode");
        appId = defaultText(appId);
        orderId = requireText(orderId, "orderId");
        outOrderSn = requireText(outOrderSn, "outOrderSn");
        productId = requireText(productId, "productId");
        selectedProductId = requireText(selectedProductId, "selectedProductId");
        account = requireText(account, "account");
        notifyUrl = requireText(notifyUrl, "notifyUrl");
        if (num <= 0) {
            throw new IllegalArgumentException("num must be positive");
        }
        if (orderPrice < 0 || productValue < 0) {
            throw new IllegalArgumentException("orderPrice and productValue must not be negative");
        }
        productName = defaultText(productName);
        couponType = defaultText(couponType);
        supplierChannel = defaultText(supplierChannel);
        requestHash = requireText(requestHash, "requestHash");
        requestHashVersion = defaultText(requestHashVersion, "v1");
        requestHashAlg = defaultText(requestHashAlg, "SHA-256");
        orderStatus = defaultText(orderStatus, dispatchAllowed ? "ACCEPTED" : "VALIDATION_FAILED");
        validationStatus = defaultText(validationStatus, dispatchAllowed
                ? ValidationStatus.PASSED.name()
                : ValidationStatus.FAILED.name());
        failCode = defaultText(failCode);
        failReason = defaultText(failReason);
        persistStatus = defaultText(persistStatus, PersistStatus.PERSIST_QUEUED.name());
        if (orderVersion <= 0) {
            throw new IllegalArgumentException("orderVersion must be positive");
        }
        if (persistedVersion < 0 || mqPublishCount < 0) {
            throw new IllegalArgumentException("persistedVersion and mqPublishCount must not be negative");
        }
    }

    /**
     * 从 MQ initial snapshot 构造兜底落库快照。
     *
     * <p>D7A 要求 producer 携带完整字段；这里仍对 `request_hash_version/request_hash_alg` 做兼容默认值，
     * 便于少量旧消息不会因为摘要元数据缺失而无法落库。业务字段如商品、账号、回调地址仍要求存在，
     * 因为这些字段缺失会直接破坏 `mc_order` 事实和后续供应商派单。
     */
    public static PersistOrderSnapshot fromInitialSnapshot(PersistTradeOrderMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        Map<String, Object> snapshot = message.initialOrderSnapshot();
        String validationStatus = text(snapshot, "validation_status", ValidationStatus.PASSED.name());
        boolean dispatchAllowed = bool(
                snapshot,
                "dispatch_allowed",
                ValidationStatus.PASSED.name().equals(validationStatus));
        return new PersistOrderSnapshot(
                text(snapshot, "org_code", message.orgCode()),
                text(snapshot, "app_id", ""),
                text(snapshot, "order_id", message.orderId()),
                text(snapshot, "out_order_sn", message.outOrderSn()),
                text(snapshot, "product_id", ""),
                text(snapshot, "selected_product_id", ""),
                text(snapshot, "account", ""),
                text(snapshot, "notify_url", ""),
                intValue(snapshot, "num", 1),
                longValue(snapshot, "order_price", 0L),
                text(snapshot, "product_name", ""),
                longValue(snapshot, "product_value", 0L),
                intValue(snapshot, "product_type", 1),
                /*
                 * TODO(D7B/D8): 用生产 mc_order.coupon_type 分布确认旧 MQ 消息缺失 coupon_type 时的下游影响；
                 * 参见 docs/交行直充业务系统详细设计.md「8.3 异步落库 Consumer」。当前保留空值而非写 UNKNOWN，
                 * 避免旧卡管按 url/code/codekey 路由时误识别一个不存在的卡密类型。
                 */
                text(snapshot, "coupon_type", ""),
                text(snapshot, "supplier_channel", ""),
                text(snapshot, "request_hash", message.requestHash()),
                text(snapshot, "request_hash_version", "v1"),
                text(snapshot, "request_hash_alg", "SHA-256"),
                text(snapshot, "order_status", dispatchAllowed ? "ACCEPTED" : "VALIDATION_FAILED"),
                validationStatus,
                text(snapshot, "fail_code", ""),
                text(snapshot, "fail_reason", ""),
                dispatchAllowed,
                bool(snapshot, "balance_reserved", dispatchAllowed),
                text(snapshot, "persist_status", PersistStatus.PERSIST_QUEUED.name()),
                longValue(snapshot, "order_version", message.orderVersion()),
                longValue(snapshot, "persisted_version", 0L),
                intValue(snapshot, "mq_publish_count", 1));
    }

    AcceptRechargeOrderCommand toAcceptCommand(long nowEpochMillis, long nextRetryEpochMillis) {
        /*
         * Redis markDbPersisted/markPersistFailed 脚本只使用 orgCode、outOrderSn、orderId 等 key 字段。
         * AcceptRechargeOrderCommand 仍要求配置版本字段；D7A 从持久化快照恢复命令时使用 1 作为中性占位，
         * 避免为 Redis mark 脚本引入另一套重复命令对象。后续若 mark 端口改为专用命令，可删除该转换。
         */
        return new AcceptRechargeOrderCommand(
                orgCode,
                defaultText(appId, "persist-consumer"),
                outOrderSn,
                orderId,
                productId,
                selectedProductId,
                account,
                notifyUrl,
                "persist-consumer-nonce-00000000",
                num,
                orderPrice,
                productName,
                productValue,
                productType,
                couponType,
                supplierChannel,
                requestHash,
                requestHashVersion,
                requestHashAlg,
                validationStatusEnum(),
                failCode,
                failReason,
                1L,
                1L,
                nowEpochMillis,
                nextRetryEpochMillis);
    }

    AcceptRechargeOrderResult toAcceptResult() {
        return new AcceptRechargeOrderResult(
                ValidationStatus.PASSED.name().equals(validationStatus)
                        ? AcceptRechargeOrderStatus.ACCEPTED
                        : AcceptRechargeOrderStatus.ACCEPTED_FAILED,
                orderId,
                validationStatus,
                failCode,
                failReason,
                dispatchAllowed,
                balanceReserved,
                orderVersion);
    }

    private ValidationStatus validationStatusEnum() {
        return ValidationStatus.PASSED.name().equals(validationStatus) ? ValidationStatus.PASSED : ValidationStatus.FAILED;
    }

    private static String text(Map<String, Object> values, String key, String fallback) {
        Object value = values.get(key);
        if (value == null) {
            return fallback;
        }
        String text = String.valueOf(value);
        return text.isBlank() ? fallback : text;
    }

    private static long longValue(Map<String, Object> values, String key, long fallback) {
        Object value = values.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value == null || String.valueOf(value).isBlank()) {
            return fallback;
        }
        return Long.parseLong(String.valueOf(value));
    }

    private static int intValue(Map<String, Object> values, String key, int fallback) {
        Object value = values.get(key);
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value == null || String.valueOf(value).isBlank()) {
            return fallback;
        }
        return Integer.parseInt(String.valueOf(value));
    }

    private static boolean bool(Map<String, Object> values, String key, boolean fallback) {
        Object value = values.get(key);
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value == null || String.valueOf(value).isBlank()) {
            return fallback;
        }
        String text = String.valueOf(value);
        return REDIS_TRUE.equals(text) || TEXT_TRUE.equalsIgnoreCase(text);
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }

    private static String defaultText(String value) {
        return value == null ? "" : value;
    }

    private static String defaultText(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}

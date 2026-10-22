package com.cardshop.trade.engine.infrastructure.supplier.miniso;

import com.cardshop.trade.engine.application.logging.LogStormGuard;
import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.cardshop.trade.engine.application.recharge.RechargeSupplierGatewayPort;
import com.cardshop.trade.engine.application.recharge.SupplierDispatchGatewayRequest;
import com.cardshop.trade.engine.application.recharge.SupplierGatewayResponse;
import com.cardshop.trade.engine.application.recharge.SupplierInvalidGatewayRequest;
import com.cardshop.trade.engine.application.recharge.SupplierQueryGatewayRequest;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 名创专属发券、查单和作废协议适配，只接受可与冻结订单逐字段核对的完整履约凭据。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「名创 MINISO 专属协议实施约束（2026-10-17）」。
 * 供应商返回不完整、网络中断或未知码均不代表明确失败：始终交回现有异步恢复链路，不触发退款。
 * 本类不读取实时商品、旧表库存或 rawPayload 中的批次，不修改订单状态；凭据交由成功事务保存。
 */
public final class MinisoRechargeSupplierGatewayAdapter implements RechargeSupplierGatewayPort {
    private static final Logger LOGGER = LoggerFactory.getLogger(MinisoRechargeSupplierGatewayAdapter.class);
    private static final String CHANNEL = "MINISO";
    private static final int CODE_SUCCESS = 200;
    private static final int HTTP_SUCCESS_START = 200;
    private static final int HTTP_SUCCESS_END = 300;
    private static final int SINGLE_COUPON = 1;
    private static final int STATUS_UNUSED = 0;
    private static final int STATUS_VOID = 3;
    private static final String UNKNOWN_CODE = "MINISO_RESULT_UNKNOWN";
    private static final String UNKNOWN_MESSAGE = "名创结果不可确认，等待原订单查证";
    private static final String DATE_PATTERN = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}";
    private static final LogStormGuard LOG_GUARD = LogStormGuard.defaultGuard();

    private final MinisoHttpClient client;
    private final MinisoOrderSnapshotReader snapshots;

    public MinisoRechargeSupplierGatewayAdapter(MinisoHttpClient client, MinisoOrderSnapshotReader snapshots) {
        this.client = Objects.requireNonNull(client, "MINISO client is required");
        this.snapshots = Objects.requireNonNull(snapshots, "MINISO order snapshot reader is required");
    }

    /** 用已落库平台订单号与批次发券；不从事件号派生新远端订单号。 */
    @Override
    public SupplierGatewayResponse dispatch(SupplierDispatchGatewayRequest request) {
        return execute(request.orderId(), "dispatch", () -> {
            Optional<TrustedOrder> order = trusted(request.supplierChannel(), request.orgCode(),
                    request.orderId(), request.account());
            if (order.isEmpty()) return unknown(request.orderId(), null);
            TrustedOrder frozen = order.get();
            MinisoApiResponse response = client.batchGrandCoupon(frozen.snapshot().account(), frozen.batchId(),
                    frozen.snapshot().num(), frozen.snapshot().orderId());
            return issuance(response, frozen, false);
        });
    }

    /** 已使用、过期或已作废均证明曾成功发券，不能映射为发券失败。 */
    @Override
    public SupplierGatewayResponse query(SupplierQueryGatewayRequest request) {
        return execute(request.orderId(), "query", () -> {
            Optional<TrustedOrder> order = trusted(request.supplierChannel(), request.orgCode(),
                    request.orderId(), request.account());
            if (order.isEmpty()) return unknown(request.orderId(), null);
            return issuance(client.queryGrand(request.orderId()), order.get(), true);
        });
    }

    /**
     * 作废先用原订单号核对完整凭据，再只提交本单未使用券；重试发现已作废可直接恢复成功。
     * used/expired 只能表示不能确认整单作废，不得释放余额。参见详细设计名创实施约束作废条目。
     */
    @Override
    public SupplierGatewayResponse invalid(SupplierInvalidGatewayRequest request) {
        return execute(request.orderId(), "invalid", () -> {
            Optional<TrustedOrder> order = trusted(request.supplierChannel(), request.orgCode(),
                    request.orderId(), request.account());
            if (order.isEmpty()) return unknown(request.orderId(), null);
            Optional<MinisoFulfillmentReceipt> local = snapshots.localReceipt(request.orgCode(), request.orderId());
            // 作废是不可逆的外部副作用：本地凭据缺失时禁止仅凭远端同手机号/同批次猜测券码。
            if (local.isEmpty()) return unknown(request.orderId(), null);
            MinisoApiResponse query = client.queryGrand(request.orderId());
            Optional<MinisoFulfillmentReceipt> receipt = receipt(query, order.get(), true, local.get());
            if (receipt.isEmpty()) return unknown(request.orderId(), query.rawBody());
            MinisoFulfillmentReceipt confirmed = receipt.get();
            if (!sameVoucherIdentity(local.get(), confirmed)) return unknown(request.orderId(), query.rawBody());
            // 同一已持久化券的远端已作废可证明前次作废成功；其余状态不一致均不能降级后再次作废。
            if (confirmed.couponStatus() == STATUS_VOID) return success(query, confirmed);
            if (confirmed.couponStatus() != local.get().couponStatus()
                    || confirmed.couponStatus() != STATUS_UNUSED) return unknown(request.orderId(), query.rawBody());
            MinisoApiResponse result = client.abandonUserCoupon(List.of(confirmed.couponCode()));
            if (!completeVoid(result, confirmed.couponCode())) return unknown(request.orderId(), result.rawBody());
            return success(result, new MinisoFulfillmentReceipt(confirmed.orderId(), confirmed.orgCode(),
                    confirmed.batchId(), confirmed.couponCode(), confirmed.phone(), confirmed.beginTime(),
                    confirmed.endTime(), STATUS_VOID));
        });
    }

    // 核销由独立 MinisoWriteOffCallbackController 接收并持久恢复，不混入主动HTTP客户端；参见详细设计9.5。

    /** 请求定位字段与已落库订单必须完全一致，且当前协议只支持单张券。 */
    private Optional<TrustedOrder> trusted(String channel, String orgCode, String orderId, String account) {
        if (!CHANNEL.equals(channel) || blank(orgCode) || blank(orderId) || blank(account)) return Optional.empty();
        return snapshots.find(orgCode, orderId).filter(snapshot -> orgCode.equals(snapshot.orgCode())
                && orderId.equals(snapshot.orderId()) && account.equals(snapshot.account())
                && snapshot.num() == SINGLE_COUPON).flatMap(snapshot -> {
                    String batch = snapshot.batchId();
                    if (batch == null || !batch.matches("[0-9]+") || batch.length() > 10) return Optional.empty();
                    // 先以 long 解析并检查范围，避免异常信息包含冻结字段原文。
                    long value = Long.parseLong(batch);
                    return value > 0 && value <= Integer.MAX_VALUE
                            ? Optional.of(new TrustedOrder(snapshot, (int) value)) : Optional.empty();
                });
    }

    private SupplierGatewayResponse issuance(MinisoApiResponse response, TrustedOrder order, boolean query) {
        return receipt(response, order, query).map(confirmed -> success(response, confirmed))
                .orElseGet(() -> unknown(order.snapshot().orderId(), response.rawBody()));
    }

    /** 不采用 Jackson 的宽松数值/文本强转，避免把缺失字段或字符串数组当成完整发券证明。 */
    private static Optional<MinisoFulfillmentReceipt> receipt(MinisoApiResponse response, TrustedOrder order, boolean query) {
        return receipt(response, order, query, null);
    }

    /**
     * 作废查询仅核对券身份和状态，有效期沿用本地持久事实；普通发券/查单仍要求完整远端有效期。
     * queryGrand的生效时间可能与发券相差1秒，不能据此阻断作废，也不能覆盖旧卡管导出有效期。
     * 参见详细设计「名创作废凭据匹配修订（2026-10-21）」；本地归属和三项身份随后独立校验。
     */
    private static Optional<MinisoFulfillmentReceipt> receipt(MinisoApiResponse response, TrustedOrder order,
            boolean query, MinisoFulfillmentReceipt persistedReceipt) {
        if (!successfulEnvelope(response)) return Optional.empty();
        JsonNode data = response.body().path("data");
        if (!data.isArray() || data.size() != order.snapshot().num()) return Optional.empty();
        JsonNode coupon = data.get(0);
        JsonNode batchId = coupon.path("batchId");
        if (!coupon.isObject() || !batchId.isIntegralNumber() || !batchId.canConvertToInt()
                || batchId.intValue() != order.batchId()
                || !text(coupon, "phone").equals(order.snapshot().account())) return Optional.empty();
        String code = text(coupon, "couponCode");
        String begin = persistedReceipt == null ? text(coupon, "beginTime") : persistedReceipt.beginTime();
        String end = persistedReceipt == null ? text(coupon, "endTime") : persistedReceipt.endTime();
        if (blank(code) || code.indexOf(',') >= 0 || !begin.matches(DATE_PATTERN) || !end.matches(DATE_PATTERN)) {
            return Optional.empty();
        }
        JsonNode status = coupon.get("status");
        int couponStatus = STATUS_UNUSED;
        if (status != null || query) {
            if (status == null || !status.isIntegralNumber() || !status.canConvertToInt()
                    || status.intValue() < STATUS_UNUSED || status.intValue() > STATUS_VOID) return Optional.empty();
            couponStatus = status.intValue();
            // 发券响应未约定状态字段：只兼容显式未使用，不能用矛盾状态制造发券成功。
            if (!query && couponStatus != STATUS_UNUSED) return Optional.empty();
        }
        try {
            return Optional.of(new MinisoFulfillmentReceipt(order.snapshot().orderId(), order.snapshot().orgCode(),
                    order.batchId(), code, order.snapshot().account(), begin, end, couponStatus));
        } catch (IllegalArgumentException exception) {
            // 完整日期校验失败仍保留供应商原文供受控审计；日志只采样安全异常栈，不包含日期或券码原值。
            logFailure(order.snapshot().orderId(), query ? "query" : "dispatch", exception);
            return Optional.empty();
        }
    }

    /**
     * 无网络地复核已保存的原始发券响应，维护工具与在线发券共用同一严格合同。
     * 参见详细设计「名创原始成功流水受控恢复」；不适用于缺手机号的查单响应，也不推断券实时状态。
     */
    public static Optional<MinisoFulfillmentReceipt> confirmIssuedReceipt(
            MinisoOrderSnapshot snapshot, MinisoApiResponse response) {
        if (snapshot == null || response == null || snapshot.num() != SINGLE_COUPON
                || blank(snapshot.account()) || blank(snapshot.orgCode()) || blank(snapshot.orderId())) {
            return Optional.empty();
        }
        String batch = snapshot.batchId();
        if (batch == null || !batch.matches("[0-9]+") || batch.length() > 10) return Optional.empty();
        long value = Long.parseLong(batch);
        if (value <= 0 || value > Integer.MAX_VALUE) return Optional.empty();
        return receipt(response, new TrustedOrder(snapshot, (int) value), false);
    }

    /**
     * 在远端作废前确认订单/商户归属，并只匹配券码、手机号、批次，不比较生效或到期时间。
     * 状态另行按单调性判断；参见详细设计「名创作废凭据匹配修订（2026-10-21）」。
     */
    private static boolean sameVoucherIdentity(MinisoFulfillmentReceipt local, MinisoFulfillmentReceipt remote) {
        return local.orgCode().equals(remote.orgCode()) && local.orderId().equals(remote.orderId())
                && local.batchId() == remote.batchId() && local.couponCode().equals(remote.couponCode())
                && local.phone().equals(remote.phone());
    }

    /** 两个数组均为合同必返字段；成功对象必须精确对应请求券且带 msg，不接受外来券或重复券。 */
    private boolean completeVoid(MinisoApiResponse response, String requestedCode) {
        if (!successfulEnvelope(response)) return false;
        JsonNode data = response.body().path("data");
        JsonNode succeed = data.path("succeed");
        JsonNode failed = data.path("failed");
        if (!data.isObject() || !succeed.isArray() || succeed.size() != SINGLE_COUPON
                || !failed.isArray() || !failed.isEmpty()) return false;
        JsonNode item = succeed.get(0);
        return item.isObject() && requestedCode.equals(text(item, "couponCode"))
                && item.path("msg").isTextual() && !item.path("msg").textValue().isBlank();
    }

    private static boolean successfulEnvelope(MinisoApiResponse response) {
        if (response.httpStatus() < HTTP_SUCCESS_START || response.httpStatus() >= HTTP_SUCCESS_END
                || !response.body().isObject()) return false;
        JsonNode code = response.body().path("code");
        return (code.isIntegralNumber() && code.canConvertToInt() && code.intValue() == CODE_SUCCESS)
                || (code.isTextual() && Integer.toString(CODE_SUCCESS).equals(code.textValue()));
    }

    private SupplierGatewayResponse success(MinisoApiResponse response, MinisoFulfillmentReceipt receipt) {
        return SupplierGatewayResponse.success(receipt.orderId(), Integer.toString(CODE_SUCCESS),
                "名创完整凭据已确认", response.rawBody()).withMinisoReceipt(receipt);
    }

    private static SupplierGatewayResponse unknown(String orderId, String raw) {
        return SupplierGatewayResponse.retryableFailed(orderId, UNKNOWN_CODE, UNKNOWN_MESSAGE, raw);
    }

    /** 高频重试只采样记录安全异常栈；原始异常的 message/cause 可能包含手机号、券码或连接密钥。 */
    private SupplierGatewayResponse execute(String orderId, String operation, SupplierCall call) {
        try {
            return call.perform();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            logFailure(orderId, operation, exception);
            return unknown(orderId, null);
        } catch (IOException | RuntimeException exception) {
            logFailure(orderId, operation, exception);
            // HTTP 已返回的不可解析正文仍是受控流水证据，不能因安全异常省略了正文就丢失原始响应。
            String raw = exception instanceof MinisoHttpClient.InvalidResponseException invalid
                    ? invalid.response().rawBody() : null;
            return unknown(orderId, raw);
        }
    }

    private static void logFailure(String orderId, String operation, Exception exception) {
        if (LOG_GUARD.shouldLog("miniso.gateway." + operation).shouldLog()) {
            Exception sanitized = new Exception("MINISO operation failed: " + exception.getClass().getSimpleName());
            sanitized.setStackTrace(exception.getStackTrace());
            LOGGER.error("MINISO supplier result unknown orderId={} operation={}", orderId, operation, sanitized);
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isTextual() ? value.textValue() : "";
    }

    private static boolean blank(String value) { return value == null || value.isBlank(); }
    private record TrustedOrder(MinisoOrderSnapshot snapshot, int batchId) { }
    @FunctionalInterface
    private interface SupplierCall {
        SupplierGatewayResponse perform() throws IOException, InterruptedException;
    }
}

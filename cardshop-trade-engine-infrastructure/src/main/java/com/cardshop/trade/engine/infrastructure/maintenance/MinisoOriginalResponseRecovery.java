package com.cardshop.trade.engine.infrastructure.maintenance;

import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.cardshop.trade.engine.domain.order.LegacyOrderStatusCode;
import com.cardshop.trade.engine.domain.order.VoucherUseState;
import com.cardshop.trade.engine.infrastructure.persistence.MinisoLegacyOrderData;
import com.cardshop.trade.engine.infrastructure.supplier.miniso.MinisoApiResponse;
import com.cardshop.trade.engine.infrastructure.supplier.miniso.MinisoOrderSnapshot;
import com.cardshop.trade.engine.infrastructure.supplier.miniso.MinisoRechargeSupplierGatewayAdapter;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.LoggerFactory;

/**
 * 原始成功流水的显式维护工具，不注册为Bean或公开接口，不包含任何供应商调用能力。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「名创原始成功流水受控恢复」。
 * 仅处理空通知地址、已扣款且查单转人工的成功发券落库缺口。默认验证后回滚；凭据和三个状态一起提交。
 * 生产CLI严格限定测试schema，连接参数只经标准输入传入，不输出券码、手机号、SQL或连接秘密。
 */
public final class MinisoOriginalResponseRecovery {
    private static final String CHANNEL = "MINISO";
    private static final String TEST_DATABASE = "cardshop_test_new";
    private static final String SUCCESS = "SUCCESS";
    private static final String MANUAL = "MANUAL_REQUIRED";
    private static final String ACCEPTED = "SUPPLIER_ACCEPTED";
    private static final int MAX_INPUT_BYTES = 16_384;
    private static final int MAX_EVIDENCE_CHARS = 65_536;
    private static final ZoneId DATABASE_ZONE = ZoneId.of("Asia/Shanghai");
    private final ObjectMapper json;

    public MinisoOriginalResponseRecovery(ObjectMapper json) {
        this.json = json.copy().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    /** 只包含非敏感审计及乐观锁定位数据，不接受外部注入的券凭据。 */
    public record Request(String orgCode, String orderId, long sourceFlowId, long queryVersion,
                          String changeId, String expectedDatabase) {
        public Request {
            require(orgCode != null && orgCode.matches("[A-Za-z0-9_-]{1,64}")
                    && orderId != null && orderId.matches("[A-Za-z0-9_-]{1,100}")
                    && sourceFlowId > 0 && queryVersion >= 0 && queryVersion < Long.MAX_VALUE
                    && changeId != null && changeId.matches("[A-Za-z0-9_-]{1,48}")
                    && expectedDatabase != null && !expectedDatabase.isBlank(), "REQUEST_INVALID");
        }
    }

    public enum Result { VERIFIED, RECOVERED, ALREADY_RECOVERED }

    /**
     * 调用方必须提供无活动事务的专用连接；锁序与在线凭据保存相同，提交结果未知不得盲目重试。
     * 此方法不触碰资金表，仅读取PRE_DEDUCT证据；任何异常立即回滚后向维护边界重抛。
     */
    public Result recover(Connection connection, Request request, boolean apply) throws SQLException {
        require(connection.getAutoCommit(), "CONNECTION_ALREADY_IN_TRANSACTION");
        require(request.expectedDatabase().equals(connection.getCatalog()), "DATABASE_MISMATCH");
        int isolation = connection.getTransactionIsolation();
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        connection.setAutoCommit(false);
        try {
            Result result = recoverInTransaction(connection, request, apply);
            if (apply) connection.commit(); else connection.rollback();
            return result;
        } catch (SQLException | RuntimeException failure) {
            connection.rollback();
            throw failure;
        } finally {
            connection.setAutoCommit(true);
            connection.setTransactionIsolation(isolation);
        }
    }

    private Result recoverInTransaction(Connection c, Request r, boolean apply) throws SQLException {
        var order = one(rows(c, "select * from mc_order where order_Id=? for update", r.orderId()));
        var ext = one(rows(c, "select * from recharge_order_ext where order_id=? for update", r.orderId()));
        require(r.orgCode().equals(order.get("org_code")) && r.orgCode().equals(ext.get("org_code"))
                && r.orderId().equals(order.get("order_id")) && r.orderId().equals(ext.get("order_id"))
                && same(order.get("out_orderid"), ext.get("out_order_sn"))
                && CHANNEL.equals(order.get("supplier_channel")) && number(order, "num") == 1
                && number(order, "order_price") > 0, "ORDER_OWNERSHIP_MISMATCH");
        require("DB_PERSISTED".equals(ext.get("persist_status"))
                && "PASSED".equals(ext.get("validation_status"))
                && List.of("1", "TRUE", "true").contains(ext.get("balance_reserved")), "ORDER_NOT_PERSISTED_OR_RESERVED");
        require(blank(order.get("callback_url")), "NOTIFY_URL_NOT_EMPTY");
        require(ext.get("voucher_use_time") == null && ext.get("voucher_invalid_time") == null, "VOUCHER_TERMINAL_FACT");

        var flows = rows(c, "select * from mc_order_supplier_flow where order_Id=? and status='success' for update", r.orderId());
        var flow = one(flows);
        require(number(flow, "id") == r.sourceFlowId() && CHANNEL.equals(flow.get("channel"))
                && same(flow.get("out_order_no"), order.get("out_orderid")), "SOURCE_FLOW_MISMATCH");
        JsonNode originalRequest = parse(flow.get("request_param"));
        for (var entry : Map.of("order_id", r.orderId(), "org_code", r.orgCode(),
                "out_order_sn", order.get("out_orderid"), "account", order.get("account"),
                "supplier_channel", CHANNEL, "product_id", order.get("parent_id"),
                "selected_product_id", order.get("produt_code"),
                "supplier_request_id", "dispatch-" + r.orderId()).entrySet()) {
            require(originalRequest.path(entry.getKey()).isTextual()
                    && entry.getValue().equals(originalRequest.path(entry.getKey()).textValue()), "ORIGINAL_REQUEST_MISMATCH");
        }
        require(originalRequest.path("order_price").isIntegralNumber()
                && originalRequest.path("order_price").canConvertToLong()
                && originalRequest.path("order_price").longValue() == number(order, "order_price"), "ORIGINAL_AMOUNT_MISMATCH");
        JsonNode wrapper = parse(flow.get("response_result"));
        require("200".equals(wrapper.path("response_code").textValue())
                && r.orderId().equals(wrapper.path("supplier_order_id").textValue())
                && wrapper.path("raw_response").isTextual(), "SOURCE_RESPONSE_MISMATCH");
        String raw = wrapper.path("raw_response").textValue();
        var snapshot = new MinisoOrderSnapshot(r.orgCode(), r.orderId(), order.get("account"),
                ext.get("supplier_batch_id"), 1);
        var receipt = MinisoRechargeSupplierGatewayAdapter.confirmIssuedReceipt(snapshot,
                new MinisoApiResponse(200, parse(raw), raw))
                .orElseThrow(() -> rejected("ORIGINAL_RECEIPT_INVALID"));

        var receipts = rows(c, "select * from recharge_miniso_receipt where order_id=? or coupon_code=? for update",
                r.orderId(), receipt.couponCode());
        require(rows(c, "select order_id from recharge_miniso_writeoff_event where order_id=? or coupon_code=? for update",
                r.orderId(), receipt.couponCode()).isEmpty(), "WRITEOFF_EVIDENCE_PRESENT");
        // 共享桥接将来启用时也不得绕过其已认证证据；当前0023尚未实施，存在表即读取，读取异常失败关闭。
        if (tableExists(c, "mc_miniso_callback_inbox")) {
            require(rows(c, "select order_id from mc_miniso_callback_inbox where order_id=? or coupon_code=? for update",
                    r.orderId(), receipt.couponCode()).isEmpty(), "SHARED_WRITEOFF_EVIDENCE_PRESENT");
        }
        var tasks = rows(c, "select * from recharge_dispatch_outbox where order_id=? for update", r.orderId());
        require(tasks.size() == 2, "UNEXPECTED_FULFILLMENT_TASKS");
        for (var task : tasks) {
            require(r.orgCode().equals(task.get("org_code")) && same(order.get("out_orderid"), task.get("out_order_sn")), "TASK_OWNERSHIP_MISMATCH");
        }
        var dispatch = one(tasks.stream().filter(t -> "SUPPLIER_DISPATCH".equals(t.get("event_type"))).toList());
        var query = one(tasks.stream().filter(t -> "SUPPLIER_QUERY".equals(t.get("event_type"))).toList());
        require(("dispatch-" + r.orderId()).equals(dispatch.get("event_id")) && SUCCESS.equals(dispatch.get("status"))
                && dispatch.get("locked_until") == null, "DISPATCH_NOT_COMPLETED");
        require(("supplier-query-" + r.orderId()).equals(query.get("event_id")) && query.get("locked_until") == null,
                "QUERY_ID_OR_LEASE_MISMATCH");
        var balance = one(rows(c, "select * from recharge_balance_delta_outbox where order_id=? for update", r.orderId()));
        require(r.orgCode().equals(balance.get("org_code")) && "PRE_DEDUCT".equals(balance.get("delta_type"))
                && number(balance, "delta_amount") == -number(order, "order_price") && SUCCESS.equals(balance.get("status")),
                "PREDEDUCT_OR_REFUND_CONFLICT");
        require(rows(c, "select order_id from recharge_notify_task where order_id=? for update", r.orderId()).isEmpty(), "NOTIFY_TASK_PRESENT");
        String audit = "ORIGINAL_FLOW_RECOVERY:" + r.changeId() + ":" + r.sourceFlowId();
        if (number(order, "order_status") == LegacyOrderStatusCode.SUCCESS.code()
                && SUCCESS.equals(ext.get("dispatch_status"))) {
            require(receipts.size() == 1 && sameReceipt(receipts.getFirst(), receipt)
                    && number(order, "flow_id") == r.sourceFlowId() && SUCCESS.equals(query.get("status"))
                    && number(query, "version") == r.queryVersion() + 1 && audit.equals(query.get("last_error")),
                    "COMPLETED_STATE_CONFLICT");
            // 旧恢复可能已成功但漏写卡管data；全部证据再次校验后只补投影，不重做履约和资金动作。
            // 参见详细设计「名创卡密导出兼容修复（2026-10-21）」。
            fillLegacyData(c, order, receipt, apply);
            return Result.ALREADY_RECOVERED;
        }
        require(number(order, "order_status") == LegacyOrderStatusCode.SUPPLIER_ACCEPTED.code()
                && ACCEPTED.equals(ext.get("dispatch_status")) && blank(ext.get("voucher_use_state"))
                && receipts.isEmpty(), "ORDER_STATE_OR_RECEIPT_CONFLICT");
        require(MANUAL.equals(query.get("status")) && number(query, "version") == r.queryVersion()
                && number(query, "retry_count") == number(query, "max_retry_count"), "QUERY_STATE_OR_VERSION_CONFLICT");
        MinisoLegacyOrderData.requireCompatible(order.get("data"), receipt);
        if (!apply) return Result.VERIFIED;

        LocalDateTime now = LocalDateTime.now(DATABASE_ZONE);
        changed(c, "insert into recharge_miniso_receipt(order_id,org_code,batch_id,coupon_code,phone,begin_time,end_time,coupon_status,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
                receipt.orderId(), receipt.orgCode(), receipt.batchId(), receipt.couponCode(), receipt.phone(),
                receipt.beginTime(), receipt.endTime(), receipt.couponStatus(), now, now);
        fillLegacyData(c, order, receipt, true);
        changed(c, "update mc_order set order_status=?,data_status='PROCESSED',flow_id=?,fail_code=null,fail_reason=null,remark=?,edit_time=? where order_Id=? and order_status=?",
                LegacyOrderStatusCode.SUCCESS.code(), r.sourceFlowId(), audit, now, r.orderId(), LegacyOrderStatusCode.SUPPLIER_ACCEPTED.code());
        changed(c, "update recharge_order_ext set dispatch_status='SUCCESS',voucher_use_state=?,last_error='',updated_at=? where order_id=? and dispatch_status=?",
                VoucherUseState.UNUSED.code(), now, r.orderId(), ACCEPTED);
        changed(c, "update recharge_dispatch_outbox set status='SUCCESS',version=version+1,last_error=?,locked_by=null,locked_until=null,updated_at=? where event_id=? and status=? and version=? and locked_until is null",
                audit, now, query.get("event_id"), MANUAL, r.queryVersion());
        return Result.RECOVERED;
    }

    private JsonNode parse(String value) {
        require(value != null && value.length() <= MAX_EVIDENCE_CHARS, "EVIDENCE_SIZE_INVALID");
        try {
            JsonNode node = json.readTree(value);
            require(node != null && node.isObject(), "EVIDENCE_NOT_OBJECT");
            return node;
        } catch (java.io.IOException failure) {
            // 原始JSON异常含业务原文，立即安全包装重抛，不把cause带入维护日志。
            throw rejected("EVIDENCE_JSON_INVALID");
        }
    }

    private static void fillLegacyData(Connection c, Map<String, String> order,
            MinisoFulfillmentReceipt receipt, boolean apply) throws SQLException {
        MinisoLegacyOrderData.requireCompatible(order.get("data"), receipt);
        if (apply && blank(order.get("data"))) {
            changed(c, "update mc_order set data=? where order_Id=? and org_code=? and (data is null or trim(data)='')",
                    MinisoLegacyOrderData.fromReceipt(receipt),
                    receipt.orderId(), receipt.orgCode());
        }
    }

    private static boolean sameReceipt(Map<String, String> row, MinisoFulfillmentReceipt receipt) {
        return receipt.orderId().equals(row.get("order_id")) && receipt.orgCode().equals(row.get("org_code"))
                && receipt.batchId() == number(row, "batch_id") && receipt.couponCode().equals(row.get("coupon_code"))
                && receipt.phone().equals(row.get("phone")) && receipt.beginTime().equals(row.get("begin_time"))
                && receipt.endTime().equals(row.get("end_time"));
    }

    private static boolean tableExists(Connection c, String table) throws SQLException {
        try (var tables = c.getMetaData().getTables(c.getCatalog(), null, table, new String[]{"TABLE"})) {
            return tables.next();
        }
    }

    private static List<Map<String, String>> rows(Connection c, String sql, Object... args) throws SQLException {
        try (var statement = c.prepareStatement(sql)) {
            statement.setQueryTimeout(10);
            for (int i = 0; i < args.length; i++) statement.setObject(i + 1, args[i]);
            try (var rs = statement.executeQuery()) {
                var result = new ArrayList<Map<String, String>>();
                while (rs.next()) {
                    var row = new LinkedHashMap<String, String>();
                    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                        row.put(rs.getMetaData().getColumnLabel(i).toLowerCase(Locale.ROOT), rs.getString(i));
                    }
                    result.add(row);
                }
                return result;
            }
        }
    }

    private static void changed(Connection c, String sql, Object... args) throws SQLException {
        try (var statement = c.prepareStatement(sql)) {
            statement.setQueryTimeout(10);
            for (int i = 0; i < args.length; i++) statement.setObject(i + 1, args[i]);
            require(statement.executeUpdate() == 1, "WRITE_CAS_MISMATCH");
        }
    }

    private static Map<String, String> one(List<Map<String, String>> rows) {
        require(rows.size() == 1, "EVIDENCE_ROW_COUNT");
        return rows.getFirst();
    }
    private static long number(Map<String, String> row, String key) {
        try { return Long.parseLong(row.get(key)); }
        catch (RuntimeException failure) { throw rejected("NUMBER_INVALID"); }
    }
    private static boolean same(String left, String right) { return left != null && left.equals(right); }
    private static boolean blank(String value) { return value == null || value.isBlank(); }
    private static void require(boolean condition, String reason) { if (!condition) throw rejected(reason); }
    private static IllegalStateException rejected(String reason) { return new IllegalStateException("RECOVERY_" + reason); }

    /**
     * 参数：[--apply|--dry-run] orgCode orderId sourceFlowId queryVersion changeId；缺省为dry-run。
     * stdin仅接收jdbcUrl/username/password，执行程序不从命令行或日志接收秘密。
     */
    public static void main(String[] args) {
        try {
            boolean explicitMode = args.length == 6;
            boolean apply = explicitMode && "--apply".equals(args[0]);
            require(args.length == 5 || (explicitMode && (apply || "--dry-run".equals(args[0]))), "CLI_ARGUMENTS");
            int start = explicitMode ? 1 : 0;
            var request = new Request(args[start], args[start + 1], Long.parseLong(args[start + 2]),
                    Long.parseLong(args[start + 3]), args[start + 4], TEST_DATABASE);
            byte[] input = System.in.readNBytes(MAX_INPUT_BYTES + 1);
            require(input.length <= MAX_INPUT_BYTES, "CLI_INPUT_TOO_LARGE");
            var tool = new MinisoOriginalResponseRecovery(new ObjectMapper());
            JsonNode config = tool.parse(new String(input, java.nio.charset.StandardCharsets.UTF_8));
            String jdbcUrl = config.path("jdbcUrl").textValue();
            require(jdbcUrl != null && jdbcUrl.startsWith("jdbc:mysql:"), "CLI_DATABASE_URL");
            require(("/" + TEST_DATABASE).equals(URI.create(jdbcUrl.substring("jdbc:".length())).getPath()), "CLI_DATABASE_URL");
            try (var connection = DriverManager.getConnection(jdbcUrl,
                    config.path("username").textValue(), config.path("password").textValue())) {
                var result = tool.recover(connection, request, apply);
                System.out.println(tool.json.writeValueAsString(Map.of("result", result.name(),
                        "orderId", request.orderId(), "orgCode", request.orgCode(),
                        "sourceFlowId", request.sourceFlowId(), "changeId", request.changeId())));
            }
        } catch (Exception failure) {
            // 一次性维护边界非高频；保留安全分类栈，不记录可能含券码/密码的原始message或cause。
            var safe = new Exception("MINISO recovery failed: " + failure.getClass().getSimpleName());
            safe.setStackTrace(failure.getStackTrace());
            LoggerFactory.getLogger(MinisoOriginalResponseRecovery.class).error("名创恢复停止，须只读核实后再处理", safe);
            String reason = failure instanceof IllegalStateException && failure.getMessage() != null
                    && failure.getMessage().matches("RECOVERY_[A-Z_]+") ? failure.getMessage() : "RECOVERY_FAILED_INSPECT_STATE";
            System.out.println("{\"result\":\"STOPPED\",\"reason\":\"" + reason + "\"}");
            System.exit(1);
        }
    }
}

package com.cardshop.trade.engine.infrastructure.maintenance;

import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.cardshop.trade.engine.infrastructure.persistence.MinisoLegacyOrderData;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.LoggerFactory;

/**
 * 已成功名创订单的旧卡管卡密字段补齐工具，不注册Bean、HTTP或Job。
 * <p>参见详细设计「名创卡密导出兼容修复（2026-10-21）」。默认dry-run，仅从已保存凭据生成投影，
 * 严格验证整批归属后同事务补齐空data；不接触供应商、资金、通知或核销事实。
 */
public final class MinisoLegacyOrderDataRepair {
    private static final String TEST_DATABASE = "cardshop_test_new";
    private static final int MAX_ORDERS = 20;
    private static final int MAX_INPUT_BYTES = 16_384;
    private static final String UPDATE_SQL = "update mc_order set data=? where order_Id=? and org_code=?"
            + " and order_status=12 and (data is null or trim(data)='')";

    /** repaired在dry-run中表示将补齐的行数；alreadyPresent表示已存在且匹配的行数，不含任何卡密。 */
    public record Result(int repaired, int alreadyPresent) { }

    /** 专用无活动事务连接；按主键排序持锁，所有校验先于写入，任一失败整批回滚。 */
    public Result repair(Connection c, String expectedDatabase, String orgCode,
                         List<String> orderIds, boolean apply) throws SQLException {
        require(c.getAutoCommit(), "CONNECTION_IN_TRANSACTION");
        require(expectedDatabase != null && expectedDatabase.equals(c.getCatalog()), "DATABASE_MISMATCH");
        require(orgCode != null && orgCode.matches("[A-Za-z0-9_-]{1,64}") && orderIds != null
                && !orderIds.isEmpty() && orderIds.size() <= MAX_ORDERS
                && orderIds.stream().allMatch(id -> id != null && id.matches("[A-Za-z0-9_-]{1,100}"))
                && new HashSet<>(orderIds).size() == orderIds.size(), "SCOPE_INVALID");
        int isolation = c.getTransactionIsolation();
        c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        c.setAutoCommit(false);
        try {
            var pending = new ArrayList<MinisoFulfillmentReceipt>();
            for (String orderId : orderIds.stream().sorted().toList()) {
                var order = row(c, "select * from mc_order where order_Id=? for update", orderId);
                var ext = row(c, "select * from recharge_order_ext where order_id=? for update", orderId);
                var saved = row(c, "select * from recharge_miniso_receipt where order_id=? for update", orderId);
                require(orgCode.equals(order.get("org_code")) && orgCode.equals(ext.get("org_code"))
                        && orgCode.equals(saved.get("org_code")) && "MINISO".equals(order.get("supplier_channel"))
                        && "1".equals(order.get("num")) && "12".equals(order.get("order_status"))
                        && "DB_PERSISTED".equals(ext.get("persist_status")) && "SUCCESS".equals(ext.get("dispatch_status"))
                        && order.get("out_orderid") != null && order.get("out_orderid").equals(ext.get("out_order_sn"))
                        && order.get("account") != null && order.get("account").equals(saved.get("phone")), "OWNERSHIP_OR_STATE_MISMATCH");
                var receipt = receipt(orderId, orgCode, saved);
                require(ext.get("supplier_batch_id") != null && ext.get("supplier_batch_id").matches("[0-9]{1,10}")
                        && Long.parseLong(ext.get("supplier_batch_id")) == receipt.batchId(), "BATCH_MISMATCH");
                MinisoLegacyOrderData.requireCompatible(order.get("data"), receipt);
                if (order.get("data") == null || order.get("data").isBlank()) pending.add(receipt);
            }
            if (apply) {
                for (var receipt : pending) {
                    try (var statement = c.prepareStatement(UPDATE_SQL)) {
                        statement.setQueryTimeout(10);
                        statement.setString(1, MinisoLegacyOrderData.fromReceipt(receipt));
                        statement.setString(2, receipt.orderId());
                        statement.setString(3, receipt.orgCode());
                        require(statement.executeUpdate() == 1, "WRITE_CONFLICT");
                    }
                }
                c.commit();
            } else c.rollback();
            return new Result(pending.size(), orderIds.size() - pending.size());
        } catch (SQLException | RuntimeException failure) {
            c.rollback();
            throw failure;
        } finally {
            c.setAutoCommit(true);
            c.setTransactionIsolation(isolation);
        }
    }

    private static MinisoFulfillmentReceipt receipt(String orderId, String orgCode, Map<String, String> row) {
        try {
            return new MinisoFulfillmentReceipt(orderId, orgCode, Integer.parseInt(row.get("batch_id")),
                    row.get("coupon_code"), row.get("phone"), row.get("begin_time"), row.get("end_time"),
                    Integer.parseInt(row.get("coupon_status")));
        } catch (RuntimeException failure) {
            // 数字/日期原始异常可能含敏感字段，立即安全包装重抛，不将cause输出到维护日志。
            throw new IllegalStateException("LEGACY_DATA_RECEIPT_INVALID");
        }
    }

    private static Map<String, String> row(Connection c, String sql, String id) throws SQLException {
        try (var statement = c.prepareStatement(sql)) {
            statement.setQueryTimeout(10);
            statement.setString(1, id);
            try (var result = statement.executeQuery()) {
                require(result.next(), "EVIDENCE_MISSING");
                var row = new LinkedHashMap<String, String>();
                for (int i = 1; i <= result.getMetaData().getColumnCount(); i++)
                    row.put(result.getMetaData().getColumnLabel(i).toLowerCase(Locale.ROOT), result.getString(i));
                require(!result.next(), "EVIDENCE_NOT_UNIQUE");
                return row;
            }
        }
    }

    private static void require(boolean condition, String reason) {
        if (!condition) throw new IllegalStateException("LEGACY_DATA_" + reason);
    }

    /** 参数：[--dry-run|--apply] orgCode orderId...；连接秘密只通过stdin传入，CLI限定测试catalog。 */
    public static void main(String[] args) {
        try {
            boolean explicit = args.length > 0 && args[0].startsWith("--");
            boolean apply = explicit && "--apply".equals(args[0]);
            int start = explicit ? 1 : 0;
            require(args.length >= start + 2 && (!explicit || apply || "--dry-run".equals(args[0])), "CLI_ARGUMENTS");
            byte[] input = System.in.readNBytes(MAX_INPUT_BYTES + 1);
            require(input.length <= MAX_INPUT_BYTES, "CLI_INPUT_TOO_LARGE");
            var config = new ObjectMapper().readTree(input);
            String url = config.path("jdbcUrl").textValue();
            require(url != null && url.startsWith("jdbc:mysql:")
                    && ("/" + TEST_DATABASE).equals(URI.create(url.substring("jdbc:".length())).getPath()), "DATABASE_MISMATCH");
            try (var c = DriverManager.getConnection(url, config.path("username").textValue(), config.path("password").textValue())) {
                var result = new MinisoLegacyOrderDataRepair().repair(c, TEST_DATABASE, args[start],
                        List.of(args).subList(start + 1, args.length), apply);
                System.out.println(new ObjectMapper().writeValueAsString(Map.of(
                        "mode", apply ? "apply" : "dry-run", "result", "VERIFIED", "counts", result)));
            }
        } catch (Exception failure) {
            // 独立维护边界非高频，保留安全类型/栈；数据库异常message/cause可能包含卡密或连接秘密。
            var safe = new Exception("MINISO legacy data repair stopped: " + failure.getClass().getSimpleName());
            safe.setStackTrace(failure.getStackTrace());
            LoggerFactory.getLogger(MinisoLegacyOrderDataRepair.class).error("名创卡密补齐停止，须只读核实", safe);
            System.out.println("{\"result\":\"STOPPED\"}");
            System.exit(1);
        }
    }
}

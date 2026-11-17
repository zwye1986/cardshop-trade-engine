package com.cardshop.trade.engine.migration;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import liquibase.Scope;
import liquibase.change.custom.CustomTaskChange;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.CustomChangeException;
import liquibase.exception.ValidationErrors;
import liquibase.exception.CustomPreconditionFailedException;
import liquibase.exception.CustomPreconditionErrorException;
import liquibase.precondition.CustomPrecondition;
import liquibase.executor.ExecutorService;
import liquibase.resource.ResourceAccessor;

/**
 * 兼容旧卡管与已迁移库的受控前向迁移，不创建旧业务表、不推断订单归属。
 * 每个阶段先验证实际元数据，DDL已提交而账本未落地时可以原状态续跑；参见详细设计8.2.1。
 */
public final class ProductionCompatibilityChange implements CustomTaskChange, CustomPrecondition {
    private static final String OWNED_COMMENT = "新引擎订单归属标记：0旧系统，1新引擎";
    private static final String KEY_COLUMN = "direct_recharge_out_order_key";
    private static final String KEY_INDEX = "uk_direct_recharge_out_order_key";
    private static final String KEY_COMMENT = "新引擎直充订单幂等唯一键";
    private static final String LEGACY_EXPRESSION = "CASE WHEN product_type=1 AND org_code IS NOT NULL "
            + "AND org_code<>'' AND out_orderId IS NOT NULL AND out_orderId<>'' "
            + "THEN concat(org_code,':',out_orderId) ELSE NULL END";
    private static final String PRODUCTION_EXPRESSION = LEGACY_EXPRESSION.replace("AND org_code<>'' ", "");
    private static final String TARGET_EXPRESSION = LEGACY_EXPRESSION.replace("WHEN ", "WHEN trade_engine_owned=1 AND ");
    // 按主键范围分批，限制单次回填锁和事务体积；批量不是生产容量/MDL预算的替代。
    private static final int BACKFILL_BATCH_ROWS = 1000;
    private static final String VALID_IDENTITY = "o.product_type=1 AND o.org_code IS NOT NULL AND o.org_code<>'' "
            + "AND o.out_orderId IS NOT NULL AND o.out_orderId<>''";
    private String phase;

    /** Liquibase参数，限定为本次发布的稳定阶段，不接受表名或业务过滤条件。 */
    public void setPhase(String phase) { this.phase = phase; }

    /** 每次入口执行都核实已记录状态，不用修改旧账本来触发守卫；参见详细设计8.2.1。 */
    @Override
    public void check(Database database) throws CustomPreconditionFailedException, CustomPreconditionErrorException {
        try {
            requireExecutable(database);
            var connection = ((JdbcConnection) database.getConnection()).getUnderlyingConnection();
            environmentGuard(connection);
            if (tableExists(connection, "databasechangelog") && "1".equals(text(connection,
                    "SELECT COUNT(*) FROM databasechangelog WHERE ID='0026-compat-postconditions' "
                            + "AND AUTHOR='cardshop' AND FILENAME='db/changelog/0026-legacy-order-idempotency-postconditions.yaml'"))) {
                verifyTarget(connection, true);
            }
        } catch (IllegalStateException exception) {
            throw new CustomPreconditionFailedException(exception.getMessage());
        } catch (SQLException exception) {
            throw new CustomPreconditionErrorException("Compatibility entry metadata read failed", exception);
        }
    }

    private static void requireExecutable(Database database) {
        // CustomTaskChange直接使用JDBC，updateSQL不会自动拦截；禁止预览伪装只读却实际回填/改表。
        require(Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database).updatesDatabase(),
                "SQL preview is not supported for dynamic compatibility migration; rehearse on disposable MySQL");
        require("mysql".equals(database.getShortName()) && database.getConnection() instanceof JdbcConnection,
                "MySQL JDBC connection required");
    }

    @Override
    public void execute(Database database) throws CustomChangeException {
        try {
            requireExecutable(database);
            var connection = ((JdbcConnection) database.getConnection()).getUnderlyingConnection();
            switch (phase) {
                case "guard" -> environmentGuard(connection);
                case "backfill" -> backfill(connection);
                case "duplicates" -> targetGuard(connection);
                case "reshape" -> reshape(connection);
                case "preconditions" -> verifyTarget(connection, false);
                case "postconditions" -> verifyTarget(connection, true);
                default -> throw new IllegalStateException("Unknown compatibility phase");
            }
        } catch (SQLException | IllegalStateException exception) {
            // 低频维护边界只转换错误并重抛，异常栈交由Liquibase记录，禁止遇错继续或伪造账本。
            throw new CustomChangeException("Compatibility phase " + phase + " stopped: " + exception.getMessage(), exception);
        }
    }

    private static void environmentGuard(Connection connection) throws SQLException {
        require(text(connection, "SELECT VERSION()").matches("8\\.0\\.(3[6-9]|[4-9][0-9])([-.].*)?"),
                "Supported MySQL 8.0.36+ required");
        require("1".equals(text(connection, "SELECT @@lower_case_table_names")), "lower_case_table_names must be 1");
        require("READ-COMMITTED".equals(text(connection, "SELECT @@transaction_isolation")), "READ-COMMITTED required");
        require("utf8mb3_general_ci".equals(text(connection, "SELECT @@collation_database")), "utf8mb3_general_ci database required");
        requiredColumns(connection, "mc_order", "order_Id", "org_code", "out_orderId", "product_type");
        requiredColumns(connection, "mc_depart_record", "order_id", "type", "before_balance", "balance");
        require(hasUnique(connection, "mc_order", List.of("order_id")), "mc_order platform order unique index required");
        primaryKey(connection);
        validateOwned(connection, false);
        validateGenerated(connection, false);
        validateKeyDependencies(connection);
        validateExt(connection);
    }

    private static void validateOwned(Connection connection, boolean required) throws SQLException {
        var column = column(connection, "mc_order", "trade_engine_owned");
        require(!required || column != null, "trade_engine_owned missing");
        if (column == null) return;
        require(column.type().matches("tinyint(\\([1-4]\\))?") && "NO".equals(column.nullable())
                        && "0".equals(column.defaultValue()) && column.extra().isEmpty()
                        && OWNED_COMMENT.equals(column.comment()), "Unknown trade_engine_owned definition");
        zero(connection, "SELECT COUNT(*) FROM mc_order WHERE trade_engine_owned IS NULL OR trade_engine_owned NOT IN (0,1)",
                "Invalid trade_engine_owned value");
    }

    private static void validateExt(Connection connection) throws SQLException {
        if (!tableExists(connection, "recharge_order_ext")) return;
        requiredColumns(connection, "recharge_order_ext", "order_id", "org_code", "out_order_sn");
        require(hasUnique(connection, "recharge_order_ext", List.of("order_id"))
                        && hasUnique(connection, "recharge_order_ext", List.of("org_code", "out_order_sn")),
                "recharge_order_ext identity unique indexes differ");
        // 关联仍复用平台号索引，但不可变身份须与Java精确文本比较一致；不改变唯一键排序规则。
        zero(connection, "SELECT COUNT(*) FROM recharge_order_ext e LEFT JOIN mc_order o ON o.order_Id=e.order_id "
                        + "WHERE o.order_Id IS NULL OR NOT (BINARY o.order_Id <=> BINARY e.order_id) "
                        + "OR NOT (BINARY o.org_code <=> BINARY e.org_code) OR NOT (BINARY o.out_orderId <=> BINARY e.out_order_sn) "
                        + "OR o.product_type IS NULL OR o.product_type<>1 OR e.org_code IS NULL OR e.org_code='' "
                        + "OR e.out_order_sn IS NULL OR e.out_order_sn=''", "Orphan or mismatched recharge_order_ext identity");
    }

    /** 先完整校验再回填；只认本库ext，不按日期、渠道、状态或历史单号猜归属。 */
    private static void backfill(Connection connection) throws SQLException {
        validateOwned(connection, true);
        validateExt(connection);
        validateOwnedIdentity(connection, false);
        if (!tableExists(connection, "recharge_order_ext")) return;
        String primary = primaryKey(connection);
        var preservedTimes = new StringBuilder();
        try (var statement = connection.prepareStatement("SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                + "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='mc_order' AND (EXTRA LIKE '%on update%' OR COLUMN_NAME='update_time')")) {
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    String name = identifier(rows.getString(1));
                    preservedTimes.append(", o.`").append(name).append("`=o.`").append(name).append('`');
                }
            }
        }
        Object after = null;
        long changed = 0;
        while (true) {
            Object upper = null;
            String range = after == null ? "" : " AND o.`" + primary + "`>?";
            try (var statement = connection.prepareStatement("SELECT o.`" + primary + "` FROM mc_order o "
                    + "JOIN recharge_order_ext e ON e.order_id=o.order_Id WHERE o.trade_engine_owned=0" + range
                    + " ORDER BY o.`" + primary + "` LIMIT " + BACKFILL_BATCH_ROWS)) {
                if (after != null) statement.setObject(1, after);
                try (var rows = statement.executeQuery()) { while (rows.next()) upper = rows.getObject(1); }
            }
            if (upper == null) break;
            try (var statement = connection.prepareStatement("UPDATE mc_order o JOIN recharge_order_ext e ON e.order_id=o.order_Id "
                    + "SET o.trade_engine_owned=1" + preservedTimes + " WHERE o.trade_engine_owned=0" + range
                    + " AND o.`" + primary + "`<=?")) {
                int parameter = 1;
                if (after != null) statement.setObject(parameter++, after);
                statement.setObject(parameter, upper);
                changed += statement.executeUpdate();
            }
            // 此changeset声明runInTransaction=false，按批提交而非把全表回填包在DDL事务中。
            if (!connection.getAutoCommit()) connection.commit();
            after = upper;
        }
        Scope.getCurrentScope().getLog(ProductionCompatibilityChange.class).info("Compatibility ownership backfill rows=" + changed);
        validateOwnedIdentity(connection, true);
    }

    private static void validateOwnedIdentity(Connection connection, boolean extMustBeOwned) throws SQLException {
        zero(connection, "SELECT COUNT(*) FROM mc_order o WHERE o.trade_engine_owned=1 AND NOT (" + VALID_IDENTITY + ")",
                "Owned order has invalid direct identity");
        // NULL产品类型必须显式拒绝，SQL三值逻辑不能用NOT替代这一项。
        zero(connection, "SELECT COUNT(*) FROM mc_order WHERE trade_engine_owned=1 AND product_type IS NULL", "Owned product_type is NULL");
        if (tableExists(connection, "recharge_order_ext")) {
            zero(connection, "SELECT COUNT(*) FROM mc_order o LEFT JOIN recharge_order_ext e ON e.order_id=o.order_Id "
                    + "WHERE o.trade_engine_owned=1 AND e.order_id IS NULL", "Owned order has no recharge_order_ext");
            if (extMustBeOwned) zero(connection, "SELECT COUNT(*) FROM recharge_order_ext e JOIN mc_order o ON o.order_Id=e.order_id "
                    + "WHERE o.trade_engine_owned<>1", "Ext order not owned");
        } else zero(connection, "SELECT COUNT(*) FROM mc_order WHERE trade_engine_owned=1", "Owned order has no recharge_order_ext table");
    }

    private static void targetGuard(Connection connection) throws SQLException {
        validateOwned(connection, true);
        validateExt(connection);
        validateOwnedIdentity(connection, true);
        validateGenerated(connection, false);
        validateKeyDependencies(connection);
        // 保留既有冒号协议，下面按实际拼接键分组拒绝碰撞，不擅自禁止接口允许的外单字符。
        zero(connection, "SELECT COUNT(*) FROM mc_order WHERE trade_engine_owned=1 AND "
                + "CHAR_LENGTH(CONCAT(org_code,':',out_orderId))>191",
                "Owned identity incompatible with concatenated key");
        zero(connection, "SELECT COUNT(*) FROM (SELECT CONCAT(org_code,':',out_orderId) AS target_key FROM mc_order "
                + "WHERE trade_engine_owned=1 GROUP BY target_key HAVING COUNT(*)>1) duplicates", "Duplicate owned direct identity");
    }

    /** 有旧唯一键时必须一次ALTER完成替换，不向可运行的业务暴露无约束中间态。 */
    private static void reshape(Connection connection) throws SQLException {
        targetGuard(connection);
        var existing = column(connection, "mc_order", KEY_COLUMN);
        if (existing != null && normalize(existing.expression()).equals(normalize(TARGET_EXPRESSION))) return;
        boolean indexed = indexes(connection, "mc_order").containsKey(KEY_INDEX);
        String definition = "varchar(191)";
        if (existing != null) definition += " CHARACTER SET " + identifier(existing.charset())
                + " COLLATE " + identifier(existing.collation());
        String ddl = "ALTER TABLE mc_order " + (indexed ? "DROP INDEX " + KEY_INDEX + ", " : "")
                + (existing == null ? "ADD COLUMN " : "MODIFY COLUMN ") + KEY_COLUMN + " " + definition
                + " GENERATED ALWAYS AS (" + TARGET_EXPRESSION + ") STORED COMMENT '" + KEY_COMMENT + "'"
                + (indexed ? ", ADD UNIQUE KEY " + KEY_INDEX + " (" + KEY_COLUMN + ")" : "");
        try (var statement = connection.createStatement()) { statement.execute(ddl); }
        validateGenerated(connection, true);
    }

    private static void validateGenerated(Connection connection, boolean targetRequired) throws SQLException {
        var existing = column(connection, "mc_order", KEY_COLUMN);
        require(!targetRequired || existing != null, "Generated direct key missing");
        if (existing == null) return;
        require("varchar(191)".equals(existing.type()) && "STORED GENERATED".equals(existing.extra())
                        && "YES".equals(existing.nullable()) && existing.defaultValue() == null
                        && List.of("utf8mb3_general_ci", "utf8mb4_general_ci").contains(existing.collation()),
                "Unknown generated direct key definition");
        String expression = normalize(existing.expression());
        boolean target = expression.equals(normalize(TARGET_EXPRESSION));
        require(target || (!targetRequired && (expression.equals(normalize(LEGACY_EXPRESSION))
                        || expression.equals(normalize(PRODUCTION_EXPRESSION)))), "Unknown generated direct key expression");
        require(!target || KEY_COMMENT.equals(existing.comment()), "Unknown target direct key comment");
    }

    private static void validateKeyDependencies(Connection connection) throws SQLException {
        for (var entry : indexes(connection, "mc_order").entrySet()) {
            if (KEY_INDEX.equals(entry.getKey())) require(entry.getValue().unique()
                    && entry.getValue().columns().equals(List.of(KEY_COLUMN)) && entry.getValue().fullLength(),
                    "Unknown direct key index definition");
            else require(!entry.getValue().columns().contains(KEY_COLUMN), "Additional index depends on generated direct key");
        }
        zero(connection, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='mc_order' "
                + "AND COLUMN_NAME<>'" + KEY_COLUMN + "' AND GENERATION_EXPRESSION LIKE '%" + KEY_COLUMN + "%'",
                "Additional generated column depends on direct key");
    }

    private static void verifyTarget(Connection connection, boolean post) throws SQLException {
        environmentGuard(connection);
        targetGuard(connection);
        validateGenerated(connection, true);
        zero(connection, "SELECT COUNT(*) FROM mc_order WHERE trade_engine_owned=0 AND direct_recharge_out_order_key IS NOT NULL",
                "Legacy order has non-NULL direct key");
        zero(connection, "SELECT COUNT(*) FROM mc_order WHERE trade_engine_owned=1 AND "
                + "NOT (direct_recharge_out_order_key <=> CONCAT(org_code,':',out_orderId))", "Owned generated value differs");
        if (!post) return;
        require(hasUnique(connection, "mc_order", List.of(KEY_COLUMN)), "Scoped direct unique key missing");
        require(indexes(connection, "mc_order").containsKey(KEY_INDEX), "Named scoped direct unique key missing");
        require(hasUnique(connection, "mc_depart_record", List.of("trade_engine_record_key")), "Fund unique key missing");
        requiredColumns(connection, "mc_order", "fail_code", "fail_reason");
        requiredColumns(connection, "recharge_order_ext", "request_hash", "request_hash_version", "request_hash_alg", "dispatch_status",
                "persist_status", "persist_source", "redis_order_version", "persisted_version", "validation_status",
                "dispatch_allowed", "balance_reserved", "persist_msg_id", "redis_order_id",
                "signing_key_id", "supplier_batch_id", "voucher_use_state", "voucher_use_time", "voucher_invalid_time",
                "balance_release_authorized_at", "balance_release_event_id", "balance_release_amount",
                "balance_no_release_decided_at", "balance_no_release_event_id");
        requiredColumns(connection, "recharge_notify_task", "delivery_channel", "route_snapshot", "encoded_payload", "claim_version");
        requiredColumns(connection, "recharge_miniso_receipt", "order_id", "coupon_code", "use_time");
        requiredColumns(connection, "recharge_miniso_writeoff_event", "event_id", "status", "version");
        requiredColumns(connection, "mc_miniso_callback_inbox", "event_id", "route_target", "order_id", "version");
        requiredColumns(connection, "mc_miniso_legacy_notify_task", "task_id", "event_id", "order_id");
        requiredColumns(connection, "mc_miniso_coupon_guard", "coupon_code", "created_at");
        requiredColumns(connection, "recharge_v12_signing_key", "key_id", "encrypted_sign_key");
        requiredColumns(connection, "recharge_v12_merchant_key", "org_code", "current_key_id");
        requiredColumns(connection, "recharge_v12_merchant_signing_key", "key_id", "org_code", "key_status");
        for (var contract : Map.ofEntries(
                Map.entry("recharge_outbox", List.of("event_id")),
                Map.entry("recharge_dispatch_outbox", List.of("event_id")),
                Map.entry("recharge_balance_delta_outbox", List.of("event_id")),
                Map.entry("recharge_notify_task", List.of("task_id")),
                Map.entry("recharge_accept_anomaly", List.of("anomaly_id")),
                Map.entry("recharge_notify_attempt", List.of("msg_id")),
                Map.entry("recharge_miniso_receipt", List.of("order_id", "coupon_code")),
                Map.entry("recharge_miniso_writeoff_event", List.of("event_id", "coupon_code")),
                Map.entry("mc_miniso_callback_inbox", List.of("event_id", "coupon_code")),
                Map.entry("mc_miniso_legacy_notify_task", List.of("task_id", "event_id")),
                Map.entry("mc_miniso_coupon_guard", List.of("coupon_code")),
                Map.entry("recharge_v12_signing_key", List.of("key_id")),
                Map.entry("recharge_v12_merchant_key", List.of("org_code")),
                Map.entry("recharge_v12_merchant_signing_key", List.of("key_id"))).entrySet()) {
            for (String key : contract.getValue()) {
                requiredColumns(connection, contract.getKey(), key);
                require(hasUnique(connection, contract.getKey(), List.of(key)), "Unique key missing: " + contract.getKey() + "." + key);
            }
        }
        validateExt(connection);
        validateOwnedIdentity(connection, true);
    }

    /** 仅规范化字面量外的MySQL格式，分隔符内容必须原样比较；参见详细设计8.2.1。 */
    private static String normalize(String expression) {
        var normalized = new StringBuilder();
        // 先分离SQL字符串（含成对单引号转义），禁止把 ': ' 或 '(:)' 清洗成约定的 ':'。
        // MySQL的GENERATION_EXPRESSION会把引号回显为反斜杠单引号，先还原此元数据表示。
        var tokens = java.util.regex.Pattern.compile("'(?:''|[^'])*'|[^']+")
                .matcher(expression.replace("\\'", "'"));
        while (tokens.find()) {
            String token = tokens.group();
            normalized.append(token.startsWith("'") ? token : token.toLowerCase(Locale.ROOT)
                    .replaceAll("_[a-z0-9]+$", "").replaceAll("[\\s`()]+", ""));
        }
        return normalized.toString();
    }

    private static String primaryKey(Connection connection) throws SQLException {
        var primary = indexes(connection, "mc_order").get("PRIMARY");
        require(primary != null && primary.fullLength() && primary.columns().size() == 1, "mc_order single primary key required for bounded backfill");
        return identifier(primary.columns().getFirst());
    }

    private static boolean hasUnique(Connection connection, String table, List<String> columns) throws SQLException {
        return indexes(connection, table).values().stream()
                .anyMatch(index -> index.unique() && index.fullLength() && index.columns().equals(columns));
    }

    private static Map<String, Index> indexes(Connection connection, String table) throws SQLException {
        var result = new LinkedHashMap<String, Index>();
        try (var statement = connection.prepareStatement("SELECT INDEX_NAME,NON_UNIQUE,COLUMN_NAME,SUB_PART,INDEX_TYPE,IS_VISIBLE "
                + "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? ORDER BY INDEX_NAME,SEQ_IN_INDEX")) {
            statement.setString(1, table);
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    String name = rows.getString(1);
                    var previous = result.get(name);
                    var columns = previous == null ? new ArrayList<String>() : new ArrayList<>(previous.columns());
                    String column = rows.getString(3);
                    columns.add(column == null ? "<expression>" : column.toLowerCase(Locale.ROOT));
                    boolean full = rows.getObject(4) == null && "BTREE".equals(rows.getString(5)) && "YES".equals(rows.getString(6));
                    result.put(name, new Index(rows.getInt(2) == 0, columns, full && (previous == null || previous.fullLength())));
                }
            }
        }
        return result;
    }

    private static void requiredColumns(Connection connection, String table, String... columns) throws SQLException {
        for (String name : columns) require(column(connection, table, name) != null, table + "." + name + " missing");
    }

    private static Column column(Connection connection, String table, String name) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT COLUMN_TYPE,IS_NULLABLE,COLUMN_DEFAULT,EXTRA,COLUMN_COMMENT,"
                + "GENERATION_EXPRESSION,CHARACTER_SET_NAME,COLLATION_NAME FROM information_schema.COLUMNS "
                + "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=?")) {
            statement.setString(1, table);
            statement.setString(2, name);
            try (var rows = statement.executeQuery()) {
                return rows.next() ? new Column(rows.getString(1), rows.getString(2), rows.getString(3), rows.getString(4),
                        rows.getString(5), rows.getString(6), rows.getString(7), rows.getString(8)) : null;
            }
        }
    }

    private static boolean tableExists(Connection connection, String table) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?")) {
            statement.setString(1, table);
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getInt(1) == 1; }
        }
    }

    private static String text(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
            require(rows.next(), "Metadata query returned no row");
            return rows.getString(1);
        }
    }
    private static void zero(Connection connection, String sql, String message) throws SQLException { require("0".equals(text(connection, sql)), message); }
    private static void require(boolean condition, String message) { if (!condition) throw new IllegalStateException(message); }
    private static String identifier(String value) {
        require(value != null && value.matches("[a-zA-Z0-9_]+"), "Unsupported metadata identifier");
        return value;
    }
    private record Column(String type, String nullable, String defaultValue, String extra, String comment, String expression, String charset, String collation) { }
    private record Index(boolean unique, List<String> columns, boolean fullLength) { }

    @Override public String getConfirmationMessage() { return "Compatibility phase verified: " + phase; }
    @Override public void setUp() { }
    @Override public void setFileOpener(ResourceAccessor resourceAccessor) { }
    @Override public ValidationErrors validate(Database database) {
        var errors = new ValidationErrors();
        if (!List.of("guard", "backfill", "duplicates", "reshape", "preconditions", "postconditions").contains(phase)) errors.addError("Unknown phase");
        return errors;
    }
}

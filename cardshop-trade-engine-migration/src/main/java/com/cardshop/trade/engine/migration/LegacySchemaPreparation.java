package com.cardshop.trade.engine.migration;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import liquibase.ChecksumVersion;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.change.core.RawSQLChange;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;

/**
 * 旧端升级的六表前置迁移，不启动Spring、Nacos、MQ或任何业务Worker。
 * 原ChangeSet对象直接交给Liquibase，避免复制DDL或改变账本身份；参见详细设计9.8的2026-10-28补充。
 */
public final class LegacySchemaPreparation {
    private static final String MASTER = "db/changelog/db.changelog-master.yaml";
    private static final String CONTRACT = "db/changelog/legacy-schema-contract.properties";
    private static final List<String> IDS = List.of(
            "0005-d2-create-recharge-order-ext", "0013-d9-add-recharge-order-ext-dispatch-status",
            "0020-miniso-fulfillment-receipts", "0021-miniso-writeoff-events",
            "0022-bocom-voucher-state", "0023-miniso-shared-callback-bridge");
    static final List<String> TABLES = List.of("recharge_order_ext", "recharge_miniso_receipt",
            "recharge_miniso_writeoff_event", "mc_miniso_callback_inbox",
            "mc_miniso_legacy_notify_task", "mc_miniso_coupon_guard");
    private static final int QUERY_TIMEOUT_SECONDS = 10;

    private LegacySchemaPreparation() { }

    /** 精确选择已批准原变更；顺序、author和逻辑路径不符时不能尝试修补后执行。 */
    public static DatabaseChangeLog changelog(ResourceAccessor resources) throws Exception {
        var log = ChangeLogParserFactory.getInstance().getParser(MASTER, resources)
                .parse(MASTER, new ChangeLogParameters(), resources);
        log.getChangeSets().removeIf(change -> !IDS.contains(change.getId()));
        require(log.getChangeSets().stream().map(change -> change.getId()).toList().equals(IDS),
                "Original changeset list/order differs");
        for (var change : log.getChangeSets()) {
            require("codex".equals(change.getAuthor()) && MASTER.equals(change.getFilePath()),
                    "Original changeset identity differs");
        }
        return log;
    }

    /**
     * preview完全离线，只输出六组原SQL；apply/verify必须显式给预期schema且从环境读取连接凭据。
     * 不提供回滚/同步账本/清checksum选项，连接失败只输出安全分类，避免驱动异常泄露连接信息。
     */
    public static void main(String[] args) {
        try (var resources = new ClassLoaderResourceAccessor()) {
            var log = changelog(resources);
            var contract = contract();
            for (var change : log.getChangeSets()) {
                require(checksum(change).equals(contract.getProperty("changeset." + change.getId())),
                        "Original changeset checksum differs");
            }
            if (args.length == 1 && "preview".equals(args[0])) {
                var output = new PrintWriter(System.out, true, StandardCharsets.UTF_8);
                for (var change : log.getChangeSets()) {
                    output.println("-- " + change.getFilePath() + "::" + change.getId() + "::"
                            + change.getAuthor() + " checksum=" + checksum(change));
                    for (var sql : change.getChanges()) {
                        require(sql instanceof RawSQLChange, "Unexpected non-SQL change");
                        output.println(((RawSQLChange) sql).getSql().stripTrailing() + ";");
                    }
                }
                return;
            }
            require(args.length == 2 && List.of("apply", "verify").contains(args[0]),
                    "Usage: preview | apply EXPECTED_SCHEMA | verify EXPECTED_SCHEMA");
            try (var connection = DriverManager.getConnection(requiredEnv("LEGACY_SCHEMA_JDBC_URL"),
                    requiredEnv("LEGACY_SCHEMA_USER"), requiredEnv("LEGACY_SCHEMA_PASSWORD"))) {
                require("MySQL".equals(connection.getMetaData().getDatabaseProductName()), "MySQL required");
                require(args[1].equals(connection.getCatalog()), "Connected schema differs from expected schema");
                // 同入口并发互斥，锁名不含地址或凭据；锁随连接关闭释放，不写业务数据。
                String lock = "legacy-schema:" + digest(args[1]).substring(0, 40);
                try (var statement = connection.prepareStatement("SELECT GET_LOCK(?, 0)")) {
                    statement.setString(1, lock);
                    statement.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                    try (var result = statement.executeQuery()) {
                        require(result.next() && result.getInt(1) == 1, "Another schema preparation is running");
                    }
                }
                boolean complete = preflight(connection, log, contract);
                if ("verify".equals(args[0])) {
                    require(complete, "Schema preparation has not been applied");
                    System.out.println("VERIFIED: six original changesets and six table contracts");
                    return;
                }
                if (complete) {
                    System.out.println("NOOP: six original changesets and six table contracts verified");
                    return;
                }
                var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
                // 与生产既有lower_case_table_names=1账本保持一致；实际身份仍来自原ChangeSet。
                database.setDatabaseChangeLogTableName("databasechangelog");
                database.setDatabaseChangeLogLockTableName("databasechangeloglock");
                try (var migration = new Liquibase(log, resources, database)) {
                    migration.update(new Contexts(), new LabelExpression());
                    require(preflight(connection, log, contract), "Post-migration contract mismatch");
                    System.out.println("APPLIED: six original changesets and six table contracts verified");
                }
            }
        } catch (Exception exception) {
            // 维护入口低频运行，ERROR保留原异常栈；去掉驱动正文/原因链，避免连接串和凭据泄露。
            System.err.println("ERROR Legacy schema preparation stopped: " + exception.getClass().getSimpleName());
            if (exception instanceof IllegalStateException) System.err.println(exception.getMessage());
            for (var frame : exception.getStackTrace()) System.err.println("\tat " + frame);
            System.exit(1);
        }
    }

    /**
     * 初次只接受完全空的共享结构；重入只接受六条原身份EXECUTED+checksum和完整结构均匹配。
     * 先于Liquibase初始化执行，因此半完成/错库不会创建账本或以MARK_RAN掩盖问题，参见详细设计9.8。
     */
    static boolean preflight(Connection connection, DatabaseChangeLog log, Properties contract) throws Exception {
        int tables = 0;
        for (String table : TABLES) if (exists(connection, table)) tables++;
        int records = 0;
        if (exists(connection, "databasechangelog")) {
            for (var change : log.getChangeSets()) {
                try (var statement = connection.prepareStatement(
                        "SELECT AUTHOR,FILENAME,MD5SUM,EXECTYPE FROM databasechangelog WHERE ID=?")) {
                    statement.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                    statement.setString(1, change.getId());
                    try (var result = statement.executeQuery()) {
                        int matches = 0;
                        while (result.next()) {
                            require(++matches == 1, "Existing changeset record count differs: " + change.getId());
                            require(change.getAuthor().equals(result.getString(1))
                                            && change.getFilePath().equals(result.getString(2))
                                            && checksum(change).equals(result.getString(3))
                                            && "EXECUTED".equals(result.getString(4)),
                                    "Existing changeset identity/checksum/execution differs: " + change.getId());
                            records++;
                        }
                    }
                }
            }
        }
        if (tables == 0 && records == 0) return false;
        require(tables == TABLES.size() && records == IDS.size(), "Partial schema/ledger: manual forward recovery required");
        for (String table : TABLES) {
            require(fingerprint(connection, table).equals(contract.getProperty("table." + table)),
                    "Table structure differs: " + table);
        }
        return true;
    }

    private static boolean exists(Connection connection, String table) throws Exception {
        try (var statement = connection.prepareStatement(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?")) {
            statement.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            statement.setString(1, table);
            try (var result = statement.executeQuery()) { result.next(); return result.getInt(1) == 1; }
        }
    }

    /**
     * 稳定结构签名：列顺序/类型/可空/默认/额外属性/注释、显式二进制排序、全部索引和引擎。
     * 不包含schema名、行数、自增当前值；未约定的默认文本排序规则仅核对字符集，参见详细设计9.8。
     */
    static String fingerprint(Connection connection, String table) throws Exception {
        var shape = new StringBuilder();
        appendRows(connection, shape, table,
                "SELECT ENGINE,TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?");
        appendRows(connection, shape, table,
                "SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_DEFAULT,EXTRA,CHARACTER_SET_NAME,"
                        + "CASE WHEN COLLATION_NAME LIKE '%bin' THEN COLLATION_NAME ELSE NULL END,COLUMN_COMMENT "
                        + "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? ORDER BY ORDINAL_POSITION");
        appendRows(connection, shape, table,
                "SELECT INDEX_NAME,NON_UNIQUE,SEQ_IN_INDEX,COLUMN_NAME,SUB_PART,INDEX_TYPE,IS_VISIBLE,EXPRESSION "
                        + "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? ORDER BY INDEX_NAME,SEQ_IN_INDEX");
        return digest(shape.toString());
    }

    private static void appendRows(Connection connection, StringBuilder shape, String table, String sql) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            statement.setString(1, table);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    for (int column = 1; column <= result.getMetaData().getColumnCount(); column++) {
                        String value = result.getString(column);
                        shape.append(value == null ? "-1:" : value.length() + ":" + value).append('|');
                    }
                    shape.append('\n');
                }
            }
        }
    }

    private static String checksum(liquibase.changelog.ChangeSet change) {
        return change.generateCheckSum(ChecksumVersion.latest()).toString();
    }

    private static Properties contract() throws Exception {
        var properties = new Properties();
        try (var input = LegacySchemaPreparation.class.getClassLoader().getResourceAsStream(CONTRACT)) {
            require(input != null, "Missing rehearsed schema contract");
            properties.load(input);
        }
        return properties;
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        require(value != null && !value.isBlank(), "Missing environment variable " + name);
        return value;
    }

    private static String digest(String value) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static void require(boolean valid, String message) {
        if (!valid) throw new IllegalStateException(message);
    }
}

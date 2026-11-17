import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.ruoyi.web.mc.domain.McBatchRecord;
import com.ruoyi.web.mc.domain.McDepartRecord;
import com.ruoyi.web.mc.domain.McOrder;
import com.ruoyi.web.mc.domain.vo.McDepartProductVo;
import com.ruoyi.web.mc.fulfillment.FulfillmentGuard;
import com.ruoyi.web.mc.mapper.FulfillmentGuardMapper;
import com.ruoyi.web.mc.mapper.FulfillmentMapper;
import com.ruoyi.web.mc.mapper.McDepartRecordMapper;
import com.ruoyi.web.mc.mapper.McOrderMapper;
import com.ruoyi.web.mc.miniso.bridge.MinisoBridgeConfiguration;
import com.ruoyi.web.mc.miniso.bridge.MinisoLegacyOrderTransactions;
import com.ruoyi.web.mc.pricing.RechargePricePolicy;
import com.ruoyi.web.mc.pricing.RechargePricingStore;
import com.ruoyi.web.mc.service.impl.McOrderServiceImpl;
import com.ruoyi.web.mc.transfer.TransferOrderTransactions;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 只用旧候选JAR的Java8类及依赖验证写入合同，不启动Spring应用或任何外部业务客户端。
 * 由隔离脚本编译运行；参见详细设计8.2.1、9.8及《集成测试.md》旧端写入者记录。
 */
public final class LegacyWriterCompatibilityProbe {
    private final DriverManagerDataSource source;
    private final JdbcTemplate jdbc;

    public LegacyWriterCompatibilityProbe() {
        check("1.8".equals(System.getProperty("java.specification.version")), "requires Java8");
        String schema = System.getenv("COMPAT_LEGACY_SCHEMA");
        String port = System.getenv("LEGACY_SCHEMA_TEST_PORT");
        check(schema != null && schema.matches("compat_legacy_[a-f0-9]{32}"), "private schema required");
        check(port != null && port.matches("[0-9]{1,5}") && Integer.parseInt(port) > 1023
                && Integer.parseInt(port) != 3306, "random loopback port required");
        source = new DriverManagerDataSource("jdbc:mysql://127.0.0.1:" + port + "/" + schema
                + "?allowPublicKeyRetrieval=true&useSSL=false&allowMultiQueries=true&serverTimezone=Asia/Shanghai"
                + "&connectTimeout=5000&socketTimeout=20000", "root", System.getenv("LEGACY_SCHEMA_TEST_PASSWORD"));
        jdbc = new JdbcTemplate(source);
        check(schema.equals(jdbc.queryForObject("select run_id from compat_fixture", String.class)), "fixture marker");
        check("8.0.36".equals(jdbc.queryForObject("select version()", String.class)), "MySQL version");
        check("READ-COMMITTED".equals(jdbc.queryForObject("select @@transaction_isolation", String.class)), "isolation");
    }

    public static void main(String[] args) throws Exception {
        check(args.length == 1, "prepare or verify required");
        LegacyWriterCompatibilityProbe probe = new LegacyWriterCompatibilityProbe();
        if ("prepare".equals(args[0])) probe.prepare();
        else if ("verify".equals(args[0])) probe.verify();
        else if ("xxl".equals(args[0])) new LegacyXxlCompatibilityProbe(probe.source, probe.mappers()).verify();
        else throw new IllegalArgumentException("unknown stage");
    }

    private void prepare() throws Exception {
        // 仅旧仓库已有的脱敏metadata投影；引擎新增列/六表仍由Java21真实入口负责发布。
        new ResourceDatabasePopulator(new FileSystemResource(System.getenv("COMPAT_LEGACY_BASELINE"))).execute(source);
        for (String path : Arrays.asList("20261019-qnm-supplier-failover.xml", "20261030-transfer-v1.xml",
                "20261102-transfer-observability.xml", "20261102-recharge-pricing.xml")) {
            try (Connection connection = source.getConnection();
                    Liquibase migration = new Liquibase("db/changelog/" + path, new ClassLoaderResourceAccessor(),
                            DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection)))) {
                migration.update("");
            }
        }
        SqlSessionTemplate session = mappers();
        McOrderMapper orders = session.getMapper(McOrderMapper.class);
        McOrder first = order("OLD-BEFORE-1", "SHARED");
        first.setOrderPrice(139L);
        first.setOrderStatus("12");
        check(orders.insertMcOrder(first) == 1, "pre-migration dynamic insert");
        check(orders.insertBachMcOrder(Collections.singletonList(order("OLD-BEFORE-2", "SHARED"))) == 1,
                "pre-migration batch insert");
        try (Connection connection = source.getConnection()) {
            System.out.println("LEGACY_WRITER_PREPARED java=" + System.getProperty("java.version")
                    + " driver=" + connection.getMetaData().getDriverVersion() + " historicalRows=2");
        }
    }

    private void verify() throws Exception {
        new MinisoBridgeConfiguration().minisoBridgeSchemaProbe(jdbc).afterPropertiesSet();
        System.out.println("LEGACY_WRITER_CASE schema-probe PASS");
        SqlSessionTemplate session = mappers();
        McOrderMapper orders = session.getMapper(McOrderMapper.class);
        McDepartRecordMapper records = session.getMapper(McDepartRecordMapper.class);
        FulfillmentGuardMapper guard = session.getMapper(FulfillmentGuardMapper.class);

        McOrder single = order("OLD-SINGLE", "SHARED");
        check(orders.insertMcOrder(single) == 1 && single.getId() != null, "dynamic insert and generated id");
        check(orders.insertBachMcOrder(Arrays.asList(order("OLD-BATCH-1", "SHARED"), order("OLD-BATCH-2", "SHARED"))) == 2,
                "same-external split batch");
        McOrder plus = order("OLD-PLUS", "SHARED");
        check(orders.insert(plus) == 1 && plus.getId() != null, "MyBatis-Plus generated insert");
        check(jdbc.queryForObject("select count(*) from mc_order where org_code='LEGACY' and out_orderId='SHARED'"
                + " and trade_engine_owned=0 and direct_recharge_out_order_key is null", Integer.class) == 6, "old default ownership");
        orders.updateStatusByOrderId("LEGACY", "OLD-SINGLE", "13", 17);
        check(jdbc.queryForObject("select refund_order_price from mc_order where order_Id='OLD-SINGLE'", Long.class) == 17L,
                "platform-specific refund field update");
        check(jdbc.queryForObject("select count(*) from mc_order where out_orderId='SHARED' and order_status='13'", Integer.class) == 1,
                "same-external siblings not updated");
        McOrder loaded = orders.selectById(plus.getId());
        loaded.setRemark("synthetic update");
        check(orders.updateById(loaded) == 1, "read/write entity with additional DB columns");
        McOrder engine = orders.selectOneByOrderId("ENGINE-COMPAT");
        check(engine != null && engine.getOrderPrice() == 137L, "old query reads new engine order");
        // 普通Mapper只验证不会回写归属；不把它等同于业务入口已具备新单操作权限保护。
        TransactionTemplate tx = new TransactionTemplate(new DataSourceTransactionManager(source));
        tx.execute(status -> {
            engine.setRemark("synthetic mapper compatibility");
            check(orders.updateById(engine) == 1, "engine ordinary entity update");
            McOrder update = new McOrder();
            update.setOrderId("ENGINE-COMPAT");
            update.setRemark("synthetic XML compatibility");
            check(orders.updateMcOrderByOrderId(update) == 1, "XML update retains engine ownership");
            check(jdbc.queryForObject("select trade_engine_owned from mc_order where order_Id='ENGINE-COMPAT'", Integer.class) == 1,
                    "ordinary update must not reset owned1");
            status.setRollbackOnly();
            return null;
        });
        System.out.println("LEGACY_WRITER_CASE mapper-single-batch-plus-update PASS oldSharedRows=6 owned=0 engineOwned=1");

        jdbc.update("insert into mc_ext_depart(org_code,balance,credit_status,depart_status) values('LEGACY',10000,'0','0')");
        McDepartRecord payment = record("OLD-LEDGER", "1", 137L);
        check(records.insertMcDepartRecord(payment) == 1, "legacy dynamic ledger insert");
        check(records.selectMcDepartRecordById(payment.getId()).getValue() == 137L, "ledger read");
        check(records.addRecord(record("OLD-LEDGER-SUBQUERY", "1", 139L)) == 1, "ledger debit balance subquery");
        check(records.saveRefundDepartRecord(record("OLD-LEDGER-SUBQUERY", "2", 17L)) == 1, "ledger refund balance subquery");
        boolean duplicateRejected = false;
        try { records.insertMcDepartRecord(payment); }
        catch (org.springframework.dao.DuplicateKeyException expected) { duplicateRejected = true; }
        check(duplicateRejected, "order/type unique constraint retained");
        System.out.println("LEGACY_WRITER_CASE ledger PASS duplicateRejected=true");

        McOrder failed = order("OLD-REFUND", "REFUND");
        failed.setOrderStatus("6");
        orders.insertMcOrder(failed);
        records.insertMcDepartRecord(record(failed.getOrderId(), "1", 137L));
        List<String> candidates = orders.selectLegacyRefundCandidates("6");
        check(candidates.contains("OLD-REFUND") && !candidates.contains("ENGINE-COMPAT"), "refund candidate ownership exclusion");
        MinisoLegacyOrderTransactions refunds = new MinisoLegacyOrderTransactions(jdbc, tx);
        Map<String, Object> engineBefore = engineFacts();
        refunds.refundFailedOrder("ENGINE-COMPAT", false, false);
        refunds.refundFailedOrder("ENGINE-COMPAT", true, true);
        check(balance() == 10000L && engineFacts().equals(engineBefore), "stale candidate final transaction guard");
        refunds.refundFailedOrder("OLD-REFUND", false, false);
        refunds.refundFailedOrder("OLD-REFUND", false, false);
        check(balance() == 10137L, "refund adds once");
        check(jdbc.queryForObject("select count(*) from mc_depart_record where order_id='OLD-REFUND' and type='2'", Integer.class) == 1,
                "refund ledger once");
        check("11".equals(jdbc.queryForObject("select order_status from mc_order where order_Id='OLD-REFUND'", String.class)), "refund state");
        check(engineFacts().equals(engineBefore), "old refund does not alter engine facts");
        System.out.println("LEGACY_WRITER_CASE refund PASS credited=137 once=true engineExcluded=true");

        RechargePricingStore pricing = new RechargePricingStore(jdbc, tx);
        McDepartProductVo product = new McDepartProductVo();
        product.setProductId("SKU"); product.setChannel("OTHER"); product.setPrice(137L); product.setParentPrice(101L);
        List<McDepartProductVo> products = Collections.singletonList(product);
        RechargePricePolicy.Evaluation evaluation = RechargePricePolicy.evaluate(products, null);
        check(pricing.save(order("OLD-PRICING", "PRICE"), products, evaluation).isCreated(), "pricing creates");
        check(!pricing.save(order("OLD-PRICING-REPLAY", "PRICE"), products, evaluation).isCreated(), "pricing replay");
        check(balance() == 10000L, "pricing debit 137 once after refund");
        check(jdbc.queryForObject("select count(*) from mc_depart_record where order_id='OLD-PRICING' and type='1'", Integer.class) == 1,
                "pricing ledger once");
        check(jdbc.queryForObject("select trade_engine_owned from mc_order where order_Id='OLD-PRICING'", Integer.class) == 0,
                "pricing default owned0");
        System.out.println("LEGACY_WRITER_CASE pricing PASS debit=137 once=true owned=0");

        TransferOrderTransactions transfers = new TransferOrderTransactions(jdbc, tx);
        long identity = transfers.claimLegacy("LEGACY", "TRANSFER");
        check(transfers.saveLegacy(identity, order("OLD-TRANSFER", "TRANSFER")), "legacy transfer main and identity transaction");
        check("OLD-TRANSFER".equals(jdbc.queryForObject("select order_id from mc_transfer_identity where id=?", String.class, identity)), "transfer binding");
        check(jdbc.queryForObject("select trade_engine_owned from mc_order where order_Id='OLD-TRANSFER'", Integer.class) == 0,
                "transfer default owned0");
        check(guard.incrementNotification("OLD-TRANSFER", 3) == 1, "fulfillment notification projection");
        check(guard.updateRemark(single.getId(), "synthetic guard update") == 1, "fulfillment precise mapper");
        check(jdbc.queryForObject("select trade_engine_owned from mc_order where order_Id='OLD-SINGLE'", Integer.class) == 0,
                "fulfillment retains ownership");
        check(balance() == 10000L && engineFacts().equals(engineBefore), "transfer binding and projection do not move engine money");
        System.out.println("LEGACY_WRITER_CASE transfer-fulfillment PASS bound=true owned=0 noExternalCalls=true");

        // 最终业务入口不能把新端ext订单当作普通旧单编辑/删除；两类探测分别回滚，避免相互影响。
        McOrderServiceImpl service = new McOrderServiceImpl();
        Map<String, Object> dependencies = new LinkedHashMap<>();
        dependencies.put("mcOrderMapper", orders);
        dependencies.put("transferOrders", transfers);
        dependencies.put("rechargePricingStore", pricing);
        dependencies.put("fulfillmentGuard", new FulfillmentGuard(session.getMapper(FulfillmentMapper.class), guard));
        for (Map.Entry<String, Object> dependency : dependencies.entrySet()) {
            java.lang.reflect.Field field = McOrderServiceImpl.class.getDeclaredField(dependency.getKey());
            field.setAccessible(true);
            field.set(service, dependency.getValue());
        }
        boolean editProtected = tx.execute(status -> {
            McOrder edit = new McOrder();
            edit.setId(engine.getId());
            edit.setOrderPrice(321L);
            int changed;
            try { changed = service.updateMcOrder(edit); }
            catch (com.ruoyi.common.exception.ServiceException denied) { changed = 0; }
            long amount = jdbc.queryForObject("select order_price from mc_order where order_Id='ENGINE-COMPAT'", Long.class);
            System.out.println("LEGACY_WRITER_EDIT_AUDIT edited=" + changed + " amountAfterEdit=" + amount + " rollback=true");
            status.setRollbackOnly();
            return changed == 0 && amount == 137L;
        });
        check(engineFacts().equals(engineBefore), "edit audit transaction really rolled back");
        boolean deleteProtected = tx.execute(status -> {
            check(jdbc.queryForObject("select order_price from mc_order where order_Id='ENGINE-COMPAT'", Long.class) == 137L,
                    "delete audit uses original unedited order");
            int deleted;
            try { deleted = service.deleteMcOrderByIds(new Long[]{engine.getId()}); }
            catch (com.ruoyi.common.exception.ServiceException denied) { deleted = 0; }
            long mainCount = jdbc.queryForObject("select count(*) from mc_order where order_Id='ENGINE-COMPAT'", Long.class);
            long extCount = jdbc.queryForObject("select count(*) from recharge_order_ext where order_id='ENGINE-COMPAT'", Long.class);
            System.out.println("LEGACY_WRITER_DELETE_AUDIT deleted=" + deleted + " mainAfter=" + mainCount
                    + " extAfter=" + extCount + " rollback=true");
            status.setRollbackOnly();
            return deleted == 0 && mainCount == 1 && extCount == 1;
        });
        check(engineFacts().equals(engineBefore), "delete audit transaction really rolled back");
        System.out.println("LEGACY_WRITER_AUDIT_ROLLED_BACK restoredPrice=137 main=1 ext=1");
        check(editProtected && deleteProtected, "new engine order must not be edited or deleted through legacy admin service");
        System.out.println("LEGACY_WRITER_CASE admin-mutation-protection PASS");
    }

    private SqlSessionTemplate mappers() throws Exception {
        MybatisConfiguration configuration = new MybatisConfiguration();
        configuration.setMapUnderscoreToCamelCase(true);
        configuration.getTypeAliasRegistry().registerAlias("McOrder", McOrder.class);
        configuration.getTypeAliasRegistry().registerAlias("McBatchRecord", McBatchRecord.class);
        configuration.getTypeAliasRegistry().registerAlias("McDepartRecord", McDepartRecord.class);
        MybatisSqlSessionFactoryBean factory = new MybatisSqlSessionFactoryBean();
        factory.setDataSource(source);
        factory.setConfiguration(configuration);
        factory.setMapperLocations(new ClassPathResource("mapper.system/McOrderMapper.xml"),
                new ClassPathResource("mapper.system/McDepartRecordMapper.xml"),
                new ClassPathResource("mapper.system/FulfillmentGuardMapper.xml"),
                new ClassPathResource("mapper.system/FulfillmentMapper.xml"));
        return new SqlSessionTemplate(factory.getObject());
    }

    private Map<String, Object> engineFacts() {
        return jdbc.queryForMap("select o.order_Id,o.org_code,o.out_orderId,o.order_price,o.order_status,o.trade_engine_owned,"
                + "e.request_hash,e.persisted_version,e.balance_reserved,e.dispatch_status from mc_order o"
                + " join recharge_order_ext e on e.order_id=o.order_Id where o.order_Id='ENGINE-COMPAT'");
    }

    private long balance() { return jdbc.queryForObject("select balance from mc_ext_depart where org_code='LEGACY'", Long.class); }

    private static McOrder order(String id, String external) {
        McOrder order = new McOrder();
        order.setOrderId(id); order.setOrgCode("LEGACY"); order.setOutOrderid(external);
        order.setOrderStatus("2"); order.setProductType(1); order.setOrderPrice(137L);
        order.setProductPrice(137L); order.setProductValue(1000L); order.setNum(1L);
        order.setProdutCode("SKU"); order.setParentId("PARENT"); order.setProductName("合成商品");
        order.setsupplierChannel("OTHER"); order.setAccount("synthetic-account"); order.setNumber(0);
        order.setCreateTime(new Date()); order.setOrderType(0); order.setCostPrice(101L);
        return order;
    }

    private static McDepartRecord record(String id, String type, long amount) {
        McDepartRecord record = new McDepartRecord();
        record.setOrderId(id); record.setOrgCode("LEGACY"); record.setType(type); record.setValue(amount);
        record.setBeforeBalance(10000L); record.setBalance(10000L - amount); record.setCreateTime(new Date());
        return record;
    }

    private static void check(boolean condition, String contract) {
        if (!condition) throw new AssertionError(contract);
    }
}

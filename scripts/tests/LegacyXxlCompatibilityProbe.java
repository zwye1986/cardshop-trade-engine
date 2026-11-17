import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ruoyi.common.constant.ConfigConstants;
import com.ruoyi.common.core.NacosConfigHelper;
import com.ruoyi.common.core.domain.AjaxResult;
import com.ruoyi.common.core.redis.RedisCache;
import com.ruoyi.common.enums.SupplierType;
import com.ruoyi.common.utils.BeanNameUtils;
import com.ruoyi.web.mc.api.service.ManualInterventionService;
import com.ruoyi.web.mc.api.service.SupplierServiceFactory;
import com.ruoyi.web.mc.api.service.SupplierTradeService;
import com.ruoyi.web.mc.api.service.impl.BankServiceImpl;
import com.ruoyi.web.mc.api.util.BLSM4Utils;
import com.ruoyi.web.mc.api.utils.BLSMApiUtils;
import com.ruoyi.web.mc.domain.McOrder;
import com.ruoyi.web.mc.domain.ManualInterventionRecord;
import com.ruoyi.web.mc.fulfillment.FulfillmentFacade;
import com.ruoyi.web.mc.fulfillment.FulfillmentGuard;
import com.ruoyi.web.mc.fulfillment.FulfillmentProperties;
import com.ruoyi.web.mc.fulfillment.ManagedQueryRoutingService;
import com.ruoyi.web.mc.mapper.*;
import com.ruoyi.web.mc.miniso.bridge.MinisoLegacyOrderTransactions;
import com.ruoyi.web.mc.service.impl.*;
import com.ruoyi.web.mc.task.AdminUnificationTask;
import com.sun.net.httpserver.HttpServer;
import com.xxl.job.core.context.XxlJobContext;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.Permission;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 原样旧Java8候选的活跃XXL服务边界验证；不启动旧应用、Nacos或调度中心。
 * SQL/退款事务均真实，仅供应商客户端与Redis锁替身；参见集成测试“活跃XXL入口”。
 */
public final class LegacyXxlCompatibilityProbe {
    private static final String ENGINE = "ENGINE-COMPAT";
    private static final int MAX_CALLBACK_ATTEMPTS = 5;
    // 仅合成测试密钥，不读取配置或真实凭据。
    private static final String SYNTHETIC_AES_KEY = "0123456789abcdef";
    private final JdbcTemplate jdbc;
    private final SqlSessionTemplate session;
    private final McOrderMapper orders;
    private final McOrderServiceImpl service = new McOrderServiceImpl();
    private final AdminUnificationTask task = new AdminUnificationTask();
    private final List<JSONObject> received = Collections.synchronizedList(new ArrayList<JSONObject>());

    public LegacyXxlCompatibilityProbe(DriverManagerDataSource source, SqlSessionTemplate session) throws Exception {
        this.jdbc = new JdbcTemplate(source);
        this.session = session;
        this.orders = session.getMapper(McOrderMapper.class);
        for (Class<?> mapper : Arrays.asList(McExtDepartMapper.class, BankPurchaseRecordMapper.class,
                BankExchangeOrderMapper.class, BankCodeGenerationRecordMapper.class)) {
            session.getConfiguration().addMapper(mapper);
        }
        session.getConfiguration().getTypeAliasRegistry().registerAlias("ManualInterventionRecord", ManualInterventionRecord.class);
        String path = "mapper/mc/ManualInterventionMapper.xml";
        try (java.io.InputStream xml = new ClassPathResource(path).getInputStream()) {
            new XMLMapperBuilder(xml, session.getConfiguration(), path, session.getConfiguration().getSqlFragments()).parse();
        }
        inject(service, ServiceImpl.class, "baseMapper", orders);
        inject(service, "mcOrderMapper", orders);
        inject(service, "extDepartMapper", session.getMapper(McExtDepartMapper.class));
        inject(service, "fulfillmentFacade", new FulfillmentFacade(session.getMapper(FulfillmentMapper.class), null));
        inject(service, "managedQueryRouting", new ManagedQueryRoutingService(session.getMapper(FulfillmentMapper.class),
                new FulfillmentFacade(session.getMapper(FulfillmentMapper.class), null), null, new FulfillmentProperties()));
        inject(service, "fulfillmentGuard", new FulfillmentGuard(session.getMapper(FulfillmentMapper.class),
                session.getMapper(FulfillmentGuardMapper.class)));
        inject(service, "minisoLegacyOrderTransactions", new MinisoLegacyOrderTransactions(jdbc,
                new TransactionTemplate(new DataSourceTransactionManager(source))));
        inject(task, "mcOrderService", service);
        Field cache = NacosConfigHelper.class.getDeclaredField("configCache");
        cache.setAccessible(true);
        @SuppressWarnings("unchecked") Map<String, Object> values = (Map<String, Object>) cache.get(null);
        values.put(ConfigConstants.CALLBACK_MAX_TIMES_KEY, MAX_CALLBACK_ATTEMPTS);
    }

    public void verify() throws Exception {
        HttpServer receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        receiver.createContext("/notify", exchange -> {
            byte[] request = readAll(exchange.getRequestBody());
            received.add(JSONObject.parseObject(new String(request, StandardCharsets.UTF_8)));
            byte[] response = "{\"code\":200}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            try (java.io.OutputStream body = exchange.getResponseBody()) { body.write(response); }
            exchange.close();
        });
        final int callbackPort = receiver.getAddress().getPort();
        final int mysqlPort = Integer.parseInt(System.getenv("LEGACY_SCHEMA_TEST_PORT"));
        final AtomicInteger blockedConnections = new AtomicInteger();
        System.setSecurityManager(new SecurityManager() {
            @Override public void checkPermission(Permission permission) {
                if (permission instanceof RuntimePermission && "setSecurityManager".equals(permission.getName()))
                    throw new SecurityException("fixture network guard cannot be removed");
            }
            @Override public void checkConnect(String host, int port) {
                if (!"127.0.0.1".equals(host) || (port != -1 && port != mysqlPort && port != callbackPort)) {
                    blockedConnections.incrementAndGet();
                    throw new SecurityException("non-fixture network forbidden");
                }
            }
        });
        boolean denied = false;
        try (Socket ignored = new Socket("203.0.113.1", 443)) { throw new AssertionError("network guard ineffective"); }
        catch (SecurityException expected) { denied = true; }
        check(denied, "nonlocal socket denied before connect");
        blockedConnections.set(0);
        System.out.println("LEGACY_XXL_CASE network-guard PASS deniedExternal=true");
        receiver.start();
        try {
            // 父进程已用真实引擎adapter创建首期A35/MINISO单；确认默认number，而非夹具强置前提。
            check(number(ENGINE) == 0, "real engine persistence inherits notification count zero");
            jdbc.update("insert into mc_ext_depart(org_code,balance,credit_status,depart_status,aes_key) values('A35',10000,'0','0',?)",
                    SYNTHETIC_AES_KEY);
            Map<String, Object> identity = identity();
            verifyQuery();
            verifyBl();
            verifyRefund();
            verifyFulu();
            verifyAutoReissue();
            verifyOtherActiveFilters();
            verifyCallback("http://127.0.0.1:" + callbackPort + "/notify");
            check(identity().equals(identity), "all task routes retain immutable engine identity");
            check(blockedConnections.get() == 0, "no task attempted a forbidden external connection");
            // 父进程还会核对新端流水/ext/两outbox，并将VIOLATION作为必需边界的红灯。
            System.out.println("LEGACY_XXL_COMPLETED routes=8 immutableIdentityUnchanged=true");
        } finally {
            receiver.stop(0);
            XxlJobContext.setXxlJobContext(null);
        }
    }

    private void verifyQuery() throws Exception {
        final List<String> queried = new ArrayList<>();
        SupplierServiceFactory factory = new SupplierServiceFactory();
        SupplierTradeService supplier = (SupplierTradeService) Proxy.newProxyInstance(
                SupplierTradeService.class.getClassLoader(), new Class<?>[]{SupplierTradeService.class}, (proxy, method, args) -> {
                    check("queryOrder".equals(method.getName()), "query route must not issue or cancel");
                    queried.add(((McOrder) args[0]).getOrderId());
                    return null;
                });
        inject(factory, "tradeServiceMap", Collections.singletonMap(
                BeanNameUtils.getBeanName(SupplierType.TF_KA.name(), "SupplierTradeService"), supplier));
        inject(service, "supplierService", factory);
        McOrder old = order("OLD-XXL-QUERY", "13", "TF_KA");
        orders.insertMcOrder(old);
        // 先跑真实旧控制，再放入新单，区分正常路由与新单拒绝造成的批扫中断。
        jdbc.update("update mc_order set order_status='2' where order_Id=?", ENGINE);
        XxlJobContext.setXxlJobContext(new XxlJobContext(1L, "", "", 0, 1));
        success(task.updateOrderCode());
        check(queried.equals(Collections.singletonList(old.getOrderId())), "old batch reaches supplier boundary");
        XxlJobContext.setXxlJobContext(new XxlJobContext(1L, old.getOrderId(), "", 0, 1));
        success(task.updateOrderCode());
        check(queried.size() == 2, "old single query route");
        jdbc.update("update mc_order set order_status='13' where order_Id=?", ENGINE);
        XxlJobContext.setXxlJobContext(new XxlJobContext(1L, ENGINE, "", 0, 1));
        check(Integer.valueOf(500).equals(task.updateOrderCode().get("code")), "new MINISO single query rejected");
        // 强制新单排在较早的旧单之前：即真实批扫orderByDesc(create_time)的一种确定性顺序。
        jdbc.update("update mc_order set create_time=date_sub(now(),interval 1 minute) where order_Id=?", old.getOrderId());
        jdbc.update("update mc_order set create_time=now() where order_Id=?", ENGINE);
        queried.clear();
        XxlJobContext.setXxlJobContext(new XxlJobContext(1L, "", "", 0, 1));
        success(task.updateOrderCode());
        check("13".equals(status(ENGINE)) && !queried.contains(ENGINE), "new MINISO remains unchanged");
        System.out.println("LEGACY_XXL_QUERY_AUDIT engineFirst=true legacyQueriesAfter=" + queried.size() + " taskCode=200");
        if (!queried.contains(old.getOrderId())) System.out.println("LEGACY_XXL_VIOLATION engine-query-guard-aborts-legacy-batch");
        new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource())).execute(status -> {
            // 同时改变渠道、商户及扩展状态，区分真正ext归属过滤与MINISO/A35/DB_PERSISTED特判。
            jdbc.update("update mc_order set supplier_channel='TF_KA',org_code='A36' where order_Id=?", ENGINE);
            jdbc.update("update recharge_order_ext set org_code='A36',persist_status='BROKEN',balance_reserved=0 where order_id=?", ENGINE);
            queried.clear();
            success(task.updateOrderCode());
            if (!queried.equals(Collections.singletonList(old.getOrderId())))
                System.out.println("LEGACY_XXL_VIOLATION query-exclusion-must-use-any-ext-not-channel-or-state");
            status.setRollbackOnly();
            return null;
        });
        jdbc.update("update mc_order set order_status='2' where order_Id in (?,?)", ENGINE, old.getOrderId());
        System.out.println("LEGACY_XXL_CASE updateOrderCode PASS oldBatchAndSingle=true engineGuard=true batchAckMayHideRejection=true");
    }

    private void verifyBl() throws Exception {
        final List<String> requests = new ArrayList<>();
        final String key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        BLSMApiUtils supplier = new BLSMApiUtils() {
            @Override public String orderQuery(String id) { requests.add(id); return response("{\"ordSts\":\"S\"}"); }
            @Override public String orderCouponsQuery(String id) {
                requests.add(id);
                return response("{\"data\":[{\"cpsTyp\":\"105\",\"redeemCode\":\"SYNTHETIC\","
                        + "\"cpsExpBeg\":\"20261101000000\",\"cpsExpEnd\":\"20271201000000\"}]}");
            }
            private String response(String clear) {
                return "{\"head\":{\"retCode\":\"SCM00000\"},\"body\":{\"encryptStr\":\""
                        + BLSM4Utils.encryptCBC(clear, key) + "\"}}";
            }
        };
        supplier.setOtherSm4Key(key);
        inject(service, "blsmUtils", supplier);
        BankPurchaseRecordServiceImpl purchase = new BankPurchaseRecordServiceImpl();
        inject(purchase, ServiceImpl.class, "baseMapper", session.getMapper(BankPurchaseRecordMapper.class));
        inject(service, "bankPurchaseRecordService", purchase);
        jdbc.execute("create table bank_purchase_record(id bigint primary key auto_increment,order_no varchar(64),phone_no varchar(64),status int)");
        McOrder old = order("OLD-XXL-BL", "13", "BL_STOCK");
        orders.insertMcOrder(old);
        jdbc.update("insert into bank_purchase_record(order_no,phone_no,status) values(?,?,0)", old.getOrderId(), old.getAccount());
        jdbc.update("update mc_order set order_status='13' where order_Id=?", ENGINE);
        success(task.blOrderUpdate());
        check(requests.equals(Arrays.asList(old.getOrderId(), old.getOrderId())), "BL query/coupon exact old platform id");
        check("12".equals(status(old.getOrderId())), "BL real main update");
        check(jdbc.queryForObject("select status from bank_purchase_record where order_no=?", Integer.class, old.getOrderId()) == 1,
                "BL purchase projection updated");
        check("13".equals(status(ENGINE)), "MINISO not selected by BL filter");
        jdbc.update("update mc_order set order_status='2' where order_Id in (?,?)", ENGINE, old.getOrderId());
        System.out.println("LEGACY_XXL_CASE blOrderUpdate PASS oldMainAndPurchaseUpdated=true engineMinisoExcluded=true");
    }

    private void verifyRefund() {
        McOrder old = order("OLD-XXL-REFUND", "6", "OTHER");
        orders.insertMcOrder(old);
        jdbc.update("insert into mc_depart_record(order_id,org_code,type,`value`,before_balance,balance,create_time) values(?,'A35','1',137,10000,9863,now())",
                old.getOrderId());
        jdbc.update("update mc_order set order_status='6' where order_Id=?", ENGINE);
        long before = jdbc.queryForObject("select balance from mc_ext_depart where org_code='A35'", Long.class);
        success(task.orderRefund());
        success(task.orderRefund());
        check("11".equals(status(old.getOrderId())) && "6".equals(status(ENGINE)), "old refunded, new failed unchanged");
        check(jdbc.queryForObject("select balance from mc_ext_depart where org_code='A35'", Long.class) == before + 137,
                "refund exactly once");
        check(jdbc.queryForObject("select count(*) from mc_depart_record where order_id=? and type='2'", Integer.class, ENGINE) == 0,
                "no engine refund ledger");
        jdbc.update("update mc_order set order_status='2' where order_Id in (?,?)", ENGINE, old.getOrderId());
        System.out.println("LEGACY_XXL_CASE orderRefund PASS credited137Once=true engineExcluded=true");
    }

    private void verifyFulu() {
        McOrder old = order("OLD-XXL-FULU", "6", "OTHER");
        old.setOrgCode("A12"); old.setCreateTime(new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(49)));
        orders.insertMcOrder(old);
        jdbc.update("update mc_order set order_status='6',number=0 where order_Id=?", ENGINE);
        success(task.callbackByFulu());
        check(number(old.getOrderId()) == MAX_CALLBACK_ATTEMPTS, "expired fulu progress really persisted");
        check(number(ENGINE) == 0, "A35 engine excluded by fulu merchant predicate");
        success(task.callbackByFulu());
        check(number(old.getOrderId()) == MAX_CALLBACK_ATTEMPTS, "expired fulu stable on rerun");
        jdbc.update("update mc_order set order_status='2' where order_Id=?", ENGINE);
        System.out.println("LEGACY_XXL_CASE callbackByFulu PASS expiredOldStopped=true engineA35Excluded=true noFuluNetwork=true");
    }

    private void verifyAutoReissue() throws Exception {
        // 只为本轮银行路径提供实体/SQL所需投影，不将其当作现场银行表metadata。
        jdbc.execute("create table bank_manual_intervention_record(id bigint primary key auto_increment,partner_recharge_id varchar(100),"
                + "phone_number varchar(20),amount decimal(10,2),error_message text,create_time datetime,status varchar(20),"
                + "type tinyint,handler varchar(50),remark text,handle_time datetime)");
        jdbc.execute("create table bank_exchange_order(id int primary key auto_increment,external_order_no varchar(64),"
                + "order_no varchar(64),exchange_code varchar(64),status int,org_id int,product_id int,phone varchar(64),"
                + "sender_sn varchar(64),total_value decimal(10,2),order_status int,settlement_status int,settlement_date date,create_time datetime)");
        jdbc.execute("create table bank_code_generation_record(id int primary key auto_increment,org_id int,product_id varchar(64),"
                + "exchange_code varchar(64),status int,phone varchar(64),exchange_time datetime,expire_time datetime,create_time datetime)");
        jdbc.update("insert into bank_manual_intervention_record(partner_recharge_id,error_message,status,create_time) values(?,'synthetic-timeout','PENDING',now())", ENGINE);
        jdbc.update("insert into bank_exchange_order(external_order_no,order_no,exchange_code,status) values(?,'BANK-ONLY','SYNTHETIC-CODE',2)", ENGINE);
        jdbc.update("insert into bank_code_generation_record(exchange_code,status) values('SYNTHETIC-CODE',1)");
        ManualInterventionService manual = new ManualInterventionService();
        inject(manual, "manualInterventionMapper", session.getMapper(ManualInterventionMapper.class));
        BankCodeGenerationRecordServiceImpl codes = new BankCodeGenerationRecordServiceImpl();
        inject(codes, ServiceImpl.class, "baseMapper", session.getMapper(BankCodeGenerationRecordMapper.class));
        ExceptionOrderServiceImpl reissue = new ExceptionOrderServiceImpl();
        inject(reissue, "manualInterventionService", manual);
        inject(reissue, "bankExchangeOrderMapper", session.getMapper(BankExchangeOrderMapper.class));
        inject(reissue, "bankCodeGenerationRecordService", codes);
        inject(reissue, "ikeaErrorMessages", "synthetic-timeout");
        final Set<String> locks = new HashSet<>();
        inject(reissue, "redisCache", new RedisCache() {
            @Override public Boolean hasKey(String key) { return locks.contains(key); }
            @Override public <T> void setCacheObject(String key, T value, Integer timeout, TimeUnit unit) { locks.add(key); }
            @Override public boolean deleteObject(String key) { return locks.remove(key); }
        });
        inject(task, "exceptionOrderService", reissue);
        Map<String, Object> before = jdbc.queryForMap("select order_status,number,order_price,remark from mc_order where order_Id=?", ENGINE);
        success(task.autoReissueExceptionOrders());
        check("PROCESSED".equals(jdbc.queryForObject("select status from bank_manual_intervention_record", String.class)), "manual record progressed");
        String exchange = jdbc.queryForObject("select exchange_code from bank_exchange_order", String.class);
        check(exchange.startsWith("SYNTHETIC-CODE(补发"), "bank code reissue marker");
        check(jdbc.queryForObject("select status from bank_code_generation_record", Integer.class) == 0, "bank generation reset");
        success(task.autoReissueExceptionOrders());
        check(exchange.equals(jdbc.queryForObject("select exchange_code from bank_exchange_order", String.class)), "bank replay no second reissue");
        check(locks.isEmpty(), "synthetic lock released");
        check(before.equals(jdbc.queryForMap("select order_status,number,order_price,remark from mc_order where order_Id=?", ENGINE)),
                "matching platform text cannot redirect bank reissue into mc_order");
        System.out.println("LEGACY_XXL_CASE autoReissueExceptionOrders PASS bankOnly=true sameTextEngineUnchanged=true redisLock=FAKE");
    }

    private void verifyOtherActiveFilters() throws Exception {
        // 电影入口只按type2/status1扫描；以过期旧电影控制证明不是仅空结果通过。
        McOrder movie = order("OLD-XXL-MOVIE", "1", "SYNTHETIC-MOVIE");
        movie.setProductType(2); movie.setCreateTime(new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(6)));
        orders.insertMcOrder(movie);
        jdbc.update("update mc_order set order_status='1' where order_Id=?", ENGINE);
        success(task.unlockMovieOrder());
        check("6".equals(status(movie.getOrderId())) && "1".equals(status(ENGINE)), "movie type boundary");
        jdbc.update("update mc_order set order_status='2' where order_Id in (?,?)", ENGINE, movie.getOrderId());
        System.out.println("LEGACY_XXL_CASE unlockMovieOrder PASS oldMovieReleased=true engineDirectRechargeExcluded=true noSeatProvider=true");

        // 银行跑批先按商户外单查mc_order；只验证已存在新单的跳过分支，不冒充完整银行建单/出库验收。
        BankServiceImpl bank = new BankServiceImpl();
        inject(bank, "mcOrderService", service);
        inject(bank, "bankExchangeOrderMapper", session.getMapper(BankExchangeOrderMapper.class));
        inject(task, "bankService", bank);
        jdbc.update("insert into bank_exchange_order(external_order_no,order_no,exchange_code,status,order_status)"
                + " values('BANK-COLLISION','SHARED','SYNTHETIC-OTHER-CODE',2,0)");
        new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource())).execute(status -> {
            // 暂移旧控制的同外单，使银行count只能命中新单；之后回滚，避免旧单掩盖验证结果。
            jdbc.update("update mc_order set out_orderId=concat('OLD-',order_Id) where trade_engine_owned=0 and out_orderId='SHARED'");
            check(jdbc.queryForObject("select count(*) from mc_order where out_orderId='SHARED'", Integer.class) == 1,
                    "bank external collision is exclusively the engine order");
            int count = jdbc.queryForObject("select count(*) from mc_order", Integer.class);
            Map<String, Object> before = identity();
            success(task.insertBatchOrder());
            success(task.insertBatchOrder());
            check(jdbc.queryForObject("select count(*) from mc_order", Integer.class) == count && identity().equals(before),
                    "existing external number does not overwrite engine or create duplicate");
            check(jdbc.queryForObject("select order_status from bank_exchange_order where external_order_no='BANK-COLLISION'", Integer.class) == 0,
                    "existing-external skip branch leaves bank unsynced");
            status.setRollbackOnly();
            return null;
        });
        System.out.println("LEGACY_XXL_CASE insertBatchOrder PASS existingExternalSkipOnly=true bankCreateAndStock=NOT_TESTED");
    }

    private void verifyCallback(String url) {
        jdbc.update("update mc_order set order_status='6',number=0,callback_url='' where order_Id=?", ENGINE);
        McOrder blank = order("OLD-XXL-CALLBACK-BLANK", "6", "OTHER");
        orders.insertMcOrder(blank);
        success(task.callback());
        int engineBlank = number(ENGINE);
        check(number(blank.getOrderId()) == MAX_CALLBACK_ATTEMPTS, "old blank URL consumed");
        check(received.isEmpty(), "blank URL has no HTTP");
        McOrder old = order("OLD-XXL-CALLBACK-HTTP", "12", "OTHER");
        old.setCallbackUrl(url); orders.insertMcOrder(old);
        jdbc.update("update mc_order set order_status='12',number=0,callback_url=? where order_Id=?", url, ENGINE);
        success(task.callback());
        int engineHttp = number(ENGINE);
        check(number(old.getOrderId()) == MAX_CALLBACK_ATTEMPTS, "old real HTTP ack progress");
        long engineNotifications = received.stream().filter(message -> ENGINE.equals(message.getString("order_id"))).count();
        long oldNotifications = received.stream().filter(message -> old.getOrderId().equals(message.getString("order_id"))).count();
        check(oldNotifications == 1, "old control sent once to local receiver");
        for (JSONObject message : received) {
            check("SHARED".equals(message.getString("out_order_sn")) && message.getLongValue("total_price") == 137L,
                    "actual HTTP payload has expected identity and amount");
            check(message.getString("sign") != null, "actual candidate signed payload");
        }
        int firstCount = received.size();
        success(task.callback());
        check(received.size() == firstCount, "ACK-stopped orders not re-notified in next scan");
        System.out.println("LEGACY_XXL_CASE callback AUDIT engineBlankNumber=" + engineBlank
                + " engineHttpNumber=" + engineHttp + " engineHttpRequests=" + engineNotifications
                + " oldHttpRequests=" + oldNotifications + " nextScanRequests=0");
        if (engineBlank != 0 || engineHttp != 0 || engineNotifications != 0) {
            System.out.println("LEGACY_XXL_VIOLATION generic-callback-owns-engine-order");
        }
        new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource())).execute(status -> {
            jdbc.update("update mc_order set order_status='11',org_code='A36',supplier_channel='OTHER',number=0,callback_url='' where order_Id=?", ENGINE);
            jdbc.update("update recharge_order_ext set org_code='A36',persist_status='BROKEN',balance_reserved=0 where order_id=?", ENGINE);
            success(task.callback());
            if (number(ENGINE) != 0) System.out.println("LEGACY_XXL_VIOLATION callback-exclusion-must-use-any-ext-not-channel-or-state");
            status.setRollbackOnly();
            return null;
        });
        // 恢复父进程约定状态，但保留number与通知证据；资金/ext/outbox由父进程独立复核。
        jdbc.update("update mc_order set order_status='6' where order_Id=?", ENGINE);
    }

    private McOrder order(String id, String status, String channel) {
        McOrder order = new McOrder();
        order.setOrderId(id); order.setOrgCode("A35"); order.setOutOrderid("SHARED");
        order.setOrderStatus(status); order.setProductType(1); order.setOrderPrice(137L);
        order.setProductPrice(137L); order.setProductValue(1000L); order.setNum(1L);
        order.setProdutCode("SKU"); order.setParentId("PARENT"); order.setProductName("合成商品");
        order.setsupplierChannel(channel); order.setAccount("synthetic-account"); order.setNumber(0);
        order.setCreateTime(new Date()); order.setOrderType(0); order.setCostPrice(101L);
        return order;
    }

    private Map<String, Object> identity() {
        return jdbc.queryForMap("select order_Id,org_code,out_orderId,order_price,trade_engine_owned from mc_order where order_Id=?", ENGINE);
    }
    private String status(String id) { return jdbc.queryForObject("select order_status from mc_order where order_Id=?", String.class, id); }
    private int number(String id) { return jdbc.queryForObject("select number from mc_order where order_Id=?", Integer.class, id); }
    private static void success(AjaxResult result) { check(Integer.valueOf(200).equals(result.get("code")), "task returned success: " + result); }
    private static void check(boolean value, String message) { if (!value) throw new AssertionError(message); }
    private static void inject(Object bean, String name, Object value) throws Exception { inject(bean, bean.getClass(), name, value); }
    private static void inject(Object bean, Class<?> owner, String name, Object value) throws Exception {
        Field field = owner.getDeclaredField(name); field.setAccessible(true); field.set(bean, value);
    }
    private static byte[] readAll(java.io.InputStream body) throws java.io.IOException {
        try (java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream()) {
            byte[] bytes = new byte[1024]; int length;
            while ((length = body.read(bytes)) != -1) buffer.write(bytes, 0, length);
            return buffer.toByteArray();
        }
    }
}

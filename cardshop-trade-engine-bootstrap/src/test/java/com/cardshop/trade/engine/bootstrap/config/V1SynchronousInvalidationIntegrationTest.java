package com.cardshop.trade.engine.bootstrap.config;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.cardshop.trade.engine.api.v1.controller.V1RechargeOrderController;
import com.cardshop.trade.engine.api.v1.codec.V1RequestBodyCodec;
import com.cardshop.trade.engine.api.v1.protocol.*;
import com.cardshop.trade.engine.api.v1.routing.*;
import com.cardshop.trade.engine.api.v1.security.*;
import com.cardshop.trade.engine.api.v1.usecase.*;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.application.v1.*;
import com.cardshop.trade.engine.application.v1.crypto.*;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.infrastructure.persistence.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.cardshop.trade.engine.infrastructure.supplier.miniso.*;
import com.fasterxml.jackson.databind.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.net.http.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;
import reactor.core.publisher.Mono;

/**
 * 新终态合同的真实WebFlux/Worker/MySQL及回环MINISO验证，参见详细设计9.3、计划SINV-F。
 * 仅接受隔离脚本创建的随机schema与标记；不启动扫描，不调用真实供应商或银行。
 * 替代旧202/queryOnly恢复断言，未知失败不能恢复成成功或退款。
 */
@EnabledIfSystemProperty(named = "miniso.mysqlTests", matches = "true")
class V1SynchronousInvalidationIntegrationTest {
    private static final String ORG = "SINV";
    private final Clock clock = Clock.systemUTC();
    private final ObjectMapper json = new ObjectMapper();
    private final V12SigningCredential credential = V12SigningCredential.generate(ORG);
    private JdbcTemplate jdbc;
    private MinisoReceiptMapper receipts;
    private final AtomicInteger abandons = new AtomicInteger();
    private final AtomicInteger queries = new AtomicInteger();
    private final AtomicReference<String> reply = new AtomicReference<>();
    private final AtomicInteger httpStatus = new AtomicInteger(200);
    private final AtomicLong supplierDelayMs = new AtomicLong();
    private final AtomicReference<CountDownLatch> authGate = new AtomicReference<>();
    private final AtomicReference<CountDownLatch> flowGate = new AtomicReference<>();
    private final AtomicReference<CountDownLatch> supplierGate = new AtomicReference<>();
    private final AtomicInteger flowEntered = new AtomicInteger();
    private final AtomicReference<CountDownLatch> authEntered = new AtomicReference<>();
    private final AtomicReference<CountDownLatch> authFinished = new AtomicReference<>();
    private final AtomicReference<CountDownLatch> connectionClosed = new AtomicReference<>();

    public static void main(String[] args) throws Exception {
        new V1SynchronousInvalidationIntegrationTest().synchronousTerminalContractOnIsolatedMysql();
    }

    @Test void synchronousTerminalContractOnIsolatedMysql() throws Exception {
        String child = System.getProperty("sinv.child");
        String run = required("MINISO_MYSQL_RUN_ID");
        assertThat(run).matches("[a-f0-9]{16}");
        String schema = required("MINISO_MYSQL_SCHEMA");
        assertThat(schema).isEqualTo("miniso_bridge_it_" + run);
        String port = required("MINISO_MYSQL_PORT");
        assertThat(port).matches("[0-9]{1,5}");
        int isolation = switch (required("MINISO_MYSQL_ISOLATION")) {
            case "READ-COMMITTED" -> Connection.TRANSACTION_READ_COMMITTED;
            case "REPEATABLE-READ" -> Connection.TRANSACTION_REPEATABLE_READ;
            default -> throw new AssertionError("unsupported isolation");
        };
        var ds = new DriverManagerDataSource("jdbc:mysql://127.0.0.1:" + port + "/" + schema
                + "?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=Asia/Shanghai", "miniso_bridge_it",
                required("MINISO_MYSQL_PASSWORD")) {
            @Override public Connection getConnection() throws java.sql.SQLException {
                var c = super.getConnection(); c.setTransactionIsolation(isolation); return c;
            }
        };
        jdbc = new JdbcTemplate(ds);
        assertThat(jdbc.queryForObject("select run_id from miniso_bridge_fixture", String.class)).isEqualTo(run);
        assertThat(jdbc.queryForObject("select version()", String.class)).isEqualTo("8.0.28");
        if (child == null) {
            jdbc.execute("alter table mc_order_supplier_flow modify id bigint not null auto_increment, "
                    + "add out_order_no varchar(255), add channel varchar(255), add request_param longtext, "
                    + "add response_result longtext, add create_time datetime");
        }
        var tm = new DataSourceTransactionManager(ds);
        var cfg = new MybatisConfiguration(); cfg.setMapUnderscoreToCamelCase(true);
        cfg.addMappers("com.cardshop.trade.engine.infrastructure.persistence.mapper");
        var factory = new MybatisSqlSessionFactoryBean(); factory.setDataSource(ds); factory.setConfiguration(cfg);
        var session = new SqlSessionTemplate(factory.getObject());
        receipts = session.getMapper(MinisoReceiptMapper.class);
        var guards = session.getMapper(MinisoCouponGuardMapper.class);
        var events = session.getMapper(MinisoWriteOffEventMapper.class);
        var protection = new MybatisMinisoWriteOffProtection(events, receipts, guards,
                session.getMapper(MinisoSharedCallbackInboxMapper.class), tm, clock, 10);
        var store = new MinisoReceiptStore(receipts, guards, clock, () -> protection);
        var target = new MybatisRechargeFulfillmentOrderAdapter(session.getMapper(McOrderMapper.class),
                session.getMapper(RechargeOrderExtMapper.class), session.getMapper(McOrderSupplierFlowMapper.class),
                json, clock, () -> store, session.getMapper(McDepartRecordMapper.class),
                session.getMapper(RechargeDispatchOutboxMapper.class), session.getMapper(RechargeBalanceDeltaOutboxMapper.class));
        var proxy = new ProxyFactory(target);
        proxy.addAdvice(new TransactionInterceptor(tm, new AnnotationTransactionAttributeSource()));
        RechargeFulfillmentOrderPort actualOrders = (RechargeFulfillmentOrderPort) proxy.getProxy();
        RechargeFulfillmentOrderPort orders = mock(RechargeFulfillmentOrderPort.class,
                org.mockito.AdditionalAnswers.delegatesTo(actualOrders));
        doAnswer(invocation -> {
            var gate = flowGate.get();
            if (gate != null) { flowEntered.incrementAndGet(); awaitGate(gate); }
            actualOrders.updateSupplierFlow(invocation.getArgument(0));
            return null;
        }).when(orders).updateSupplierFlow(any());
        var outbox = new MybatisRechargeDispatchOutboxAdapter(session.getMapper(RechargeDispatchOutboxMapper.class), json,
                clock, session.getMapper(McOrderMapper.class), session.getMapper(RechargeOrderExtMapper.class), events,
                receipts, guards, () -> protection, () -> orders, tm);
        var supplier = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var supplierThreads = Executors.newVirtualThreadPerTaskExecutor();
        supplier.setExecutor(supplierThreads);
        supplier.createContext("/tripartite/coupon/", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            if (!exchange.getRequestURI().getPath().endsWith("abandonUserCoupon")) {
                queries.incrementAndGet(); exchange.sendResponseHeaders(500, -1); exchange.close(); return;
            }
            abandons.incrementAndGet();
            var gate = supplierGate.get(); if (gate != null) awaitGate(gate);
            try { Thread.sleep(supplierDelayMs.get()); }
            catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new IllegalStateException(ex); }
            String coupon = json.readTree(body).path("couponCodes").asText();
            String response = reply.get();
            if (response == null) response = json.writeValueAsString(Map.of("code", 200, "data", Map.of(
                    "succeed", List.of(Map.of("couponCode", coupon, "msg", "已作废")), "failed", List.of())));
            byte[] bytes = response.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(httpStatus.get(), bytes.length);
            try (var stream = exchange.getResponseBody()) { stream.write(bytes); }
            finally { exchange.close(); }
        });
        supplier.start();
        var gateway = new MinisoRechargeSupplierGatewayAdapter(new MinisoHttpClient(new MinisoSettings(
                URI.create(System.getProperty("sinv.supplier", "http://127.0.0.1:" + supplier.getAddress().getPort())), "MINISO", "fixture", "tripartite1.0",
                "synthetic", Duration.ofSeconds(30)), HttpClient.newHttpClient(), json), new MinisoOrderSnapshotReader() {
                    public Optional<MinisoOrderSnapshot> find(String org, String id) { return Optional.ofNullable(receipts.selectSnapshot(org, id)); }
                    public Optional<MinisoFulfillmentReceipt> localReceipt(String org, String id) {
                        return Optional.ofNullable(receipts.selectReceipt(id)).filter(r -> org.equals(r.orgCode()));
                    }
                });
        var worker = new RechargeOutboxWorkerService(outbox, orders, gateway, outbox,
                mock(DirectRechargeFrequencyPort.class), BalanceDeltaFlushOutboxPort.noop(),
                command -> BalanceReservationReleaseResult.alreadyReleasedSuccess(), mock(RechargeNotifyTaskPort.class),
                mock(MerchantNotifyPayloadSigner.class), RechargeDispatchTimerPort.noop(), clock,
                new RechargeOutboxWorkerSettings(Duration.ofSeconds(child == null ? 120 : 1), Duration.ofMillis(50),
                        Duration.ofSeconds(1), Duration.ofSeconds(10), Duration.ofSeconds(1)), () -> protection);
        var precheck = new V1ProtocolPrecheckService(new V1RequestBodyCodec(), new V1TimestampValidator(
                new V1TimestampSettings(Duration.ofSeconds(300)), clock), new V12SignatureVerifier(), new V1ProtocolSettings(false));
        var useCase = new V1InvalidRechargeOrderUseCaseService(precheck,
                (app, org) -> Optional.of(new V1MerchantProfile(app, org, "", true, 1L)),
                new MybatisRechargeOrderQueryAdapter(session.getMapper(McOrderMapper.class), session.getMapper(RechargeOrderExtMapper.class)),
                new RechargeOrderInvalidationService(outbox, worker, "http-local"));
        var auth = new V1TokenAuthenticator(new V1TokenSettings("fixture", "fixture:", Duration.ofMinutes(1)), clock, Optional.empty()) {
            @Override public Optional<AuthenticatedMerchant> authenticate(String token) {
                var entered = authEntered.get(); if (entered != null) entered.countDown();
                var gate = authGate.get(); if (gate != null) awaitGate(gate);
                var finished = authFinished.get(); if (finished != null) finished.countDown();
                return "fixture".equals(token) ? Optional.of(new AuthenticatedMerchant("app", ORG, credential)) : Optional.empty();
            }
        };
        var controller = new V1RechargeOrderController(new V1RouteDecisionService(new V1RoutingSettings(true,
                "http://unused", Duration.ofSeconds(3), List.of(ORG), List.of("app"), List.of("/v1/cardapi/order/invalid"))),
                auth, (request, body) -> Mono.error(new AssertionError("legacy route")),
                command -> CompletableFuture.failedFuture(new AssertionError("create must not run")),
                command -> V1CardApiResultStatus.SYSTEM_BUSY.result(), useCase);
        try (var context = new AnnotationConfigApplicationContext()) {
            context.register(WebConfig.class); context.getBeanFactory().registerSingleton("invalidationController", controller); context.refresh();
            var server = reactor.netty.http.server.HttpServer.create().host("127.0.0.1").port(0)
                    .doOnConnection(connection -> {
                        var closed = connectionClosed.get();
                        if (closed != null) connection.onDispose().subscribe(ignored -> {}, failure -> {}, closed::countDown);
                    })
                    .handle(new ReactorHttpHandlerAdapter(WebHttpHandlerBuilder.applicationContext(context).build())).bindNow();
            try {
                URI endpoint = URI.create("http://127.0.0.1:" + server.port() + "/v1/cardapi/order/invalid");
                if (child != null) {
                    if ("resume".equals(child)) worker.processEvent("invalid-CRASH", OutboxEventType.INVALID_ORDER, "new-jvm");
                    post(endpoint, "CRASH", "start".equals(child) ? "200" : "503");
                    return;
                }
                // 4秒MINISO回包后停住第一项本地回写：银行立即200，跨节点只读同轮SUCCESS也200。
                seed("EARLY"); supplierDelayMs.set(4000); flowGate.set(new CountDownLatch(1));
                var first = post(endpoint, "EARLY", "200");
                assertThat(first.path("data").path("use_state").asText()).isEqualTo("3");
                awaitCondition(() -> flowEntered.get() > 0);
                assertThat(jdbc.queryForObject("select dispatch_status from recharge_order_ext where order_id='EARLY'", String.class)).isEqualTo("SUCCESS");
                var repeated = post(endpoint, "EARLY", "200");
                assertThat(repeated.path("data").path("invalid_time")).isEqualTo(first.path("data").path("invalid_time"));
                flowGate.getAndSet(null).countDown(); supplierDelayMs.set(0); awaitDelta("EARLY");
                assertThat(abandons.get()).isEqualTo(1);

                // 明确failed保存后后续有效HTTP（含相同nonce）追加新轮；旧轮观察始终失败。
                seed("REAPPLY");
                reply.set("{\"code\":200,\"data\":{\"succeed\":[],\"failed\":[{\"couponCode\":\"COUPON-REAPPLY\",\"msg\":\"非白名单拒绝\"}]}}");
                String sameBody = signedBody("REAPPLY");
                send(endpoint, sameBody, "503");
                awaitCondition(() -> outbox.findInvalidOrder(ORG, "out-REAPPLY").orElseThrow().status() == RechargeOrderInvalidationState.Status.FAILED);
                reply.set(null); send(endpoint, sameBody, "200"); awaitDelta("REAPPLY");
                assertThat(outbox.findInvalidOrder(ORG, "out-REAPPLY", 1).orElseThrow().status()).isEqualTo(RechargeOrderInvalidationState.Status.FAILED);
                assertThat(outbox.findInvalidOrder(ORG, "out-REAPPLY").orElseThrow().attempt().number()).isEqualTo(2);
                assertThat(jdbc.queryForObject("select count(*) from mc_order_supplier_flow where order_id='REAPPLY'", Integer.class)).isEqualTo(2);
                assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox where order_id='REAPPLY'", Integer.class)).isEqualTo(1);

                for (var entry : Map.of("MISSING", "{\"code\":200}", "BUSINESS", "{\"code\":505}", "TEXT", "not-json").entrySet()) {
                    seed(entry.getKey()); reply.set(entry.getValue()); int before = abandons.get();
                    post(endpoint, entry.getKey(), "503");
                    awaitCondition(() -> outbox.findInvalidOrder(ORG, "out-" + entry.getKey()).orElseThrow().status() == RechargeOrderInvalidationState.Status.FAILED);
                    reply.set(null); post(endpoint, entry.getKey(), "503");
                    worker.processEvent("invalid-" + entry.getKey(), OutboxEventType.INVALID_ORDER, "audit-control");
                    assertThat(abandons.get()).isEqualTo(before + 1); assertNoFunds(entry.getKey());
                }
                seed("HTTP"); httpStatus.set(502); post(endpoint, "HTTP", "503");
                awaitCondition(() -> jdbc.queryForObject("select response_result from mc_order_supplier_flow where order_id='HTTP'", String.class).contains("502"));
                httpStatus.set(200); post(endpoint, "HTTP", "503"); assertNoFunds("HTTP");

                // 在途合并不重复发送；加入HTTP取消不得影响原轮；一次HTTP绝不自行开下一轮。
                seed("JOIN"); supplierGate.set(new CountDownLatch(1)); int beforeJoin = abandons.get();
                try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                    var owner = pool.submit(() -> post(endpoint, "JOIN", "200"));
                    awaitCondition(() -> abandons.get() == beforeJoin + 1);
                    var closed = new CountDownLatch(1); connectionClosed.set(closed);
                    var entered = new CountDownLatch(1); authEntered.set(entered);
                    try (var socket = new Socket("127.0.0.1", server.port())) {
                        sendSocket(socket, "JOIN"); assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                    }
                    assertThat(closed.await(2, TimeUnit.SECONDS)).isTrue();
                    connectionClosed.set(null); authEntered.set(null);
                    var join = pool.submit(() -> post(endpoint, "JOIN", "200"));
                    Thread.sleep(200); supplierGate.getAndSet(null).countDown();
                    owner.get(5, TimeUnit.SECONDS); join.get(5, TimeUnit.SECONDS);
                }
                awaitDelta("JOIN"); assertThat(abandons.get()).isEqualTo(beforeJoin + 1);

                // SQL终态回滚不能覆盖已知MINISO成功；只补本地收尾，不再调用远端。
                seed("ROLLBACK");
                jdbc.execute("create trigger sinv_terminal_rollback before update on recharge_order_ext for each row begin "
                        + "if NEW.order_id='ROLLBACK' and NEW.dispatch_status='CANCELLED' then signal sqlstate '45000' set message_text='fixture rollback'; end if; end");
                post(endpoint, "ROLLBACK", "200");
                awaitCondition(() -> "FAILED".equals(jdbc.queryForObject("select status from recharge_dispatch_outbox where order_id='ROLLBACK'", String.class)));
                assertNoFunds("ROLLBACK"); jdbc.execute("drop trigger sinv_terminal_rollback");
                awaitCondition(() -> outbox.findDueByEventId("invalid-ROLLBACK", OutboxEventType.INVALID_ORDER).isPresent());
                worker.processEvent("invalid-ROLLBACK", OutboxEventType.INVALID_ORDER, "recovery"); awaitDelta("ROLLBACK");
                post(endpoint, "ROLLBACK", "200");

                seed("NO-RESERVATION");
                jdbc.update("update recharge_order_ext set balance_reserved=0 where order_id='NO-RESERVATION'");
                jdbc.update("delete from mc_depart_record where order_id='NO-RESERVATION'");
                post(endpoint, "NO-RESERVATION", "200");
                awaitCondition(() -> jdbc.queryForObject("select balance_no_release_event_id from recharge_order_ext where order_id='NO-RESERVATION'", String.class) != null);
                assertNoFunds("NO-RESERVATION");

                var writeOff = new MybatisMinisoWriteOffStore(events, receipts, guards, mock(RechargeNotifyTaskPort.class),
                        new V12RedemptionNotifyPayloadSigner(mock(V12SigningCredentialStorePort.class)), tm, clock, 10, Duration.ofSeconds(1), 10);
                seed("REDEEM-FIRST"); int beforeConflict = abandons.get();
                writeOff.accept(new MinisoWriteOffNotification("COUPON-REDEEM-FIRST", LocalDateTime.now(clock).withNano(0)));
                post(endpoint, "REDEEM-FIRST", "503"); assertNoFunds("REDEEM-FIRST");
                assertThat(abandons.get()).isEqualTo(beforeConflict);
                seed("CONFLICT"); supplierGate.set(new CountDownLatch(1));
                try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                    var response = pool.submit(() -> post(endpoint, "CONFLICT", "200"));
                    awaitCondition(() -> abandons.get() == beforeConflict + 1);
                    writeOff.accept(new MinisoWriteOffNotification("COUPON-CONFLICT", LocalDateTime.now(clock).withNano(0)));
                    supplierGate.getAndSet(null).countDown(); response.get(5, TimeUnit.SECONDS);
                }
                awaitCondition(() -> "MANUAL_REQUIRED".equals(jdbc.queryForObject("select status from recharge_dispatch_outbox where order_id='CONFLICT'", String.class)));
                assertNoFunds("CONFLICT"); post(endpoint, "CONFLICT", "200");

                seed("AUDIT-FAILURE");
                jdbc.execute("create trigger sinv_audit_failure before update on mc_order_supplier_flow for each row begin "
                        + "if NEW.order_id='AUDIT-FAILURE' then signal sqlstate '45000' set message_text='fixture audit failure'; end if; end");
                post(endpoint, "AUDIT-FAILURE", "200");
                awaitCondition(() -> "FAILED".equals(jdbc.queryForObject("select status from recharge_dispatch_outbox where order_id='AUDIT-FAILURE'", String.class)));
                assertNoFunds("AUDIT-FAILURE"); jdbc.execute("drop trigger sinv_audit_failure");
                awaitCondition(() -> outbox.findDueByEventId("invalid-AUDIT-FAILURE", OutboxEventType.INVALID_ORDER).isPresent());
                worker.processEvent("invalid-AUDIT-FAILURE", OutboxEventType.INVALID_ORDER, "recover-audit"); awaitDelta("AUDIT-FAILURE");

                // 受理前TCP断连关闭受理机会；认证稍后完成也不能生成事件。
                seed("PRE-CLOSE"); authGate.set(new CountDownLatch(1));
                var entered = new CountDownLatch(1); authEntered.set(entered);
                var finished = new CountDownLatch(1); authFinished.set(finished);
                var closed = new CountDownLatch(1); connectionClosed.set(closed);
                try (var socket = new Socket("127.0.0.1", server.port())) {
                    sendSocket(socket, "PRE-CLOSE"); assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                }
                assertThat(closed.await(2, TimeUnit.SECONDS)).isTrue();
                authGate.getAndSet(null).countDown(); assertThat(finished.await(2, TimeUnit.SECONDS)).isTrue();
                authEntered.set(null); authFinished.set(null); connectionClosed.set(null);
                post(endpoint, "EARLY", "200");
                for (int i = 0; i < 25; i++) {
                    assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox where order_id='PRE-CLOSE'", Integer.class)).isZero();
                    Thread.sleep(20);
                }

                // 新进程在已登记发送后被SIGKILL，重启只裁决未知并留证，不查询、不重发、不退款。
                seed("CRASH"); var crashGate = new CountDownLatch(1); supplierGate.set(crashGate);
                int beforeCrash = abandons.get();
                Path crashLog = Files.createTempFile("sinv-crash-", ".log"), resumeLog = Files.createTempFile("sinv-resume-", ".log");
                Process crashed = null, resumed = null;
                try {
                    crashed = childProcess("start", supplier.getAddress().getPort(), crashLog);
                    for (int i = 0; i < 1500 && abandons.get() == beforeCrash && crashed.isAlive(); i++) Thread.sleep(20);
                    assertThat(abandons.get()).as("child reached real supplier HTTP").isEqualTo(beforeCrash + 1);
                    assertThat(actualOrders.hasSupplierInvalidationAttempt("CRASH", "invalid-CRASH")).isTrue();
                    assertNoFunds("CRASH"); crashed.destroyForcibly();
                    assertThat(crashed.waitFor(5, TimeUnit.SECONDS)).isTrue(); assertThat(crashed.exitValue()).isEqualTo(137);
                    crashGate.countDown(); supplierGate.set(null);
                    awaitCondition(() -> outbox.findDueByEventId("invalid-CRASH", OutboxEventType.INVALID_ORDER).isPresent());
                    resumed = childProcess("resume", supplier.getAddress().getPort(), resumeLog);
                    assertThat(resumed.waitFor(30, TimeUnit.SECONDS)).isTrue();
                    assertThat(resumed.exitValue()).withFailMessage(() -> readLog(resumeLog)).isZero();
                    post(endpoint, "CRASH", "503"); assertNoFunds("CRASH");
                    assertThat(abandons.get()).isEqualTo(beforeCrash + 1); assertThat(queries.get()).isZero();
                    assertThat(outbox.findInvalidOrder(ORG, "out-CRASH").orElseThrow().attempt().result()).isEqualTo(InvalidationAttempt.Result.UNKNOWN);
                    System.out.println("SINV SIGKILL/restart: killed=137 newJvm=0 abandon=1 query=0 delta=0");
                } finally {
                    crashGate.countDown(); supplierGate.set(null);
                    for (var process : Arrays.asList(crashed, resumed)) {
                        if (process != null && process.isAlive()) { process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS); }
                    }
                    Files.deleteIfExists(crashLog); Files.deleteIfExists(resumeLog);
                }

                // 真正30秒HTTP超时，不把可缩短的测试超时当默认配置证据。
                seed("TIMEOUT"); supplierGate.set(new CountDownLatch(1)); long start = System.nanoTime();
                post(endpoint, "TIMEOUT", "503");
                assertThat(Duration.ofNanos(System.nanoTime() - start).toMillis()).isBetween(29000L, 34000L);
                supplierGate.getAndSet(null).countDown(); post(endpoint, "TIMEOUT", "503"); assertNoFunds("TIMEOUT");
                assertThat(queries.get()).isZero();

                // 默认整体等待真实跑满300秒一次（RC）；RR复用确定性纳秒边界与同一组件。
                if (isolation == Connection.TRANSACTION_READ_COMMITTED) {
                    seed("FIVE-MINUTE"); authGate.set(new CountDownLatch(1)); start = System.nanoTime();
                    post(endpoint, "FIVE-MINUTE", "500");
                    assertThat(Duration.ofNanos(System.nanoTime() - start).toMillis()).isBetween(299000L, 305000L);
                    authGate.getAndSet(null).countDown(); Thread.sleep(200);
                    assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox where order_id='FIVE-MINUTE'", Integer.class)).isZero();
                }
                System.out.println("SINV terminal HTTP/MySQL verified isolation=" + isolation + " abandon=" + abandons.get() + " query=0");
            } finally { release(authGate); release(flowGate); release(supplierGate); server.disposeNow(); }
        } finally { supplier.stop(0); supplierThreads.close(); }
    }

    private Process childProcess(String mode, int supplierPort, Path log) throws Exception {
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dsinv.child=" + mode, "-Dsinv.supplier=http://127.0.0.1:" + supplierPort,
                "-cp", System.getProperty("java.class.path"), getClass().getName())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }
    private static String readLog(Path path) {
        try { return Files.readString(path); }
        catch (java.io.IOException ex) { throw new java.io.UncheckedIOException(ex); }
    }
    private void sendSocket(Socket socket, String id) throws Exception {
        byte[] payload = signedBody(id).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String headers = "POST /v1/cardapi/order/invalid HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                + "Authorization: fixture\r\nContent-Type: application/json\r\nContent-Length: " + payload.length + "\r\n\r\n";
        socket.getOutputStream().write(headers.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        socket.getOutputStream().write(payload); socket.getOutputStream().flush();
    }
    private JsonNode post(URI endpoint, String id, String expected) throws Exception { return send(endpoint, signedBody(id), expected); }
    private JsonNode send(URI endpoint, String body, String expected) throws Exception {
        long start = System.nanoTime();
        var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(310))
                .header("Authorization", "fixture").header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        var result = json.readTree(response.body()); assertThat(result.path("code").asText()).isEqualTo(expected);
        if (!"200".equals(expected)) assertThat(result.path("data").isNull()).isTrue();
        System.out.println("SINV HTTP code=" + expected + " elapsedMs=" + Duration.ofNanos(System.nanoTime() - start).toMillis());
        return result;
    }
    private String signedBody(String id) throws Exception {
        var payload = new LinkedHashMap<String, Object>();
        payload.put("out_order_sn", "out-" + id); payload.put("timestamp", String.valueOf(clock.millis()));
        payload.put("nonce_str", UUID.randomUUID().toString().replace("-", ""));
        payload.put("sign", V12MessageSignatures.sign(V12MessageSignatures.Operation.INVALID, payload, credential.signKey()));
        return json.writeValueAsString(payload);
    }
    private void seed(String id) {
        jdbc.update("insert into mc_order(order_Id,org_code,out_orderId,product_type,num,supplier_channel,account,order_status,order_price,callback_url,data_status,parent_id,produt_code) values (?,?,?,1,1,'MINISO','10000000000',12,137,'','PROCESSED','parent','sku')", id, ORG, "out-" + id);
        jdbc.update("insert into recharge_order_ext(order_id,org_code,out_order_sn,request_hash,request_hash_version,request_hash_alg,persist_status,persist_source,validation_status,balance_reserved,dispatch_allowed,dispatch_status,supplier_batch_id,voucher_use_state) values (?,?,?,'fixture','V1','SHA-256','DB_PERSISTED','FIXTURE','PASSED',1,1,'SUCCESS','100023','1')", id, ORG, "out-" + id);
        jdbc.update("insert into mc_depart_record(order_id,org_code,type,`value`) values (?,?,'1',137)", id, ORG);
        receipts.insert(new MinisoFulfillmentReceipt(id, ORG, 100023, "COUPON-" + id, "10000000000", "2026-10-01 00:00:00", "2026-12-01 00:00:00", 0));
    }
    private void assertNoFunds(String id) {
        assertThat(jdbc.queryForObject("select count(*) from recharge_balance_delta_outbox where order_id=?", Integer.class, id)).isZero();
        assertThat(jdbc.queryForObject("select balance_release_event_id from recharge_order_ext where order_id=?", String.class, id)).isNull();
    }
    private void awaitDelta(String id) throws Exception { awaitCondition(() -> jdbc.queryForObject("select count(*) from recharge_balance_delta_outbox where order_id=?", Integer.class, id) == 1); }
    private static void awaitCondition(java.util.function.BooleanSupplier condition) throws Exception {
        for (int i = 0; i < 250; i++) { if (condition.getAsBoolean()) return; Thread.sleep(20); }
        throw new AssertionError("condition did not become true");
    }
    private static void awaitGate(CountDownLatch gate) {
        try { if (!gate.await(310, TimeUnit.SECONDS)) throw new IllegalStateException("fixture gate timeout"); }
        catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new IllegalStateException(ex); }
    }
    private static void release(AtomicReference<CountDownLatch> ref) { var gate = ref.getAndSet(null); if (gate != null) gate.countDown(); }
    private static String required(String name) { return Objects.requireNonNull(System.getenv(name), name); }
    @Configuration(proxyBeanMethods = false) @EnableWebFlux static class WebConfig {}
}

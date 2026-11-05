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
import com.cardshop.trade.engine.infrastructure.persistence.*;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.*;
import com.cardshop.trade.engine.infrastructure.supplier.miniso.*;
import com.fasterxml.jackson.databind.*;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
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
 * 真实WebFlux/Worker/MySQL与回环MINISO的隔离合同验证；参见详细设计9.3.2—9.3.7。
 * 只接受脚本创建的随机schema及标记，关闭全部扫描。认证主体为本机夹具，验签及持久流程真实。
 */
@EnabledIfSystemProperty(named = "miniso.mysqlTests", matches = "true")
class V1SynchronousInvalidationIntegrationTest {
    private static final String ORG = "SINV";
    // 固定每秒的900ms边界，覆盖MySQL datetime舍入使首次due晚于即时唤醒的问题。
    private final Clock clock = Clock.offset(Clock.tickSeconds(ZoneOffset.UTC), Duration.ofMillis(900));
    private final ObjectMapper json = new ObjectMapper();
    private final V12SigningCredential credential = V12SigningCredential.generate(ORG);
    private JdbcTemplate jdbc;
    private MybatisRechargeDispatchOutboxAdapter outbox;
    private RechargeFulfillmentOrderPort orders;
    private MinisoReceiptMapper receipts;
    private final AtomicInteger abandons = new AtomicInteger();
    private final AtomicInteger queries = new AtomicInteger();
    private final AtomicBoolean dropReply = new AtomicBoolean();
    private final AtomicReference<CountDownLatch> authBlock = new AtomicReference<>();
    private final CountDownLatch authEntered = new CountDownLatch(1);
    private final CountDownLatch authFinished = new CountDownLatch(1);
    private final AtomicReference<CountDownLatch> supplierBlock = new AtomicReference<>();
    private final CountDownLatch supplierEntered = new CountDownLatch(1);
    private final AtomicReference<CountDownLatch> connectionClosed = new AtomicReference<>();
    private final CountDownLatch fundsEntered = new CountDownLatch(1);
    private final CountDownLatch fundsRelease = new CountDownLatch(1);
    private final AtomicBoolean rejectExecution = new AtomicBoolean();

    /** 独立JVM运行同一HTTP/持久组件；父进程SIGKILL后由新JVM读取原attempt恢复，参见详细设计9.3.7。 */
    public static void main(String[] args) throws Exception {
        new V1SynchronousInvalidationIntegrationTest().realHttpCommitsOnceAndObservesBeforeFundsRecoveryWithoutScanning();
    }

    @Test void realHttpCommitsOnceAndObservesBeforeFundsRecoveryWithoutScanning() throws Exception {
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
        // 桥接测试的旧流水表仅含更新字段；本用例另补已有生产读写字段，只作用于本次随机schema。
        boolean child = System.getProperty("sinv.child") != null;
        if (!child) jdbc.execute("alter table mc_order_supplier_flow modify id bigint not null auto_increment, "
                + "add out_order_no varchar(255), add channel varchar(255), add request_param longtext, "
                + "add response_result longtext, add create_time datetime");
        var tm = new DataSourceTransactionManager(ds);
        var cfg = new MybatisConfiguration();
        cfg.setMapUnderscoreToCamelCase(true);
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
        orders = (RechargeFulfillmentOrderPort) proxy.getProxy();
        outbox = new MybatisRechargeDispatchOutboxAdapter(session.getMapper(RechargeDispatchOutboxMapper.class), json,
                clock, session.getMapper(McOrderMapper.class), session.getMapper(RechargeOrderExtMapper.class), events,
                receipts, guards, () -> protection, () -> orders, tm);
        var supplier = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        supplier.createContext("/tripartite/coupon/", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            String response;
            if (exchange.getRequestURI().getPath().endsWith("abandonUserCoupon")) {
                abandons.incrementAndGet();
                if (dropReply.get()) { exchange.close(); return; }
                String coupon = json.readTree(body).path("couponCodes").asText();
                var gate = supplierBlock.get();
                if (gate != null) {
                    supplierEntered.countDown();
                    awaitGate(gate);
                }
                response = json.writeValueAsString(Map.of("code", 200, "data", Map.of("succeed",
                        List.of(Map.of("couponCode", coupon, "msg", "已作废")), "failed", List.of())));
            } else {
                queries.incrementAndGet();
                String id = json.readTree(body).path("orderNo").asText();
                response = json.writeValueAsString(Map.of("code", 200, "data", List.of(Map.of(
                        "batchId", 100023, "couponCode", "COUPON-" + id, "status", 3))));
            }
            byte[] bytes = response.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes); exchange.close();
        });
        supplier.start();
        var gateway = new MinisoRechargeSupplierGatewayAdapter(new MinisoHttpClient(new MinisoSettings(
                URI.create(System.getProperty("sinv.supplier", "http://127.0.0.1:" + supplier.getAddress().getPort())), "MINISO", "fixture", "tripartite1.0",
                "synthetic", Duration.ofSeconds(3)), HttpClient.newHttpClient(), json), new MinisoOrderSnapshotReader() {
                    public Optional<MinisoOrderSnapshot> find(String org, String id) {
                        return Optional.ofNullable(receipts.selectSnapshot(org, id));
                    }
                    public Optional<MinisoFulfillmentReceipt> localReceipt(String org, String id) {
                        return Optional.ofNullable(receipts.selectReceipt(id)).filter(r -> org.equals(r.orgCode()));
                    }
                });
        BalanceReservationReleasePort balance = command -> {
            if (command.orderId().equals("SINV-EARLY")) {
                fundsEntered.countDown();
                try { if (!fundsRelease.await(8, TimeUnit.SECONDS)) throw new IllegalStateException("funds gate timeout"); }
                catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new IllegalStateException(ex); }
            }
            return BalanceReservationReleaseResult.alreadyReleasedSuccess();
        };
        var worker = spy(new RechargeOutboxWorkerService(outbox, orders, gateway, outbox,
                mock(DirectRechargeFrequencyPort.class), BalanceDeltaFlushOutboxPort.noop(), balance,
                mock(RechargeNotifyTaskPort.class), mock(MerchantNotifyPayloadSigner.class), RechargeDispatchTimerPort.noop(),
                clock, new RechargeOutboxWorkerSettings(Duration.ofSeconds(child ? 1 : 120), Duration.ofMillis(50),
                        Duration.ofSeconds(1), Duration.ofSeconds(10), Duration.ofSeconds(1)), () -> protection));
        doAnswer(invocation -> {
            if (rejectExecution.get()) {
                // 在提交后推进边界注入真实JDK执行器拒绝，不改变生产同步processEvent实现。
                try (var stopped = Executors.newSingleThreadExecutor()) {
                    stopped.shutdown();
                    stopped.submit(() -> {});
                }
                throw new AssertionError("executor must reject");
            }
            return invocation.callRealMethod();
        }).when(worker).processEvent(anyString(), any(), anyString());
        var precheck = new V1ProtocolPrecheckService(new V1RequestBodyCodec(), new V1TimestampValidator(
                new V1TimestampSettings(Duration.ofSeconds(300)), clock), new V12SignatureVerifier(), new V1ProtocolSettings(false));
        V1MerchantProfilePort merchants = (app, org) -> Optional.of(new V1MerchantProfile(app, org, "", true, 1L));
        var useCase = new V1InvalidRechargeOrderUseCaseService(precheck, merchants,
                new MybatisRechargeOrderQueryAdapter(session.getMapper(McOrderMapper.class),
                        session.getMapper(RechargeOrderExtMapper.class)), new RechargeOrderInvalidationService(outbox, worker, "http-local"));
        var auth = new V1TokenAuthenticator(new V1TokenSettings("fixture", "fixture:", Duration.ofMinutes(1)),
                clock, Optional.empty()) {
            @Override public Optional<AuthenticatedMerchant> authenticate(String token) {
                var gate = authBlock.get();
                if (gate != null) {
                    authEnteredRef.get().countDown();
                    try { awaitGate(gate); } finally { authFinishedRef.get().countDown(); }
                }
                return "fixture".equals(token) ? Optional.of(new AuthenticatedMerchant("app", ORG, credential)) : Optional.empty();
            }
        };
        var controller = new V1RechargeOrderController(new V1RouteDecisionService(new V1RoutingSettings(true,
                "http://unused", Duration.ofSeconds(3), List.of(ORG), List.of("app"), List.of("/v1/cardapi/order/invalid"))),
                auth, (request, body) -> Mono.error(new AssertionError("legacy route")),
                command -> CompletableFuture.failedFuture(new AssertionError("create must not run")),
                command -> V1CardApiResultStatus.SYSTEM_BUSY.result(), useCase);
        try (var context = new AnnotationConfigApplicationContext()) {
            context.register(WebConfig.class);
            context.getBeanFactory().registerSingleton("invalidationController", controller);
            context.refresh();
            var server = reactor.netty.http.server.HttpServer.create().host("127.0.0.1").port(0)
                    .doOnConnection(connection -> {
                        var closed = connectionClosed.get();
                        if (closed != null) connection.onDispose(closed::countDown);
                    })
                    .handle(new ReactorHttpHandlerAdapter(WebHttpHandlerBuilder.applicationContext(context).build())).bindNow();
            try {
                URI endpoint = URI.create("http://127.0.0.1:" + server.port() + "/v1/cardapi/order/invalid");
                if (child) {
                    var result = post(endpoint, "SINV-CRASH");
                    assertThat(result.path("code").asText()).isEqualTo("200");
                    awaitDelta("SINV-CRASH");
                    System.out.println("SINV child recovered=200 delta=1");
                    return;
                }
                seed("SINV-EARLY");
                JsonNode first = post(endpoint, "SINV-EARLY");
                assertThat(first.path("code").asText()).isEqualTo("200");
                assertThat(first.path("data").path("state_type").asText()).isEqualTo("3");
                assertThat(first.path("data").path("use_state").asText()).isEqualTo("3");
                assertThat(first.path("data").path("invalid_time").asText()).isNotBlank();
                assertThat(fundsEntered.getCount()).isZero();
                assertThat(jdbc.queryForObject("select balance_reserved from recharge_order_ext where order_id=?",
                        Boolean.class, "SINV-EARLY")).isTrue();
                assertThat(outbox.findInvalidOrder(ORG, "out-SINV-EARLY").orElseThrow().status())
                        .isEqualTo(RechargeOrderInvalidationState.Status.COMPLETED);
                var repeat = post(endpoint, "SINV-EARLY");
                assertThat(repeat.path("data").path("invalid_time")).isEqualTo(first.path("data").path("invalid_time"));
                assertThat(abandons.get()).isEqualTo(1);
                fundsRelease.countDown();
                awaitDelta("SINV-EARLY");

                // 已发生远端副作用后丢回包：原attempt保留，重复请求只能queryGrand。
                seed("SINV-LOST"); dropReply.set(true);
                var pending = post(endpoint, "SINV-LOST");
                assertThat(pending.path("code").asText()).isEqualTo("202");
                assertThat(pending.path("data").path("invalid_time").asText()).isEmpty();
                assertThat(abandons.get()).isEqualTo(2);
                dropReply.set(false);
                var recovered = post(endpoint, "SINV-LOST");
                assertThat(recovered.path("code").asText()).isEqualTo("200");
                assertThat(queries.get()).isEqualTo(1);
                assertThat(abandons.get()).isEqualTo(2);
                awaitDelta("SINV-LOST");
                // 损坏资金裁决不得利用券3/3快捷成功，也不允许再次abandon。
                jdbc.update("update recharge_order_ext set balance_release_amount=138 where order_id=?", "SINV-LOST");
                assertThat(post(endpoint, "SINV-LOST").path("code").asText()).isEqualTo("202");
                assertThat(abandons.get()).isEqualTo(2);
                assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox where org_code=?",
                        Integer.class, ORG)).isEqualTo(2);
                System.out.println("SINV real HTTP/MySQL: early=200 before funds; stable time; lost reply=202 then query-only=200; corrupt funds=202; abandon=2 query=1; isolation=" + isolation);

                seed("SINV-NONE");
                jdbc.update("update recharge_order_ext set balance_reserved=0 where order_id=?", "SINV-NONE");
                jdbc.update("delete from mc_depart_record where order_id=?", "SINV-NONE");
                assertThat(post(endpoint, "SINV-NONE").path("code").asText()).isEqualTo("200");
                assertThat(jdbc.queryForObject("select balance_no_release_event_id from recharge_order_ext where order_id=?",
                        String.class, "SINV-NONE")).isEqualTo("invalid-SINV-NONE");
                assertThat(jdbc.queryForObject("select count(*) from recharge_balance_delta_outbox where order_id=?",
                        Integer.class, "SINV-NONE")).isZero();

                seed("SINV-CONCURRENT");
                try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                    var responses = pool.<JsonNode>invokeAll(List.of(() -> post(endpoint, "SINV-CONCURRENT"),
                            () -> post(endpoint, "SINV-CONCURRENT")));
                    var firstConcurrent = responses.get(0).get();
                    for (var response : responses) {
                        assertThat(response.get().path("code").asText()).isEqualTo("200");
                        assertThat(response.get().path("data").path("invalid_time"))
                                .isEqualTo(firstConcurrent.path("data").path("invalid_time"));
                    }
                }
                awaitDelta("SINV-CONCURRENT");
                assertThat(abandons.get()).isEqualTo(4);

                // 真实数据库事务分别在4秒/6秒后才允许提交，验证截止两侧而非延长供应商3秒预算。
                jdbc.execute("create trigger sinv_admission_delay before insert on recharge_dispatch_outbox for each row "
                        + "begin if NEW.order_id='SINV-FOUR' then do sleep(4); "
                        + "elseif NEW.order_id='SINV-SIX' then do sleep(6); "
                        + "elseif NEW.order_id='SINV-ROLLBACK' then signal sqlstate '45000' set message_text='fixture admission rollback'; end if; end");
                seed("SINV-FOUR");
                assertThat(post(endpoint, "SINV-FOUR").path("code").asText()).isEqualTo("200");
                awaitDelta("SINV-FOUR");
                seed("SINV-SIX");
                assertThat(post(endpoint, "SINV-SIX").path("code").asText()).isEqualTo("202");
                awaitDelta("SINV-SIX");
                assertThat(outbox.findInvalidOrder(ORG, "out-SINV-SIX").orElseThrow().status())
                        .isEqualTo(RechargeOrderInvalidationState.Status.COMPLETED);
                seed("SINV-ROLLBACK");
                assertThat(post(endpoint, "SINV-ROLLBACK").path("code").asText()).isEqualTo("500");
                assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox where order_id=?",
                        Integer.class, "SINV-ROLLBACK")).isZero();
                assertThat(abandons.get()).isEqualTo(6);

                // 认证超时后放开阻塞，不能再迟到受理。
                seed("SINV-AUTH");
                var authRelease = new CountDownLatch(1);
                authBlock.set(authRelease);
                try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                    var timedOut = pool.submit(() -> post(endpoint, "SINV-AUTH"));
                    assertThat(authEntered.await(2, TimeUnit.SECONDS)).isTrue();
                    var authResult = timedOut.get(6, TimeUnit.SECONDS);
                    assertThat(authResult.path("code").asText()).isEqualTo("500");
                    assertThat(authResult.path("data").isNull()).isTrue();
                } finally { authRelease.countDown(); authBlock.set(null); }
                assertThat(authFinished.await(2, TimeUnit.SECONDS)).isTrue();
                assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox where order_id=?",
                        Integer.class, "SINV-AUTH")).isZero();

                // 已持久登记attempt并发出HTTP后，真实TCP断开不取消结果落库。
                seed("SINV-DISCONNECT");
                var supplierRelease = new CountDownLatch(1);
                supplierBlock.set(supplierRelease);
                var closed = new CountDownLatch(1);
                connectionClosed.set(closed);
                try (var socket = new Socket("127.0.0.1", server.port())) {
                    byte[] payload = signedBody("SINV-DISCONNECT").getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    String headers = "POST /v1/cardapi/order/invalid HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                            + "Authorization: fixture\r\nContent-Type: application/json\r\nContent-Length: " + payload.length + "\r\n\r\n";
                    socket.getOutputStream().write(headers.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                    socket.getOutputStream().write(payload); socket.getOutputStream().flush();
                    assertThat(supplierEntered.await(2, TimeUnit.SECONDS)).isTrue();
                } finally {
                    assertThat(closed.await(2, TimeUnit.SECONDS)).isTrue();
                    supplierRelease.countDown(); supplierBlock.set(null); connectionClosed.set(null);
                }
                awaitDelta("SINV-DISCONNECT");
                assertThat(outbox.findInvalidOrder(ORG, "out-SINV-DISCONNECT").orElseThrow().status())
                        .isEqualTo(RechargeOrderInvalidationState.Status.COMPLETED);
                assertThat(abandons.get()).isEqualTo(7);
                assertThat(queries.get()).isEqualTo(1);
                System.out.println("SINV additional HTTP/MySQL: no-release=200; concurrent=one abandon/delta; commit4s=200 commit6s=202 then completed; rollback=no event; slow-auth=500 no late event; TCP-close=completed; abandon=7 query=1; isolation=" + isolation);

                var writeOff = new MybatisMinisoWriteOffStore(events, receipts, guards, mock(RechargeNotifyTaskPort.class),
                        new V12RedemptionNotifyPayloadSigner(mock(V12SigningCredentialStorePort.class)),
                        tm, clock, 10, Duration.ofSeconds(1), 10);
                seed("SINV-REDEEM-FIRST");
                writeOff.accept(new MinisoWriteOffNotification("COUPON-SINV-REDEEM-FIRST", LocalDateTime.now(clock).withNano(0)));
                assertThat(post(endpoint, "SINV-REDEEM-FIRST").path("code").asText()).isEqualTo("503");
                assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox where order_id=?",
                        Integer.class, "SINV-REDEEM-FIRST")).isZero();
                assertThat(abandons.get()).isEqualTo(7);

                seed("SINV-CONFLICT");
                var conflictRelease = new CountDownLatch(1);
                supplierBlock.set(conflictRelease);
                try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                    var conflicting = pool.submit(() -> post(endpoint, "SINV-CONFLICT"));
                    for (int i = 0; i < 100 && abandons.get() < 8; i++) Thread.sleep(20);
                    assertThat(abandons.get()).isEqualTo(8);
                    // HTTP已在锁外，另一连接核销先提交；远端成功回包后必须重新阻断授权。
                    writeOff.accept(new MinisoWriteOffNotification("COUPON-SINV-CONFLICT", LocalDateTime.now(clock).withNano(0)));
                    conflictRelease.countDown();
                    assertThat(conflicting.get(6, TimeUnit.SECONDS).path("code").asText()).isEqualTo("202");
                } finally { conflictRelease.countDown(); supplierBlock.set(null); }
                assertThat(jdbc.queryForObject("select balance_release_event_id from recharge_order_ext where order_id=?",
                        String.class, "SINV-CONFLICT")).isNull();
                assertThat(jdbc.queryForObject("select count(*) from recharge_balance_delta_outbox where order_id=?",
                        Integer.class, "SINV-CONFLICT")).isZero();
                assertThat(jdbc.queryForObject("select status from recharge_dispatch_outbox where order_id=?",
                        String.class, "SINV-CONFLICT")).isEqualTo("MANUAL_REQUIRED");

                seed("SINV-TERMINAL");
                jdbc.execute("create trigger sinv_terminal_rollback before update on recharge_order_ext for each row "
                        + "begin if NEW.order_id='SINV-TERMINAL' and NEW.dispatch_status='CANCELLED' then "
                        + "signal sqlstate '45000' set message_text='fixture terminal rollback'; end if; end");
                assertThat(post(endpoint, "SINV-TERMINAL").path("code").asText()).isEqualTo("202");
                assertThat(jdbc.queryForObject("select balance_release_event_id from recharge_order_ext where order_id=?",
                        String.class, "SINV-TERMINAL")).isNull();
                jdbc.execute("drop trigger sinv_terminal_rollback");
                assertThat(post(endpoint, "SINV-TERMINAL").path("code").asText()).isEqualTo("200");
                awaitDelta("SINV-TERMINAL");
                assertThat(abandons.get()).isEqualTo(9);
                assertThat(queries.get()).isEqualTo(2);
                System.out.println("SINV conflict/rollback: redeemed-first=503 no event; redeemed-in-flight=202 manual no authorization/delta; terminal rollback=202 then query-only=200; abandon=9 query=2; isolation=" + isolation);

                // 受理前真实TCP断连：等服务器确认channel关闭后再放开认证，不以客户端close完成替代。
                seed("SINV-PRE-CLOSE");
                var preAuthEntered = new CountDownLatch(1);
                var preAuthFinished = new CountDownLatch(1);
                var preRelease = new CountDownLatch(1);
                authEnteredRef.set(preAuthEntered); authFinishedRef.set(preAuthFinished);
                authBlock.set(preRelease);
                var preClosed = new CountDownLatch(1);
                connectionClosed.set(preClosed);
                try (var socket = new Socket("127.0.0.1", server.port())) {
                    sendSocket(socket, "SINV-PRE-CLOSE");
                    assertThat(preAuthEntered.await(2, TimeUnit.SECONDS)).isTrue();
                } finally {
                    assertThat(preClosed.await(2, TimeUnit.SECONDS)).isTrue();
                    preRelease.countDown(); authBlock.set(null); connectionClosed.set(null);
                }
                assertThat(preAuthFinished.await(2, TimeUnit.SECONDS)).isTrue();
                // 用下一次已完成请求作控制，并持续观察晚入队；认证已完成不是下游执行已完成。
                assertThat(post(endpoint, "SINV-EARLY").path("code").asText()).isEqualTo("200");
                for (int i = 0; i < 25; i++) {
                    assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox where order_id=?",
                            Integer.class, "SINV-PRE-CLOSE")).isZero();
                    Thread.sleep(20);
                }
                assertThat(abandons.get()).isEqualTo(9);
                System.out.println("SINV P0 pre-admission TCP close: auth resumed; event=0 abandon=0");

                seed("SINV-AUDIT");
                jdbc.execute("create trigger sinv_audit_failure before update on mc_order_supplier_flow for each row "
                        + "begin if NEW.order_id='SINV-AUDIT' then signal sqlstate '45000' set message_text='fixture audit update failure'; end if; end");
                assertThat(post(endpoint, "SINV-AUDIT").path("code").asText()).isEqualTo("202");
                assertThat(jdbc.queryForObject("select balance_release_event_id from recharge_order_ext where order_id=?",
                        String.class, "SINV-AUDIT")).isNull();
                assertThat(orders.hasSupplierInvalidationAttempt("SINV-AUDIT", "invalid-SINV-AUDIT")).isTrue();
                assertThat(jdbc.queryForObject("select status from mc_order_supplier_flow where order_id=?",
                        String.class, "SINV-AUDIT")).isEqualTo("new");
                assertThat(jdbc.queryForObject("select response_result from mc_order_supplier_flow where order_id=?",
                        String.class, "SINV-AUDIT")).isEmpty();
                assertThat(jdbc.queryForObject("select count(*) from recharge_balance_delta_outbox where order_id=?",
                        Integer.class, "SINV-AUDIT")).isZero();
                assertThat(abandons.get()).isEqualTo(10); assertThat(queries.get()).isEqualTo(2);
                jdbc.execute("drop trigger sinv_audit_failure");
                assertThat(post(endpoint, "SINV-AUDIT").path("code").asText()).isEqualTo("200");
                awaitDelta("SINV-AUDIT");
                assertThat(abandons.get()).isEqualTo(10); assertThat(queries.get()).isEqualTo(3);
                System.out.println("SINV P0 SQL audit failure: 202 no authorization; trigger removed; query-only=200 abandon=1 query=1 delta=1");

                seed("SINV-REJECT"); rejectExecution.set(true);
                try {
                    assertThat(post(endpoint, "SINV-REJECT").path("code").asText()).isEqualTo("202");
                    assertThat(jdbc.queryForObject("select status from recharge_dispatch_outbox where order_id=?",
                            String.class, "SINV-REJECT")).isEqualTo("INIT");
                    assertThat(orders.hasSupplierInvalidationAttempt("SINV-REJECT", "invalid-SINV-REJECT")).isFalse();
                    assertThat(abandons.get()).isEqualTo(10);
                } finally { rejectExecution.set(false); }
                assertThat(post(endpoint, "SINV-REJECT").path("code").asText()).isEqualTo("200");
                awaitDelta("SINV-REJECT");
                assertThat(abandons.get()).isEqualTo(11);
                System.out.println("SINV P0 executor rejection: 202 event=INIT attempt=0; retry=200 abandon=1 delta=1");

                seed("SINV-CRASH");
                var crashGate = new CountDownLatch(1);
                supplierBlock.set(crashGate);
                var crashLog = Files.createTempFile("sinv-crash-", ".log");
                var resumeLog = Files.createTempFile("sinv-resume-", ".log");
                Process crashed = null;
                Process resumed = null;
                try {
                    crashed = childProcess(supplier.getAddress().getPort(), crashLog);
                    for (int i = 0; i < 1500 && abandons.get() < 12 && crashed.isAlive(); i++) Thread.sleep(20);
                    assertThat(abandons.get()).as("child reached real supplier HTTP").isEqualTo(12);
                    assertThat(orders.hasSupplierInvalidationAttempt("SINV-CRASH", "invalid-SINV-CRASH")).isTrue();
                    assertThat(jdbc.queryForObject("select status from recharge_dispatch_outbox where order_id=?",
                            String.class, "SINV-CRASH")).isEqualTo("PROCESSING");
                    assertThat(jdbc.queryForObject("select balance_release_event_id from recharge_order_ext where order_id=?",
                            String.class, "SINV-CRASH")).isNull();
                    assertThat(jdbc.queryForObject("select count(*) from recharge_balance_delta_outbox where order_id=?",
                            Integer.class, "SINV-CRASH")).isZero();
                    crashed.destroyForcibly();
                    assertThat(crashed.waitFor(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(crashed.exitValue()).isEqualTo(137);
                    crashGate.countDown(); supplierBlock.set(null);
                    // 子JVM夹具租约为1秒，等待真实租约自然过期，不改SQL、不重置事件/attempt。
                    for (int i = 0; i < 200 && outbox.findDueByEventId("invalid-SINV-CRASH",
                            com.cardshop.trade.engine.domain.order.OutboxEventType.INVALID_ORDER).isEmpty(); i++) Thread.sleep(20);
                    assertThat(outbox.findDueByEventId("invalid-SINV-CRASH",
                            com.cardshop.trade.engine.domain.order.OutboxEventType.INVALID_ORDER)).isPresent();
                    resumed = childProcess(supplier.getAddress().getPort(), resumeLog);
                    assertThat(resumed.waitFor(30, TimeUnit.SECONDS)).isTrue();
                    assertThat(resumed.exitValue()).withFailMessage(() -> readLog(resumeLog)).isZero();
                    assertThat(abandons.get()).isEqualTo(12); assertThat(queries.get()).isEqualTo(4);
                    awaitDelta("SINV-CRASH");
                    var crashTime = outbox.findInvalidOrder(ORG, "out-SINV-CRASH").orElseThrow();
                    assertThat(crashTime.status()).isEqualTo(RechargeOrderInvalidationState.Status.COMPLETED);
                    var repeatedCrash = post(endpoint, "SINV-CRASH");
                    assertThat(repeatedCrash.path("code").asText()).isEqualTo("200");
                    assertThat(outbox.findInvalidOrder(ORG, "out-SINV-CRASH").orElseThrow().order().invalidTime())
                            .isEqualTo(crashTime.order().invalidTime());
                    assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox where order_id=?",
                            Integer.class, "SINV-CRASH")).isEqualTo(1);
                    assertThat(abandons.get()).isEqualTo(12); assertThat(queries.get()).isEqualTo(4);
                    System.out.println("SINV P0 SIGKILL/restart: killedExit=" + crashed.exitValue()
                            + " newJvmExit=0 abandon=1 query=1 event=1 delta=1 stableTime=true isolation=" + isolation);
                } finally {
                    crashGate.countDown(); supplierBlock.set(null);
                    for (var process : Arrays.asList(crashed, resumed)) {
                        if (process != null && process.isAlive()) { process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS); }
                    }
                    System.out.println("SINV child crash output:\n" + readLog(crashLog));
                    System.out.println("SINV child resume output:\n" + readLog(resumeLog));
                    Files.deleteIfExists(crashLog); Files.deleteIfExists(resumeLog);
                }

                // 真实Controller调度拒绝发生在受理之前；与提交后推进边界的故障注入分开留证。
                seed("SINV-SCHEDULER");
                var rejectedSchedules = new AtomicInteger();
                var executor = new ScheduledThreadPoolExecutor(1, (task, pool) -> {
                    rejectedSchedules.incrementAndGet();
                    new ThreadPoolExecutor.AbortPolicy().rejectedExecution(task, pool);
                });
                executor.shutdown();
                var rejectedScheduler = reactor.core.scheduler.Schedulers.fromExecutorService(executor);
                var schedulers = reactor.core.scheduler.Schedulers.setFactoryWithSnapshot(
                        new reactor.core.scheduler.Schedulers.Factory() {
                            @Override public reactor.core.scheduler.Scheduler newBoundedElastic(int threads,
                                    int queue, ThreadFactory factory, int ttlSeconds) {
                                return rejectedScheduler;
                            }
                        });
                try {
                    var rejected = post(endpoint, "SINV-SCHEDULER");
                    assertThat(rejected.path("code").asText()).isEqualTo("500");
                    assertThat(rejected.path("data").isNull()).isTrue();
                    assertThat(rejectedSchedules.get()).isPositive();
                    assertThat(jdbc.queryForObject("select count(*) from recharge_dispatch_outbox where order_id=?",
                            Integer.class, "SINV-SCHEDULER")).isZero();
                    assertThat(abandons.get()).isEqualTo(12);
                } finally {
                    reactor.core.scheduler.Schedulers.resetFrom(schedulers);
                    rejectedScheduler.dispose();
                }
                assertThat(post(endpoint, "SINV-SCHEDULER").path("code").asText()).isEqualTo("200");
                awaitDelta("SINV-SCHEDULER");
                assertThat(abandons.get()).isEqualTo(13); assertThat(queries.get()).isEqualTo(4);
                System.out.println("SINV P0 real scheduler rejection: rejected=" + rejectedSchedules.get()
                        + " code=500 event=0; restored=200 abandon=1 delta=1 isolation=" + isolation);
            } finally { fundsRelease.countDown(); server.disposeNow(); }
        } finally { supplier.stop(0); }
    }

    private final AtomicReference<CountDownLatch> authEnteredRef = new AtomicReference<>(authEntered);
    private final AtomicReference<CountDownLatch> authFinishedRef = new AtomicReference<>(authFinished);

    private Process childProcess(int supplierPort, Path log) throws Exception {
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dsinv.child=true", "-Dsinv.supplier=http://127.0.0.1:" + supplierPort,
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

    private String signedBody(String id) throws Exception {
        var payload = new LinkedHashMap<String, Object>();
        payload.put("out_order_sn", "out-" + id); payload.put("timestamp", String.valueOf(clock.millis()));
        payload.put("nonce_str", UUID.randomUUID().toString().replace("-", ""));
        payload.put("sign", V12MessageSignatures.sign(V12MessageSignatures.Operation.INVALID, payload, credential.signKey()));
        return json.writeValueAsString(payload);
    }

    private JsonNode post(URI endpoint, String id) throws Exception {
        String body = signedBody(id);
        long start = System.nanoTime();
        var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(10))
                .header("Authorization", "fixture").header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        var result = json.readTree(response.body());
        long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();
        System.out.println("SINV HTTP order=" + id + " code=" + result.path("code").asText() + " elapsedMs=" + elapsedMs);
        if ("202".equals(result.path("code").asText()) || "SINV-AUTH".equals(id)) {
            assertThat(elapsedMs).isBetween(4500L, 6000L);
        } else assertThat(elapsedMs).isLessThan(5000L);
        return result;
    }

    private static void awaitGate(CountDownLatch gate) {
        try {
            if (!gate.await(8, TimeUnit.SECONDS)) throw new IllegalStateException("fixture gate timeout");
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt(); throw new IllegalStateException("fixture interrupted", ex);
        }
    }

    private void seed(String id) {
        jdbc.update("insert into mc_order(order_Id,org_code,out_orderId,product_type,num,supplier_channel,account,order_status,order_price,callback_url,data_status,parent_id,produt_code) values (?,?,?,1,1,'MINISO','10000000000',12,137,'','PROCESSED','parent','sku')", id, ORG, "out-" + id);
        jdbc.update("insert into recharge_order_ext(order_id,org_code,out_order_sn,request_hash,request_hash_version,request_hash_alg,persist_status,persist_source,validation_status,balance_reserved,dispatch_allowed,dispatch_status,supplier_batch_id,voucher_use_state) values (?,?,?,'fixture','V1','SHA-256','DB_PERSISTED','FIXTURE','PASSED',1,1,'SUCCESS','100023','1')", id, ORG, "out-" + id);
        jdbc.update("insert into mc_depart_record(order_id,org_code,type,`value`) values (?,?,'1',137)", id, ORG);
        receipts.insert(new MinisoFulfillmentReceipt(id, ORG, 100023, "COUPON-" + id, "10000000000",
                "2026-10-01 00:00:00", "2026-12-01 00:00:00", 0));
    }

    private void awaitDelta(String id) throws Exception {
        for (int i = 0; i < 200; i++) {
            if (jdbc.queryForObject("select count(*) from recharge_balance_delta_outbox where order_id=?", Integer.class, id) == 1) return;
            Thread.sleep(20);
        }
        throw new AssertionError("release delta not persisted");
    }
    private static String required(String name) { return Objects.requireNonNull(System.getenv(name), name); }
    @Configuration(proxyBeanMethods = false) @EnableWebFlux static class WebConfig {}
}

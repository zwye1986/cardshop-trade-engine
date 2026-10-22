package com.cardshop.trade.engine.infrastructure.persistence;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.cardshop.trade.engine.application.recharge.*;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeDispatchOutboxMapper;
import com.cardshop.trade.engine.infrastructure.redis.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * Task17组合断点：生产Worker、主库事务代理和真实release Lua共同恢复。
 * 参见 docs/交行直充业务系统详细设计.md「9.8」；进程不启动任何外部应用服务。
 */
final class MinisoMysqlReleaseRecovery implements AutoCloseable {
    Process server;
    final java.nio.file.Path scratch;
    LettuceConnectionFactory factory;
    ReactiveStringRedisTemplate redis;
    final RechargeRedisKeyBuilder keys = new RechargeRedisKeyBuilder("task17:" + System.nanoTime(), 128);
    RedisBalanceReservationReleaseAdapter release;

    MinisoMysqlReleaseRecovery() throws Exception { this("redis-server"); }

    /** 测试可选故意立即退出的可执行文件，证明构造失败清理；生产release adapter不提供此钩子。 */
    MinisoMysqlReleaseRecovery(String executable) throws Exception {
        scratch = Files.createTempDirectory("miniso-task17-redis-");
        try {
            int port;
            try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
            server = new ProcessBuilder(executable, "--bind", "127.0.0.1", "--port", Integer.toString(port),
                    "--dir", scratch.toString(), "--save", "", "--appendonly", "no")
                    .redirectErrorStream(true).redirectOutput(scratch.resolve("redis.log").toFile()).start();
            boolean ready = false;
            for (int i=0;i<100;i++) {
                try (var socket = new java.net.Socket("127.0.0.1",port)) { ready=true; break; }
                catch (java.io.IOException ex) { java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20)); }
            }
            assertThat(ready).isTrue();
            factory = new LettuceConnectionFactory("127.0.0.1",port); factory.afterPropertiesSet(); factory.start();
            redis = new ReactiveStringRedisTemplate(factory);
            release = new RedisBalanceReservationReleaseAdapter(new LettuceRechargeRedisSessionFactory(factory), keys,
                    new RechargeRedisScriptLoader(), MinisoMysqlArbitrationFixture.CLOCK, Duration.ofSeconds(3));
        } catch (Exception | AssertionError failure) {
            // 构造失败时try-with-resources尚未接管实例，也必须回收已启动进程及临时文件，参见详细设计9.8。
            try { close(); } catch (Exception cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            throw failure;
        }
    }

    static void assertFailedStartupCleansScratch() throws Exception {
        java.nio.file.Path directory = java.nio.file.Path.of(System.getProperty("java.io.tmpdir"));
        java.util.Set<java.nio.file.Path> before;
        try (var entries = Files.list(directory)) {
            before = entries.filter(p -> p.getFileName().toString().startsWith("miniso-task17-redis-"))
                    .collect(java.util.stream.Collectors.toSet());
        }
        assertThatThrownBy(() -> new MinisoMysqlReleaseRecovery("/usr/bin/false")).isInstanceOf(AssertionError.class);
        try (var entries = Files.list(directory)) {
            assertThat(entries.filter(p -> p.getFileName().toString().startsWith("miniso-task17-redis-")).toList())
                    .containsExactlyInAnyOrderElementsOf(before);
        }
        System.out.println("Task17 Redis failed startup: temporary process exited and scratch removed");
    }

    void seed(String id) {
        // 同一商户每轮重置本夹具余额并使用独立订单key；不调用FLUSHDB，也不连任何现有Redis。
        redis.opsForHash().putAll(keys.balanceKey("TASK17"), Map.of("available","1000","frozen","137")).block();
        redis.opsForHash().putAll(keys.orderKey("TASK17","out-" + id),
                Map.of("order_id",id,"org_code","TASK17","out_order_sn","out-"+id,"balance_reserved","1","order_status","SUCCESS")).block();
        var command = command(id);
        redis.opsForHash().putAll(keys.releaseBalanceReservationKeys(command).get(2),
                Map.of("order_id",id,"org_code","TASK17","out_order_sn","out-"+id,"amount","137","status","DB_PERSISTED","funds_status","RESERVED")).block();
    }

    BalanceReservationReleaseCommand command(String id) {
        return new BalanceReservationReleaseCommand("TASK17",id,"out-"+id,137,"RELEASE","source-"+id,"fixture");
    }

    void balance(long available, long frozen) {
        assertThat(redis.opsForHash().get(keys.balanceKey("TASK17"),"available").block()).isEqualTo(""+available);
        assertThat(redis.opsForHash().get(keys.balanceKey("TASK17"),"frozen").block()).isEqualTo(""+frozen);
    }

    void recoverEveryWindow(MinisoMysqlArbitrationFixture f) {
        for (String window : List.of("before-authorize-commit", "after-authorize", "lost-response",
                "before-delta", "after-delta", "after-flag", "before-success")) {
            String id = "task17-crash-" + window, coupon = "Task17-crash-" + window;
            f.seed(id); seed(id);
            f.tx.executeWithoutResult(s -> f.receipts.save(f.receipt(id,coupon,0)));
            if (window.equals("after-authorize")) {
                // 合并dev独立资金协议：真实成功消费后删除易失订单快照，后续许可仍可退款。
                assertThat(release.confirm(f.orders.findByOrderId(id).orElseThrow())).isTrue();
                assertThat(redis.opsForHash().get(keys.reservationKey("TASK17", id), "funds_status").block())
                        .isEqualTo("CONSUMED");
                redis.delete(keys.orderKey("TASK17", "out-" + id)).block();
                balance(1000, 0);
            }
            if (window.equals("before-authorize-commit")) {
                f.fault.set(window);
                assertThatThrownBy(() -> f.orders.authorizeMinisoTerminalRelease(f.terminal(id,coupon),"source-"+id))
                        .isInstanceOf(RuntimeException.class);
                assertThat(f.fault.get()).as("必须击中授权SQL后的真实回滚断点").isEmpty();
                assertThat(f.orders.findMinisoReleaseAuthorization(id)).isEmpty();
                assertThat(f.jdbc.queryForObject("select dispatch_status from recharge_order_ext where order_id=?",String.class,id))
                        .isEqualTo("SUCCESS");
                f.writeOff.accept(new MinisoWriteOffNotification(coupon,java.time.LocalDateTime.of(2026,10,22,9,0)));
                assertThat(f.writeOff.processBatch(1,0,100,"task17").processed()).isEqualTo(1);
                assertThatThrownBy(() -> f.orders.authorizeMinisoTerminalRelease(f.terminal(id,coupon),"source-"+id))
                        .isInstanceOf(MinisoWriteOffException.class);
                balance(1000,137);
                assertThat(f.jdbc.queryForObject("select count(*) from recharge_balance_delta_outbox where order_id=?",Integer.class,id)).isZero();
                // 原授权未提交，后来核销胜出；生产Worker必须在供应商/Redis之前被保护阻断。
                assertThat(worker(f,release,false).processDue(1,0,1,"task17-no-auth",List.of(OutboxEventType.INVALID_ORDER),1)
                        .succeeded()).isZero();
                balance(1000,137);
                assertThat(f.orders.findMinisoReleaseAuthorization(id)).isEmpty();
                assertThat(f.jdbc.queryForObject("select count(*) from recharge_balance_delta_outbox where order_id=?",Integer.class,id)).isZero();
                assertThat(f.jdbc.queryForObject("select status from recharge_dispatch_outbox where order_id=?",String.class,id)).isEqualTo("MANUAL_REQUIRED");
                System.out.println("Task17 window="+window+" auth=absent late=PROCESSED available=1000 frozen=137 delta=0");
                continue;
            }
            var authorization = f.orders.authorizeMinisoTerminalRelease(f.terminal(id,coupon),"source-"+id).orElseThrow();
            assertThat(authorization).isEqualTo(new MinisoBalanceReleaseAuthorization(id,"TASK17","out-"+id,
                    "source-"+id,137,f.CLOCK.instant()));
            balance(1000, window.equals("after-authorize") ? 0 : 137);
            if (!window.equals("after-authorize")) {
                BalanceReservationReleasePort firstRelease = release;
                if (window.equals("lost-response")) firstRelease = command -> {
                    assertThat(release.release(command).succeeded()).isTrue();
                    return BalanceReservationReleaseResult.failed("FIXTURE_RESPONSE_LOST","fixture lost successful Lua reply");
                };
                if (window.equals("before-delta") || window.equals("after-delta") || window.equals("after-flag")) f.fault.set(window);
                assertThat(worker(f, firstRelease, window.equals("before-success")).processDue(1,0,1,"task17-crash",
                        List.of(OutboxEventType.INVALID_ORDER),1).succeeded()).isZero();
                balance(1137,0);
                assertThat(f.fault.get()).as("断点必须执行").isEmpty();
                boolean complete = window.equals("before-success");
                assertThat(f.jdbc.queryForObject("select balance_reserved from recharge_order_ext where order_id=?",Boolean.class,id))
                        .isEqualTo(!complete);
                assertThat(f.jdbc.queryForObject("select count(*) from recharge_balance_delta_outbox where order_id=?",Integer.class,id))
                        .isEqualTo(complete ? 1 : 0);
            }
            // 每个已提交许可窗口恢复前插入迟到核销，不能取消冻结授权或截断原任务。
            f.late(id,coupon);
            assertThat(f.orders.findMinisoReleaseAuthorization(id)).contains(authorization);
            if (window.equals("after-authorize")) {
                // 独立事件B恢复由A授予的许可：错误地取当前Worker事件会写入不同Lua幂等key并被断言捕获。
                f.jdbc.update("update recharge_dispatch_outbox set next_retry_time='2099-01-01' where order_id=?",id);
                f.jdbc.update("""
                        insert into recharge_dispatch_outbox(event_id,event_type,org_code,order_id,out_order_sn,payload,status,
                          retry_count,max_retry_count,next_retry_time,version,shard_key)
                        values (?,'SUPPLIER_QUERY','TASK17',?,?,'{}','INIT',0,10,'2026-10-21',0,?)
                        """, "retry-"+id,id,"out-"+id,"retry-"+id);
                assertThat(worker(f,release,false).processDue(1,0,1,"task17-event-B",List.of(OutboxEventType.SUPPLIER_QUERY),1)
                        .succeeded()).isEqualTo(1);
                balance(1137,0);
                assertThat(redis.opsForHash().get(keys.reservationKey("TASK17",id),"release_event_id").block()).isEqualTo("source-"+id);
                assertThat(f.jdbc.queryForObject("select status from recharge_dispatch_outbox where event_id=?",String.class,"retry-"+id)).isEqualTo("SUCCESS");
            }
            f.jdbc.update("update recharge_dispatch_outbox set next_retry_time='2026-10-21',locked_until=null where order_id=?",id);
            assertThat(worker(f,release,false).processDue(1,0,1,"task17-recover", List.of(OutboxEventType.INVALID_ORDER),1)
                    .succeeded()).isEqualTo(1);
            balance(1137,0);
            assertThat(f.orders.findMinisoReleaseAuthorization(id)).contains(authorization);
            assertThat(f.jdbc.queryForObject("select balance_reserved from recharge_order_ext where order_id=?",Boolean.class,id)).isFalse();
            assertThat(f.jdbc.queryForList("select event_id,org_code,order_id,out_order_sn,delta_type,delta_amount from recharge_balance_delta_outbox where order_id=?",id))
                    .containsExactly(Map.of("event_id","balance-flush-release-"+id,"org_code","TASK17","order_id",id,
                            "out_order_sn","out-"+id,"delta_type","RELEASE","delta_amount",137L));
            assertThat(f.jdbc.queryForObject("select status from recharge_dispatch_outbox where event_id=?",String.class,"source-"+id)).isEqualTo("SUCCESS");
            assertThat(redis.opsForHash().get(keys.reservationKey("TASK17",id),"release_event_id").block()).isEqualTo(authorization.sourceEventId());
            assertThat(release.release(command(id)).succeeded()).isTrue(); balance(1137,0);
            System.out.println("Task17 window="+window+" source="+authorization.sourceEventId()+" late=MANUAL_REQUIRED available=1137 frozen=0 delta=137/RELEASE count=1 reserved=0 original=SUCCESS");
        }
    }

    /**
     * 第一JVM提交裁决并击中通知/原任务失败窗口；第二JVM只从MySQL读取事实恢复。
     * 使用真实通知Mapper，签名仅用离线桩，绝不发送HTTP；参见详细设计9.8。
     */
    void prepareNoReleaseWindows(MinisoMysqlArbitrationFixture f) {
        for (String terminal : List.of("CANCELLED", "FAILED")) {
            for (String window : List.of("notify", "success")) {
                String id = "no-release-" + terminal + "-" + window;
                seedNoRelease(f, id);
                var update = noReleaseTerminal(f, id, terminal);
                assertThat(f.orders.authorizeMinisoTerminalRelease(update, "source-" + id)).isEmpty();
                assertThat(f.orders.isMinisoTerminalNoReleaseRequired(id)).isTrue();
                f.jdbc.update("update recharge_dispatch_outbox set event_type=? where order_id=?",
                        terminal.equals("CANCELLED") ? "INVALID_ORDER" : "SUPPLIER_DISPATCH", id);
                RechargeNotifyTaskPort notifications = window.equals("notify")
                        ? task -> { throw new IllegalStateException("fixture notify store unavailable"); }
                        : notifications(f);
                assertThat(worker(f, forbiddenRelease(), window.equals("success"), notifications, signer())
                        .processDue(1, 0, 1, "no-release-first", List.of(sourceType(terminal)), 1).failed()).isEqualTo(1);
                assertThat(f.jdbc.queryForObject("select count(*) from recharge_notify_task where order_id=?", Integer.class, id))
                        .isEqualTo(window.equals("notify") ? 0 : 1);
                assertThat(f.jdbc.queryForObject("select status from recharge_dispatch_outbox where order_id=?", String.class, id))
                        .isEqualTo("FAILED");
                // 裁决提交后迟到核销只留人工证据，后续JVM不能因此阻断通知或再做资金裁决。
                f.late(id, id);
                System.out.println("NO_RELEASE prepared id=" + id + " pid=" + ProcessHandle.current().pid());
            }
        }
        String rollback = "no-release-rollback";
        seedNoRelease(f, rollback);
        f.fault.set("before-no-release-commit");
        assertThatThrownBy(() -> f.orders.authorizeMinisoTerminalRelease(
                noReleaseTerminal(f, rollback, "CANCELLED"), "source-" + rollback)).isInstanceOf(RuntimeException.class);
        assertThat(f.fault.get()).as("必须击中真实裁决SQL后、提交前的回滚窗口").isEmpty();
        assertThat(f.orders.isMinisoTerminalNoReleaseRequired(rollback)).isFalse();
        assertThat(f.jdbc.queryForMap("select dispatch_status,voucher_invalid_time from recharge_order_ext where order_id=?", rollback))
                .containsEntry("dispatch_status", "SUCCESS").containsEntry("voucher_invalid_time", null);
        f.writeOff.accept(new MinisoWriteOffNotification(rollback, java.time.LocalDateTime.of(2026, 10, 22, 9, 0)));
        assertThatThrownBy(() -> f.orders.authorizeMinisoTerminalRelease(
                noReleaseTerminal(f, rollback, "CANCELLED"), "source-" + rollback)).isInstanceOf(MinisoWriteOffException.class);
        f.jdbc.update("update recharge_dispatch_outbox set next_retry_time='2099-01-01' where order_id=?", rollback);
    }

    void resumeNoReleaseWindows(MinisoMysqlArbitrationFixture f) throws Exception {
        for (String terminal : List.of("CANCELLED", "FAILED")) {
            for (String window : List.of("notify", "success")) {
                String id = "no-release-" + terminal + "-" + window;
                var decision = f.jdbc.queryForMap("select * from recharge_order_ext where order_id=?", id);
                assertThat(decision).containsEntry("balance_no_release_event_id", "source-" + id);
                assertThat(decision.get("balance_no_release_decided_at")).isNotNull();
                var existing = f.jdbc.queryForList("select * from recharge_notify_task where order_id=?", id);
                // 事件B先恢复事件A的裁决，随后A收尾；两次恢复只能留下同一份冻结通知。
                f.jdbc.update("""
                        insert into recharge_dispatch_outbox(event_id,event_type,org_code,order_id,out_order_sn,payload,status,
                          retry_count,max_retry_count,next_retry_time,version,shard_key)
                        values (?,'SUPPLIER_QUERY','TASK17',?,?,'{}','INIT',0,10,'2026-10-21',0,?)
                        """, "retry-" + id, id, "out-" + id, "retry-" + id);
                assertThat(worker(f, forbiddenRelease(), false, notifications(f), signer())
                        .processDue(1, 0, 1, "no-release-B", List.of(OutboxEventType.SUPPLIER_QUERY), 1).succeeded()).isEqualTo(1);
                var frozen = f.jdbc.queryForList("select * from recharge_notify_task where order_id=?", id);
                if (!existing.isEmpty()) assertThat(frozen).isEqualTo(existing);
                assertThat(frozen).singleElement().satisfies(task -> {
                    assertThat(task.get("biz_status")).isEqualTo(terminal.equals("CANCELLED") ? "CANCELLED" : "6");
                    assertThat(task.get("notify_url")).isEqualTo("https://fixture.invalid/no-release");
                });
                var payload = new ObjectMapper().readTree((String) frozen.getFirst().get("payload"));
                assertThat(payload.path("orderId").asText()).isEqualTo(id);
                if (terminal.equals("CANCELLED")) assertThat(payload.path("invalidTime").asText()).isEqualTo(f.CLOCK.instant().toString());
                f.jdbc.update("update recharge_dispatch_outbox set next_retry_time='2026-10-21',locked_until=null where event_id=?", "source-" + id);
                assertThat(worker(f, forbiddenRelease(), false, notifications(f), signer())
                        .processDue(1, 0, 1, "no-release-A", List.of(sourceType(terminal)), 1).succeeded()).isEqualTo(1);
                assertThat(f.jdbc.queryForList("select * from recharge_notify_task where order_id=?", id)).isEqualTo(frozen);
                assertThat(f.jdbc.queryForMap("select * from recharge_order_ext where order_id=?", id)).isEqualTo(decision);
                assertThat(f.jdbc.queryForList("select status from recharge_dispatch_outbox where order_id=?", String.class, id))
                        .containsExactly("SUCCESS", "SUCCESS");
                assertThat(f.jdbc.queryForObject("select count(*) from recharge_balance_delta_outbox where order_id=?", Integer.class, id)).isZero();
                assertThat(f.jdbc.queryForObject("select count(*) from mc_depart_record where order_id=?", Integer.class, id)).isZero();
                assertThat(f.jdbc.queryForObject("select status from recharge_miniso_writeoff_event where coupon_code=?", String.class, id))
                        .isEqualTo("MANUAL_REQUIRED");
                System.out.println("NO_RELEASE recovered id=" + id + " pid=" + ProcessHandle.current().pid()
                        + " notify=1 delta=0 funds=forbidden supplier=forbidden A=SUCCESS B=SUCCESS late=MANUAL_REQUIRED");
            }
        }
    }

    private void seedNoRelease(MinisoMysqlArbitrationFixture f, String id) {
        f.seed(id);
        f.jdbc.update("update recharge_order_ext set balance_reserved=0 where order_id=?", id);
        f.jdbc.update("delete from mc_depart_record where order_id=?", id);
        f.jdbc.update("update mc_order set callback_url='https://fixture.invalid/no-release' where order_Id=?", id);
        f.tx.executeWithoutResult(s -> f.receipts.save(f.receipt(id, id, 0)));
    }

    private OrderFulfillmentStatusUpdate noReleaseTerminal(MinisoMysqlArbitrationFixture f, String id, String terminal) {
        return terminal.equals("CANCELLED") ? f.terminal(id, id)
                : new OrderFulfillmentStatusUpdate(id, FulfillmentDispatchStatus.FAILED, 6, "PROCESSED", null,
                        "", "", "fixture", f.receipt(id, id, 3), null);
    }

    private static OutboxEventType sourceType(String terminal) {
        return terminal.equals("CANCELLED") ? OutboxEventType.INVALID_ORDER : OutboxEventType.SUPPLIER_DISPATCH;
    }

    private static BalanceReservationReleasePort forbiddenRelease() {
        return command -> { throw new AssertionError("no-release recovery must never call funds"); };
    }

    private RechargeNotifyTaskPort notifications(MinisoMysqlArbitrationFixture f) {
        return new MybatisRechargeNotifyTaskAdapter(f.mapper(
                com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeNotifyTaskMapper.class), new ObjectMapper(), f.CLOCK);
    }

    private static MerchantNotifyPayloadSigner signer() {
        var signer = mock(MerchantNotifyPayloadSigner.class);
        when(signer.signOrderStatus(any(), anyString(), any())).thenAnswer(call -> Map.of(
                "orderId", ((RechargeFulfillmentOrder) call.getArgument(0)).orderId(), "status", call.getArgument(1),
                "nonce", java.util.UUID.randomUUID().toString()));
        when(signer.signWriteOff(any(), any())).thenAnswer(call -> Map.of(
                "orderId", ((RechargeFulfillmentOrder) call.getArgument(0)).orderId(), "invalidTime", call.getArgument(1).toString(),
                "nonce", java.util.UUID.randomUUID().toString()));
        return signer;
    }

    RechargeOutboxWorkerService worker(MinisoMysqlArbitrationFixture f, BalanceReservationReleasePort releaser, boolean failSuccess) {
        return worker(f, releaser, failSuccess, mock(RechargeNotifyTaskPort.class), mock(MerchantNotifyPayloadSigner.class));
    }

    private RechargeOutboxWorkerService worker(MinisoMysqlArbitrationFixture f, BalanceReservationReleasePort releaser,
            boolean failSuccess, RechargeNotifyTaskPort notifications, MerchantNotifyPayloadSigner signer) {
        var outbox = new MybatisRechargeDispatchOutboxAdapter(f.mapper(RechargeDispatchOutboxMapper.class),new ObjectMapper(),f.CLOCK) {
            @Override public void markSuccess(RechargeOutboxEvent event) {
                if (failSuccess) throw new IllegalStateException("Task17 crash before original SUCCESS");
                super.markSuccess(event);
            }
        };
        // 资金/订单/outbox为生产实现；网络及无关异步端口用禁止调用桩，确保恢复不重新请求供应商或刷账。
        var supplier = mock(RechargeSupplierGatewayPort.class, invocation -> { throw new AssertionError("recovery must not call supplier"); });
        return new RechargeOutboxWorkerService(outbox,f.orders,supplier,outbox,mock(DirectRechargeFrequencyPort.class),
                mock(BalanceDeltaFlushOutboxPort.class, invocation -> { throw new AssertionError("must use main transaction delta"); }),
                releaser, notifications, signer,
                RechargeDispatchTimerPort.noop(),f.CLOCK,new RechargeOutboxWorkerSettings(Duration.ofSeconds(60),
                Duration.ofSeconds(1),Duration.ofSeconds(1),Duration.ofSeconds(60),Duration.ofSeconds(1)), () -> f.protection);
    }

    @Override public void close() throws Exception {
        try {
            if (factory != null) factory.destroy();
        } finally {
            try {
                if (server != null) {
                    server.destroy();
                    if (!server.waitFor(3,TimeUnit.SECONDS)) {
                        server.destroyForcibly(); assertThat(server.waitFor(3,TimeUnit.SECONDS)).isTrue();
                    }
                }
            } finally {
                if (Files.exists(scratch)) {
                    try (var paths = Files.walk(scratch)) {
                        for (var path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
                    }
                }
            }
        }
    }
}

package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;
import com.cardshop.trade.engine.application.recharge.AcceptRechargeOrderCommand;
import com.cardshop.trade.engine.application.v1.V1ProductCacheSettings;
import com.cardshop.trade.engine.application.v1.V1ProductSelection;
import com.cardshop.trade.engine.domain.order.ValidationStatus;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/** 显式启用的本地Redis合同测试，无外部中间件或凭证；参见详细设计名创实施约束。 */
@EnabledIfSystemProperty(named="miniso.localRedisTests",matches="true")
class MinisoLocalRedisContractTest {
    @Test void batchSurvivesRealCacheLuaAcceptLuaAndRedisPersistenceSnapshot() throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        var log = Files.createTempFile("miniso-local-redis-", ".log");
        var process = new ProcessBuilder("redis-server", "--bind", "127.0.0.1", "--port", Integer.toString(port),
                "--save", "", "--appendonly", "no").redirectErrorStream(true).redirectOutput(log.toFile()).start();
        LettuceConnectionFactory factory = null;
        try {
            boolean ready = false;
            for (int i=0;i<100;i++) {
                try (var socket = new java.net.Socket("127.0.0.1",port)) { ready=true; break; }
                catch (java.io.IOException ex) { Thread.sleep(20); }
            }
            assertThat(ready).isTrue();
            factory = new LettuceConnectionFactory("127.0.0.1",port); factory.afterPropertiesSet(); factory.start();
            var redis = new ReactiveStringRedisTemplate(factory);
            var keys = new RechargeRedisKeyBuilder("miniso:local-test",128);
            var settings = new V1ProductCacheSettings(Duration.ofMinutes(25),Duration.ofMinutes(30),Duration.ofHours(1),
                    Duration.ofSeconds(5),Duration.ofSeconds(1),Duration.ofSeconds(10));
            var cache = new RedisV1ProductSelectionCacheAdapter(redis,keys,settings,Duration.ofSeconds(3));
            var selection = V1ProductSelection.passed("P","SKU","MINISO","测试直充","codekey",1,
                    100L,100L,50L,1L).withSupplierBatchId("100023");
            var token = cache.tryAcquireRefreshLock("A39","P").orElseThrow();
            var now = Instant.now();
            assertThat(cache.writeSnapshotIfLockOwned("A39","P",token,selection,now)).isTrue();
            assertThat(cache.find("A39","P").orElseThrow().selection()).isEqualTo(selection);
            var command = new AcceptRechargeOrderCommand("A39","app","OUT1","ORDER1","P","SKU","13800138000",
                    "","abcdefghijklmnopqrstuvwx12345678",1,100L,"测试直充",100L,1,"codekey","MINISO","hash","v1","SHA-256",
                    ValidationStatus.PASSED,"","",1L,1L,selection.cacheFingerprint(),true,now.toEpochMilli(),now.plusSeconds(30).toEpochMilli())
                    .withSupplierBatchId("100023");
            redis.opsForHash().putAll(keys.merchantConfigKey("A39"),Map.of("status","ENABLED","version","1")).block();
            redis.opsForHash().putAll(keys.balanceKey("A39"),Map.of("available","1000","frozen","0")).block();
            var arguments = AcceptRechargeOrderLuaArguments.from(command,keys,Duration.ofDays(7),Duration.ofMinutes(10));
            var script = new DefaultRedisScript<java.util.List>();
            script.setLocation(new org.springframework.core.io.ClassPathResource("redis/lua/accept_recharge_order.lua"));
            script.setResultType(java.util.List.class);
            var result = redis.execute(script,arguments.keys(),arguments.args()).next().block();
            assertThat(result).contains("ACCEPTED");
            assertThat(redis.opsForHash().get(keys.reservationKey("A39", "ORDER1"), "funds_status").block()).isEqualTo("RESERVED");
            assertThat(redis.opsForHash().get(keys.orderKey("A39","OUT1"),"supplier_batch_id").block()).isEqualTo("100023");
            var reader = new RedisPersistOrderSnapshotAdapter(redis,keys,Duration.ofSeconds(3));
            assertThat(reader.findLatestSnapshot("A39","OUT1","ORDER1").orElseThrow().supplierBatchId()).isEqualTo("100023");
        } finally {
            if (factory!=null) factory.destroy();
            process.destroy();
            if (!process.waitFor(3,java.util.concurrent.TimeUnit.SECONDS)) process.destroyForcibly();
            Files.deleteIfExists(log);
        }
    }
}

package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Redis Lua 脚本加载器契约测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.14 实施任务拆分」任务 5。
 * 脚本必须从 classpath 固定路径加载，后续适配器才能使用 Spring Data Redis 的 SHA 缓存机制执行脚本。
 */
class RechargeRedisScriptLoaderTest {

    @Test
    void loadsAcceptRechargeOrderScriptFromClasspath() {
        var loader = new RechargeRedisScriptLoader();

        var script = loader.acceptRechargeOrderScript();

        assertThat(script.getResultType()).isEqualTo(List.class);
        assertThat(script.getSha1()).isNotBlank();
        assertThat(script.getScriptAsString())
                .contains("accept_recharge_order.lua")
                .contains("ACCEPTED")
                .contains("DUPLICATE")
                .contains("BALANCE_NOT_ENOUGH");
    }

    @Test
    void loadsCancelUnconfirmedAcceptScriptFromClasspath() {
        var loader = new RechargeRedisScriptLoader();

        var script = loader.cancelUnconfirmedAcceptScript();

        assertThat(script.getResultType()).isEqualTo(List.class);
        assertThat(script.getSha1()).isNotBlank();
        assertThat(script.getScriptAsString())
                .contains("cancel_unconfirmed_accept.lua")
                .contains("CANCELLED")
                .contains("HINCRBY', KEYS[1], 'available'")
                .contains("ZREM', KEYS[5]");
    }

    @Test
    void loadsMarkPersistQueuedScriptFromClasspath() {
        var loader = new RechargeRedisScriptLoader();

        var script = loader.markPersistQueuedScript();

        assertThat(script.getResultType()).isEqualTo(List.class);
        assertThat(script.getSha1()).isNotBlank();
        assertThat(script.getScriptAsString())
                .contains("mark_persist_queued.lua")
                .contains("PERSIST_QUEUED")
                .contains("persist_msg_id")
                .doesNotContain("ZREM");
    }
}

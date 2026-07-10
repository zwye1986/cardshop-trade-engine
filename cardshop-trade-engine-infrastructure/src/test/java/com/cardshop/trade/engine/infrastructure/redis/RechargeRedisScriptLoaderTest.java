package com.cardshop.trade.engine.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Redis Lua 脚本加载器契约测试。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「13. 测试与验收标准」任务 5。
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

    @Test
    void loadsMarkDbPersistedScriptFromClasspath() {
        var loader = new RechargeRedisScriptLoader();

        var script = loader.markDbPersistedScript();

        assertThat(script.getResultType()).isEqualTo(List.class);
        assertThat(script.getSha1()).isNotBlank();
        assertThat(script.getScriptAsString())
                .contains("mark_db_persisted.lua")
                .contains("DB_PERSISTED")
                .contains("persisted_version")
                .contains("currentOrderVersion > nextPersistedVersion")
                .contains("ZREM', KEYS[3]")
                .contains("ZREM', KEYS[6]");
    }

    @Test
    void markDbPersistedKeepsPendingWhenRedisOrderHasNewerVersion() {
        var loader = new RechargeRedisScriptLoader();

        String script = loader.markDbPersistedScript().getScriptAsString();
        int newerVersionBranch = script.indexOf("if currentOrderVersion > nextPersistedVersion then");
        int keepPendingReturn = script.indexOf("return result('MARKED', '', '')", newerVersionBranch);
        int removePendingMember = script.indexOf("redis.call('ZREM', KEYS[3], orderId)", newerVersionBranch);

        assertThat(newerVersionBranch).isNotNegative();
        assertThat(keepPendingReturn).isGreaterThan(newerVersionBranch);
        assertThat(removePendingMember)
                .describedAs("Redis 当前版本高于本次落库版本时必须保留 pending，等待补刷新版本")
                .isGreaterThan(keepPendingReturn);
    }

    @Test
    void markDbPersistedRefreshesOrgIndexBeforeReturningWhenOrderSnapshotIsMissing() {
        var loader = new RechargeRedisScriptLoader();

        String script = loader.markDbPersistedScript().getScriptAsString();
        int missingOrderBranch = script.indexOf("if redis.call('EXISTS', KEYS[1]) == 0 then");
        int removePendingMember = script.indexOf("redis.call('ZREM', KEYS[3], orderId)", missingOrderBranch);
        int refreshOrgIndex = script.indexOf("refreshPendingOrgIndex()", missingOrderBranch);
        int returnOrderNotFound = script.indexOf("return result('MARK_FAILED', 'ORDER_NOT_FOUND'", missingOrderBranch);

        assertThat(missingOrderBranch).isNotNegative();
        assertThat(removePendingMember).isBetween(missingOrderBranch, returnOrderNotFound);
        assertThat(refreshOrgIndex).isBetween(removePendingMember, returnOrderNotFound);
    }

    @Test
    void loadsMarkPersistFailedScriptFromClasspath() {
        var loader = new RechargeRedisScriptLoader();

        var script = loader.markPersistFailedScript();

        assertThat(script.getResultType()).isEqualTo(List.class);
        assertThat(script.getSha1()).isNotBlank();
        assertThat(script.getScriptAsString())
                .contains("mark_persist_failed.lua")
                .contains("PERSIST_FAILED")
                .contains("last_error")
                .contains("ZADD', KEYS[3]");
    }

    @Test
    void loadsReleaseBalanceReservationScriptFromClasspath() {
        var loader = new RechargeRedisScriptLoader();

        var script = loader.releaseBalanceReservationScript();

        assertThat(script.getResultType()).isEqualTo(List.class);
        assertThat(script.getSha1()).isNotBlank();
        assertThat(script.getScriptAsString())
                .contains("release_balance_reservation.lua")
                .contains("ALREADY_RELEASED")
                .contains("HINCRBY', KEYS[1], 'available'")
                .contains("releaseEventId");
    }

    @Test
    void releaseBalanceReservationFailsClosedWhenReservationOrFrozenAmountIsInconsistent() {
        var loader = new RechargeRedisScriptLoader();

        String script = loader.releaseBalanceReservationScript().getScriptAsString();

        assertThat(script)
                .contains("RESERVATION_NOT_FOUND")
                .contains("RESERVATION_NOT_RELEASED")
                .contains("FROZEN_BALANCE_NOT_ENOUGH")
                .contains("tonumber(redis.call('HGET', KEYS[1], 'frozen') or '0')");
    }
}

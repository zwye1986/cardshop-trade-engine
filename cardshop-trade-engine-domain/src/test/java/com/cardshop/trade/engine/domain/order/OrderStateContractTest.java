package com.cardshop.trade.engine.domain.order;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * D2 领域状态契约测试。
 *
 * <p>状态定义对应 {@code docs/交行直充业务系统详细设计.md} 的「20.3 错误码与状态码」
 * 和「20.4 领域对象和端口接口」。状态枚举先作为跨模块语言，后续状态机实现必须复用这些枚举。
 */
class OrderStateContractTest {

    @Test
    void persistStatusDefinesRedisMqMysqlForwardOnlyStates() {
        assertThat(PersistStatus.PENDING_MQ.isTerminal()).isFalse();
        assertThat(PersistStatus.PERSIST_QUEUED.isTerminal()).isFalse();
        assertThat(PersistStatus.PERSISTING.isTerminal()).isFalse();
        assertThat(PersistStatus.PERSIST_FAILED.isTerminal()).isFalse();
        assertThat(PersistStatus.DB_PERSISTED.isTerminal()).isTrue();
        assertThat(PersistStatus.MANUAL_REQUIRED.isTerminal()).isTrue();
    }

    @Test
    void persistSourceAndValidationStatusUseDocumentedCodes() {
        assertThat(PersistSource.REDIS_SNAPSHOT.name()).isEqualTo("REDIS_SNAPSHOT");
        assertThat(PersistSource.MQ_SNAPSHOT_FALLBACK.name()).isEqualTo("MQ_SNAPSHOT_FALLBACK");
        assertThat(ValidationStatus.PASSED.allowsDispatch()).isTrue();
        assertThat(ValidationStatus.FAILED.allowsDispatch()).isFalse();
    }

    @Test
    void legacyOrderStatusCodesMatchOldCardshopContract() {
        assertThat(LegacyOrderStatusCode.ACCEPTED.code()).isEqualTo(2);
        assertThat(LegacyOrderStatusCode.FAILED.code()).isEqualTo(6);
        assertThat(LegacyOrderStatusCode.REFUNDED.code()).isEqualTo(11);
        assertThat(LegacyOrderStatusCode.SUCCESS.code()).isEqualTo(12);
        assertThat(LegacyOrderStatusCode.SUPPLIER_ACCEPTED.code()).isEqualTo(13);
    }

    @Test
    void legacyOrderStatusCodesMapToInternalQueryStatuses() {
        assertThat(LegacyOrderStatusCode.fromCode(2)).isEqualTo(LegacyOrderStatusCode.ACCEPTED);
        assertThat(LegacyOrderStatusCode.fromCode(6)).isEqualTo(LegacyOrderStatusCode.FAILED);
        assertThat(LegacyOrderStatusCode.fromCode(11)).isEqualTo(LegacyOrderStatusCode.REFUNDED);
        assertThat(LegacyOrderStatusCode.fromCode(12)).isEqualTo(LegacyOrderStatusCode.SUCCESS);
        assertThat(LegacyOrderStatusCode.fromCode(13)).isEqualTo(LegacyOrderStatusCode.SUPPLIER_ACCEPTED);
        assertThat(LegacyOrderStatusCode.fromCode(0)).isEqualTo(LegacyOrderStatusCode.UNKNOWN);
        assertThat(LegacyOrderStatusCode.SUCCESS.toQueryStatus()).isEqualTo("SUCCESS");
        assertThat(LegacyOrderStatusCode.SUPPLIER_ACCEPTED.toQueryStatus()).isEqualTo("SUPPLIER_ACCEPTED");
        assertThat(LegacyOrderStatusCode.UNKNOWN.toQueryStatus()).isEqualTo("UNKNOWN");
    }
}

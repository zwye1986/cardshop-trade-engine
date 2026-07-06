package com.cardshop.trade.engine.application.recharge;

import com.cardshop.trade.engine.domain.order.AcceptAnomalyType;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Redis 接单异常登记请求测试。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」。
 * 异常上下文是后续核查 Redis/MySQL/MQ 的证据入口，必须允许“字段存在但值未知”的 null 值，
 * 同时不能被调用方在构造后继续修改。
 */
class AcceptAnomalyRecordTest {

    @Test
    void keepsNullableRawContextEvidenceImmutable() {
        LinkedHashMap<String, Object> rawContext = new LinkedHashMap<>();
        rawContext.put("wait_reason", "WAIT_ERROR");
        rawContext.put("redis_check_result", null);

        AcceptAnomalyRecord record = new AcceptAnomalyRecord(
                "accept-WAIT_CANCEL_FAILED-202607051337458961474292",
                AcceptAnomalyType.WAIT_CANCEL_FAILED,
                "A36",
                "OUT-WAIT-UNKNOWN",
                "202607051337458961474292",
                "request-hash-1",
                "",
                rawContext,
                "WAIT_ERROR");

        rawContext.put("wait_reason", "MUTATED");

        assertThat(record.rawContext())
                .containsEntry("wait_reason", "WAIT_ERROR")
                .containsEntry("redis_check_result", null);
        assertThatThrownBy(() -> record.rawContext().put("extra", "value"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}

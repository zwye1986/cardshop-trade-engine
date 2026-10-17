package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.MinisoWriteOffException;

import java.sql.SQLException;

/** 仅保留异常类型、SQLState和错误码，丢弃含券码的SQL文本/cause；参见docs/交行直充业务系统详细设计.md「9.5」。 */
public final class MinisoWriteOffFailures {
    private MinisoWriteOffFailures() {}

    public static MinisoWriteOffException storage(Throwable failure) {
        String detail = "type=" + failure.getClass().getSimpleName();
        Throwable current = failure;
        for (int depth = 0; current != null && depth < 8; depth++, current = current.getCause()) {
            if (current instanceof SQLException sql) {
                String state = sql.getSQLState();
                detail +=
                        ";sqlState="
                                + (state != null && state.matches("[A-Za-z0-9]{5}")
                                        ? state
                                        : "unknown")
                                + ";code="
                                + sql.getErrorCode();
                break;
            }
        }
        var safe =
                new MinisoWriteOffException(MinisoWriteOffException.Reason.DB_UNAVAILABLE, detail);
        // 保留真实抛出位置的栈帧，但不保留含SQL/券码的message与cause。
        safe.setStackTrace(failure.getStackTrace());
        return safe;
    }
}

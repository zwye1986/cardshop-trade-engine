package com.cardshop.trade.engine.infrastructure.supplier.miniso;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * 保留供应商 HTTP 状态、JSON 与原文供 Gateway 判定及受控流水留存；客户端不推断业务终态。
 *
 * <p>见 {@code docs/交行直充业务系统详细设计.md}「名创 MINISO 专属协议实施约束（2026-10-17）」。
 * 原文与 JSON 可能含手机号及券码，因此字符串表示只保留 HTTP 状态，不能用于输出完整响应日志。
 */
public record MinisoApiResponse(int httpStatus, JsonNode body, String rawBody) {

    @Override
    public String toString() {
        return "MinisoApiResponse[httpStatus=" + httpStatus + ", body=redacted]";
    }
}

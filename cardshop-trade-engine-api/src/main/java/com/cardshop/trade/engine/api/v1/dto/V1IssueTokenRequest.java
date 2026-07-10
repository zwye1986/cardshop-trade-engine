package com.cardshop.trade.engine.api.v1.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

/**
 * 交行 V1 Token 申请请求 DTO。
 *
 * <p>字段对应旧系统 {@code LoginV2Dto} 和 {@code docs/交行直充业务系统详细设计.md}
 * 「3. API/V1 接入层」。appSecret 是敏感凭证，只能用于服务端校验，禁止写入日志。
 *
 * @param appId 商户应用 ID，对应旧表 `mc_ext_depart.app_id`
 * @param appSecret 商户应用密钥，对应旧表 `mc_ext_depart.app_secret`
 */
public record V1IssueTokenRequest(
        @JsonProperty("app_id") @JsonAlias("appId") @NotBlank String appId,
        @JsonProperty("app_secret") @JsonAlias("appSecret") @NotBlank String appSecret) {
}

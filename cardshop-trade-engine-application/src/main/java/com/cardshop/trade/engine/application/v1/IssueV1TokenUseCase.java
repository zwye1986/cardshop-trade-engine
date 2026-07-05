package com.cardshop.trade.engine.application.v1;

/**
 * 交行 V1 Token 申请用例。
 */
public interface IssueV1TokenUseCase {

    V1CardApiResult issueToken(String rawBody);
}

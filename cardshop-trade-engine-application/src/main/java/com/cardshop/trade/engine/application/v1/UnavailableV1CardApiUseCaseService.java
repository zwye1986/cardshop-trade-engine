package com.cardshop.trade.engine.application.v1;

/**
 * `/v1/cardapi` 新链路占位用例。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「20.13 `/v1/cardapi` 小粒度详细设计」。
 * 当前类只保证灰度命中新链路时不会误代理旧系统，也不会假装完成受理；真实 Token 生成、协议预校验、
 * 商品候选、Redis 受理、MQ Outbox 和 MySQL 落库会由后续正式应用服务替换。
 */
public class UnavailableV1CardApiUseCaseService implements IssueV1TokenUseCase,
        CreateRechargeOrderUseCase,
        QueryRechargeOrderUseCase,
        InvalidRechargeOrderUseCase {

    @Override
    public V1CardApiResult issueToken(String rawBody) {
        // TODO(D5): 接入商户 appSecret 校验、JWT 生成和 Redis token 写入；参见 docs/交行直充业务系统详细设计.md「20.13.5 Token 生成接口」。
        return V1CardApiResult.unavailable("新交易引擎Token生成暂未启用");
    }

    @Override
    public V1CardApiResult createRechargeOrder(V1MerchantCommand command) {
        // TODO(D5/D6): 串联商户安全资料、协议预校验、商品候选、AcceptRechargeOrderPort、WAIT 和 MQ Outbox；参见 docs/交行直充业务系统详细设计.md「20.13.6 Redis Lua 下单结果」和「20.13.9 请求哈希和幂等设计」。
        return V1CardApiResult.unavailable("新交易引擎直充受理暂未启用");
    }

    @Override
    public V1CardApiResult queryRechargeOrder(V1MerchantCommand command) {
        // TODO(D6): 接入 Redis 优先、MySQL 兜底的查单聚合；参见 docs/交行直充业务系统详细设计.md「20.13.10 查单设计」。
        return V1CardApiResult.unavailable("新交易引擎查单暂未启用");
    }

    @Override
    public V1CardApiResult invalidRechargeOrder(V1MerchantCommand command) {
        // TODO(D6): 接入作废状态机、持久化保护和异步补偿；参见 docs/交行直充业务系统详细设计.md「20.13.11 作废设计」。
        return V1CardApiResult.unavailable("新交易引擎作废暂未启用");
    }
}

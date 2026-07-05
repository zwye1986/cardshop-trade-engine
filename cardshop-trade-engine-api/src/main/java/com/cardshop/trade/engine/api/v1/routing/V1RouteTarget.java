package com.cardshop.trade.engine.api.v1.routing;

/**
 * `/v1/cardapi` 单次请求的目标链路。
 */
public enum V1RouteTarget {

    /**
     * 新交易引擎链路。
     */
    NEW,

    /**
     * 原卡管系统链路。
     */
    LEGACY
}

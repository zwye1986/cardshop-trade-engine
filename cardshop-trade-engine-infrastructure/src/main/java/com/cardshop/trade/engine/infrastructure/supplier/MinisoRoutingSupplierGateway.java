package com.cardshop.trade.engine.infrastructure.supplier;

import com.cardshop.trade.engine.application.recharge.*;
import java.util.Objects;

/** 显式开启名创真实协议后才装配此路由，其它渠道保留通用Gateway；参见详细设计名创实施约束。 */
public final class MinisoRoutingSupplierGateway implements RechargeSupplierGatewayPort {
    private static final String MINISO = "MINISO";
    private final RechargeSupplierGatewayPort miniso;
    private final RechargeSupplierGatewayPort fallback;
    public MinisoRoutingSupplierGateway(RechargeSupplierGatewayPort miniso, RechargeSupplierGatewayPort fallback) {
        this.miniso = Objects.requireNonNull(miniso); this.fallback = Objects.requireNonNull(fallback);
    }
    private RechargeSupplierGatewayPort route(String channel) { return MINISO.equals(channel) ? miniso : fallback; }
    @Override public SupplierGatewayResponse dispatch(SupplierDispatchGatewayRequest request) {
        return route(request.supplierChannel()).dispatch(request);
    }
    @Override public SupplierGatewayResponse query(SupplierQueryGatewayRequest request) {
        return route(request.supplierChannel()).query(request);
    }
    @Override public SupplierGatewayResponse invalid(SupplierInvalidGatewayRequest request) {
        return route(request.supplierChannel()).invalid(request);
    }
}

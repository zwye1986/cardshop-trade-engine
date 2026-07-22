# Cardshop Trade Engine

This context captures the shared language for recharge order acceptance, supplier fulfillment, and balance safety in the trade engine.

## Language

**不可确认失败**:
供应商履约中已经进入或可能进入供应商调用窗口，但本地没有拿到可证明成功、处理中或明确失败的业务语义。该状态不是供应商明确失败，不能触发退款释放、失败回调或盲目换渠，只能通过同一幂等号查单、保守重试或人工核实继续确认。
_Avoid_: 供应商失败, 普通失败, 接口失败

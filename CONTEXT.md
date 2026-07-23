# Cardshop Trade Engine

This context captures the shared language for recharge order acceptance, supplier fulfillment, and balance safety in the trade engine.

## Language

**不可确认失败**:
供应商履约中已经进入或可能进入供应商调用窗口，但本地没有拿到可证明成功、处理中或明确失败的业务语义。该状态不是供应商明确失败，不能触发退款释放、失败回调或盲目换渠，只能通过同一幂等号查单、保守重试或人工核实继续确认。
_Avoid_: 供应商失败, 普通失败, 接口失败

**MINISO直充**:
名创 MINISO 在新交易引擎中按直充商品处理，履约目标是客户手机号对应的名创账户。客户最终获得的是该账户内的卡号卡密权益，但交易引擎对外不按卡券卡密出库，不把供应商返回结果建模为本地卡券履约产物。
_Avoid_: MINISO卡券出库, MINISO按卡密交付, 名创卡券

**履约Timer热路径**:
进程内 timer 只负责按 `event_id` 低延迟唤醒派单/查单 worker，不是任务事实来源。任务事实、重启恢复、多节点互斥和人工补偿仍以 `recharge_dispatch_outbox` 的 `next_retry_time`、租约和 CAS 状态为准。
压测观测优先看 `trade.engine.dispatch.timer.pending`、`trade.engine.dispatch.timer.max.pending`、`trade.engine.dispatch.timer.rejected.total` 判断本地热唤醒是否触顶。
_Avoid_: Timer持久化任务, 内存任务替代outbox, 只靠timer派单

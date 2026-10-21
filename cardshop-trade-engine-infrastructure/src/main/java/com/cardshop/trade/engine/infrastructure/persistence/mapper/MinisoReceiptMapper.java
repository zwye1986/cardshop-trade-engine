package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.cardshop.trade.engine.application.recharge.MinisoFulfillmentReceipt;
import com.cardshop.trade.engine.infrastructure.supplier.miniso.MinisoOrderSnapshot;
import org.apache.ibatis.annotations.*;

/** 名创直充履约凭据单表与可信归属查询；参见详细设计「名创 MINISO 专属协议实施约束（2026-10-17）」。 */
@Mapper
public interface MinisoReceiptMapper {
    /** 分别读取单表后校验归属，保持仓库禁止多表JOIN的持久化合同。 */
    record FrozenOrder(String orgCode, String orderId, String outOrderSn, String batchId, String persistStatus) { }
    record OrderIdentity(String orgCode, String orderId, String outOrderSn, String account, int num, String channel) { }

    String FROZEN_SQL =
            "select org_code as orgCode, order_id as orderId, out_order_sn as outOrderSn,"
                + " supplier_batch_id as batchId, persist_status as persistStatus from"
                + " recharge_order_ext where order_id=#{orderId}";
    String ORDER_SQL =
            "select org_code as orgCode, order_Id as orderId, out_orderId as outOrderSn, account,"
                + " num, supplier_channel as channel from mc_order where order_Id=#{orderId}";
    @Select(FROZEN_SQL) FrozenOrder selectFrozen(@Param("orderId") String orderId);
    @Select(FROZEN_SQL + " for update") FrozenOrder lockFrozen(@Param("orderId") String orderId);
    @Select(ORDER_SQL) OrderIdentity selectOrder(@Param("orderId") String orderId);
    @Select(ORDER_SQL + " for update") OrderIdentity lockOrder(@Param("orderId") String orderId);

    default MinisoOrderSnapshot selectSnapshot(String orgCode, String orderId) {
        var snapshot = owned(selectFrozen(orderId), selectOrder(orderId));
        return snapshot != null && orgCode.equals(snapshot.orgCode()) ? snapshot : null;
    }
    /** 与终态写入相同，先锁主单再锁扩展行；同订单串行保存凭据。 */
    default MinisoOrderSnapshot lockSnapshot(String orderId) {
        var order = lockOrder(orderId);
        return owned(lockFrozen(orderId), order);
    }
    private static MinisoOrderSnapshot owned(FrozenOrder ext, OrderIdentity order) {
        if (ext == null || order == null || !"DB_PERSISTED".equals(ext.persistStatus())
                || !"MINISO".equals(order.channel()) || !ext.orgCode().equals(order.orgCode())
                || !ext.orderId().equals(order.orderId()) || !ext.outOrderSn().equals(order.outOrderSn())) return null;
        return new MinisoOrderSnapshot(ext.orgCode(), ext.orderId(), order.account(), ext.batchId(), order.num());
    }

    @Select(
            """
select order_id as orderId, org_code as orgCode, batch_id as batchId, coupon_code as couponCode,
       phone, begin_time as beginTime, end_time as endTime, coupon_status as couponStatus
from recharge_miniso_receipt where order_id=#{orderId}
""")
    MinisoFulfillmentReceipt selectReceipt(@Param("orderId") String orderId);

    /** 主单已由lockSnapshot锁定；与凭据/成功终态共用事务，参见详细设计「名创卡密导出兼容修复」。 */
    @Select("select data from mc_order where order_Id=#{orderId} and org_code=#{orgCode}")
    String selectLegacyData(@Param("orderId") String orderId, @Param("orgCode") String orgCode);

    @Update("""
            update mc_order set data=#{data}
            where order_Id=#{orderId} and org_code=#{orgCode} and (data is null or trim(data)='')
            """)
    int fillLegacyData(@Param("orderId") String orderId, @Param("orgCode") String orgCode,
                       @Param("data") String data);

    String RECEIPT_COLUMNS =
            "order_id as orderId,org_code as orgCode,batch_id as batchId,coupon_code as"
                + " couponCode,phone,begin_time as beginTime,end_time as endTime,coupon_status as"
                + " couponStatus";

    @Select(
            "select "
                    + RECEIPT_COLUMNS
                    + " from recharge_miniso_receipt where coupon_code=#{coupon}")
    MinisoFulfillmentReceipt selectByCoupon(@Param("coupon") String coupon);

    @Select(
            "select "
                    + RECEIPT_COLUMNS
                    + " from recharge_miniso_receipt where order_id=#{id} for update")
    MinisoFulfillmentReceipt lockReceipt(@Param("id") String id);

    @Select("select use_time from recharge_miniso_receipt where order_id=#{id}")
    java.time.LocalDateTime useTime(@Param("id") String id);

    @Update(
            "update recharge_miniso_receipt set"
                + " coupon_status=1,use_time=#{used},updated_at=current_timestamp where"
                + " order_id=#{id}")
    int redeem(@Param("id") String id, @Param("used") java.time.LocalDateTime used);

    @Insert(
            """
insert into recharge_miniso_receipt(order_id,org_code,batch_id,coupon_code,phone,begin_time,end_time,coupon_status)
values(#{orderId},#{orgCode},#{batchId},#{couponCode},#{phone},#{beginTime},#{endTime},#{couponStatus})
""")
    int insert(MinisoFulfillmentReceipt receipt);

    @Update(
            """
update recharge_miniso_receipt set coupon_status=#{status}, updated_at=current_timestamp
where order_id=#{orderId}
""")
    int updateStatus(@Param("orderId") String orderId, @Param("status") int status);
}

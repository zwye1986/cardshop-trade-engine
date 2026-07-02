package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.infrastructure.persistence.entity.McOrderDO;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

/**
 * 旧系统订单事实表 `mc_order` Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「20.7 MyBatis Mapper 细节」。
 * 查询优先使用 MyBatis-Plus LambdaWrapper；幂等插入因需要 `on duplicate key update` 使用单表注解 SQL。
 */
@Mapper
public interface McOrderMapper extends BaseMapper<McOrderDO> {

    /**
     * 按直充幂等键写入订单事实，唯一冲突时保持原订单不变。
     */
    @Insert("""
            insert into mc_order (
              org_code,
              create_time,
              edit_time,
              produt_code,
              product_name,
              product_price,
              product_value,
              product_type,
              num,
              order_Id,
              out_orderId,
              out_order_no,
              order_price,
              refund_order_price,
              order_status,
              data_status,
              callback_url,
              request_ip,
              remark,
              supplier_channel,
              res_data,
              parent_id,
              account,
              coupon_type,
              order_type,
              fail_code,
              fail_reason
            )
            values (
              #{orgCode},
              #{createTime},
              #{editTime},
              #{productCode},
              #{productName},
              #{productPrice},
              #{productValue},
              #{productType},
              #{num},
              #{orderId},
              #{outOrderId},
              #{outOrderNo},
              #{orderPrice},
              #{refundOrderPrice},
              #{orderStatus},
              #{dataStatus},
              #{callbackUrl},
              #{requestIp},
              #{remark},
              #{supplierChannel},
              #{resData},
              #{parentId},
              #{account},
              #{couponType},
              #{orderType},
              #{failCode},
              #{failReason}
            )
            on duplicate key update
              order_Id = order_Id
            """)
    int insertIfAbsent(McOrderDO order);

    /**
     * 按平台订单号读取旧表订单事实。
     */
    default McOrderDO selectByOrderId(String orderId) {
        return selectOne(new LambdaQueryWrapper<McOrderDO>()
                .eq(McOrderDO::getOrderId, orderId)
                .last("limit 1"));
    }

    /**
     * 按商户订单号读取直充订单事实。
     */
    default McOrderDO selectByOrgAndOutOrder(String orgCode, String outOrderSn) {
        return selectOne(new LambdaQueryWrapper<McOrderDO>()
                .eq(McOrderDO::getOrgCode, orgCode)
                .eq(McOrderDO::getOutOrderId, outOrderSn)
                .eq(McOrderDO::getProductType, 1)
                .last("limit 1"));
    }
}

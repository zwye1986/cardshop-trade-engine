package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.cardshop.trade.engine.application.recharge.*;
import java.time.LocalDateTime;
import org.apache.ibatis.annotations.*;

/** 单条SQL核验有效租约并登记请求，失败不发送；参见详细设计2104实施细化。 */
@Mapper
public interface RechargeNotifyAttemptMapper {
    @Insert("""
        insert into recharge_notify_attempt(task_id,claim_version,msg_id,started_at,status,http_status)
        select task_id,claim_version,#{msgId},#{now},'IN_FLIGHT',0
          from recharge_notify_task
         where task_id=#{r.taskId} and status='PROCESSING' and claim_version=#{r.claimVersion}
           and locked_by=#{r.lockedBy} and locked_until>#{now}
           and delivery_channel='BOCOM_OPENAPI' and org_code=#{r.orgCode}
           and order_id=#{r.orderId} and out_order_sn=#{r.outOrderSn}
        """)
    int begin(@Param("r") MerchantNotifyRequest request,@Param("msgId") String msgId,@Param("now") LocalDateTime now);

    @Update("""
        update recharge_notify_attempt
           set status=#{result.status},http_status=#{result.httpStatus},biz_state=#{result.bizState},
               rsp_code=#{result.rspCode},business_code=#{result.businessCode},ref_msg_id=#{result.refMsgId},finished_at=#{now}
         where msg_id=#{msgId} and task_id=#{r.taskId} and claim_version=#{r.claimVersion} and status='IN_FLIGHT'
        """)
    int finish(@Param("r") MerchantNotifyRequest request,@Param("msgId") String msgId,
            @Param("result") MerchantNotifyAttemptResult result,@Param("now") LocalDateTime now);
}

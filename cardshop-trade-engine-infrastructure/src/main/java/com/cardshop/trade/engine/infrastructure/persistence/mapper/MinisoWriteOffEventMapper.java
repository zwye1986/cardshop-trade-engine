package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import org.apache.ibatis.annotations.*;

import java.time.LocalDateTime;
import java.util.List;

/** 名创核销接收事件及受控单表查询；参见docs/交行直充业务系统详细设计.md「9.5」。 */
@Mapper
public interface MinisoWriteOffEventMapper {
    int QUERY_TIMEOUT_SECONDS = 5;

    /**
     * 字段逐一对应0021 DDL COMMENT，禁止把含券码的记录默认输出日志。
     *
     * @param eventId 核销事件唯一ID
     * @param couponCode 名创券码
     * @param useTime 核销时间
     * @param orderId 平台订单号
     * @param orgCode 商户编码
     * @param status 处理状态
     * @param retryCount 重试次数
     * @param nextRetryTime 下次处理时间
     * @param lastError 最近处理错误
     * @param version 记录版本号
     */
    record Event(
            String eventId,
            String couponCode,
            LocalDateTime useTime,
            String orderId,
            String orgCode,
            String status,
            int retryCount,
            LocalDateTime nextRetryTime,
            String lastError,
            long version) {
        @Override
        public String toString() {
            return "MinisoWriteOffEvent[id=" + eventId + ",status=" + status + "]";
        }
    }

    /** 仅供可信核销通知所需的旧单事实；参见docs/交行直充业务系统详细设计.md「9.5」。 */
    record OrderFacts(
            String orderId,
            String orgCode,
            String outOrderSn,
            String account,
            int num,
            String channel,
            int orderStatus,
            String notifyUrl) {
        @Override
        public String toString() {
            return "MinisoOrderFacts[orderId=" + orderId + "]";
        }
    }

    /** 已生成任务只校验身份与核销事实，不按当前密钥重新签名比较。 */
    record TaskIdentity(
            String orgCode,
            String orderId,
            String outOrderSn,
            String notifyUrl,
            String bizStatus,
            String payload) {
        @Override
        public String toString() {
            return "MinisoNotifyIdentity[orderId=" + orderId + "]";
        }
    }

    String COLUMNS =
            "event_id as eventId,coupon_code as couponCode,use_time as useTime,order_id as"
                + " orderId,org_code as orgCode,status,retry_count as retryCount,next_retry_time as"
                + " nextRetryTime,last_error as lastError,version";

    @Options(timeout = QUERY_TIMEOUT_SECONDS)
    @Select(
            "select "
                    + COLUMNS
                    + " from recharge_miniso_writeoff_event where event_id=#{id} for update")
    Event lock(@Param("id") String id);

    @Options(timeout = QUERY_TIMEOUT_SECONDS)
    @Select(
            "select "
                    + COLUMNS
                    + " from recharge_miniso_writeoff_event where coupon_code=#{coupon} for update")
    Event lockByCoupon(@Param("coupon") String coupon);

    @Options(timeout = QUERY_TIMEOUT_SECONDS)
    @Select(
            "select "
                    + COLUMNS
                    + " from recharge_miniso_writeoff_event where status in ('INIT','RETRY') "
                    + "and next_retry_time<=#{now} and MOD(CRC32(event_id),#{shards})=#{shard} "
                    + "order by next_retry_time,received_at,event_id limit #{limit}")
    List<Event> due(
            @Param("now") LocalDateTime now,
            @Param("shards") int shards,
            @Param("shard") int shard,
            @Param("limit") int limit);

    @Insert(
            """
insert into recharge_miniso_writeoff_event(event_id,coupon_code,use_time,order_id,org_code,status,
  retry_count,next_retry_time,last_error,version,received_at,updated_at)
values(#{id},#{coupon},#{used},#{order},#{org},'INIT',0,#{now},'',0,#{now},#{now})
on duplicate key update event_id=event_id
""")
    int insertIfAbsent(
            @Param("id") String id,
            @Param("coupon") String coupon,
            @Param("used") LocalDateTime used,
            @Param("order") String order,
            @Param("org") String org,
            @Param("now") LocalDateTime now);

    @Update(
            """
update recharge_miniso_writeoff_event set status='PROCESSED',order_id=#{order},org_code=#{org},
  processed_at=#{now},updated_at=#{now},last_error='',version=version+1 where event_id=#{id}
""")
    int complete(
            @Param("id") String id,
            @Param("order") String order,
            @Param("org") String org,
            @Param("now") LocalDateTime now);

    /** 核销与券事实同事务提交，订单状态不变；参见详细设计9.6。 */
    @Update("update recharge_order_ext set voucher_use_state='2',voucher_use_time=#{used},"
            + "updated_at=#{now} where order_id=#{id} and org_code=#{org} "
            + "and (voucher_use_state is null or voucher_use_state <> '3')")
    int redeemVoucher(@Param("id") String id, @Param("org") String org,
            @Param("used") LocalDateTime used, @Param("now") LocalDateTime now);

    @Update(
            """
update recharge_miniso_writeoff_event set status=#{status},retry_count=#{retries},next_retry_time=#{next},
  last_error=#{error},updated_at=#{now},version=version+1
where event_id=#{id} and version=#{version} and status in ('INIT','RETRY')
""")
    int reschedule(
            @Param("id") String id,
            @Param("version") long version,
            @Param("retries") int retries,
            @Param("status") String status,
            @Param("next") LocalDateTime next,
            @Param("error") String error,
            @Param("now") LocalDateTime now);

    @Options(timeout = QUERY_TIMEOUT_SECONDS)
    @Select(
            """
            select order_Id as orderId,org_code as orgCode,out_orderId as outOrderSn,account,num,
              supplier_channel as channel,order_status as orderStatus,callback_url as notifyUrl
            from mc_order where order_Id=#{id} for update
            """)
    OrderFacts lockOrder(@Param("id") String id);

    @Options(timeout = QUERY_TIMEOUT_SECONDS)
    @Select("select dispatch_status from recharge_order_ext where order_id=#{id}")
    String dispatchStatus(@Param("id") String id);

    @Options(timeout = QUERY_TIMEOUT_SECONDS)
    @Select(
            """
select org_code as orgCode,order_id as orderId,out_order_sn as outOrderSn,notify_url as notifyUrl,
  biz_status as bizStatus,payload from recharge_notify_task where task_id=#{id} for update
""")
    TaskIdentity task(@Param("id") String id);

    /** 指标仅在Job轮末聚合刷新，不按单条请求扫描全表。 */
    record PendingStats(long pending, long manual, LocalDateTime oldest) {}

    @Options(timeout = QUERY_TIMEOUT_SECONDS)
    @Select(
            """
select coalesce(sum(case when status in ('INIT','RETRY') then 1 else 0 end),0) as pending,
  coalesce(sum(case when status='MANUAL_REQUIRED' then 1 else 0 end),0) as manual,
  min(case when status in ('INIT','RETRY') then received_at else null end) as oldest
from recharge_miniso_writeoff_event where status in ('INIT','RETRY','MANUAL_REQUIRED')
""")
    PendingStats pendingStats();

    @Options(timeout = QUERY_TIMEOUT_SECONDS)
    @Select(
            "select count(*) from recharge_notify_task where notify_type='VOUCHER_STATE' and"
                    + " biz_status='REDEEMED' and status in ('INIT','FAILED','PROCESSING','MANUAL_REQUIRED')")
    long pendingNotifications();

    @Update(
            """
            update recharge_dispatch_outbox set status='MANUAL_REQUIRED',locked_until=null,
              last_error='REDEEM_VOID_CONFLICT',updated_at=#{now} where event_id=#{id}
            """)
    int stopInvalidation(@Param("id") String id, @Param("now") LocalDateTime now);

    @Update(
            """
update mc_order set order_status=12,data_status='PROCESSED',remark='MINISO核销证据阻止作废',edit_time=#{now}
where order_Id=#{id} and order_status=13
""")
    int restoreSuccess(@Param("id") String id, @Param("now") LocalDateTime now);

    @Update(
            """
update recharge_order_ext set dispatch_status='SUCCESS',last_error='REDEEM_VOID_CONFLICT',updated_at=#{now}
where order_id=#{id} and dispatch_status='CANCEL_REQUESTED'
""")
    int restoreDispatch(@Param("id") String id, @Param("now") LocalDateTime now);
}

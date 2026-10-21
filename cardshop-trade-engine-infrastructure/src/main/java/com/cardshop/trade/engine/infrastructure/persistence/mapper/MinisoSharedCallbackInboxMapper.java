package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import java.time.LocalDateTime;
import org.apache.ibatis.annotations.*;

/** 共享已认证核销证据的单表当前读，不向领域层暴露旧端表；参见docs/交行直充业务系统详细设计.md「9.8」。 */
@Mapper
public interface MinisoSharedCallbackInboxMapper {
    /**
     * 字段注释对应0023 DDL COMMENT；不含券码，避免默认toString泄露敏感凭据。
     * @param eventId 核销事件编号
     * @param useTime 核销时间
     * @param status 接收事件状态
     */
    record Evidence(String eventId, LocalDateTime useTime, String status) {}

    /** guard之后读取任意状态，已转投或人工状态同样是核销证据，不能以归属不明放行。 */
    @Options(timeout = MinisoWriteOffEventMapper.QUERY_TIMEOUT_SECONDS)
    @Select("select event_id as eventId,use_time as useTime,status from mc_miniso_callback_inbox "
            + "where coupon_code=#{coupon} for update")
    Evidence lockByCoupon(@Param("coupon") String coupon);
}

package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import java.time.LocalDateTime;
import org.apache.ibatis.annotations.*;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 名创券码实体互斥行，仅承载事务锁，不代表核销证据。
 * 参见docs/交行直充业务系统详细设计.md「9.8」及共享分流规格6.1。
 */
@Mapper
public interface MinisoCouponGuardMapper {
    @Options(timeout = MinisoWriteOffEventMapper.QUERY_TIMEOUT_SECONDS)
    @Insert("insert into mc_miniso_coupon_guard(coupon_code,created_at) values(#{coupon},#{now}) "
            + "on duplicate key update coupon_code=coupon_code")
    int insertIfAbsent(@Param("coupon") String coupon, @Param("now") LocalDateTime now);

    @Options(timeout = MinisoWriteOffEventMapper.QUERY_TIMEOUT_SECONDS)
    @Select("select coupon_code from mc_miniso_coupon_guard where coupon_code=#{coupon} for update")
    String lock(@Param("coupon") String coupon);

    /**
     * 必须使用调用方主事务连接，锁至提交/回滚；不用INSERT IGNORE掩盖其它数据库错误。
     * 已知订单先锁主单/扩展/凭据，未知接收拿锁后不得反向寻找主单，见详细设计9.8。
     */
    default void lockInTransaction(String coupon, LocalDateTime now) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("MINISO coupon guard requires order or reception transaction");
        }
        insertIfAbsent(coupon, now);
        if (!coupon.equals(lock(coupon))) {
            throw new IllegalStateException("MINISO coupon guard missing");
        }
    }
}

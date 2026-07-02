package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

/**
 * 新链路订单扩展表 Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「20.7 MyBatis Mapper 细节」。
 * 更新持久化状态时必须保证版本只前进，防止旧 MQ 快照覆盖新 Redis 订单版本。
 */
@Mapper
public interface RechargeOrderExtMapper extends BaseMapper<RechargeOrderExtDO> {

    /**
     * 插入或推进订单持久化状态。
     */
    @Insert("""
            insert into recharge_order_ext (
              order_id,
              org_code,
              out_order_sn,
              request_hash,
              request_hash_version,
              request_hash_alg,
              redis_order_id,
              persist_msg_id,
              persist_status,
              persist_source,
              redis_order_version,
              persisted_version,
              validation_status,
              dispatch_allowed,
              balance_reserved,
              mq_publish_count,
              persist_retry_count,
              last_error,
              created_at,
              updated_at
            )
            values (
              #{orderId},
              #{orgCode},
              #{outOrderSn},
              #{requestHash},
              #{requestHashVersion},
              #{requestHashAlg},
              #{redisOrderId},
              #{persistMsgId},
              #{persistStatus},
              #{persistSource},
              #{redisOrderVersion},
              #{persistedVersion},
              #{validationStatus},
              #{dispatchAllowed},
              #{balanceReserved},
              #{mqPublishCount},
              #{persistRetryCount},
              #{lastError},
              #{createdAt},
              #{updatedAt}
            )
            on duplicate key update
              persist_status = case
                when values(persisted_version) > persisted_version then values(persist_status)
                when values(persisted_version) = persisted_version
                  and persist_status in ('PENDING_MQ', 'PERSIST_QUEUED', 'PERSISTING')
                  and values(persist_status) in ('DB_PERSISTED', 'MANUAL_REQUIRED')
                then values(persist_status)
                else persist_status
              end,
              persist_source = case
                when values(persisted_version) > persisted_version then values(persist_source)
                when values(persisted_version) = persisted_version and persist_status <> 'DB_PERSISTED'
                then values(persist_source)
                else persist_source
              end,
              persisted_version = greatest(persisted_version, values(persisted_version)),
              redis_order_version = greatest(redis_order_version, values(redis_order_version)),
              last_error = case
                when values(persisted_version) >= persisted_version then values(last_error)
                else last_error
              end,
              updated_at = values(updated_at)
            """)
    int insertOrUpdatePersistState(RechargeOrderExtDO ext);

    /**
     * 按平台订单号读取扩展状态。
     */
    default RechargeOrderExtDO selectByOrderId(String orderId) {
        return selectOne(new LambdaQueryWrapper<RechargeOrderExtDO>()
                .eq(RechargeOrderExtDO::getOrderId, orderId)
                .last("limit 1"));
    }

    /**
     * 按商户订单号读取扩展状态。
     */
    default RechargeOrderExtDO selectByOrgAndOutOrder(String orgCode, String outOrderSn) {
        return selectOne(new LambdaQueryWrapper<RechargeOrderExtDO>()
                .eq(RechargeOrderExtDO::getOrgCode, orgCode)
                .eq(RechargeOrderExtDO::getOutOrderSn, outOrderSn)
                .last("limit 1"));
    }
}

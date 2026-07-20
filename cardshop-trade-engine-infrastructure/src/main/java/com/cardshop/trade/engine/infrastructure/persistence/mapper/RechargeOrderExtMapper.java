package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOrderExtDO;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * 新链路订单扩展表 Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「8. MySQL/Liquibase 持久化层」。
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
              dispatch_status,
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
              #{dispatchStatus},
              #{mqPublishCount},
              #{persistRetryCount},
              #{lastError},
              #{createdAt},
              #{updatedAt}
            )
            on duplicate key update
              persist_source = case
                when values(persisted_version) > persisted_version then values(persist_source)
                when values(persisted_version) = persisted_version and persist_status <> 'DB_PERSISTED'
                then values(persist_source)
                else persist_source
              end,
              last_error = case
                when values(persisted_version) >= persisted_version then values(last_error)
                else last_error
              end,
              dispatch_status = case
                when values(persisted_version) > persisted_version then values(dispatch_status)
                when values(persisted_version) = persisted_version and dispatch_status is null then values(dispatch_status)
                else dispatch_status
              end,
              persist_status = case
                when values(persisted_version) > persisted_version then values(persist_status)
                when values(persisted_version) = persisted_version
                  and persist_status in ('PENDING_MQ', 'PERSIST_QUEUED', 'PERSISTING')
                  and values(persist_status) in ('DB_PERSISTED', 'MANUAL_REQUIRED')
                then values(persist_status)
                else persist_status
              end,
              persisted_version = greatest(persisted_version, values(persisted_version)),
              redis_order_version = greatest(redis_order_version, values(redis_order_version)),
              updated_at = values(updated_at)
            """)
    int insertOrUpdatePersistState(RechargeOrderExtDO ext);

    /**
     * 批量插入或推进订单持久化状态。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。版本前进规则与单条
     * upsert 保持一致，只把同批数据合并为一条 multi-row SQL。
     */
    @Insert("""
            <script>
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
              dispatch_status,
              mq_publish_count,
              persist_retry_count,
              last_error,
              created_at,
              updated_at
            )
            values
            <foreach collection="rows" item="row" separator=",">
            (
              #{row.orderId},
              #{row.orgCode},
              #{row.outOrderSn},
              #{row.requestHash},
              #{row.requestHashVersion},
              #{row.requestHashAlg},
              #{row.redisOrderId},
              #{row.persistMsgId},
              #{row.persistStatus},
              #{row.persistSource},
              #{row.redisOrderVersion},
              #{row.persistedVersion},
              #{row.validationStatus},
              #{row.dispatchAllowed},
              #{row.balanceReserved},
              #{row.dispatchStatus},
              #{row.mqPublishCount},
              #{row.persistRetryCount},
              #{row.lastError},
              #{row.createdAt},
              #{row.updatedAt}
            )
            </foreach>
            on duplicate key update
              persist_source = case
                when values(persisted_version) &gt; persisted_version then values(persist_source)
                when values(persisted_version) = persisted_version and persist_status != 'DB_PERSISTED'
                then values(persist_source)
                else persist_source
              end,
              last_error = case
                when values(persisted_version) &gt;= persisted_version then values(last_error)
                else last_error
              end,
              dispatch_status = case
                when values(persisted_version) &gt; persisted_version then values(dispatch_status)
                when values(persisted_version) = persisted_version and dispatch_status is null then values(dispatch_status)
                else dispatch_status
              end,
              persist_status = case
                when values(persisted_version) &gt; persisted_version then values(persist_status)
                when values(persisted_version) = persisted_version
                  and persist_status in ('PENDING_MQ', 'PERSIST_QUEUED', 'PERSISTING')
                  and values(persist_status) in ('DB_PERSISTED', 'MANUAL_REQUIRED')
                then values(persist_status)
                else persist_status
              end,
              persisted_version = greatest(persisted_version, values(persisted_version)),
              redis_order_version = greatest(redis_order_version, values(redis_order_version)),
              updated_at = values(updated_at)
            </script>
            """)
    int insertExtBatchOrUpdatePersistState(@Param("rows") List<RechargeOrderExtDO> rows);

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

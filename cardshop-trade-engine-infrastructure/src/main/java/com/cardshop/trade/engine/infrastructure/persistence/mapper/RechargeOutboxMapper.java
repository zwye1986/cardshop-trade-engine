package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeOutboxDO;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * 可靠事件表 Mapper。
 *
 * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md} 的「6.6 新增表」。
 * 新旧系统共享旧订单表期间，扫描和抢占都必须通过 `recharge_order_ext` 确认订单归属。
 */
@Mapper
public interface RechargeOutboxMapper extends BaseMapper<RechargeOutboxDO> {

    /**
     * 幂等写入可靠事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
     * Persist Consumer 可能因为 RabbitMQ 重投重复处理同一订单，`SUPPLIER_DISPATCH` outbox 必须按
     * `event_id` 幂等插入，避免唯一键冲突被误判为数据库故障。
     */
    @Insert("""
            insert into recharge_outbox (
              event_id,
              event_type,
              org_code,
              order_id,
              out_order_sn,
              payload,
              status,
              retry_count,
              max_retry_count,
              next_retry_time,
              locked_by,
              locked_until,
              version,
              last_error,
              shard_key,
              created_at,
              updated_at
            )
            values (
              #{eventId},
              #{eventType},
              #{orgCode},
              #{orderId},
              #{outOrderSn},
              #{payload},
              #{status},
              #{retryCount},
              #{maxRetryCount},
              #{nextRetryTime},
              #{lockedBy},
              #{lockedUntil},
              #{version},
              #{lastError},
              #{shardKey},
              #{createdAt},
              #{updatedAt}
            )
            on duplicate key update
              event_id = event_id
            """)
    int insertIfAbsent(RechargeOutboxDO outbox);

    /**
     * 汇总未完成的余额 delta。
     *
     * <p>参见 {@code docs/交行直充业务系统高可用方案设计.md}「17.5 余额对账任务」。对账时不能只比较
     * `mc_ext_depart.balance` 和 Redis available；还必须把尚未刷入旧表的 `BALANCE_DELTA_FLUSH` 事件纳入
     * 投影，否则正在重试的刷账事件会被误报为资金差异。
     */
    @Select("""
            select coalesce(sum(cast(json_unquote(json_extract(o.payload, '$.delta_amount')) as signed)), 0)
            from recharge_outbox o
            where o.org_code = #{orgCode}
              and o.event_type = 'BALANCE_DELTA_FLUSH'
              and o.status <> 'SUCCESS'
              and exists (
                select 1
                from recharge_order_ext ext
                where ext.order_id = o.order_id
                  and ext.org_code = o.org_code
                  and ext.out_order_sn = o.out_order_sn
                  and ext.persist_status = 'DB_PERSISTED'
              )
            """)
    Long sumUnfinishedBalanceDelta(@Param("orgCode") String orgCode);

    /**
     * 汇总 outbox 未完成事件数量和最大滞留时间。
     *
     * <p>参见 {@code docs/开发进度.md}「5.1 监控告警最小闭环执行边界」。该 SQL 只返回低基数
     * event_type/status 聚合值，不返回订单号或商户号，避免业务监控制造高基数标签。
     */
    @Select("""
            select
              o.event_type,
              o.status,
              count(*) as count,
              coalesce(timestampdiff(second, min(coalesce(o.created_at, o.updated_at, #{now})), #{now}), 0) as max_age_seconds
            from recharge_outbox o
            where o.status in ('INIT', 'PROCESSING', 'FAILED', 'MANUAL_REQUIRED')
              and exists (
                select 1
                from recharge_order_ext ext
                where ext.order_id = o.order_id
                  and ext.org_code = o.org_code
                  and ext.out_order_sn = o.out_order_sn
                  and ext.persist_status = 'DB_PERSISTED'
              )
            group by o.event_type, o.status
            """)
    List<Map<String, Object>> selectObservabilityOutboxMetrics(@Param("now") LocalDateTime now);

    /**
     * 扫描到期事件和租约失效事件。
     */
    default List<RechargeOutboxDO> selectDueForShard(int shardTotal, int shardIndex, int limit) {
        return selectDueForShardByEventTypes(
                shardTotal,
                shardIndex,
                limit,
                List.of(
                        OutboxEventType.SUPPLIER_DISPATCH.name(),
                        OutboxEventType.SUPPLIER_QUERY.name(),
                        OutboxEventType.INVALID_ORDER.name()));
    }

    /**
     * 按事件类型扫描到期事件和租约失效事件。
     *
     * <p>D9/D10 的履约 worker 与 D12 的余额刷账 worker 共享 outbox 表，但处理语义不同；扫描时必须
     * 先按 event_type 隔离，避免通用供应商 worker 把 `BALANCE_DELTA_FLUSH` 误标为不支持事件。
     */
    default List<RechargeOutboxDO> selectDueForShardByEventTypes(
            int shardTotal,
            int shardIndex,
            int limit,
            List<String> eventTypes) {
        return selectDueForShardByEventTypes(shardTotal, shardIndex, limit, eventTypes, LocalDateTime.now());
    }

    /**
     * 使用调用方注入的时钟扫描到期事件，便于固定 Clock 的测试和生产排障统一时间来源。
     */
    default List<RechargeOutboxDO> selectDueForShardByEventTypes(
            int shardTotal,
            int shardIndex,
            int limit,
            List<String> eventTypes,
            LocalDateTime now) {
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        return selectDueForShardByEventTypesSql(shardTotal, shardIndex, safeLimit, eventTypes, now);
    }

    /**
     * 扫描新交易引擎拥有的到期事件和租约失效事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。新旧系统共享
     * {@code mc_order} 期间，只有能在 {@code recharge_order_ext} 中确认 `DB_PERSISTED` 的订单事件
     * 才允许进入 outbox 自动任务；可执行集合按 {@code created_at, id} 排序，保证先生成的事件优先处理。
     */
    @Select("""
            <script>
            select
              o.id,
              o.event_id,
              o.event_type,
              o.org_code,
              o.order_id,
              o.out_order_sn,
              o.payload,
              o.status,
              o.retry_count,
              o.max_retry_count,
              o.next_retry_time,
              o.locked_by,
              o.locked_until,
              o.version,
              o.last_error,
              o.shard_key,
              o.created_at,
              o.updated_at
            from recharge_outbox o
            where o.event_type in
              <foreach collection="eventTypes" item="eventType" open="(" separator="," close=")">
                #{eventType}
              </foreach>
              and (
                (o.status in ('INIT', 'FAILED') and o.next_retry_time &lt;= #{now})
                or (o.status = 'PROCESSING' and (o.locked_until &lt; #{now} or o.locked_until is null))
              )
              and exists (
                select 1
                from recharge_order_ext ext
                where ext.order_id = o.order_id
                  and ext.org_code = o.org_code
                  and ext.out_order_sn = o.out_order_sn
                  and ext.persist_status = 'DB_PERSISTED'
              )
              and mod(crc32(o.shard_key), #{shardTotal}) = #{shardIndex}
            order by o.created_at asc, o.id asc
            limit #{limit}
            </script>
            """)
    List<RechargeOutboxDO> selectDueForShardByEventTypesSql(
            @Param("shardTotal") int shardTotal,
            @Param("shardIndex") int shardIndex,
            @Param("limit") int limit,
            @Param("eventTypes") List<String> eventTypes,
            @Param("now") LocalDateTime now);

    /**
     * 基于事件 ID 和版本号抢占事件。
     */
    default int claimProcessing(String eventId, long currentVersion, String workerId, LocalDateTime lockedUntil) {
        return claimProcessing(eventId, currentVersion, workerId, lockedUntil, LocalDateTime.now());
    }

    /**
     * 使用调用方注入的时钟执行僵尸任务回收判断。
     */
    default int claimProcessing(
            String eventId,
            long currentVersion,
            String workerId,
            LocalDateTime lockedUntil,
            LocalDateTime now) {
        return claimProcessingSql(eventId, currentVersion, workerId, lockedUntil, now);
    }

    /**
     * 基于事件 ID、版本号和订单归属哨兵抢占事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。抢占阶段重复校验
     * {@code recharge_order_ext}，避免扫描后扩展状态变化或绕过扫描的调用方处理旧系统订单。
     */
    @Update("""
            update recharge_outbox o
            set status = 'PROCESSING',
                locked_by = #{workerId},
                locked_until = #{lockedUntil},
                updated_at = #{now},
                version = version + 1
            where o.event_id = #{eventId}
              and o.version = #{currentVersion}
              and (
                (o.status in ('INIT', 'FAILED') and retry_count < max_retry_count)
                or (o.status = 'PROCESSING' and (o.locked_until < #{now} or o.locked_until is null))
              )
              and exists (
                select 1
                from recharge_order_ext ext
                where ext.order_id = o.order_id
                  and ext.org_code = o.org_code
                  and ext.out_order_sn = o.out_order_sn
                  and ext.persist_status = 'DB_PERSISTED'
              )
            """)
    int claimProcessingSql(
            @Param("eventId") String eventId,
            @Param("currentVersion") long currentVersion,
            @Param("workerId") String workerId,
            @Param("lockedUntil") LocalDateTime lockedUntil,
            @Param("now") LocalDateTime now);
}

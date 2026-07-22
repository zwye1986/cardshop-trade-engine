package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.domain.order.OutboxEventType;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeDispatchOutboxDO;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * 供应商履约可靠事件表 {@code recharge_dispatch_outbox} Mapper。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」「8.3 异步落库 Consumer」。
 * 新旧系统共享 {@code mc_order} 期间，扫描与抢占都必须通过 {@code recharge_order_ext} 确认
 * {@code persist_status = DB_PERSISTED} 归属边界（详细设计「9.1」归属哨兵）。
 *
 * <p>本 Mapper 只服务履约生命周期，不处理余额刷账；余额事件见
 * {@link RechargeBalanceDeltaOutboxMapper}（详细设计「9.2 余额 Delta 刷账 Outbox」）。
 * 高可用扫描/租约语义见 {@code docs/交行直充业务系统高可用方案设计.md}「17.2 分片规则」。
 */
@Mapper
public interface RechargeDispatchOutboxMapper extends BaseMapper<RechargeDispatchOutboxDO> {

    /**
     * 幂等写入派单履约事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」。
     * Persist Consumer 可能因 RabbitMQ 重投重复处理同一订单；{@code SUPPLIER_DISPATCH} 等事件必须按
     * {@code event_id} 幂等插入，避免唯一键冲突被误判为数据库故障。
     */
    @Insert("""
            insert into recharge_dispatch_outbox (
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
    int insertIfAbsent(RechargeDispatchOutboxDO outbox);

    /**
     * 批量幂等写入派单履约事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」批写约束：
     * 必须调用显式 multi-row 方法 {@code insertDispatchOutboxBatchIfAbsent}，并单独上报 phase
     * {@code recharge_dispatch_outbox}。余额刷账事件不得混入本批写入。
     */
    @Insert("""
            <script>
            insert into recharge_dispatch_outbox (
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
            values
            <foreach collection="rows" item="row" separator=",">
            (
              #{row.eventId},
              #{row.eventType},
              #{row.orgCode},
              #{row.orderId},
              #{row.outOrderSn},
              #{row.payload},
              #{row.status},
              #{row.retryCount},
              #{row.maxRetryCount},
              #{row.nextRetryTime},
              #{row.lockedBy},
              #{row.lockedUntil},
              #{row.version},
              #{row.lastError},
              #{row.shardKey},
              #{row.createdAt},
              #{row.updatedAt}
            )
            </foreach>
            on duplicate key update
              event_id = event_id
            </script>
            """)
    int insertDispatchOutboxBatchIfAbsent(@Param("rows") List<RechargeDispatchOutboxDO> rows);

    /**
     * 汇总派单 outbox 未完成事件数量和最大滞留时间。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」可观测性要求，以及
     * {@code docs/交行直充业务系统高可用方案设计.md} 监控低基数标签约束。只按
     * {@code event_type}/{@code status} 聚合，不返回订单号或商户号。
     */
    @Select("""
            select
              o.event_type,
              o.status,
              count(*) as count,
              coalesce(timestampdiff(second, min(coalesce(o.created_at, o.updated_at, #{now})), #{now}), 0) as max_age_seconds,
              coalesce(timestampdiff(second, min(case when o.next_retry_time <= #{now} then o.next_retry_time end), #{now}), 0) as max_age_since_due_seconds
            from recharge_dispatch_outbox o
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
    List<Map<String, Object>> selectObservabilityDispatchOutboxMetrics(@Param("now") LocalDateTime now);

    /**
     * 扫描当前分片到期/租约失效的履约事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」：首期只扫描
     * {@code SUPPLIER_DISPATCH}/{@code SUPPLIER_QUERY}/{@code INVALID_ORDER}。
     */
    default List<RechargeDispatchOutboxDO> selectDueForShard(int shardTotal, int shardIndex, int limit) {
        return selectDueForShardByEventTypes(
                shardTotal,
                shardIndex,
                limit,
                List.of(OutboxEventType.SUPPLIER_DISPATCH.name()));
    }

    /**
     * 按事件类型扫描到期事件和租约失效事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。拆表后余额刷账已独立，
     * 本方法仍接受 eventTypes 参数以便测试注入；生产默认路径只传入履约三类事件。
     */
    default List<RechargeDispatchOutboxDO> selectDueForShardByEventTypes(
            int shardTotal,
            int shardIndex,
            int limit,
            List<String> eventTypes) {
        return selectDueForShardByEventTypes(shardTotal, shardIndex, limit, eventTypes, LocalDateTime.now());
    }

    /**
     * 使用调用方注入的时钟扫描到期事件，便于固定 Clock 的测试和生产排障统一时间来源。
     */
    default List<RechargeDispatchOutboxDO> selectDueForShardByEventTypes(
            int shardTotal,
            int shardIndex,
            int limit,
            List<String> eventTypes,
            LocalDateTime now) {
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        return selectDueForShardByEventTypesSql(shardTotal, shardIndex, safeLimit, eventTypes, now);
    }

    /**
     * 扫描新交易引擎拥有的到期履约事件和租约失效事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」：
     * <ul>
     *   <li>归属哨兵：{@code recharge_order_ext} 同键且 {@code DB_PERSISTED}；</li>
     *   <li>{@code next_retry_time} 仅作执行资格条件；</li>
     *   <li>可执行集合按 {@code created_at ASC, id ASC} 保证先生成先处理；</li>
     *   <li>分片使用 {@code shard_key}（固定等于 event_id）的 crc32 取模。</li>
     * </ul>
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
            from recharge_dispatch_outbox o
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
    List<RechargeDispatchOutboxDO> selectDueForShardByEventTypesSql(
            @Param("shardTotal") int shardTotal,
            @Param("shardIndex") int shardIndex,
            @Param("limit") int limit,
            @Param("eventTypes") List<String> eventTypes,
            @Param("now") LocalDateTime now);

    /**
     * 基于事件 ID 和版本号抢占履约事件。
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
     * 基于事件 ID、版本号和订单归属哨兵抢占履约事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」。抢占阶段重复校验
     * {@code recharge_order_ext}，避免扫描后扩展状态变化或绕过扫描的调用方处理旧系统订单。
     * 租约秒数由 {@code trade.engine.fulfillment.outbox-claim-lease-seconds} 控制
     *（高可用方案「17.2 分片规则」）。
     */
    @Update("""
            update recharge_dispatch_outbox o
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

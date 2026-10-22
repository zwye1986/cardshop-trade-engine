package com.cardshop.trade.engine.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeBalanceDeltaOutboxDO;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * 余额增量刷账事件表 {@code recharge_balance_delta_outbox} Mapper。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.2 余额 Delta 刷账 Outbox」「8.3 异步落库 Consumer」。
 * 写入使用结构化 {@code delta_type}/{@code delta_amount}，不再写 JSON payload。
 * 扫描、CAS 抢占后按 {@code org_code} 聚合刷入 {@code mc_ext_depart.balance} 的编排在
 * {@code MybatisRechargeBalanceDeltaFlushAdapter}；本 Mapper 只提供表级 SQL。
 *
 * <p>对账与刷账协议见 {@code docs/交行直充业务系统高可用方案设计.md}
 * 「14.7 {@code mc_ext_depart.balance} 刷账协议」「17.5 余额对账任务」。
 * 归属哨兵与派单表一致：必须存在 {@code recharge_order_ext.persist_status = DB_PERSISTED}。
 */
@Mapper
public interface RechargeBalanceDeltaOutboxMapper extends BaseMapper<RechargeBalanceDeltaOutboxDO> {
    int FUNDS_QUERY_TIMEOUT_SECONDS = 5;

    /**
     * MINISO释放收尾在主事务中当前锁读已有delta，只核对资金身份，不重置刷账进度。
     * 参见 {@code docs/交行直充业务系统详细设计.md}「9.8」；不以插入影响行数代替一致性证明。
     */
    @Select("""
            select event_id, org_code, order_id, out_order_sn, delta_type, delta_amount
            from recharge_balance_delta_outbox where event_id = #{eventId} for update
            """)
    RechargeBalanceDeltaOutboxDO lockByEventId(@Param("eventId") String eventId);

    /**
     * 幂等写入余额刷账事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」「9.2 余额 Delta 刷账 Outbox」。
     * 稳定 {@code event_id} 规则：预扣 {@code balance-flush-{orderId}}，释放
     * {@code balance-flush-release-{orderId}}。重复 MQ/补投不得产生重复刷账行。
     */
    @Insert("""
            insert into recharge_balance_delta_outbox (
              event_id,
              org_code,
              order_id,
              out_order_sn,
              delta_type,
              delta_amount,
              status,
              retry_count,
              max_retry_count,
              next_retry_time,
              locked_by,
              locked_until,
              version,
              last_error,
              created_at,
              updated_at
            )
            values (
              #{eventId},
              #{orgCode},
              #{orderId},
              #{outOrderSn},
              #{deltaType},
              #{deltaAmount},
              #{status},
              #{retryCount},
              #{maxRetryCount},
              #{nextRetryTime},
              #{lockedBy},
              #{lockedUntil},
              #{version},
              #{lastError},
              #{createdAt},
              #{updatedAt}
            )
            on duplicate key update
              event_id = event_id
            """)
    int insertIfAbsent(RechargeBalanceDeltaOutboxDO outbox);

    /** 资金入口在订单锁内核对不可变Delta内容；参见详细设计独立资金协议。 */
    @org.apache.ibatis.annotations.Options(timeout = FUNDS_QUERY_TIMEOUT_SECONDS)
    @Select("""
            select event_id as eventId, org_code as orgCode, order_id as orderId,
              out_order_sn as outOrderSn, delta_type as deltaType, delta_amount as deltaAmount,
              status from recharge_balance_delta_outbox where event_id=#{eventId} for update
            """)
    RechargeBalanceDeltaOutboxDO selectFundsEventForUpdate(@Param("eventId") String eventId);

    /**
     * 批量幂等写入余额刷账事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「8.3 异步落库 Consumer」批写约束：
     * 必须调用显式 multi-row 方法 {@code insertBalanceDeltaBatchIfAbsent}，并单独上报 phase
     * {@code recharge_balance_delta_outbox}。写表顺序在派单表之后。
     */
    @Insert("""
            <script>
            insert into recharge_balance_delta_outbox (
              event_id,
              org_code,
              order_id,
              out_order_sn,
              delta_type,
              delta_amount,
              status,
              retry_count,
              max_retry_count,
              next_retry_time,
              locked_by,
              locked_until,
              version,
              last_error,
              created_at,
              updated_at
            )
            values
            <foreach collection="rows" item="row" separator=",">
            (
              #{row.eventId},
              #{row.orgCode},
              #{row.orderId},
              #{row.outOrderSn},
              #{row.deltaType},
              #{row.deltaAmount},
              #{row.status},
              #{row.retryCount},
              #{row.maxRetryCount},
              #{row.nextRetryTime},
              #{row.lockedBy},
              #{row.lockedUntil},
              #{row.version},
              #{row.lastError},
              #{row.createdAt},
              #{row.updatedAt}
            )
            </foreach>
            on duplicate key update
              event_id = event_id
            </script>
            """)
    int insertBalanceDeltaBatchIfAbsent(@Param("rows") List<RechargeBalanceDeltaOutboxDO> rows);

    /**
     * 汇总指定商户尚未 SUCCESS 的余额 delta。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.2 余额 Delta 刷账 Outbox」与
     * {@code docs/交行直充业务系统高可用方案设计.md}「17.5 余额对账任务」。
     * 对账只汇总本表未终态 delta，不再扫描派单表或旧 {@code recharge_outbox}。
     */
    @Select("""
            select coalesce(sum(o.delta_amount), 0)
            from recharge_balance_delta_outbox o
            where o.org_code = #{orgCode}
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
     * 汇总余额刷账未完成事件数量和最大滞留时间。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.2」可观测性：只按 {@code status} 聚合，
     * 禁止把 {@code org_code} 做成 Prometheus 高基数标签；按商户排障用 SQL 临时采样。
     */
    @Select("""
            select
              o.status,
              count(*) as count,
              coalesce(timestampdiff(second, min(coalesce(o.created_at, o.updated_at, #{now})), #{now}), 0) as max_age_seconds
            from recharge_balance_delta_outbox o
            where o.status in ('INIT', 'PROCESSING', 'FAILED', 'MANUAL_REQUIRED')
              and exists (
                select 1
                from recharge_order_ext ext
                where ext.order_id = o.order_id
                  and ext.org_code = o.org_code
                  and ext.out_order_sn = o.out_order_sn
                  and ext.persist_status = 'DB_PERSISTED'
              )
            group by o.status
            """)
    List<Map<String, Object>> selectObservabilityBalanceDeltaMetrics(@Param("now") LocalDateTime now);

    /**
     * 扫描当前分片到期/租约失效的余额刷账事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.2 余额 Delta 刷账 Outbox」：
     * 先按 {@code status + next_retry_time} 找到期事件，并按 {@code status + locked_until}
     * 恢复租约过期的 {@code PROCESSING}；残余分片谓词使用 {@code crc32(event_id)}。
     */
    default List<RechargeBalanceDeltaOutboxDO> selectDueForFlush(
            int shardTotal, int shardIndex, int limit, LocalDateTime now) {
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        return selectDueForFlushSql(shardTotal, shardIndex, safeLimit, now);
    }

    /**
     * 扫描归属本引擎且到期/租约失效的余额刷账事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.2 余额 Delta 刷账 Outbox」。
     * 批大小默认受 {@code trade.engine.outbox.balance-delta-flush-batch-size} 约束
     *（详细设计「11. 配置中心」outbox 配置族）。
     */
    @Select("""
            <script>
            select
              o.id,
              o.event_id,
              o.org_code,
              o.order_id,
              o.out_order_sn,
              o.delta_type,
              o.delta_amount,
              o.status,
              o.retry_count,
              o.max_retry_count,
              o.next_retry_time,
              o.locked_by,
              o.locked_until,
              o.version,
              o.last_error,
              o.created_at,
              o.updated_at
            from recharge_balance_delta_outbox o
            where (
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
              and mod(crc32(o.event_id), #{shardTotal}) = #{shardIndex}
            order by o.created_at asc, o.id asc
            limit #{limit}
            </script>
            """)
    List<RechargeBalanceDeltaOutboxDO> selectDueForFlushSql(
            @Param("shardTotal") int shardTotal,
            @Param("shardIndex") int shardIndex,
            @Param("limit") int limit,
            @Param("now") LocalDateTime now);

    /**
     * 基于事件 ID 和版本号抢占余额刷账事件。
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
     * 基于事件 ID、版本号和订单归属哨兵抢占余额刷账事件。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「9.2 余额 Delta 刷账 Outbox」。
     * 租约秒数由 {@code trade.engine.outbox.balance-delta-claim-lease-seconds} 控制，必须覆盖单轮
     * 刷账事务 P99（高可用方案「14.7」）。
     */
    @Update("""
            update recharge_balance_delta_outbox o
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

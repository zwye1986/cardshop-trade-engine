package com.cardshop.trade.engine.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.time.LocalDateTime;

/**
 * 余额增量刷账事件表 {@code recharge_balance_delta_outbox} 行映射。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.2 余额 Delta 刷账 Outbox」与「14. 字段字典」中
 * {@code recharge_balance_delta_outbox} 段；刷账协议与租约/退避见
 * {@code docs/交行直充业务系统高可用方案设计.md}「14.7 {@code mc_ext_depart.balance} 刷账协议」。
 *
 * <p>本表只承载余额沉淀事件，使用结构化字段 {@code delta_type}/{@code delta_amount}，
 * 不再保存通用事件类型、JSON 载荷或分片键列，以降低订单落库事务中的 JSON 写入和无效通用索引成本
 *（详细设计「9.2」）。供应商履约事件属于 {@link RechargeDispatchOutboxDO}
 *（详细设计「9.1 派单 Outbox」）。
 *
 * <p>字段 COMMENT 与 Liquibase changeSet {@code 0015-d2-create-recharge-balance-delta-outbox} 保持一致。
 * {@code PRE_DEDUCT} 的 {@code delta_amount} 为负，{@code RELEASE} 为正，单位分。
 */
@TableName("recharge_balance_delta_outbox")
public class RechargeBalanceDeltaOutboxDO {

    /** 表主键；DDL COMMENT: 表主键 */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /** 余额刷账事件唯一ID；DDL COMMENT: 余额刷账事件唯一ID；唯一键 uk_balance_delta_event_id */
    @TableField("event_id")
    private String eventId;

    /** 商户编码；DDL COMMENT: 商户编码；刷账按商户聚合的主键维度 */
    @TableField("org_code")
    private String orgCode;

    /** 平台订单号；DDL COMMENT: 平台订单号 */
    @TableField("order_id")
    private String orderId;

    /** 商户订单号；DDL COMMENT: 商户订单号 */
    @TableField("out_order_sn")
    private String outOrderSn;

    /** 余额变动类型；DDL COMMENT: 余额变动类型，例如 PRE_DEDUCT 或 RELEASE */
    @TableField("delta_type")
    private String deltaType;

    /** 余额变动金额（分）；DDL COMMENT: 余额变动金额，单位分；扣减为负，释放为正 */
    @TableField("delta_amount")
    private Long deltaAmount;

    /** 刷账状态；DDL COMMENT: 刷账状态 */
    @TableField("status")
    private String status;

    /** 已重试次数；DDL COMMENT: 已重试次数 */
    @TableField("retry_count")
    private Integer retryCount;

    /** 最大重试次数；DDL COMMENT: 最大重试次数 */
    @TableField("max_retry_count")
    private Integer maxRetryCount;

    /** 下次允许执行时间；DDL COMMENT: 下次允许执行时间 */
    @TableField("next_retry_time")
    private LocalDateTime nextRetryTime;

    /** 当前抢占该事件的应用实例ID；DDL COMMENT: 当前抢占该事件的应用实例ID */
    @TableField("locked_by")
    private String lockedBy;

    /** 抢占租约到期时间；DDL COMMENT: 抢占租约到期时间；配合 idx_balance_delta_lease 回收 */
    @TableField("locked_until")
    private LocalDateTime lockedUntil;

    /** 乐观锁版本号；DDL COMMENT: 乐观锁版本号 */
    @TableField("version")
    private Long version;

    /** 最近一次失败原因；DDL COMMENT: 最近一次失败原因 */
    @TableField("last_error")
    private String lastError;

    /** 创建时间；DDL COMMENT: 创建时间 */
    @TableField("created_at")
    private LocalDateTime createdAt;

    /** 更新时间；DDL COMMENT: 更新时间 */
    @TableField("updated_at")
    private LocalDateTime updatedAt;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getOrgCode() {
        return orgCode;
    }

    public void setOrgCode(String orgCode) {
        this.orgCode = orgCode;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOutOrderSn() {
        return outOrderSn;
    }

    public void setOutOrderSn(String outOrderSn) {
        this.outOrderSn = outOrderSn;
    }

    public String getDeltaType() {
        return deltaType;
    }

    public void setDeltaType(String deltaType) {
        this.deltaType = deltaType;
    }

    public Long getDeltaAmount() {
        return deltaAmount;
    }

    public void setDeltaAmount(Long deltaAmount) {
        this.deltaAmount = deltaAmount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
    }

    public Integer getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(Integer maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }

    public LocalDateTime getNextRetryTime() {
        return nextRetryTime;
    }

    public void setNextRetryTime(LocalDateTime nextRetryTime) {
        this.nextRetryTime = nextRetryTime;
    }

    public String getLockedBy() {
        return lockedBy;
    }

    public void setLockedBy(String lockedBy) {
        this.lockedBy = lockedBy;
    }

    public LocalDateTime getLockedUntil() {
        return lockedUntil;
    }

    public void setLockedUntil(LocalDateTime lockedUntil) {
        this.lockedUntil = lockedUntil;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}

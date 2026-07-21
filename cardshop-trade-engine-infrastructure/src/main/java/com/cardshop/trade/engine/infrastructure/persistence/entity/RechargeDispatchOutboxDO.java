package com.cardshop.trade.engine.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.time.LocalDateTime;

/**
 * 供应商履约可靠事件表 {@code recharge_dispatch_outbox} 行映射。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.1 派单 Outbox」与「14. 字段字典」中
 * {@code recharge_dispatch_outbox} 段；高可用约束见
 * {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」拆表后的派单表职责。
 *
 * <p>本表只承载供应商履约生命周期事件：{@code SUPPLIER_DISPATCH}、{@code SUPPLIER_QUERY}、
 * {@code INVALID_ORDER}。余额刷账事件不得写入本表，应进入
 * {@link RechargeBalanceDeltaOutboxDO}（详细设计「9.2 余额 Delta 刷账 Outbox」）。
 * 旧混合表 {@code recharge_outbox} 仅作未上线历史实现，目标代码不得再写入。
 *
 * <p>字段 COMMENT 与 Liquibase changeSet {@code 0014-d2-create-recharge-dispatch-outbox} 保持一致。
 */
@TableName("recharge_dispatch_outbox")
public class RechargeDispatchOutboxDO {

    /** 表主键；DDL COMMENT: 表主键 */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /** 履约事件唯一ID；DDL COMMENT: 履约事件唯一ID；唯一键 uk_dispatch_outbox_event_id */
    @TableField("event_id")
    private String eventId;

    /** 履约事件类型；DDL COMMENT: 履约事件类型 */
    @TableField("event_type")
    private String eventType;

    /** 商户编码；DDL COMMENT: 商户编码 */
    @TableField("org_code")
    private String orgCode;

    /** 平台订单号；DDL COMMENT: 平台订单号 */
    @TableField("order_id")
    private String orderId;

    /** 商户订单号；DDL COMMENT: 商户订单号 */
    @TableField("out_order_sn")
    private String outOrderSn;

    /** 履约事件参数 JSON；DDL COMMENT: 履约事件参数JSON；不得包含密钥 */
    @TableField("payload")
    private String payload;

    /** 事件状态；DDL COMMENT: 事件状态 */
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

    /** 抢占租约到期时间；DDL COMMENT: 抢占租约到期时间 */
    @TableField("locked_until")
    private LocalDateTime lockedUntil;

    /** 乐观锁版本号；DDL COMMENT: 乐观锁版本号 */
    @TableField("version")
    private Long version;

    /** 最近一次失败原因；DDL COMMENT: 最近一次失败原因 */
    @TableField("last_error")
    private String lastError;

    /**
     * 分片键；DDL COMMENT: 固定使用event_id，避免不同任务分片语义冲突。
     *
     * <p>详见详细设计「9.1 派单 Outbox」分片与扫描规则。
     */
    @TableField("shard_key")
    private String shardKey;

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

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
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

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
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

    public String getShardKey() {
        return shardKey;
    }

    public void setShardKey(String shardKey) {
        this.shardKey = shardKey;
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

package com.cardshop.trade.engine.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.time.LocalDateTime;

/**
 * 新链路订单扩展表 `recharge_order_ext`。
 *
 * <p>字段对应 {@code docs/交行直充业务系统高可用方案设计.md} 的「6.6 新增表」。
 * 该表保存Redis/MQ/MySQL一致性、履约状态及独立券事实；充值订单状态以mc_order为准，
 * 核销/作废状态和事实时间独立保存，参见详细设计9.6，不能把作废混为订单失败。
 */
@TableName("recharge_order_ext")
public class RechargeOrderExtDO {

    /** 供应商批次号快照；DDL COMMENT同名。参见详细设计名创实施约束。 */
    @TableField("supplier_batch_id")
    private String supplierBatchId;

    public String getSupplierBatchId() { return supplierBatchId; }
    public void setSupplierBatchId(String value) { supplierBatchId = value; }


    /**
     * 表主键。
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 平台订单号，对应 `mc_order.order_Id`。
     */
    @TableField("order_id")
    private String orderId;

    /**
     * 商户编码。
     */
    @TableField("org_code")
    private String orgCode;

    /**
     * 商户订单号，对应 `mc_order.out_orderId`。
     */
    @TableField("out_order_sn")
    private String outOrderSn;

    /**
     * 标准化请求摘要。
     */
    @TableField("request_hash")
    private String requestHash;

    /**
     * 请求摘要规范版本。
     */
    @TableField("request_hash_version")
    private String requestHashVersion;

    /**
     * 请求摘要算法。
     */
    @TableField("request_hash_alg")
    private String requestHashAlg;

    /**
     * Redis 订单 key 或 Redis 侧订单标识。
     */
    @TableField("redis_order_id")
    private String redisOrderId;

    /**
     * RabbitMQ 落库消息 ID。
     */
    @TableField("persist_msg_id")
    private String persistMsgId;

    /**
     * Redis/MQ/MySQL 持久化状态。
     */
    @TableField("persist_status")
    private String persistStatus;

    /**
     * MySQL 落库使用的快照来源。
     */
    @TableField("persist_source")
    private String persistSource;

    /**
     * Redis 当前订单版本。
     */
    @TableField("redis_order_version")
    private Long redisOrderVersion;

    /**
     * 已成功写入 MySQL 的订单版本。
     */
    @TableField("persisted_version")
    private Long persistedVersion;

    /**
     * 业务校验状态。
     */
    @TableField("validation_status")
    private String validationStatus;

    /**
     * 是否允许供应商派单。
     */
    @TableField("dispatch_allowed")
    private Boolean dispatchAllowed;

    /**
     * 是否已完成 Redis 余额预扣。
     */
    @TableField("balance_reserved")
    private Boolean balanceReserved;

    /**
     * 业务履约状态。
     */
    @TableField("dispatch_status")
    private String dispatchStatus;

    /** 交行券状态；参见详细设计9.6，DDL COMMENT同名。 */
    @TableField("voucher_use_state")
    private String voucherUseState;
    /** 核销时间；参见详细设计9.6，DDL COMMENT同名。 */
    @TableField("voucher_use_time")
    private LocalDateTime voucherUseTime;
    /** 作废完成时间；参见详细设计9.6，DDL COMMENT同名。 */
    @TableField("voucher_invalid_time")
    private LocalDateTime voucherInvalidTime;

    public String getVoucherUseState() { return voucherUseState; }
    public void setVoucherUseState(String value) { voucherUseState = value; }
    public LocalDateTime getVoucherUseTime() { return voucherUseTime; }
    public void setVoucherUseTime(LocalDateTime value) { voucherUseTime = value; }
    public LocalDateTime getVoucherInvalidTime() { return voucherInvalidTime; }
    public void setVoucherInvalidTime(LocalDateTime value) { voucherInvalidTime = value; }

    /**
     * 落库消息发布次数。
     */
    @TableField("mq_publish_count")
    private Integer mqPublishCount;

    /**
     * Redis 到 MySQL 异步落库重试次数。
     */
    @TableField("persist_retry_count")
    private Integer persistRetryCount;

    /**
     * 最近一次持久化失败原因。
     */
    @TableField("last_error")
    private String lastError;

    /**
     * 创建时间。
     */
    @TableField("created_at")
    private LocalDateTime createdAt;

    /**
     * 更新时间。
     */
    @TableField("updated_at")
    private LocalDateTime updatedAt;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOrgCode() {
        return orgCode;
    }

    public void setOrgCode(String orgCode) {
        this.orgCode = orgCode;
    }

    public String getOutOrderSn() {
        return outOrderSn;
    }

    public void setOutOrderSn(String outOrderSn) {
        this.outOrderSn = outOrderSn;
    }

    public String getRequestHash() {
        return requestHash;
    }

    public void setRequestHash(String requestHash) {
        this.requestHash = requestHash;
    }

    public String getRequestHashVersion() {
        return requestHashVersion;
    }

    public void setRequestHashVersion(String requestHashVersion) {
        this.requestHashVersion = requestHashVersion;
    }

    public String getRequestHashAlg() {
        return requestHashAlg;
    }

    public void setRequestHashAlg(String requestHashAlg) {
        this.requestHashAlg = requestHashAlg;
    }

    public String getRedisOrderId() {
        return redisOrderId;
    }

    public void setRedisOrderId(String redisOrderId) {
        this.redisOrderId = redisOrderId;
    }

    public String getPersistMsgId() {
        return persistMsgId;
    }

    public void setPersistMsgId(String persistMsgId) {
        this.persistMsgId = persistMsgId;
    }

    public String getPersistStatus() {
        return persistStatus;
    }

    public void setPersistStatus(String persistStatus) {
        this.persistStatus = persistStatus;
    }

    public String getPersistSource() {
        return persistSource;
    }

    public void setPersistSource(String persistSource) {
        this.persistSource = persistSource;
    }

    public Long getRedisOrderVersion() {
        return redisOrderVersion;
    }

    public void setRedisOrderVersion(Long redisOrderVersion) {
        this.redisOrderVersion = redisOrderVersion;
    }

    public Long getPersistedVersion() {
        return persistedVersion;
    }

    public void setPersistedVersion(Long persistedVersion) {
        this.persistedVersion = persistedVersion;
    }

    public String getValidationStatus() {
        return validationStatus;
    }

    public void setValidationStatus(String validationStatus) {
        this.validationStatus = validationStatus;
    }

    public Boolean getDispatchAllowed() {
        return dispatchAllowed;
    }

    public void setDispatchAllowed(Boolean dispatchAllowed) {
        this.dispatchAllowed = dispatchAllowed;
    }

    public Boolean getBalanceReserved() {
        return balanceReserved;
    }

    public void setBalanceReserved(Boolean balanceReserved) {
        this.balanceReserved = balanceReserved;
    }

    public String getDispatchStatus() {
        return dispatchStatus;
    }

    public void setDispatchStatus(String dispatchStatus) {
        this.dispatchStatus = dispatchStatus;
    }

    public Integer getMqPublishCount() {
        return mqPublishCount;
    }

    public void setMqPublishCount(Integer mqPublishCount) {
        this.mqPublishCount = mqPublishCount;
    }

    public Integer getPersistRetryCount() {
        return persistRetryCount;
    }

    public void setPersistRetryCount(Integer persistRetryCount) {
        this.persistRetryCount = persistRetryCount;
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

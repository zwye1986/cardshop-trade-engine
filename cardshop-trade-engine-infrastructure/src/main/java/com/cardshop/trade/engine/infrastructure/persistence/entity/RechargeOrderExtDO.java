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
 * 该表只保存 Redis/MQ/MySQL 一致性状态，订单最终业务事实仍以 `mc_order` 为准。
 */
@TableName("recharge_order_ext")
public class RechargeOrderExtDO {

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    @TableField("order_id")
    private String orderId;

    @TableField("org_code")
    private String orgCode;

    @TableField("out_order_sn")
    private String outOrderSn;

    @TableField("request_hash")
    private String requestHash;

    @TableField("request_hash_version")
    private String requestHashVersion;

    @TableField("request_hash_alg")
    private String requestHashAlg;

    @TableField("redis_order_id")
    private String redisOrderId;

    @TableField("persist_msg_id")
    private String persistMsgId;

    @TableField("persist_status")
    private String persistStatus;

    @TableField("persist_source")
    private String persistSource;

    @TableField("redis_order_version")
    private Long redisOrderVersion;

    @TableField("persisted_version")
    private Long persistedVersion;

    @TableField("validation_status")
    private String validationStatus;

    @TableField("dispatch_allowed")
    private Boolean dispatchAllowed;

    @TableField("balance_reserved")
    private Boolean balanceReserved;

    @TableField("mq_publish_count")
    private Integer mqPublishCount;

    @TableField("persist_retry_count")
    private Integer persistRetryCount;

    @TableField("last_error")
    private String lastError;

    @TableField("created_at")
    private LocalDateTime createdAt;

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

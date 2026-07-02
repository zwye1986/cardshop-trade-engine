package com.cardshop.trade.engine.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.time.LocalDateTime;

/**
 * Redis 受理状态未知异常表 `recharge_accept_anomaly`。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「20.13.6 Redis Lua 下单结果」。
 * 该表不是订单事实表，只记录 WAIT 失败且无法确认 Redis 脚本结果等需要核查的窗口。
 */
@TableName("recharge_accept_anomaly")
public class RechargeAcceptAnomalyDO {

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    @TableField("anomaly_id")
    private String anomalyId;

    @TableField("org_code")
    private String orgCode;

    @TableField("out_order_sn")
    private String outOrderSn;

    @TableField("order_id")
    private String orderId;

    @TableField("request_hash")
    private String requestHash;

    @TableField("persist_msg_id")
    private String persistMsgId;

    @TableField("anomaly_type")
    private String anomalyType;

    @TableField("status")
    private String status;

    @TableField("raw_context")
    private String rawContext;

    @TableField("redis_check_result")
    private String redisCheckResult;

    @TableField("mysql_check_result")
    private String mysqlCheckResult;

    @TableField("mq_check_result")
    private String mqCheckResult;

    @TableField("resolution")
    private String resolution;

    @TableField("resolved_by")
    private String resolvedBy;

    @TableField("resolved_at")
    private LocalDateTime resolvedAt;

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

    public String getAnomalyId() {
        return anomalyId;
    }

    public void setAnomalyId(String anomalyId) {
        this.anomalyId = anomalyId;
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

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getRequestHash() {
        return requestHash;
    }

    public void setRequestHash(String requestHash) {
        this.requestHash = requestHash;
    }

    public String getPersistMsgId() {
        return persistMsgId;
    }

    public void setPersistMsgId(String persistMsgId) {
        this.persistMsgId = persistMsgId;
    }

    public String getAnomalyType() {
        return anomalyType;
    }

    public void setAnomalyType(String anomalyType) {
        this.anomalyType = anomalyType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getRawContext() {
        return rawContext;
    }

    public void setRawContext(String rawContext) {
        this.rawContext = rawContext;
    }

    public String getRedisCheckResult() {
        return redisCheckResult;
    }

    public void setRedisCheckResult(String redisCheckResult) {
        this.redisCheckResult = redisCheckResult;
    }

    public String getMysqlCheckResult() {
        return mysqlCheckResult;
    }

    public void setMysqlCheckResult(String mysqlCheckResult) {
        this.mysqlCheckResult = mysqlCheckResult;
    }

    public String getMqCheckResult() {
        return mqCheckResult;
    }

    public void setMqCheckResult(String mqCheckResult) {
        this.mqCheckResult = mqCheckResult;
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public String getResolvedBy() {
        return resolvedBy;
    }

    public void setResolvedBy(String resolvedBy) {
        this.resolvedBy = resolvedBy;
    }

    public LocalDateTime getResolvedAt() {
        return resolvedAt;
    }

    public void setResolvedAt(LocalDateTime resolvedAt) {
        this.resolvedAt = resolvedAt;
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

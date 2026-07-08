package com.cardshop.trade.engine.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.time.LocalDateTime;

/**
 * 旧系统渠道调用流水表 `mc_order_supplier_flow`。
 *
 * <p>字段 COMMENT 对齐旧卡管领域对象：订单号、接口外部订单号、渠道、下单状态、请求参数、响应结果。
 * 参见 {@code docs/交行直充业务系统高可用方案设计.md}「6.6 新增表」中供应商派单流水说明。
 */
@TableName("mc_order_supplier_flow")
public class McOrderSupplierFlowDO {

    /**
     * 表主键。
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 订单号，对应旧表字段 `order_Id`。
     */
    @TableField("order_Id")
    private String orderId;

    /**
     * 接口外部订单号。
     */
    @TableField("out_order_no")
    private String outOrderNo;

    /**
     * 渠道。
     */
    @TableField("channel")
    private String channel;

    /**
     * 下单状态 new success fail processing。
     */
    @TableField("status")
    private String status;

    /**
     * 备注信息，记录渠道调用失败原因或补充说明。
     */
    @TableField("remark")
    private String remark;

    /**
     * 请求参数，JSON 格式完整入参。
     */
    @TableField("request_param")
    private String requestParam;

    /**
     * 响应结果，JSON 格式完整出参。
     */
    @TableField("response_result")
    private String responseResult;

    /**
     * 创建时间。
     */
    @TableField("create_time")
    private LocalDateTime createTime;

    /**
     * 更新时间。
     */
    @TableField("update_time")
    private LocalDateTime updateTime;

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

    public String getOutOrderNo() {
        return outOrderNo;
    }

    public void setOutOrderNo(String outOrderNo) {
        this.outOrderNo = outOrderNo;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getRequestParam() {
        return requestParam;
    }

    public void setRequestParam(String requestParam) {
        this.requestParam = requestParam;
    }

    public String getResponseResult() {
        return responseResult;
    }

    public void setResponseResult(String responseResult) {
        this.responseResult = responseResult;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }
}

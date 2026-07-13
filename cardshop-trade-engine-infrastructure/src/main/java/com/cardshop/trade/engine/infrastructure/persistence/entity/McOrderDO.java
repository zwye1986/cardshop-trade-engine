package com.cardshop.trade.engine.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.time.LocalDateTime;

/**
 * 旧系统订单事实表 `mc_order` 的最小持久化对象。
 *
 * <p>字段映射对应 {@code docs/交行直充业务系统详细设计.md} 的「6.2 `mc_order` 字段说明」
 * 和「8. MySQL/Liquibase 持久化层」。旧表字段名保持 `order_Id/out_orderId/produt_code` 等历史命名，
 * 新服务代码侧使用规范 Java 属性名，并通过 MyBatis-Plus 注解显式映射。
 */
@TableName("mc_order")
public class McOrderDO {

    /**
     * 旧表自增主键。
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 商户编码。
     */
    @TableField("org_code")
    private String orgCode;

    /**
     * 创建时间。
     */
    @TableField("create_time")
    private LocalDateTime createTime;

    /**
     * 编辑时间。
     */
    @TableField("edit_time")
    private LocalDateTime editTime;

    /**
     * 商品编码，对应旧字段 `produt_code`。
     */
    @TableField("produt_code")
    private String productCode;

    /**
     * 外部请求 product_id。旧字段名为 parent_id，但新链路不再按父商品寻找 sibling 商品。
     */
    @TableField("parent_id")
    private String parentId;

    /**
     * 商品名称。
     */
    @TableField("product_name")
    private String productName;

    /**
     * 商品价格，单位分。
     */
    @TableField("product_price")
    private Long productPrice;

    /**
     * 商品面值，单位分。
     */
    @TableField("product_value")
    private Long productValue;

    /**
     * 商品类型，首期交行直充使用 `1`。
     */
    @TableField("product_type")
    private Integer productType;

    /**
     * 下单数量。
     */
    @TableField("num")
    private Integer num;

    /**
     * 平台订单号，对应旧字段 `order_Id`。
     */
    @TableField("order_Id")
    private String orderId;

    /**
     * 商户订单号，对应旧字段 `out_orderId`。
     */
    @TableField("out_orderId")
    private String outOrderId;

    /**
     * 旧链路外部订单号字段。
     */
    @TableField("out_order_no")
    private String outOrderNo;

    /**
     * 订单金额，单位分。
     */
    @TableField("order_price")
    private Long orderPrice;

    /**
     * 退款金额，单位分。
     */
    @TableField("refund_order_price")
    private Long refundOrderPrice;

    /**
     * 旧系统业务订单状态。
     */
    @TableField("order_status")
    private Integer orderStatus;

    /**
     * 新链路固定写 `PROCESSED`，真实持久化状态写 `recharge_order_ext.persist_status`。
     */
    @TableField("data_status")
    private String dataStatus;

    /**
     * 商户回调地址。
     */
    @TableField("callback_url")
    private String callbackUrl;

    /**
     * 请求来源 IP。
     */
    @TableField("request_ip")
    private String requestIp;

    /**
     * 运营可读备注。
     */
    @TableField("remark")
    private String remark;

    /**
     * 供应商渠道。
     */
    @TableField("supplier_channel")
    private String supplierChannel;

    /**
     * 供应商流水主键，对应旧字段 `flow_id`。
     */
    @TableField("flow_id")
    private Long flowId;

    /**
     * 旧系统排障数据 JSON。
     */
    @TableField("res_data")
    private String resData;

    /**
     * 充值账号。
     */
    @TableField("account")
    private String account;

    /**
     * 卡券类型或直充兼容类型，沿用旧系统 `url/code/codekey` 等字符串语义。
     */
    @TableField("coupon_type")
    private String couponType;

    /**
     * 订单类型。
     */
    @TableField("order_type")
    private Integer orderType;

    /**
     * 业务或系统失败码，D2 新增兼容字段。
     */
    @TableField("fail_code")
    private String failCode;

    /**
     * 业务或系统失败原因，D2 新增兼容字段。
     */
    @TableField("fail_reason")
    private String failReason;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOrgCode() {
        return orgCode;
    }

    public void setOrgCode(String orgCode) {
        this.orgCode = orgCode;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public LocalDateTime getEditTime() {
        return editTime;
    }

    public void setEditTime(LocalDateTime editTime) {
        this.editTime = editTime;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Long getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(Long productPrice) {
        this.productPrice = productPrice;
    }

    public Long getProductValue() {
        return productValue;
    }

    public void setProductValue(Long productValue) {
        this.productValue = productValue;
    }

    public Integer getProductType() {
        return productType;
    }

    public void setProductType(Integer productType) {
        this.productType = productType;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOutOrderId() {
        return outOrderId;
    }

    public void setOutOrderId(String outOrderId) {
        this.outOrderId = outOrderId;
    }

    public String getOutOrderNo() {
        return outOrderNo;
    }

    public void setOutOrderNo(String outOrderNo) {
        this.outOrderNo = outOrderNo;
    }

    public Long getOrderPrice() {
        return orderPrice;
    }

    public void setOrderPrice(Long orderPrice) {
        this.orderPrice = orderPrice;
    }

    public Long getRefundOrderPrice() {
        return refundOrderPrice;
    }

    public void setRefundOrderPrice(Long refundOrderPrice) {
        this.refundOrderPrice = refundOrderPrice;
    }

    public Integer getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(Integer orderStatus) {
        this.orderStatus = orderStatus;
    }

    public String getDataStatus() {
        return dataStatus;
    }

    public void setDataStatus(String dataStatus) {
        this.dataStatus = dataStatus;
    }

    public String getCallbackUrl() {
        return callbackUrl;
    }

    public void setCallbackUrl(String callbackUrl) {
        this.callbackUrl = callbackUrl;
    }

    public String getRequestIp() {
        return requestIp;
    }

    public void setRequestIp(String requestIp) {
        this.requestIp = requestIp;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getSupplierChannel() {
        return supplierChannel;
    }

    public void setSupplierChannel(String supplierChannel) {
        this.supplierChannel = supplierChannel;
    }

    public Long getFlowId() {
        return flowId;
    }

    public void setFlowId(Long flowId) {
        this.flowId = flowId;
    }

    public String getResData() {
        return resData;
    }

    public void setResData(String resData) {
        this.resData = resData;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getCouponType() {
        return couponType;
    }

    public void setCouponType(String couponType) {
        this.couponType = couponType;
    }

    public Integer getOrderType() {
        return orderType;
    }

    public void setOrderType(Integer orderType) {
        this.orderType = orderType;
    }

    public String getFailCode() {
        return failCode;
    }

    public void setFailCode(String failCode) {
        this.failCode = failCode;
    }

    public String getFailReason() {
        return failReason;
    }

    public void setFailReason(String failReason) {
        this.failReason = failReason;
    }
}

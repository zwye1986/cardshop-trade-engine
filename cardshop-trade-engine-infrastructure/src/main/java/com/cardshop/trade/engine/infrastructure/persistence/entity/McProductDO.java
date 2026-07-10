package com.cardshop.trade.engine.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * 旧卡管商品表 `mc_product` 的最小持久化对象。
 *
 * <p>字段来源对应 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」。
 * 新链路用它补齐实际子商品名称、渠道、面值、底价、类型和配置版本。
 */
@TableName("mc_product")
public class McProductDO {

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    @TableField("status")
    private String status;

    @TableField("product_id")
    private String productId;

    @TableField("channel")
    private String channel;

    @TableField("coupon_type")
    private String couponType;

    @TableField("product_name")
    private String productName;

    @TableField("value")
    private Long value;

    @TableField("base_price")
    private Long basePrice;

    @TableField("type")
    private Integer type;

    @TableField("account_type")
    private Integer accountType;

    @TableField("version")
    private Long version;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getCouponType() {
        return couponType;
    }

    public void setCouponType(String couponType) {
        this.couponType = couponType;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public Long getBasePrice() {
        return basePrice;
    }

    public void setBasePrice(Long basePrice) {
        this.basePrice = basePrice;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Integer getAccountType() {
        return accountType;
    }

    public void setAccountType(Integer accountType) {
        this.accountType = accountType;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }
}

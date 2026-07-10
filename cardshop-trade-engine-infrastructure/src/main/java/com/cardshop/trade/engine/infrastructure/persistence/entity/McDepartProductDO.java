package com.cardshop.trade.engine.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * 旧卡管商户商品关系表 `mc_depart_product` 的最小持久化对象。
 *
 * <p>字段来源对应 {@code docs/交行直充业务系统详细设计.md}「5. Domain 状态与业务规则」。
 * 该表表达商户对父商品下某个子商品的销售权限和商户售价。
 */
@TableName("mc_depart_product")
public class McDepartProductDO {

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    @TableField("org_code")
    private String orgCode;

    @TableField("status")
    private String status;

    @TableField("product_id")
    private String productId;

    @TableField("price")
    private Long price;

    @TableField("sort")
    private Long sort;

    @TableField("parent_id")
    private String parentId;

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

    public Long getPrice() {
        return price;
    }

    public void setPrice(Long price) {
        this.price = price;
    }

    public Long getSort() {
        return sort;
    }

    public void setSort(Long sort) {
        this.sort = sort;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }
}

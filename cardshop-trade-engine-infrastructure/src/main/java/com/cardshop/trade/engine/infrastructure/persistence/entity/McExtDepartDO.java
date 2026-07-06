package com.cardshop.trade.engine.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * 旧卡管商户扩展表 `mc_ext_depart` 的最小持久化对象。
 *
 * <p>字段来源对应 {@code docs/交行直充业务系统详细设计.md}「20.13.3 SM4 解密与 SM3 验签」。
 * 新链路只读取 appId、appSecret、orgCode、aesKey 和启停状态，禁止把请求体里的 orgCode 当作可信商户身份。
 */
@TableName("mc_ext_depart")
public class McExtDepartDO {

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 商户机构编码。
     */
    @TableField("org_code")
    private String orgCode;

    /**
     * 旧接口应用 ID。
     */
    @TableField("app_id")
    private String appId;

    /**
     * 旧接口应用密钥，禁止写日志。
     */
    @TableField("app_secret")
    private String appSecret;

    /**
     * 兼容旧系统的 SM4/SM3 密钥，禁止写日志。
     */
    @TableField("aes_key")
    private String aesKey;

    /**
     * 旧系统商户状态，0=正常，1=停用。
     */
    @TableField("depart_status")
    private String departStatus;

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

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppSecret() {
        return appSecret;
    }

    public void setAppSecret(String appSecret) {
        this.appSecret = appSecret;
    }

    public String getAesKey() {
        return aesKey;
    }

    public void setAesKey(String aesKey) {
        this.aesKey = aesKey;
    }

    public String getDepartStatus() {
        return departStatus;
    }

    public void setDepartStatus(String departStatus) {
        this.departStatus = departStatus;
    }
}

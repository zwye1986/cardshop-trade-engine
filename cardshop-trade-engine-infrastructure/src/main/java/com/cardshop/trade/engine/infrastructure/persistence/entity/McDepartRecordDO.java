package com.cardshop.trade.engine.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.time.LocalDateTime;

/**
 * 旧系统商户资金流水表 `mc_depart_record` 的最小持久化对象。
 *
 * <p>测试库现有字段仍是旧命名：`type` 表示流水类型，`value` 表示发生金额，
 * `balance` 表示变动后余额。D7/D8 异步落库阶段只写 Redis 预扣对应的扣款流水，
 * 不同步刷新 `mc_ext_depart.balance`，因此 `before_balance/balance` 暂不填充，后续
 * `BALANCE_DELTA_FLUSH` 和对账任务再负责最终余额沉淀。
 */
@TableName("mc_depart_record")
public class McDepartRecordDO {

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
     * 变动前余额；当前 Redis 预扣流水不携带旧 MySQL 实时余额，保留为空。
     */
    @TableField("before_balance")
    private Long beforeBalance;

    /**
     * 变动后余额；当前 Redis 预扣流水不同步刷新旧余额，保留为空。
     */
    @TableField("balance")
    private Long balance;

    /**
     * 流水创建时间，沿用旧库 Asia/Shanghai datetime 字面值。
     */
    @TableField("create_time")
    private LocalDateTime createTime;

    /**
     * 旧表流水类型。测试库样本中 `1` 表示订单扣款，代码侧映射为 PRE_DEDUCT。
     */
    @TableField("type")
    private String type;

    /**
     * 平台订单号。
     */
    @TableField("order_id")
    private String orderId;

    /**
     * 发生金额，单位分。旧表扣款按正数记录，余额变化由流水类型表达。
     */
    @TableField("value")
    private Long value;

    /**
     * 新交易引擎资金流水幂等键，由 Liquibase 生成列维护，代码侧只用于映射和排查。
     */
    @TableField("trade_engine_record_key")
    private String tradeEngineRecordKey;

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

    public Long getBeforeBalance() {
        return beforeBalance;
    }

    public void setBeforeBalance(Long beforeBalance) {
        this.beforeBalance = beforeBalance;
    }

    public Long getBalance() {
        return balance;
    }

    public void setBalance(Long balance) {
        this.balance = balance;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public String getTradeEngineRecordKey() {
        return tradeEngineRecordKey;
    }

    public void setTradeEngineRecordKey(String tradeEngineRecordKey) {
        this.tradeEngineRecordKey = tradeEngineRecordKey;
    }
}

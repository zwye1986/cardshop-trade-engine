package com.cardshop.trade.engine.infrastructure.persistence;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecord;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyRecordPort;
import com.cardshop.trade.engine.domain.order.AcceptAnomalyStatus;
import com.cardshop.trade.engine.infrastructure.persistence.entity.RechargeAcceptAnomalyDO;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeAcceptAnomalyMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * 基于 MyBatis 的接单状态未知异常登记适配器。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「20.13.6 Redis Lua 下单结果」和
 * 「20.8 接单异常核查表」。该适配器只负责把 application 层的异常登记请求落入
 * `recharge_accept_anomaly`，不在这里执行 Redis/MySQL/MQ 三方核查；核查和状态推进属于后续 D5B/XXL-Job。
 */
public class MybatisAcceptAnomalyRecordAdapter implements AcceptAnomalyRecordPort {

    private final AcceptAnomalyWriter writer;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    /**
     * 生产构造器，使用 Mapper 的幂等登记 SQL。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.8 接单异常核查表」。同一订单同一异常类型可能因上游
     * 重试重复进入登记逻辑，生产路径不能依赖唯一键异常作为幂等控制，否则会把预期重复登记误报成数据库写入故障。
     */
    public MybatisAcceptAnomalyRecordAdapter(
            RechargeAcceptAnomalyMapper mapper,
            ObjectMapper objectMapper,
            Clock clock) {
        this(mapper::insertOrRefreshByAnomalyId, objectMapper, clock);
    }

    /**
     * 测试构造器，用极薄写入抽象替换 BaseMapper，避免为了字段映射单测启动 MyBatis 容器。
     */
    MybatisAcceptAnomalyRecordAdapter(
            AcceptAnomalyWriter writer,
            ObjectMapper objectMapper,
            Clock clock) {
        this.writer = Objects.requireNonNull(writer, "writer must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * 写入一条初始状态的接单异常记录。
     *
     * <p>异常登记属于主下单流程之外的证据留存；如果底层数据库抛错，由上层用例记录告警并保持
     * “稍后查单”响应，避免把待核查订单误转成接口运行时异常。重复登记由基础设施层按 `anomaly_id` 幂等刷新，
     * 不应向上抛出唯一键冲突。
     */
    @Override
    public void record(AcceptAnomalyRecord record) {
        Objects.requireNonNull(record, "record must not be null");
        writer.insertOrRefresh(toRow(record));
    }

    /**
     * 将 application 层稳定语义映射为 `recharge_accept_anomaly` 表行。
     */
    private RechargeAcceptAnomalyDO toRow(AcceptAnomalyRecord record) {
        RechargeAcceptAnomalyDO row = new RechargeAcceptAnomalyDO();
        LocalDateTime now = LocalDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        row.setAnomalyId(record.anomalyId());
        row.setOrgCode(record.orgCode());
        row.setOutOrderSn(record.outOrderSn());
        row.setOrderId(record.orderId());
        row.setRequestHash(record.requestHash());
        row.setPersistMsgId(record.persistMsgId());
        row.setAnomalyType(record.anomalyType().name());
        row.setStatus(AcceptAnomalyStatus.INIT.name());
        row.setRawContext(rawContextJson(record));
        row.setLastError(record.lastError());
        row.setCreatedAt(now);
        row.setUpdatedAt(now);
        return row;
    }

    /**
     * 序列化 `raw_context`，保留 WAIT/cancel/accept 三类关键状态，供 D5B 自动核查任务读取。
     */
    private String rawContextJson(AcceptAnomalyRecord record) {
        try {
            return objectMapper.writeValueAsString(record.rawContext());
        } catch (JsonProcessingException ex) {
            /*
             * raw_context 是后续核查的关键证据。如果序列化失败，说明调用方放入了不可 JSON 化的对象；
             * 这里直接抛出，让上层记录告警，但不改变接口返回，避免吞掉异常造成排查线索缺失。
             */
            throw new IllegalStateException("serialize accept anomaly raw context failed", ex);
        }
    }
}

/**
 * 异常表写入极薄抽象，便于单元测试在不 mock MyBatis BaseMapper 的情况下验证字段映射。
 *
 * <p>生产实现对应 `RechargeAcceptAnomalyMapper#insertOrRefreshByAnomalyId`，测试实现只关心 adapter 是否正确
 * 组装 `RechargeAcceptAnomalyDO`。
 */
interface AcceptAnomalyWriter {

    int insertOrRefresh(RechargeAcceptAnomalyDO row);
}

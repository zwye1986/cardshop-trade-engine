package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.application.recharge.AcceptAnomalyAutoCheckService;
import com.cardshop.trade.engine.application.recharge.AcceptAnomalyCheckRepositoryPort;
import com.cardshop.trade.engine.application.v1.RechargeOrderQueryPort;
import com.cardshop.trade.engine.job.recharge.AcceptAnomalyAutoCheckJob;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 交易可靠性与补偿类 application service 装配。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md}「9.3 Redis HA Guard」、
 * 「17. 分片补偿与对账任务」和「20.13.6 Redis Lua 下单结果」。bootstrap 层负责把 application 服务和
 * infrastructure 端口组装起来；具体 XXL-Job handler 后续只需要调用这些已装配好的服务。
 */
@Configuration(proxyBeanMethods = false)
public class TradeEngineReliabilityConfiguration {

    /**
     * 接单异常自动核查服务。
     *
     * <p>当前 D5B 服务覆盖 Redis/MySQL 读侧可判定的最小闭环；RabbitMQ confirm 轨迹和 Consumer 结果会在
     * D7/D8 接入后继续扩展到同一个服务，不改变 job 调用入口。
     */
    @Bean
    @ConditionalOnBean({AcceptAnomalyCheckRepositoryPort.class, RechargeOrderQueryPort.class})
    @ConditionalOnMissingBean(AcceptAnomalyAutoCheckService.class)
    public AcceptAnomalyAutoCheckService acceptAnomalyAutoCheckService(
            AcceptAnomalyCheckRepositoryPort repository,
            RechargeOrderQueryPort orderQueryPort) {
        return new AcceptAnomalyAutoCheckService(repository, orderQueryPort);
    }

    /**
     * 接单异常自动核查任务入口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md}「17. 分片补偿与对账任务」。D5B 先装配无框架依赖的
     * job facade，后续引入 XXL-Job handler 时只负责解析调度参数并调用该 Bean，避免重复实现核查批处理。
     */
    @Bean
    @ConditionalOnBean(AcceptAnomalyAutoCheckService.class)
    @ConditionalOnMissingBean(AcceptAnomalyAutoCheckJob.class)
    public AcceptAnomalyAutoCheckJob acceptAnomalyAutoCheckJob(
            AcceptAnomalyAutoCheckService autoCheckService) {
        return new AcceptAnomalyAutoCheckJob(autoCheckService);
    }
}

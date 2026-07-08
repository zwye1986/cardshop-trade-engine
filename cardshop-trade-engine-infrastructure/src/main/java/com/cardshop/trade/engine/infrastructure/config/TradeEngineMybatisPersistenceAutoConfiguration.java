package com.cardshop.trade.engine.infrastructure.config;

import com.cardshop.trade.engine.application.recharge.RechargeOrderPersistencePort;
import com.cardshop.trade.engine.infrastructure.persistence.MybatisRechargeOrderPersistenceAdapter;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McDepartRecordMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.McOrderMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOrderExtMapper;
import com.cardshop.trade.engine.infrastructure.persistence.mapper.RechargeOutboxMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.boot.autoconfigure.MybatisAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * MyBatis 落库端口的后置自动配置。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.13.8 异步落库 Consumer」。真实 dev/test/prod
 * 环境中的 Mapper Bean 由 MyBatis 自动配置和 mapper scanner 注册；普通业务 {@code @Configuration} 早于
 * {@link MybatisAutoConfiguration} 解析时，按 Mapper 做 {@code @ConditionalOnBean} 可能被过早判定为不满足，
 * 从而跳过 {@link RechargeOrderPersistencePort}。该端口缺失会继续导致
 * {@code PersistRechargeOrderConsumerService} 和 Rabbit listener 缺失，最终表现为应用健康但 RabbitMQ 没有
 * connection/consumer。
 *
 * <p>因此 MySQL 异步落库端口在 MyBatis 自动配置之后再兜底创建。基础设施普通配置类中的同名 Bean 仍保留给
 * slice test 或显式提供 Mapper 的局部上下文；本自动配置只要求 {@link SqlSessionFactory} 已经存在，然后把四个
 * 必需 Mapper 作为构造参数直接注入。若 mapper scan 漏掉任一 Mapper，dev/test/prod 应在启动期 fail-fast，
 * 不能静默缺失 Consumer 后继续对外显示健康。
 */
@AutoConfiguration(after = MybatisAutoConfiguration.class)
@ConditionalOnClass({MybatisAutoConfiguration.class, MybatisRechargeOrderPersistenceAdapter.class})
public class TradeEngineMybatisPersistenceAutoConfiguration {

    /**
     * 创建 Redis/MQ 到 MySQL 异步落库端口。
     *
     * <p>只以 {@link SqlSessionFactory} 作为 MyBatis 就绪条件，不再以 Mapper Bean 做条件判断。Mapper 作为方法参数
     * 注入，能够在所有 BeanDefinition 注册完成后的实例化阶段解析；若缺失则启动失败，避免半闭环消费 MQ 消息。
     */
    @Bean
    @ConditionalOnBean(SqlSessionFactory.class)
    @ConditionalOnMissingBean(RechargeOrderPersistencePort.class)
    public RechargeOrderPersistencePort rechargeOrderPersistencePort(
            McOrderMapper orderMapper,
            RechargeOrderExtMapper extMapper,
            RechargeOutboxMapper outboxMapper,
            McDepartRecordMapper departRecordMapper,
            ObjectMapper objectMapper) {
        return new MybatisRechargeOrderPersistenceAdapter(
                orderMapper,
                extMapper,
                outboxMapper,
                departRecordMapper,
                objectMapper);
    }
}

package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob;
import com.xxl.job.core.executor.impl.XxlJobSpringExecutor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * XXL-Job 真实调度入口装配。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「20.11 XXL-Job Handler 契约」和
 * {@code docs/开发进度.md} 第 7 节第 2 项。bootstrap 层是运行时框架适配边界：这里可以依赖
 * XXL-Job SDK，但 job/application/domain 模块继续保持无调度框架依赖，避免补偿业务逻辑和调度中心绑定。
 */
@Configuration(proxyBeanMethods = false)
public class TradeEngineXxlJobConfiguration {

    /**
     * 注册 Redis pending MQ 补投的真实 XXL-Job handler Bean。
     *
     * <p>handler Bean 默认创建，便于 {@link XxlJobSpringExecutor} 启动后扫描 {@code @XxlJob} 方法；
     * 但真正的 executor 网络注册由 {@code trade.engine.xxl-job.enabled} 控制。这样 dev/test 仍可只用人工入口，
     * 不会因为未配置调度中心而启动失败。
     */
    @Bean
    @ConditionalOnBean(RechargePersistMqRepublishJob.class)
    @ConditionalOnMissingBean(TradeEnginePersistMqRepublishXxlJobHandler.class)
    public TradeEnginePersistMqRepublishXxlJobHandler tradeEnginePersistMqRepublishXxlJobHandler(
            RechargePersistMqRepublishJob persistMqRepublishJob) {
        return new TradeEnginePersistMqRepublishXxlJobHandler(persistMqRepublishJob);
    }

    /**
     * 创建 XXL-Job Spring executor。
     *
     * <p>开启 executor 会启动 SDK 内嵌 RPC server 并向调度中心注册，因此默认关闭。启用时对关键配置做
     * fail-fast 校验，避免执行器以空 AppName、空 admin 地址或空 token 注册到错误环境。
     */
    @Bean
    @ConditionalOnProperty(prefix = "trade.engine.xxl-job", name = "enabled", havingValue = "true")
    @ConditionalOnMissingBean(XxlJobSpringExecutor.class)
    public XxlJobSpringExecutor xxlJobSpringExecutor(TradeEngineProperties properties) {
        TradeEngineProperties.XxlJob xxlJob = properties.getXxlJob();
        TradeEngineProperties.XxlJob.Executor executorProperties = xxlJob.getExecutor();
        validateRequired("trade.engine.xxl-job.admin-addresses", xxlJob.getAdminAddresses());
        validateRequired("trade.engine.xxl-job.access-token", xxlJob.getAccessToken());
        validateRequired("trade.engine.xxl-job.executor.appname", executorProperties.getAppname());
        validateRequired("trade.engine.xxl-job.executor.logpath", executorProperties.getLogpath());
        if (executorProperties.getPort() < 0) {
            throw new IllegalArgumentException("trade.engine.xxl-job.executor.port must be >= 0");
        }

        XxlJobSpringExecutor executor = new XxlJobSpringExecutor();
        executor.setAdminAddresses(xxlJob.getAdminAddresses());
        executor.setAccessToken(xxlJob.getAccessToken());
        executor.setAppname(executorProperties.getAppname());
        executor.setAddress(blankToNull(executorProperties.getAddress()));
        executor.setIp(blankToNull(executorProperties.getIp()));
        executor.setPort(executorProperties.getPort());
        executor.setLogPath(executorProperties.getLogpath());
        executor.setLogRetentionDays(Math.max(1, executorProperties.getLogRetentionDays()));
        return executor;
    }

    private static void validateRequired(String propertyName, String value) {
        if (!StringUtils.hasText(value)) {
            throw new IllegalArgumentException(propertyName + " must not be blank when XXL-Job executor is enabled");
        }
    }

    private static String blankToNull(String value) {
        return StringUtils.hasText(value) ? value : null;
    }
}

package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import com.cardshop.trade.engine.application.recharge.RechargeNotifyWorkerService;
import com.cardshop.trade.engine.job.recharge.AcceptAnomalyAutoCheckJob;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceDeltaFlushJob;
import com.cardshop.trade.engine.job.recharge.RechargeBalanceReconcileJob;
import com.cardshop.trade.engine.job.recharge.RechargeCacheRepairJob;
import com.cardshop.trade.engine.job.recharge.RechargeOutboxPublishJob;
import com.cardshop.trade.engine.job.recharge.RechargePersistMqRepublishJob;
import com.cardshop.trade.engine.job.recharge.RechargeSupplierDispatchJob;
import com.cardshop.trade.engine.job.recharge.RechargeSupplierQueryJob;
import com.xxl.job.core.executor.impl.XxlJobSpringExecutor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * XXL-Job 真实调度入口装配。
 *
 * <p>参见 {@code docs/交行直充业务系统详细设计.md}「10.1 XXL-Job Handler 契约」和
 * {@code docs/开发进度.md} 第 7 节第 2 项。bootstrap 层是运行时框架适配边界：这里可以依赖
 * XXL-Job SDK，但 job/application/domain 模块继续保持无调度框架依赖，避免补偿业务逻辑和调度中心绑定。
 */
@Configuration(proxyBeanMethods = false)
public class TradeEngineXxlJobConfiguration {

    /**
     * 注册所有已实现补偿任务的真实 XXL-Job handler Bean。
     *
     * <p>参见详细设计「10.1 XXL-Job Handler 契约」。handler Bean 必须早于 {@link XxlJobSpringExecutor}
     * 启动扫描存在，但各 job facade 可能由后置 auto-configuration 创建，所以这里注入 {@link ObjectProvider}
     * 并在触发时晚绑定真实任务。这样可以避免 test188 已出现的 executor 在线但 handler registry 为空的问题。
     */
    @Bean
    @ConditionalOnMissingBean(TradeEngineXxlJobHandlers.class)
    public TradeEngineXxlJobHandlers tradeEngineXxlJobHandlers(
            ObjectProvider<RechargePersistMqRepublishJob> persistMqRepublishJobProvider,
            ObjectProvider<RechargeOutboxPublishJob> outboxPublishJobProvider,
            ObjectProvider<RechargeSupplierDispatchJob> supplierDispatchJobProvider,
            ObjectProvider<RechargeSupplierQueryJob> supplierQueryJobProvider,
            ObjectProvider<RechargeNotifyWorkerService> notifyWorkerServiceProvider,
            ObjectProvider<RechargeBalanceDeltaFlushJob> balanceDeltaFlushJobProvider,
            ObjectProvider<RechargeBalanceReconcileJob> balanceReconcileJobProvider,
            ObjectProvider<RechargeCacheRepairJob> cacheRepairJobProvider,
            ObjectProvider<AcceptAnomalyAutoCheckJob> acceptAnomalyAutoCheckJobProvider) {
        return new TradeEngineXxlJobHandlers(
                persistMqRepublishJobProvider,
                outboxPublishJobProvider,
                supplierDispatchJobProvider,
                supplierQueryJobProvider,
                notifyWorkerServiceProvider,
                balanceDeltaFlushJobProvider,
                balanceReconcileJobProvider,
                cacheRepairJobProvider,
                acceptAnomalyAutoCheckJobProvider);
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

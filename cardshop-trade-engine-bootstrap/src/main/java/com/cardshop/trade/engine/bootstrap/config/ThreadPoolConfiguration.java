package com.cardshop.trade.engine.bootstrap.config;

import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * 交易引擎线程池配置。
 *
 * <p>线程池隔离策略来自 {@code docs/交行直充业务系统详细设计.md} 的「11. 配置中心、部署与健康检查」
 * 和 {@code docs/交行直充业务系统高可用方案设计.md} 的「3.7 线程池与隔离」。
 * 数据库落库、MQ Confirm、供应商 I/O 分别使用独立执行器，避免单个外部依赖阻塞拖垮整条交易链路。
 */
@Configuration(proxyBeanMethods = false)
public class ThreadPoolConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ThreadPoolConfiguration.class);
    private static final int AWAIT_TERMINATION_SECONDS = 10;

    /**
     * 异步数据库落库执行器。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「11. MySQL 持久化设计」。
     */
    @Bean
    @Qualifier("tradeDbTaskExecutor")
    public Executor tradeDbTaskExecutor(TradeEngineProperties properties) {
        var threadPool = properties.getThreadPool();
        return executor(new ExecutorSpec(
                "trade-db-",
                threadPool.getDbCoreSize(),
                threadPool.getDbMaxSize(),
                threadPool.getQueueCapacity()));
    }

    /**
     * MQ Publisher Confirm 执行器。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「7. RabbitMQ 持久化消息层」。
     */
    @Bean
    @Qualifier("tradeMqConfirmTaskExecutor")
    public Executor tradeMqConfirmTaskExecutor(TradeEngineProperties properties) {
        var workerSize = properties.getThreadPool().getMqConfirmWorkerSize();
        return executor(new ExecutorSpec(
                "trade-mq-confirm-",
                workerSize,
                workerSize,
                properties.getMq().getConfirmWorkerQueueCapacity()));
    }

    /**
     * 外部供应商 I/O 执行器。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「9.1 派单 Outbox」。
     */
    @Bean
    @Qualifier("tradeSupplierIoTaskExecutor")
    public Executor tradeSupplierIoTaskExecutor(TradeEngineProperties properties) {
        var workerSize = properties.getThreadPool().getSupplierIoSize();
        return executor(new ExecutorSpec(
                "trade-supplier-io-",
                workerSize,
                workerSize,
                properties.getThreadPool().getQueueCapacity()));
    }

    private ThreadPoolTaskExecutor executor(ExecutorSpec spec) {
        var executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix(spec.threadNamePrefix());
        executor.setCorePoolSize(spec.coreSize());
        executor.setMaxPoolSize(spec.maxSize());
        executor.setQueueCapacity(spec.queueCapacity());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(AWAIT_TERMINATION_SECONDS);
        executor.initialize();

        log.info(
                "初始化交易线程池 threadNamePrefix={}, coreSize={}, maxSize={}, queueCapacity={}, awaitTerminationSeconds={}",
                spec.threadNamePrefix(),
                spec.coreSize(),
                spec.maxSize(),
                spec.queueCapacity(),
                AWAIT_TERMINATION_SECONDS);
        return executor;
    }

    /**
     * 线程池规格值对象。
     *
     * <p>使用 Java record 保持配置不可变；最大线程数会兜底不小于核心线程数，避免错误配置导致启动失败。
     */
    private record ExecutorSpec(String threadNamePrefix, int coreSize, int maxSize, int queueCapacity) {

        private ExecutorSpec {
            maxSize = Math.max(coreSize, maxSize);
        }
    }
}

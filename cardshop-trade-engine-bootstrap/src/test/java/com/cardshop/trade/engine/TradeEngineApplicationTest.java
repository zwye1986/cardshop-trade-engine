package com.cardshop.trade.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.cardshop.trade.engine.infrastructure.config.TradeEngineProperties;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * 启动集成测试。
 *
 * <p>验证 D1 骨架可以在隔离外部中间件的情况下启动，并加载
 * {@code docs/交行直充业务系统详细设计.md}「5. Maven 多模块工程设计」和「16.2 健康检查」
 * 要求的基础配置 Bean。
 */
@ActiveProfiles("test")
@SpringBootTest(
        classes = TradeEngineApplication.class,
        properties = {
                "spring.cloud.nacos.config.enabled=false",
                "spring.cloud.nacos.discovery.enabled=false",
                "spring.cloud.discovery.enabled=false",
                "spring.autoconfigure.exclude="
                        + "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration,"
                        + "org.mybatis.spring.boot.autoconfigure.MybatisAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration"
        })
class TradeEngineApplicationTest {

    @Autowired
    private TradeEngineProperties properties;

    @Autowired
    @Qualifier("tradeDbTaskExecutor")
    private Executor dbExecutor;

    @Autowired
    @Qualifier("tradeMqConfirmTaskExecutor")
    private Executor mqConfirmExecutor;

    @Autowired
    @Qualifier("tradeSupplierIoTaskExecutor")
    private Executor supplierIoExecutor;

    @Test
    void startsWithTradeConfiguration() {
        assertThat(properties.getRedis().getPendingOrgBucketCount()).isEqualTo(128);

        // 三类线程池对应详细设计中的数据库落库、MQ Confirm、供应商 I/O 隔离要求。
        assertThat(dbExecutor).isNotNull();
        assertThat(mqConfirmExecutor).isNotNull();
        assertThat(supplierIoExecutor).isNotNull();
    }
}

package com.cardshop.trade.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

/**
 * 多环境资源隔离测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「7. 配置中心与多环境隔离」和
 * 「14. 环境配置样例」中的 local、dev、test、prod profile 约束，确保 Nacos 命名空间、RabbitMQ vhost、
 * Liquibase 和 Redis 写开关不会跨环境串用。
 */
class ProfileResourceIsolationTest {

    @Test
    void bootstrapProfilesUseSeparatedNacosNamespacesAndGroups() {
        Properties dev = load("bootstrap-dev.yml");
        Properties test = load("bootstrap-test.yml");
        Properties prod = load("bootstrap-prod.yml");

        assertThat(dev.getProperty("spring.cloud.nacos.config.namespace"))
                .isEqualTo("${NACOS_NAMESPACE:cardshop-trade-engine-dev}");
        assertThat(dev.getProperty("spring.cloud.nacos.config.group"))
                .isEqualTo("${NACOS_GROUP:TRADE_ENGINE_DEV}");
        assertThat(test.getProperty("spring.cloud.nacos.config.namespace"))
                .isEqualTo("${NACOS_NAMESPACE:cardshop-trade-engine-test}");
        assertThat(test.getProperty("spring.cloud.nacos.config.group"))
                .isEqualTo("${NACOS_GROUP:TRADE_ENGINE_TEST}");
        assertThat(prod.getProperty("spring.cloud.nacos.config.namespace"))
                .isEqualTo("${NACOS_NAMESPACE:cardShop}");
        assertThat(prod.getProperty("spring.cloud.nacos.config.group"))
                .isEqualTo("${NACOS_GROUP:TRADE_ENGINE_PROD}");
    }

    @Test
    void localProfileDisablesExternalMiddlewareForLaptopDevelopment() {
        Properties bootstrapLocal = load("bootstrap-local.yml");
        Properties applicationLocal = load("application-local.yml");

        assertThat(bootstrapLocal.getProperty("spring.cloud.discovery.enabled")).isEqualTo("false");
        assertThat(bootstrapLocal.getProperty("spring.cloud.nacos.config.enabled")).isEqualTo("false");
        assertThat(bootstrapLocal.getProperty("spring.cloud.nacos.discovery.enabled")).isEqualTo("false");

        assertThat(applicationLocal.getProperty("server.port")).isEqualTo("${SERVER_PORT:18081}");
        assertThat(applicationLocal.getProperty("spring.liquibase.enabled")).isEqualTo("false");
        assertThat(applicationLocal.getProperty("trade.engine.environment")).isEqualTo("local");
        assertThat(applicationLocal.getProperty("trade.engine.redis.write-enabled")).isEqualTo("false");
        assertThat(applicationLocal.getProperty("trade.engine.routing.enabled")).isEqualTo("false");
        assertThat(applicationLocal.getProperty("trade.engine.health.external-check-enabled")).isEqualTo("false");
        assertThat(applicationLocal.values())
                .contains(
                        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
                        "com.baomidou.mybatisplus.autoconfigure.MybatisPlusAutoConfiguration",
                        "org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration",
                        "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration",
                        "org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration");
    }

    @Test
    void applicationProfilesSeparateLiquibaseAndWriteSwitches() {
        Properties dev = load("application-dev.yml");
        Properties test = load("application-test.yml");
        Properties prod = load("application-prod.yml");

        assertThat(dev.getProperty("spring.liquibase.enabled")).isEqualTo("true");
        assertThat(dev).doesNotContainKeys(
                "spring.datasource.url",
                "spring.datasource.username",
                "spring.datasource.password");
        assertThat(dev.getProperty("trade.engine.redis.write-enabled")).isEqualTo("true");
        assertThat(dev.getProperty("spring.rabbitmq.virtual-host")).isEqualTo("cardshop-trade-dev");
        assertThat(dev.getProperty("spring.rabbitmq.publisher-confirm-type")).isEqualTo("correlated");
        assertThat(dev.getProperty("spring.rabbitmq.publisher-returns")).isEqualTo("true");
        assertThat(dev.getProperty("spring.rabbitmq.template.mandatory")).isEqualTo("true");
        assertThat(dev.values())
                .doesNotContain(
                        "jdbc:mysql://",
                        "spring.datasource.password",
                        "NACOS_PASSWORD");

        assertThat(test.getProperty("spring.liquibase.enabled")).isEqualTo("true");
        assertThat(test.getProperty("trade.engine.redis.write-enabled")).isEqualTo("true");
        assertThat(test.getProperty("spring.rabbitmq.virtual-host")).isEqualTo("cardshop-trade-test");
        assertThat(test.getProperty("spring.rabbitmq.publisher-confirm-type")).isEqualTo("correlated");
        assertThat(test.getProperty("spring.rabbitmq.publisher-returns")).isEqualTo("true");
        assertThat(test.getProperty("spring.rabbitmq.template.mandatory")).isEqualTo("true");

        assertThat(prod.getProperty("spring.liquibase.enabled")).isEqualTo("false");
        assertThat(prod.getProperty("trade.engine.redis.write-enabled")).isEqualTo("false");
        assertThat(prod.getProperty("spring.rabbitmq.virtual-host")).isEqualTo("cardshop-trade-prod");
        assertThat(prod.getProperty("spring.rabbitmq.publisher-confirm-type")).isEqualTo("correlated");
        assertThat(prod.getProperty("spring.rabbitmq.publisher-returns")).isEqualTo("true");
        assertThat(prod.getProperty("spring.rabbitmq.template.mandatory")).isEqualTo("true");
    }

    @Test
    void baseApplicationEnablesRabbitPublisherConfirmReturnAndMandatoryTemplate() {
        Properties application = load("application.yml");

        assertThat(application.getProperty("spring.rabbitmq.publisher-confirm-type")).isEqualTo("correlated");
        assertThat(application.getProperty("spring.rabbitmq.publisher-returns")).isEqualTo("true");
        assertThat(application.getProperty("spring.rabbitmq.template.mandatory")).isEqualTo("true");
        assertThat(application.getProperty("spring.rabbitmq.listener.simple.acknowledge-mode")).isEqualTo("manual");
        assertThat(application.getProperty("spring.rabbitmq.listener.simple.prefetch")).isEqualTo("50");
        assertThat(application.getProperty("spring.rabbitmq.listener.simple.concurrency")).isEqualTo("4");
        assertThat(application.getProperty("spring.rabbitmq.listener.simple.max-concurrency")).isEqualTo("16");
        assertThat(application.getProperty("spring.rabbitmq.listener.simple.consumer-batch-enabled")).isEqualTo("false");
        assertThat(application.getProperty("spring.rabbitmq.listener.simple.batch-size")).isEqualTo("10");
        assertThat(application.getProperty("trade.engine.mq.persist-queue"))
                .isEqualTo("recharge.persist.mysql.queue");
        assertThat(application.getProperty("trade.engine.mq.persist-dead-letter-exchange"))
                .isEqualTo("recharge.persist.dlx");
        assertThat(application.getProperty("trade.engine.mq.persist-dead-letter-queue"))
                .isEqualTo("recharge.persist.mysql.dlq");
        assertThat(application.getProperty("trade.engine.mq.persist-dead-letter-routing-key"))
                .isEqualTo("recharge.persist.mysql.dlq");
        assertThat(application.getProperty("trade.engine.mq.persist-retry-exchange"))
                .isEqualTo("recharge.persist.retry.exchange");
        assertThat(application.getProperty("trade.engine.mq.persist-retry-queue"))
                .isEqualTo("recharge.persist.mysql.retry.queue");
        assertThat(application.getProperty("trade.engine.mq.persist-retry-routing-key"))
                .isEqualTo("recharge.persist.mysql.retry");
        assertThat(application.getProperty("trade.engine.mq.consumer-retry-delay-ms"))
                .isEqualTo("30000");
        assertThat(application.getProperty("trade.engine.mq.persist-batch-enabled"))
                .isEqualTo("false");
        assertThat(application.getProperty("trade.engine.mq.persist-batch-size"))
                .isEqualTo("10");
        assertThat(application.getProperty("trade.engine.mq.persist-batch-receive-timeout-ms"))
                .isEqualTo("200");
    }

    private Properties load(String resourcePath) {
        var factory = new YamlPropertiesFactoryBean();
        factory.setResources(new ClassPathResource(resourcePath));
        var properties = factory.getObject();
        assertThat(properties).isNotNull();
        return properties;
    }
}

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
 * 「14. 环境配置样例」中的 dev、test、prod profile 约束，确保 Nacos 命名空间、RabbitMQ vhost、
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
    void applicationProfilesSeparateLiquibaseAndWriteSwitches() {
        Properties dev = load("application-dev.yml");
        Properties test = load("application-test.yml");
        Properties prod = load("application-prod.yml");

        assertThat(dev.getProperty("spring.liquibase.enabled")).isEqualTo("true");
        assertThat(dev.getProperty("trade.engine.redis.write-enabled")).isEqualTo("true");
        assertThat(dev.getProperty("spring.rabbitmq.virtual-host")).isEqualTo("cardshop-trade-dev");

        assertThat(test.getProperty("spring.liquibase.enabled")).isEqualTo("true");
        assertThat(test.getProperty("trade.engine.redis.write-enabled")).isEqualTo("true");
        assertThat(test.getProperty("spring.rabbitmq.virtual-host")).isEqualTo("cardshop-trade-test");

        assertThat(prod.getProperty("spring.liquibase.enabled")).isEqualTo("false");
        assertThat(prod.getProperty("trade.engine.redis.write-enabled")).isEqualTo("false");
        assertThat(prod.getProperty("spring.rabbitmq.virtual-host")).isEqualTo("cardshop-trade-prod");
    }

    private Properties load(String resourcePath) {
        var factory = new YamlPropertiesFactoryBean();
        factory.setResources(new ClassPathResource(resourcePath));
        var properties = factory.getObject();
        assertThat(properties).isNotNull();
        return properties;
    }
}

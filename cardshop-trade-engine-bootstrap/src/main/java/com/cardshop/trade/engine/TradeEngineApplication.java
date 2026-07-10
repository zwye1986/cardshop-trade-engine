package com.cardshop.trade.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

/**
 * 交行直充交易引擎启动入口。
 *
 * <p>该类承载独立服务的 Spring Boot 启动边界，对应
 * {@code docs/交行直充业务系统详细设计.md} 的「2. 系统模块与依赖关系」和「11. 配置中心、部署与健康检查」。
 * 启动日志只记录 profile、端口、Java 版本等排障上下文，不输出 Nacos、数据库或商户密钥等敏感配置。
 */
@SpringBootApplication(scanBasePackages = "com.cardshop.trade.engine")
public class TradeEngineApplication {

    private static final Logger log = LoggerFactory.getLogger(TradeEngineApplication.class);

    /**
     * 服务主入口。
     *
     * <p>参见 {@code docs/交行直充业务系统详细设计.md} 的「11. 配置中心、部署与健康检查」。
     */
    public static void main(String[] args) {
        var context = SpringApplication.run(TradeEngineApplication.class, args);
        logStarted(context);
    }

    private static void logStarted(ConfigurableApplicationContext context) {
        Environment environment = context.getEnvironment();
        var activeProfiles = environment.getActiveProfiles();
        var profileText = activeProfiles.length == 0
                ? String.join(",", environment.getDefaultProfiles())
                : String.join(",", activeProfiles);
        var applicationName = environment.getProperty("spring.application.name", "cardshop-trade-engine");
        var port = environment.getProperty("server.port", "8080");
        var nacosServerConfigured = StringUtils.hasText(environment.getProperty("spring.cloud.nacos.server-addr"));

        log.info(
                "交行直充交易引擎启动完成 applicationName={}, activeProfiles={}, serverPort={}, javaVersion={}, nacosServerConfigured={}, beanDefinitionCount={}",
                applicationName,
                profileText,
                port,
                Runtime.version(),
                nacosServerConfigured,
                context.getBeanDefinitionCount());
    }
}

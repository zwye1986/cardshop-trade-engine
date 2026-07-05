package com.cardshop.trade.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * 部署交付物契约测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「16. 部署设计」中定义的打包脚本、启动脚本、
 * zip 内启动说明和环境变量模板约束，避免后续测试/生产部署时把中间件配置散落到多个入口。
 */
class DeploymentArtifactContractTest {

    @Test
    void packageAndStartScriptsDeclareDeploymentContract() throws IOException {
        Path root = repositoryRoot();
        Path packageScript = root.resolve("scripts/package.sh");
        Path startScript = root.resolve("deploy/bin/start.sh");
        Path publishDevNacosScript = root.resolve("scripts/publish-dev-nacos.sh");
        Path envExample = root.resolve("deploy/config/env.example");
        Path startupGuide = root.resolve("deploy/STARTUP.md");
        Path devNacosExample = root.resolve("deploy/nacos/cardshop-trade-engine-dev.example.yaml");

        assertThat(packageScript).exists().isRegularFile();
        assertThat(startScript).exists().isRegularFile();
        assertThat(publishDevNacosScript).exists().isRegularFile();
        assertThat(envExample).exists().isRegularFile();
        assertThat(startupGuide).exists().isRegularFile();
        assertThat(devNacosExample).exists().isRegularFile();

        String packageText = Files.readString(packageScript);
        assertThat(packageText)
                .contains("CARD_SHOP_TRADE_JAVA_HOME")
                .contains("JDK 21")
                .contains("cardshop-trade-engine-bootstrap")
                .contains("cardshop-trade-engine-dev.example.yaml")
                .contains("zip");

        String startText = Files.readString(startScript);
        assertThat(startText)
                .contains("start|stop|restart|status")
                .contains("config/env.${APP_PROFILE}")
                .contains("NACOS_SERVER_ADDR")
                .contains("normalize_nacos_addr")
                .contains("NACOS_USERNAME")
                .contains("NACOS_PASSWORD")
                .contains("WARN: Env file is missing")
                .contains("JDK 21")
                .contains("lsof");

        String publishDevNacosText = Files.readString(publishDevNacosScript);
        assertThat(publishDevNacosText)
                .contains("CARD_SHOP_REDIS_HOST")
                .contains("CARD_SHOP_REDIS_PORT")
                .contains("CARD_SHOP_REDIS_DATABASE")
                .contains("CARD_SHOP_REDIS_PASSWORD")
                .contains("CARD_SHOP_REDIS_LOAD_FROM_CARDSHOP_NACOS")
                .contains("spring.data.redis")
                .contains("data:")
                .contains("redis:")
                .contains("password: $(yaml_quote");

        String envText = Files.readString(envExample);
        assertThat(envText)
                .contains("APP_PROFILE=dev")
                .contains("NACOS_SERVER_ADDR=47.117.130.100:8848")
                .contains("NACOS_NAMESPACE=cardshop-trade-engine-dev")
                .contains("NACOS_GROUP=TRADE_ENGINE_DEV")
                .contains("CARD_SHOP_TRADE_JAVA_HOME=");
        assertThat(envText)
                .contains("NACOS_PASSWORD=change-me")
                .doesNotContain("MYSQL_PASSWORD=")
                .doesNotContain("jdbc:mysql://");

        String guideText = Files.readString(startupGuide);
        assertThat(guideText)
                .contains("cardshop-trade-engine-dev.yaml")
                .contains("cardshop_test")
                .contains("legacy cardshop dev Redis settings")
                .contains("config/env.<profile>")
                .contains("./bin/start.sh start dev")
                .doesNotContain("NACOS_PASSWORD=")
                .doesNotContain("MYSQL_PASSWORD=");

        String devNacosText = Files.readString(devNacosExample);
        assertThat(devNacosText)
                .contains("spring:")
                .contains("datasource:")
                .contains("data:")
                .contains("redis:")
                .contains("cardshop_test")
                .contains("cardshop-trade-dev")
                .contains("password: <mysql-password>")
                .contains("database: 15")
                .contains("password: <cardshop-dev-redis-password>")
                .doesNotContain("NACOS_PASSWORD=")
                .doesNotContain("MYSQL_PASSWORD=");
    }

    private Path repositoryRoot() {
        Path current = Path.of("").toAbsolutePath();
        while (current != null && !Files.exists(current.resolve("pom.xml"))) {
            current = current.getParent();
        }
        assertThat(current).isNotNull();
        if (Files.exists(current.resolve("cardshop-trade-engine-bootstrap/pom.xml"))) {
            return current;
        }
        return current.getParent();
    }
}

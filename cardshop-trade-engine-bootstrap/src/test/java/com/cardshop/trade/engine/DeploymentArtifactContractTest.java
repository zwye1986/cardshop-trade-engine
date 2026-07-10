package com.cardshop.trade.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 部署交付物契约测试。
 *
 * <p>覆盖 {@code docs/交行直充业务系统详细设计.md}「11. 配置中心、部署与健康检查」中定义的打包脚本、启动脚本、
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
        Path gitignore = root.resolve(".gitignore");
        Path startupGuide = root.resolve("deploy/STARTUP.md");
        Path devNacosExample = root.resolve("deploy/nacos/cardshop-trade-engine-dev.example.yaml");
        Path applicationYaml = root.resolve("cardshop-trade-engine-bootstrap/src/main/resources/application.yml");

        assertThat(packageScript).exists().isRegularFile();
        assertThat(startScript).exists().isRegularFile();
        assertThat(publishDevNacosScript).exists().isRegularFile();
        assertThat(envExample).exists().isRegularFile();
        assertThat(gitignore).exists().isRegularFile();
        assertThat(startupGuide).exists().isRegularFile();
        assertThat(devNacosExample).exists().isRegularFile();
        assertThat(applicationYaml).exists().isRegularFile();

        String packageText = Files.readString(packageScript);
        assertThat(packageText)
                .contains("CARD_SHOP_TRADE_JAVA_HOME")
                .contains("JDK 21")
                .contains("cardshop-trade-engine-bootstrap")
                .contains("cardshop-trade-engine-dev.example.yaml")
                .contains("zip")
                .doesNotContain("deploy/config/.dev.env")
                .doesNotContain("deploy/config/.test.env")
                .doesNotContain("deploy/config/.prod.env");

        String startText = Files.readString(startScript);
        assertThat(startText)
                .contains("start|stop|restart|status")
                .contains("config/.${APP_PROFILE}.env")
                .contains("APP_PROFILE")
                .contains("SPRING_RABBITMQ_HOST")
                .contains("V1_CARDAPI_JWT_SECRET")
                .contains("NACOS_SERVER_ADDR")
                .contains("normalize_nacos_addr")
                .contains("NACOS_USERNAME")
                .contains("NACOS_PASSWORD")
                .contains("WARN: Env file is missing")
                .contains("JDK 21")
                .contains("lsof");
        assertThat(startText)
                .doesNotContain("spring.cloud.nacos.password")
                .doesNotContain("spring.rabbitmq.password")
                .doesNotContain("trade.engine.v1.jwt-secret");

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
                .contains("password: $(yaml_quote")
                .contains("publisher-confirm-type: correlated")
                .contains("publisher-returns: true")
                .contains("mandatory: true");

        String envText = Files.readString(envExample);
        assertThat(envText)
                .contains("APP_PROFILE=dev")
                .contains("NACOS_SERVER_ADDR=<nacos-host>:8848")
                .contains("NACOS_NAMESPACE=cardshop-trade-engine-dev")
                .contains("NACOS_GROUP=TRADE_ENGINE_DEV")
                .contains("CARD_SHOP_TRADE_JAVA_HOME=");
        assertThat(envText)
                .contains("NACOS_PASSWORD='change-me'")
                .contains("SPRING_RABBITMQ_HOST=<rabbitmq-host>")
                .contains("SPRING_RABBITMQ_USERNAME=<rabbitmq-user>")
                .contains("SPRING_RABBITMQ_PASSWORD='change-me'")
                .contains("V1_CARDAPI_JWT_SECRET='change-me'")
                .doesNotContain("NACOS_PASSWORD=change-me")
                .doesNotContain("SPRING_RABBITMQ_PASSWORD=change-me")
                .doesNotContain("V1_CARDAPI_JWT_SECRET=change-me")
                .doesNotContain("SPRING_RABBITMQ_HOST=47.100.11.188")
                .doesNotContain("SPRING_RABBITMQ_USERNAME=admin_cardshop")
                .doesNotContain("MYSQL_PASSWORD=")
                .doesNotContain("jdbc:mysql://");

        String guideText = Files.readString(startupGuide);
        assertThat(guideText)
                .contains("cardshop-trade-engine-dev.yaml")
                .contains("cardshop_test")
                .contains("legacy cardshop dev Redis settings")
                .contains("config/.<profile>.env")
                .contains("./bin/start.sh start dev")
                .contains("NACOS_PASSWORD='<nacos-password-if-it-contains-shell-metacharacters>'")
                .doesNotContain("NACOS_PASSWORD=change-me")
                .doesNotContain("MYSQL_PASSWORD=");

        String gitignoreText = Files.readString(gitignore);
        assertThat(gitignoreText)
                .contains("deploy/config/.*.env")
                .contains("!deploy/config/env.example");

        String devNacosText = Files.readString(devNacosExample);
        assertThat(devNacosText)
                .contains("spring:")
                .contains("datasource:")
                .contains("data:")
                .contains("redis:")
                .contains("cardshop_test")
                .contains("cardshop-trade-dev")
                .contains("publisher-confirm-type: correlated")
                .contains("publisher-returns: true")
                .contains("mandatory: true")
                .contains("password: <mysql-password>")
                .contains("database: 15")
                .contains("password: <cardshop-dev-redis-password>")
                .doesNotContain("NACOS_PASSWORD=")
                .doesNotContain("MYSQL_PASSWORD=");

        String applicationYamlText = Files.readString(applicationYaml);
        assertThat(applicationYamlText)
                .contains("host: ${SPRING_RABBITMQ_HOST:}")
                .contains("username: ${SPRING_RABBITMQ_USERNAME:}")
                .contains("write-enabled: false")
                .doesNotContain("SPRING_RABBITMQ_HOST:47.100.11.188")
                .doesNotContain("SPRING_RABBITMQ_USERNAME:admin_cardshop");
    }

    @Test
    void startScriptKeepsSecretsOutOfJvmArgumentsAndExportsEnvironment(@TempDir Path tempDir) throws Exception {
        Path root = repositoryRoot();
        Path appHome = tempDir.resolve("app-home");
        Path fakeJdk = tempDir.resolve("fake-jdk");
        Path javaArgsFile = tempDir.resolve("java-args.txt");
        Path javaEnvFile = tempDir.resolve("java-env.txt");
        Files.createDirectories(appHome.resolve("bin"));
        Files.createDirectories(appHome.resolve("config"));
        Files.createDirectories(appHome.resolve("app"));
        Files.createDirectories(fakeJdk.resolve("bin"));
        Files.copy(root.resolve("deploy/bin/start.sh"), appHome.resolve("bin/start.sh"), StandardCopyOption.REPLACE_EXISTING);
        appHome.resolve("bin/start.sh").toFile().setExecutable(true);
        Files.writeString(appHome.resolve("app/cardshop-trade-engine-bootstrap-test.jar"), "fake jar");
        Files.writeString(fakeJdk.resolve("bin/java"), """
                #!/usr/bin/env bash
                set -euo pipefail
                if [[ "${1:-}" == "-version" ]]; then
                  printf 'java version "21.0.1"\\n' >&2
                  exit 0
                fi
                printf '%s\\n' "$@" > "$JAVA_ARGS_FILE"
                {
                  printf 'APP_PROFILE=%s\\n' "${APP_PROFILE:-}"
                  printf 'SERVER_PORT=%s\\n' "${SERVER_PORT:-}"
                  printf 'NACOS_SERVER_ADDR=%s\\n' "${NACOS_SERVER_ADDR:-}"
                  printf 'NACOS_PASSWORD=%s\\n' "${NACOS_PASSWORD:-}"
                  printf 'SPRING_RABBITMQ_PASSWORD=%s\\n' "${SPRING_RABBITMQ_PASSWORD:-}"
                  printf 'V1_CARDAPI_JWT_SECRET=%s\\n' "${V1_CARDAPI_JWT_SECRET:-}"
                } > "$JAVA_ENV_FILE"
                while true; do sleep 1; done
                """);
        fakeJdk.resolve("bin/java").toFile().setExecutable(true);

        Files.writeString(appHome.resolve("config/.dev.env"), """
                APP_PROFILE=dev
                SERVER_PORT=19081
                CARD_SHOP_TRADE_JAVA_HOME='%s'
                JAVA_OPTS="-Xms128m -Xmx128m"
                NACOS_SERVER_ADDR=http://nacos.example:8848/nacos
                NACOS_NAMESPACE=cardshop-trade-engine-dev
                NACOS_GROUP=TRADE_ENGINE_DEV
                NACOS_USERNAME=nacos
                NACOS_PASSWORD=secret-for-test
                SPRING_RABBITMQ_HOST=rabbit.example
                SPRING_RABBITMQ_PORT=5672
                SPRING_RABBITMQ_USERNAME=rabbit-user
                SPRING_RABBITMQ_PASSWORD=rabbit-secret
                SPRING_RABBITMQ_VIRTUAL_HOST=cardshop-trade-dev
                LEGACY_CARD_SHOP_BASE_URL=http://legacy.example
                V1_CARDAPI_JWT_SECRET=jwt-secret-for-test
                JAVA_ARGS_FILE='%s'
                JAVA_ENV_FILE='%s'
                """.formatted(fakeJdk, javaArgsFile, javaEnvFile));

        try {
            String startOutput = runScript(appHome, "start", "dev");
            assertThat(startOutput).contains("Started cardshop-trade-engine profile=dev");

            String argsText = Files.readString(javaArgsFile);
            assertThat(argsText)
                    .contains("-jar")
                    .contains("cardshop-trade-engine-bootstrap-test.jar")
                    .doesNotContain("secret-for-test")
                    .doesNotContain("rabbit-secret")
                    .doesNotContain("jwt-secret-for-test")
                    .doesNotContain("--spring.profiles.active")
                    .doesNotContain("--server.port");

            String envText = Files.readString(javaEnvFile);
            assertThat(envText)
                    .contains("APP_PROFILE=dev")
                    .contains("SERVER_PORT=19081")
                    .contains("NACOS_SERVER_ADDR=nacos.example:8848")
                    .contains("NACOS_PASSWORD=secret-for-test")
                    .contains("SPRING_RABBITMQ_PASSWORD=rabbit-secret")
                    .contains("V1_CARDAPI_JWT_SECRET=jwt-secret-for-test");
        } finally {
            if (Files.exists(appHome.resolve("run/cardshop-trade-engine-dev.pid"))) {
                runScript(appHome, "stop", "dev");
            }
        }
    }

    private String runScript(Path appHome, String... args) throws IOException, InterruptedException {
        Path script = appHome.resolve("bin/start.sh");
        var command = new String[args.length + 1];
        command[0] = script.toString();
        System.arraycopy(args, 0, command, 1, args.length);
        Process process = new ProcessBuilder(command)
                .directory(appHome.toFile())
                .redirectErrorStream(true)
                .start();
        boolean exited = process.waitFor(10, TimeUnit.SECONDS);
        String output = new String(process.getInputStream().readAllBytes());
        assertThat(exited).as(output).isTrue();
        assertThat(process.exitValue()).as(output).isZero();
        return output;
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

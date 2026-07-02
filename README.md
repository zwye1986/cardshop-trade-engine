# cardshop-trade-engine

`cardshop-trade-engine` 是面向交行直充场景拆分出的独立交易服务。当前工程处于 D1 骨架阶段，已落地 Maven 多模块工作区、Spring Boot 启动模块、多环境/Nacos bootstrap、Actuator 探针、Liquibase 入口和 `trade.engine.*` 共享配置绑定。

目标设计见：

- [交行直充业务系统详细设计](docs/交行直充业务系统详细设计.md)
- [交行直充接口 Redis 与 MySQL 一致性及高可用设计实现方案](docs/交行直充业务系统高可用方案设计.md)
- [交行直充业务系统开发时间表](docs/交行直充业务系统开发时间表.md)
- [贡献者指南](AGENTS.md)

## 技术栈

- Java 21，Maven 3.9+
- Spring Boot 3.3.6，Spring WebFlux，Actuator
- Spring Cloud 2023.0.4，Spring Cloud Alibaba 2023.0.1.2，Nacos
- Redis Reactive，RabbitMQ，MyBatis，MySQL
- Liquibase 4.29.2
- JUnit 5，AssertJ，Spring Boot Test

## 目标架构

服务通过 `/v1/cardapi` 兼容旧直充接口，按配置支持灰度路由和无侵入切流。目标链路以 Redis Lua 承接高并发下单和余额冻结，RabbitMQ 负责异步持久化与补偿，MySQL 保留最终业务状态，XXL-Job 承担补投、对账、缓存修复等后台任务。Redis 写入、MQ Confirm、MySQL 落库和灰度切流均通过配置开关渐进启用。

## 模块结构

| Module | Responsibility |
| --- | --- |
| `cardshop-trade-engine-domain` | 领域模型和业务规则；不依赖 Spring、MyBatis、Redis、RabbitMQ。 |
| `cardshop-trade-engine-application` | 应用服务、用例编排和端口接口。 |
| `cardshop-trade-engine-api` | WebFlux Controller、请求/响应 DTO、接口适配。 |
| `cardshop-trade-engine-infrastructure` | Redis、RabbitMQ、MyBatis、Nacos 配置和外部适配器。 |
| `cardshop-trade-engine-job` | XXL-Job Handler 和后台任务入口。 |
| `cardshop-trade-engine-migration` | Liquibase changelog，入口位于 `src/main/resources/db/changelog/`。 |
| `cardshop-trade-engine-test-support` | 集成测试复用 fixture。 |
| `cardshop-trade-engine-bootstrap` | Spring Boot 启动、profile、Actuator、线程池和 readiness 配置。 |

生产代码放在各模块 `src/main/java`，测试放在 `src/test/java`，资源放在 `src/main/resources`。

## 开发规范

- 开发过程中如果发现既定方案需要调整，必须先同步更新 `docs/交行直充业务系统高可用方案设计.md` 和 `docs/交行直充业务系统详细设计.md`，再修改代码。
- 实现 `docs/交行直充接口技术文档-V1.1.docx` 覆盖的接口前，必须先查看旧系统 `/Users/mac/Documents/codeSpace/cardshop` 中对应实现，新系统业务逻辑应与已有实现保持基本一致，除非设计文档或用户明确要求调整。
- 遇到需求、接口语义、旧系统行为或设计细节有歧义/模糊不清时，先和用户确认，再继续实现。
- 代码中的关键类、方法、流程和非显然决策必须写详细中文注释，并链接到 `docs/交行直充业务系统详细设计.md` 的对应章节。
- DTO 的类注释和字段注释必须与对应 DDL 中的 `COMMENT` 保持一致。
- 优先使用 JDK 21 能提升清晰性、安全性或简洁性的语言和标准库特性。
- 关键业务、路由、Redis、MQ、MySQL、补偿和对账链路必须记录足够排障上下文；日志不能输出密钥、密码或敏感报文。

## 构建与测试

如本机存在多 JDK，先切到 JDK 21：

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
```

常用命令：

```bash
mvn clean package
mvn test
mvn -pl cardshop-trade-engine-bootstrap -am test
```

## 本地启动

默认 profile 为 `dev`。本地需要提供 Nacos 地址，Redis、RabbitMQ、MySQL 等连接信息应来自环境变量、Nacos 或部署密钥。

```bash
APP_PROFILE=dev \
NACOS_SERVER_ADDR=127.0.0.1:8848 \
NACOS_NAMESPACE=cardshop-trade-engine-dev \
NACOS_GROUP=TRADE_ENGINE_DEV \
mvn -pl cardshop-trade-engine-bootstrap -am spring-boot:run
```

服务默认端口为 `8080`，可通过 `SERVER_PORT` 覆盖。

## 环境与配置

| Profile | Nacos namespace default | Nacos group default | Liquibase | RabbitMQ vhost |
| --- | --- | --- | --- | --- |
| `dev` | `cardshop-trade-engine-dev` | `TRADE_ENGINE_DEV` | enabled | `cardshop-trade-dev` |
| `test` | `cardshop-trade-engine-test` | `TRADE_ENGINE_TEST` | enabled | `cardshop-trade-test` |
| `prod` | `cardShop` | `TRADE_ENGINE_PROD` | disabled | `cardshop-trade-prod` |

Nacos 会按顺序加载：

- `cardshop-trade-engine-common.yaml`
- `cardshop-trade-engine-${spring.profiles.active}.yaml`
- `cardshop-trade-engine-supplier.yaml`
- `cardshop-trade-engine-risk.yaml`

核心配置前缀为 `trade.engine.*`。生产环境默认 `trade.engine.redis.write-enabled=false`，必须在 Redis、MQ、MySQL、Liquibase 和灰度路由验证通过后再开启写行为。

## 健康检查

- `/actuator/health/liveness`
- `/actuator/health/readiness`
- `/actuator/prometheus`

`tradeEngineReadiness` 会暴露运行环境、Redis 写开关、Redis fail-closed、MQ breaker、路由开关、旧系统地址配置状态和外部检查开关。

## 安全要求

不要提交账号、密码、密钥或真实连接串。Nacos、Redis、RabbitMQ、MySQL 和旧系统地址等配置必须通过环境变量、Nacos 或部署密钥注入。提交前确认 `target/`、日志、IDE 文件和本地配置未进入版本库。

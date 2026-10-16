# 交行直充交易服务开发与测试环境部署说明

## 1. 适用范围

本项目为全新开发的交易服务，目前处于开发到测试阶段。本说明用于部署和验证 `dev`、`test` 环境，直接使用 V1.2 协议，不涉及旧业务数据迁移、旧订单换绑、旧回调任务清理或生产流量切换。

2026年10月16日版本已按商户级独立签名密钥实现：token刷新不轮换，订单不绑定密钥，服务端、模拟器和调用方源码示例使用同一生命周期。实际开发/测试环境的HTTPS入口及中间件联调仍需在部署后确认。

首次部署需要准备测试依赖、初始化数据库结构、配置服务端密钥及联调入口。后续部署继续使用同一环境的配置和密钥。`local` 仅用于本机轻量开发，不作为集成测试环境。

## 2. 构建与部署包目录

在源码仓库使用 JDK 21 和 Maven 3.9 或更新版本执行：

```bash
./scripts/package.sh
```

脚本执行构建和测试，在 `dist/` 下生成部署包。解压后目录为：

```text
cardshop-trade-engine-<version>/
  app/cardshop-trade-engine-bootstrap-<version>.jar
  bin/start.sh
  config/env.example
  monitoring/
  nacos/cardshop-trade-engine-dev.example.yaml
  logs/
  STARTUP.md
```

真实环境的账号、密码和密钥在部署时填写，不包含在部署包中。

## 3. 准备开发与测试环境

准备可访问的 MySQL、Redis、RabbitMQ、Nacos，以及业务测试需要的供应商模拟服务或测试服务。开发和测试环境分别确认数据库、Redis 使用范围、RabbitMQ 虚拟主机、Nacos 命名空间和配置分组，防止测试数据相互干扰。

`dev` 默认使用 Nacos 命名空间 `cardshop-trade-engine-dev`、配置分组 `TRADE_ENGINE_DEV` 和 RabbitMQ 虚拟主机 `cardshop-trade-dev`。`test` 使用对应测试环境配置，RabbitMQ 虚拟主机默认为 `cardshop-trade-test`。

开发模板以 `cardshop_test` 为测试库名，Redis 数据库编号为 `15`，实际连接参数以本次部署确定的环境为准，不要直接使用主机、账号和密码占位值。

## 4. 配置启动环境变量

在部署包根目录复制模板，开发环境使用 `.dev.env`，测试环境使用 `.test.env`：

```bash
cp config/env.example config/.dev.env
vi config/.dev.env
chmod 600 config/.dev.env
```

测试环境改用 `config/.test.env`，并调整其中的 `APP_PROFILE`、Nacos 命名空间和分组、RabbitMQ 虚拟主机。`config/.<profile>.env` 为文件名规则，`<profile>` 替换为 `dev` 或 `test`。

环境文件主要配置：

- 运行环境、服务端口、JDK 路径及 JVM、日志、进程文件目录。
- Nacos 地址、命名空间、分组和认证信息。
- RabbitMQ 连接参数，以及 Nacos 配置引用的 Redis 密码等环境变量。
- `V1_CARDAPI_JWT_SECRET`：本环境签发和校验 token 的服务端密钥。
- `V1_SIGNING_KEY_ENCRYPTION_KEY`：加密保存 `signKey` 的独立存储主密钥。

MySQL、Redis、供应商参数及业务开关通过 Nacos 管理，避免同一环境各实例配置不一致。`LEGACY_CARD_SHOP_BASE_URL` 可保持为空，当前 V1.2 接口不通过旧系统代理处理请求。

环境文件由 Bash 加载，密码、JWT 密钥等值使用单引号包裹，尤其是含美元符号、感叹号、空格、反引号等特殊字符时：

```bash
V1_CARDAPI_JWT_SECRET='<本环境独立生成的JWT密钥>'
V1_SIGNING_KEY_ENCRYPTION_KEY='<独立随机32字节的无填充Base64URL编码>'
NACOS_PASSWORD='<nacos-password-if-it-contains-shell-metacharacters>'
```

示例仅为占位说明，不能直接作为实际密钥。启动脚本检查必填项并导出环境变量，Spring 通过占位符读取。密码和密钥不要写入 `JAVA_OPTS`、JVM 的 `-D` 参数、日志或 Git。

## 5. 配置 Nacos

启动前在对应命名空间和配置分组中准备以下 YAML 类型的 DataId：

- `cardshop-trade-engine-common.yaml`：公共配置。
- `cardshop-trade-engine-dev.yaml` 或 `cardshop-trade-engine-test.yaml`：当前环境配置。
- `cardshop-trade-engine-supplier.yaml`：供应商配置。
- `cardshop-trade-engine-risk.yaml`：风控配置。

参考部署包中的 `nacos/cardshop-trade-engine-dev.example.yaml`。MySQL 和 Redis 配置示例如下：

```yaml
spring:
  datasource:
    # 批量落库需要 rewriteBatchedStatements=true。
    url: jdbc:mysql://<host>/cardshop_test?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8&allowMultiQueries=true&rewriteBatchedStatements=true
    username: <mysql-user>
    password: <mysql-password>
    hikari:
      maximum-pool-size: 16
      minimum-idle: 4
      connection-timeout: 2000
  data:
    redis:
      host: <redis-host>
      port: 6379
      database: 15
      password: <redis-password>
      timeout: 3s
  liquibase:
    enabled: true

trade:
  engine:
    v1:
      sign-alg: HMAC-SM3
      signing-key-encryption-key: ${V1_SIGNING_KEY_ENCRYPTION_KEY:}
```

存储主密钥通过环境变量引用，不需要把明文写入 Nacos。RabbitMQ、Redis 写入开关及其他业务参数继续按对应环境模板配置。

## 6. 初始化数据库结构与测试基础数据

这里的数据库准备是新服务的表结构初始化与后续结构更新，不是旧业务数据迁移。`dev`、`test` 默认启用 Liquibase，应用启动时按 `db.changelog-master.yaml` 顺序执行尚未完成的变更，并记录结果。

新增 `0019-v12-merchant-signing-keys` 创建商户当前版本指针表 `recharge_v12_merchant_key` 与密钥版本表 `recharge_v12_merchant_signing_key`。`0018` 为既有结构记录，其旧凭证表和订单 `signing_key_id` 字段保留但当前程序不再读写，不搬迁旧业务数据。实际部署按完整变更链初始化，不手工重复执行单个编号或重复建表。

当前变更链依赖 `mc_order`、`mc_ext_depart` 及商品、商户授权等基础表，不能视为完全空库的全部建表脚本。测试库先准备项目依赖的基础表结构，再由 Liquibase 初始化交易引擎新增表和字段。建立基础表不要求导入旧订单或旧回调数据。

联调前准备测试所需的商户账号、`app_id/app_secret`、商户启用状态、SM4 加密密钥、商品及授权、余额和供应商路由等基础数据。数据范围以测试用例为准。启动账号需具备结构初始化所需的数据库权限；Liquibase 执行失败时，先解决结构或权限问题，再继续联调。

## 7. 配置 V1.2 密钥

`V1_CARDAPI_JWT_SECRET` 用于 token，`V1_SIGNING_KEY_ENCRYPTION_KEY` 用于数据库中 `signKey` 的加解密。两者独立生成；开发和测试环境分别配置，不复用商户的 `app_secret` 或 `aesKey`。

存储主密钥使用安全随机的 32 字节，编码为 43 字符、无填充的 Base64URL 字符串。同一环境所有实例使用相同值，重启和重新部署时保留该值，以便继续读取已产生的测试凭证。该主密钥只配置在服务端，不提供给调用方，不在每次启动时重新生成。

调用方通过 `getToken` 获取 `token/keyId/signKey`。商户首次成功获取时由平台初始化32字节安全随机signKey及唯一keyId；后续普通获取或刷新复用该商户当前版本，无须部署人员逐个配置。调用方解码后完整使用32字节计算HMAC-SM3，商户aesKey仅用于SM4。并发首次初始化须保证同一商户只有一个当前有效版本。

token默认3小时到期，建议约2小时刷新，刷新后旧token失效。普通刷新仅更新token，keyId/signKey不变；签名密钥独立有效至专门轮换或撤销，不随token到期、刷新或服务重启自动失效。请求认证通过后按可信商户加载当前有效签名密钥，不需要token或订单与keyId绑定。

订单不保存密钥绑定。平台创建回调任务时使用该商户当前有效密钥，并将keyId和已签名报文保存在任务中；正常重试原样发送。接收方按可信商户和报文keyId查找未撤销的对应版本验签，再检查订单归属和业务幂等，不能用未知keyId尝试其他版本。

密钥轮换和撤销由服务端 `V12SigningCredentialStorePort.rotate/revoke` 内部维护端口执行，没有对外HTTP管理接口；仅交由受控运维工具调用，禁止向调用方开放数据库或维护权限。密钥轮换属于专门流程，不通过刷新token触发：双方协调暂停商户新请求及新回调生成，平台启用新版本，调用方经HTTPS获取并保存后恢复。新请求及新回调使用新版；旧版仅保留用于已有签名任务重试，直到重试和约定人工补发期限结束。撤销旧版则立即停止其发送和验签，必要时以新版显式重建未完成通知，无须订单换绑；普通getToken不能自动恢复已撤销密钥。

## 8. 确认 HTTPS 联调入口

由环境负责人提供开发或测试环境实际可用的 HTTPS 域名、网关前缀及证书链。调用方和模拟器配置该入口，请求路径在此前缀后拼接，例如 `/v1/cardapi/auth/getToken`。

HTTPS 保护 `app_secret`、token 和 `signKey` 在传输中的安全，属于当前协议联调要求，与旧数据迁移无关。模拟器仅允许本机回环地址使用 HTTP，远端测试入口使用 HTTPS 并保持正常证书校验。本文不预设尚未确认的测试域名。

## 9. 启动、查看状态与停止

启动脚本检查 JDK 21。服务器默认 JDK 不是 21 时，在环境文件中配置 `CARD_SHOP_TRADE_JAVA_HOME`。

开发环境：

```bash
./bin/start.sh start dev
./bin/start.sh status dev
./bin/start.sh stop dev
./bin/start.sh restart dev
```

测试环境将最后一个参数替换为 `test`，例如 `./bin/start.sh start test`。启动后先检查日志中的 Nacos 加载、Liquibase 执行和中间件连接结果，再发起业务请求。

日志默认位于 `logs/`，可通过环境文件中的 `LOG_DIR` 调整：

- `logs/cardshop-trade-engine.log`：主要业务日志，由 Logback 管理，按天和单文件 200MB 轮转，归档保留 90 天。
- `logs/cardshop-trade-engine-<profile>.out`：开发、测试环境的 JVM 标准输出兜底文件，启动脚本负责轮转，用于定位启动阶段问题。

健康检查和指标路径：

```text
/actuator/health/liveness
/actuator/health/readiness
/actuator/prometheus
```

部署包中的 `monitoring/prometheus-rules/cardshop-trade-engine-alerts.yml` 和 `monitoring/grafana/cardshop-trade-engine-overview.json` 可导入测试环境监控系统。RabbitMQ 队列深度通过其监控导出或运维探测采集，不需要把管理端凭证配置进业务服务。

## 10. 执行联调验证

原测试控制台已删除且不可恢复，当前使用仓库内模拟器、可执行协议示例和自动化测试验证。

1. 检查应用健康、数据库结构和测试商户、商品、余额等基础数据。
2. 调用 `getToken`，确认返回 `token/keyId/signKey`；实际值由调用方安全保存，不打印到日志。
3. 使用完整 `signKey` 签名下单、查单和作废请求，按环境配置启用 SM4。
4. 验证同商户并发首次获取仅初始化一把密钥、刷新token不改变keyId/signKey、订单无需密钥绑定，以及跨版本回调验签、原报文重试、专门轮换和撤销行为；未知或跨商户keyId必须拒绝。
5. 将实际环境、配置、数据范围、步骤及结果证据记录到仓库的 `docs/集成测试.md`。本地单元测试不代替真实环境联调结果。

本阶段部署顺序为：准备测试依赖和基础表、配置 Nacos 与环境变量、启动并完成结构初始化、确认 HTTPS 可达、执行协议与业务联调。无需增加旧业务数据迁移或旧回调任务处置步骤。

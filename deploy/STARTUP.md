# cardshop-trade-engine Startup Guide

## Package Layout

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

## Configure Bootstrap Environment

Copy the template and keep one file per runtime profile:

```bash
cp config/env.example config/.dev.env
vi config/.dev.env
```

Only settings read directly from environment placeholders belong in
`config/.<profile>.env`: `APP_PROFILE`, `SERVER_PORT`, Nacos, RabbitMQ,
legacy-cardshop URL, V1 JWT secret, JDK path, and JVM/log/pid settings. The
start script validates required values and exports them for Spring placeholders;
secrets such as `NACOS_PASSWORD`, `SPRING_RABBITMQ_PASSWORD`, and
`V1_CARDAPI_JWT_SECRET` must not be placed in `JAVA_OPTS` or JVM `-D` arguments.
MySQL, Redis, XXL-Job, gray routing lists, supplier settings, and business
switches should be managed by Nacos to avoid per-server configuration drift.

`config/.<profile>.env` is loaded by Bash. Wrap values that contain shell
metacharacters such as `$`, `!`, spaces, backticks, `&`, `|`, `;`, `<`, or `>` in
single quotes. This is especially important for the legacy V1 JWT secret copied
from old cardshop `TokenUtils`, because it may contain `$` characters:

```bash
V1_CARDAPI_JWT_SECRET='<legacy-token-utils-secret>'
NACOS_PASSWORD='<nacos-password-if-it-contains-shell-metacharacters>'
```

For dev, use namespace `cardshop-trade-engine-dev` and group
`TRADE_ENGINE_DEV`. The dev datasource points to the shared legacy cardshop
test database `cardshop_test`, and Redis connection values should match the
current legacy cardshop dev Redis settings. Keep the actual JDBC URL, Redis
host, and password in `cardshop-trade-engine-dev.yaml`, not in this package.

## Required Nacos DataIds

Prepare these YAML DataIds in the selected namespace and group before starting:

- `cardshop-trade-engine-common.yaml`
- `cardshop-trade-engine-dev.yaml`, `cardshop-trade-engine-test.yaml`, or `cardshop-trade-engine-prod.yaml`
- `cardshop-trade-engine-supplier.yaml`
- `cardshop-trade-engine-risk.yaml`

Minimal dev datasource and Redis content belongs in `cardshop-trade-engine-dev.yaml`:

```yaml
spring:
  datasource:
    # rewriteBatchedStatements=true is required before enabling persist batch consumer.
    url: jdbc:mysql://<host>/cardshop_test?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8&allowMultiQueries=true&rewriteBatchedStatements=true
    username: <mysql-user>
    password: <mysql-password>
    hikari:
      maximum-pool-size: 16
      minimum-idle: 4
      connection-timeout: 2000
  data:
    redis:
      host: <cardshop-dev-redis-host>
      port: 6379
      database: 15
      password: <cardshop-dev-redis-password>
      timeout: 3s
```

## Start And Stop

The script validates JDK 21 before launching. If the server default `JAVA_HOME`
is not JDK 21, set `CARD_SHOP_TRADE_JAVA_HOME` in `config/.<profile>.env`.

```bash
./bin/start.sh start dev
./bin/start.sh status dev
./bin/start.sh stop dev
./bin/start.sh restart dev
```

Logs are written under `logs/` (override with `LOG_DIR` in `config/.<profile>.env`):

- `logs/cardshop-trade-engine.log` — primary business log, managed by logback.
  Rolls over by day and when a single file reaches 200MB; archives keep 90 days
  (named `cardshop-trade-engine.<YYYY-MM-dd>.<seq>.log`).
- `logs/cardshop-trade-engine-<profile>.out` — JVM native output fallback for
  non-prod profiles. The start script runs a background rotation daemon that
  applies the same 200MB + daily copytruncate rollover and keeps archives for 90
  days.

The primary log is authoritative for application logging; `.out` is a safety net
and is normally small in `test` (no console appender). `prod` disables the
stdout fallback by default and redirects JVM stdout/stderr to `/dev/null`, so it
does not create `logs/cardshop-trade-engine-prod.out`. For a temporary JVM
startup diagnosis window, set `STDOUT_LOG_ENABLED=true` in `config/.prod.env`
and restart; turn it off again after collecting evidence.

Health endpoints:

```text
/actuator/health/liveness
/actuator/health/readiness
/actuator/prometheus
```

## Monitoring Templates

The release package includes `monitoring/prometheus-rules/cardshop-trade-engine-alerts.yml`
and `monitoring/grafana/cardshop-trade-engine-overview.json`. They are importable
templates for Prometheus/Alertmanager and Grafana, not proof that production
alerts have been published. RabbitMQ queue depth still comes from the RabbitMQ
management Prometheus/exporter or an operations probe; do not add RabbitMQ
management credentials to the business application.

## V1.2 升级前置条件

本版本统一使用HMAC-SM3，旧SM3签名和签名压测旁路均不再接受。先停止新流量并处理存量回调，再执行Liquibase `0018-v12-signing-credentials`，为所有实例注入相同的独立存储主密钥 `V1_SIGNING_KEY_ENCRYPTION_KEY`，最后让调用方重新getToken后开放流量。

主密钥为安全随机32字节的43字符无填充Base64URL编码，只存部署密钥系统或受限env文件；不得复用JWT/app_secret/aesKey，不得放进JAVA_OPTS、日志、Git。该密钥用于AES-256-GCM保护数据库中的signKey；重启必须保留，直接替换会导致所有历史密钥无法解密。轮换需要另行完成全量密文迁移及验证。

getToken返回token/keyId/signKey；请求凭证默认3小时到期，建议2小时刷新。刷新会替换当前token，但不删除历史signKey。订单创建绑定keyId，历史回调和重试继续使用该绑定；撤销后禁止新签名和重试发送。密钥表不设自动TTL或清理任务。

撤销操作仅由有数据库维护权限的运维人员执行：在目标环境核对商户、keyId和受影响订单后，按`org_code + key_id`设置`recharge_v12_signing_key.revoked_at`。不删除记录，不自动绑定最新密钥。若需更换存量订单绑定，应先停发并与调用方同步新绑定，再事务更新指定订单及重建待发送回调快照；本次未提供自动批量换绑入口。普通重试保留原签名报文。

旧订单`signing_key_id=''`和旧SM3回调快照会被拒绝发送；上线前必须清空处理完成的旧任务，或逐单完成双方确认的迁移。不能通过回退旧签名解决。已有签名检查通过并进入网络发送的在途请求不保证能被随后撤销的操作追回。

测试环境HTTPS域名/路径及证书链仍须由部署负责人确认；本次不猜测地址，也未进行远端联调。模拟器拒绝非回环HTTP入口，禁止关闭TLS证书校验。

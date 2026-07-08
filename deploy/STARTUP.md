# cardshop-trade-engine Startup Guide

## Package Layout

```text
cardshop-trade-engine-<version>/
  app/cardshop-trade-engine-bootstrap-<version>.jar
  bin/start.sh
  config/env.example
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
    url: jdbc:mysql://<host>/cardshop_test?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8&allowMultiQueries=true
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

Logs are written to `logs/cardshop-trade-engine-<profile>.out` by default.

Health endpoints:

```text
/actuator/health/liveness
/actuator/health/readiness
/actuator/prometheus
```

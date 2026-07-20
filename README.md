# cardshop-trade-engine

`cardshop-trade-engine` 是面向交行直充场景拆分出的独立交易服务。当前工程处于 D1 骨架阶段，已落地 Maven 多模块工作区、Spring Boot 启动模块、多环境/Nacos bootstrap、Actuator 探针、Liquibase 入口和 `trade.engine.*` 共享配置绑定。

目标设计见：

- [交行直充业务系统详细设计](docs/交行直充业务系统详细设计.md)
- [交行直充接口 Redis 与 MySQL 一致性及高可用设计实现方案](docs/交行直充业务系统高可用方案设计.md)
- [交行直充业务系统开发时间表](docs/交行直充业务系统开发时间表.md)
- [跨项目联调记忆](docs/跨项目联调记忆.md)
- [贡献者指南](AGENTS.md)

## 技术栈

- Java 21，Maven 3.9+
- Spring Boot 3.3.6，Spring WebFlux，Actuator
- Spring Cloud 2023.0.4，Spring Cloud Alibaba 2023.0.1.2，Nacos
- Redis Reactive，RabbitMQ，MyBatis，MySQL
- Liquibase 4.29.2
- JUnit 5，AssertJ，Spring Boot Test

## 关联测试辅助前端

dev/test 集成测试阶段同时维护独立前端项目 `/Users/mac/Documents/codeSpace/frontend/cardshop-trade-engine-test-console`。该前端用于发送 V1/Actuator 请求、生成 curl、聚合 Redis/RabbitMQ/MySQL 证据和导出执行记录 Markdown。两个项目不在同一个 Git 仓库中，但接口、观测指标、Redis pending 补投入口和集成测试用例变更需要同步维护，具体约束见 [跨项目联调记忆](docs/跨项目联调记忆.md)。

test188 HTTP 访问入口：`http://apitest.kalaiwang.com/testSuite/console/`。

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
- 暂未实现的功能必须在代码对应位置添加 `TODO` 注释，说明剩余工作，并链接到相关设计文档章节。

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
./scripts/package.sh
./scripts/publish-dev-nacos.sh
```

## 本地启动

本机轻量开发优先使用 `local` profile。该模式禁用 Nacos、Redis、RabbitMQ、MySQL、Liquibase 自动连接，只用于启动应用、调试 Actuator 和开发接口骨架。

```bash
./scripts/dev-local.sh
```

`local` 默认端口为 `18081`，可通过 `SERVER_PORT` 覆盖。脚本默认使用 `/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home`；如需指定其他 JDK 21 路径，设置 `CARD_SHOP_TRADE_JAVA_HOME`。

需要联调真实中间件时使用 `dev` profile。本地只提供 profile、端口、Nacos、RabbitMQ、旧卡管代理地址、V1 JWT secret、JDK/JVM 参数等启动引导信息，MySQL、Redis、XXL-Job 和业务开关应来自 Nacos 或部署密钥。`dev` 使用独立 namespace `cardshop-trade-engine-dev`，MySQL datasource 共用原卡管测试库 `cardshop_test`，Redis 连接参数与旧卡管 dev 当前配置保持一致，发布到新服务 Nacos 时使用 Spring Boot 3 的 `spring.data.redis.*`。

```bash
APP_PROFILE=dev \
NACOS_SERVER_ADDR=47.117.130.100:8848 \
NACOS_NAMESPACE=cardshop-trade-engine-dev \
NACOS_GROUP=TRADE_ENGINE_DEV \
NACOS_USERNAME=nacos \
NACOS_PASSWORD='***' \
mvn -pl cardshop-trade-engine-bootstrap -am spring-boot:run
```

`dev` 默认端口为 `8080`，可通过 `SERVER_PORT` 覆盖。

## A39 业务模拟器

`cardshop-trade-engine-simulator` 是独立于主服务发布包的 dev/test 工具，按 `docs/交行直充接口技术文档-V1.1.docx` 模拟 A39 商户侧调用。它支持三种模式：`scenario` 用于单笔联调，完整执行 token、下单、查单和回调接收；`load` 用于下单入口压测，只获取一次 token 后按固定并发创建订单；`supplier-mock` 用于在本机启动供应商 HTTP mock，供交易引擎 dev/test 联调 MINISO 等供应商路由。

### 工具定位与安全边界

模拟器不进入 `./scripts/package.sh` 生成的主服务包，也不会在打包阶段注入 `base_url`、`app_id`、`product_id`、`account`、`out_order_sn`、`app_secret`、`aesKey`、`notify_url` 等业务参数。打包只生成可执行 jar、运行脚本和配置模板；实际参数在运行阶段通过命令行参数或环境变量传入。

`--app-secret` / `A39_APP_SECRET` 和 `--aes-key` / `A39_AES_KEY` 是 `scenario` / `load` 模式的必填敏感参数。`supplier-mock` 模式不需要商户凭证，可单独启动。真实值只应写入部署机本地 `config/a39-simulator.env`、临时环境变量或密钥系统，不能提交到 Git，也不建议直接写进会进入 shell 历史的命令行。

`scenario` / `load` 与 `supplier-mock` 可在同一台机器上并行运行：前者调用交易引擎 V1 接口，后者只监听供应商 HTTP 端口，互不依赖。

`load` 模式只允许在 dev/test 环境使用，必须使用 test188 专用测试商户、测试产品和安全的供应商 mock，避免压到真实供应商或真实商户回调链路。test188 当前 `MINISO` 供应商已在 `cardshop-trade-engine-supplier.yaml` 指向本机 mock：`127.0.0.1:18082/tripartite/coupon/*`，`timeout-ms=10000`；mock 下单接口会**立即**返回 `PROCESSING`，最终结果在 `final-result-delay-ms-min/max`（默认 `1200-9800ms`）窗口内通过查单接口变为 `SUCCESS`/`FAILED`。

### 打包与 test188 配置

本地用 JDK 21 单独打包模拟器：

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

./scripts/package-a39-simulator.sh
```

打包产物位于 `dist/a39-business-simulator-<version>.zip`，zip 内容如下：

```text
a39-business-simulator-<version>/
  app/cardshop-trade-engine-simulator-<version>.jar
  bin/run-a39-simulator.sh
  config/a39-simulator.env.example
```

test188 当前约定部署目录：

```bash
cd /data/apps/cardshop-trade-engine
unzip a39-business-simulator-<version>.zip
cd a39-business-simulator-<version>

cp config/a39-simulator.env.example config/a39-simulator.env
vi config/a39-simulator.env
chmod 600 config/a39-simulator.env
```

`config/a39-simulator.env` 示例。test188 当前 A39 MINISO 样本使用父商品号 `80100022`，不是子商品号 `80100023`：

```bash
export CARD_SHOP_TRADE_JAVA_HOME='/opt/java/jdk-21.0.11'

export A39_APP_SECRET='***'
export A39_AES_KEY='***'

export A39_BASE_URL='http://apitest.kalaiwang.com'
export A39_APP_ID='A35cecjXS7lZ1W30NMN'
export A39_PRODUCT_ID='80100022'
export A39_ACCOUNT='18186222336'
export A39_NUM='1'
export A39_ORDER_PRICE='0'
export A39_PRODUCT_TYPE='1'
export A39_REQUEST_SM4='true'
export A39_CALLBACK_BIND_HOST='0.0.0.0'
export A39_CALLBACK_PORT='19039'
```

运行脚本会自动 `source config/a39-simulator.env`，并参考 `scripts/package.sh` 的规则探测 JDK 21：优先使用 `CARD_SHOP_TRADE_JAVA_HOME`，其次是 `JAVA_HOME`，再依次尝试 test188 当前路径 `/opt/java/jdk-21.0.11`、`/usr/local/jdk-21`、`/opt/jdk-21` 和 macOS 默认路径 `/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home`。命令行参数优先级高于同名环境变量。

test188 的 nginx 只把 `http://apitest.kalaiwang.com/v1/cardapi/**` 转发到新交易引擎；`http://apitest.kalaiwang.com` 域名下的其他路径仍按原有 `location /` 进入旧卡管兜底。因此模拟器默认 `base-url` 可用于当前 A39 业务联调，是因为模拟器只调用 `/v1/cardapi/**` 接口。`/testSuite/trade-api/dev/**` 仍保留给测试辅助前端作为同域代理入口。

### scenario 单笔联调模式

`scenario` 是默认模式，用于验证一笔订单从 token、下单、查单到回调接收的完整链路。它会启动本机回调接收器，按 `query-attempts/query-interval` 查单，并在 `callback-wait` 时间内等待回调。

```bash
./bin/run-a39-simulator.sh --help

./bin/run-a39-simulator.sh \
  --mode scenario \
  --out-order-sn A39TEST$(date +%Y%m%d%H%M%S) \
  --notify-url http://127.0.0.1:19039/callback/order \
  --query-attempts 3 \
  --query-interval 30s \
  --callback-wait 10m
```

如果模拟器和被测交易引擎不在同一台机器，`--notify-url` 不能使用 `127.0.0.1`，必须换成被测平台能访问到的内网 IP、nginx 代理地址或公网隧道地址。

源码运行适合本机联调或快速验证参数解析：

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

A39_APP_SECRET='***' \
A39_AES_KEY='***' \
mvn -f cardshop-trade-engine-simulator/pom.xml exec:java \
  -Dexec.args="--mode scenario --notify-url http://可被平台访问的地址/callback/order"
```

### load 下单压测模式

`load` 模式用于 test188 A39 创建订单入口基准压测。它会先获取一次 token，然后按固定并发发送固定总量的创建订单请求；默认把业务码 `200` 和 `510` 计为成功。该模式固定使用空字符串 `notify_url`，不查单，不启动 callback server，也不等待回调，因此不会制造商户回调任务。

基线命令：

```bash
./bin/run-a39-simulator.sh \
  --mode load \
  --concurrency 10 \
  --total-orders 200 \
  --request-timeout 10s \
  --out-order-prefix PERF-A39-$(date +%Y%m%d%H%M%S)
```

压测前确认 `A39_PRODUCT_ID` 指向 test188 专用测试父产品，供应商路由指向安全 mock，并确认压测窗口内没有其它同商户大流量测试。所有请求会生成唯一 `out_order_sn` 和 `nonce_str`；进程退出码为 `0` 表示全部请求完成且无失败，否则为非 `0`。

### supplier-mock 供应商 HTTP mock 模式

`supplier-mock` 模式在本机启动供应商 HTTP mock，注册下单/查单四条路径（`/supplier/dispatch`、`/supplier/query` 以及 MINISO 兼容路径 `/tripartite/coupon/batchGrandCoupon`、`/tripartite/coupon/queryGrand`）。该模式**不需要** `app-secret` / `aes-key`，可与 `scenario` / `load` 在同一台机器并行运行。

```bash
./bin/run-a39-simulator.sh \
  --mode=supplier-mock \
  --bind-host=0.0.0.0 \
  --port=18082 \
  --final-result-delay-ms-min=1200 \
  --final-result-delay-ms-max=9800 \
  --success-rate=0.95
```

Nacos / `cardshop-trade-engine-supplier.yaml` 中将 `dispatch-url` / `query-url` 指向 mock 基址即可，例如：

- MINISO 兼容路径：`http://127.0.0.1:18082/tripartite/coupon/batchGrandCoupon` 与 `http://127.0.0.1:18082/tripartite/coupon/queryGrand`
- 通用路径：`http://127.0.0.1:18082/supplier/dispatch` 与 `http://127.0.0.1:18082/supplier/query`

`127.0.0.1` 是从**每个 trade-engine 进程所在机器**解析的：mock 必须与发起派单的引擎实例同机，或把 URL 改成 mock 所在主机的内网 IP。pro143/pro140/pro120 多实例共用同一 Nacos 时，只在本机起了 mock 的节点才能通；其余节点会出现连接失败并触发 Outbox 可重试。该模式仅限 dev/test/联调，禁止把生产供应商流量指到 mock。

下单接口立即返回 `PROCESSING`；查单接口在配置的延迟窗口过后返回最终 `SUCCESS`/`FAILED` 结果。

### 参数与环境变量

通用参数：

| 参数 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--mode` | `A39_MODE` | `scenario` | 运行模式：`scenario`、`load` 或 `supplier-mock`。 |
| `--base-url` | `A39_BASE_URL` | `http://apitest.kalaiwang.com` | 请求前缀，不包含接口路径。 |
| `--app-id` | `A39_APP_ID` | `A35cecjXS7lZ1W30NMN` | A39 机构 `app_id`。 |
| `--app-secret` | `A39_APP_SECRET` | 必填 | A39 机构密钥，必须用 `***` 之外的真实值在本地安全注入。 |
| `--aes-key` | `A39_AES_KEY` | 必填 | A39 SM3/SM4 密钥，必须用 `***` 之外的真实值在本地安全注入。 |
| `--product-id` | `A39_PRODUCT_ID` | `1011` | 直充父产品 ID；test188 MINISO 样本当前使用 `80100022`。 |
| `--account` | `A39_ACCOUNT` | `18186222336` | 充值账号。 |
| `--num` | `A39_NUM` | `1` | 下单数量。 |
| `--order-price` | `A39_ORDER_PRICE` | `0` | 订单金额兼容字段，单位分；当前新链路接单金额以后端选中的商品快照为准，该字段不参与旧协议签名。 |
| `--product-type` | `A39_PRODUCT_TYPE` | `1` | 产品类型。 |
| `--request-sm4` | `A39_REQUEST_SM4` | `true` | 默认模拟真实 V1 订单/查单链路，请求体按 `aesKey` 做 SM4 加密；联调明文环境时显式设为 `false`。 |
| `--request-timeout` | `A39_REQUEST_TIMEOUT` | `10s` | 单次 HTTP 请求超时时间，支持 `ms`、`s`、`m` 或 ISO-8601 duration。 |

`scenario` 参数：

| 参数 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--out-order-sn` | `A39_OUT_ORDER_SN` | 自动生成 | 商户订单号。 |
| `--notify-url` | `A39_NOTIFY_URL` | `http://127.0.0.1:<callback-port>/callback/order` | 下单时发送给平台的回调地址，`scenario` 模式必须非空。 |
| `--callback-bind-host` | `A39_CALLBACK_BIND_HOST` | `0.0.0.0` | 本机回调接收器监听地址。 |
| `--callback-port` | `A39_CALLBACK_PORT` | `19039` | 本机回调接收器监听端口。 |
| `--query-attempts` | `A39_QUERY_ATTEMPTS` | `3` | 下单后最多查单次数。 |
| `--query-interval` | `A39_QUERY_INTERVAL` | `30s` | 查单间隔。 |
| `--callback-wait` | `A39_CALLBACK_WAIT` | `10m` | 下单和查单后等待回调的最长时间。 |

`load` 参数：

| 参数 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--concurrency` | `A39_LOAD_CONCURRENCY` | `10` | 最大在途下单请求数。 |
| `--total-orders` | `A39_LOAD_TOTAL_ORDERS` | `200` | 本次压测创建订单总数。 |
| `--out-order-prefix` | `A39_OUT_ORDER_PREFIX` | 自动生成 | 压测订单号前缀，实际订单号会追加运行 ID 和序号。 |
| `--expected-create-codes` | `A39_EXPECTED_CREATE_CODES` | `200,510` | 计为成功的创建订单业务码。 |
| `--error-sample-limit` | `A39_ERROR_SAMPLE_LIMIT` | `20` | 报告中最多保留的失败样本数。 |
| `--progress-interval` | `A39_LOAD_PROGRESS_INTERVAL` | `10s` | 压测进度日志间隔；传 `0` 可关闭。 |

### notify_url 口径

`notify_url` 是下单接口 `/v1/cardapi/order/createRechargeOrder` 请求体里的回调地址，模拟器命令行参数名是 `--notify-url`，环境变量名是 `A39_NOTIFY_URL`。非空时必须是 `http` 或 `https` URL，并且必须是“被测平台能访问到”的地址，而不是只对当前终端可见的地址。

V1 下单接口允许 `notify_url` 为空字符串；为空时订单仍会正常受理、持久化和履约，但平台不会生成商户回调任务，也不会写入 `recharge_notify_task`。`scenario` 模式用于回调链路联调，要求 `notify_url` 非空；`load` 模式为了避免回调噪声，强制使用空 `notify_url`，如果显式传入非空值会直接拒绝启动。

`--callback-bind-host` 和 `--callback-port` 只决定模拟器本机监听在哪个网卡和端口；`--notify-url` 才是发送给平台、让平台回调的完整 URL。两者通常端口一致，但含义不同。

常见设置方式：

| 场景 | 推荐 `notify_url` | 说明 |
| --- | --- | --- |
| 模拟器和被测平台在同一台机器 | `http://127.0.0.1:19039/callback/order` | 只有平台进程和模拟器进程同机时才可以用 `127.0.0.1`。 |
| 模拟器部署在 test188 或同网段机器 | `http://<模拟器内网IP>:19039/callback/order` | 确保被测平台机器能访问该 IP 和端口，防火墙需放行。 |
| 通过 nginx 代理回调 | `http://<nginx域名或IP>/a39/callback/order` | nginx 需要把该路径转发到 `http://<模拟器IP>:19039/callback/order`。 |
| 模拟器在开发电脑，被测平台在远端 | 公网隧道或可达代理地址 | 远端平台无法访问开发电脑上的 `127.0.0.1`，需使用内网穿透、VPN、nginx 或部署到平台可达机器。 |

注意：`notify_url` 里的 `127.0.0.1` 是从被测平台机器视角解析的。如果平台部署在远端服务器，`http://127.0.0.1:19039/callback/order` 指向的是远端服务器自己，不是开发者电脑。

### 日志、报告与排障

`scenario` 模式会在标准输出打印三类联调日志：

- `[A39][HTTP][request]`：发送请求的 curl，`app_secret` 和 `Authorization` 会脱敏。
- `[A39][HTTP][response]`：对应 HTTP 响应状态码和响应体原文。
- `[A39][ORDER][status-change]`：查单得到的 `order_status` 发生变化时记录商户订单号、状态码和来源。

`load` 模式默认关闭逐笔 HTTP 请求/响应日志，只输出启动摘要、周期性进度和最终 `key=value` 报告。报告包含 `mode/baseUrl/runId/outOrderPrefix/concurrency/totalOrders/requestTimeout/completed/successful/failed/code.200/code.510/minMs/maxMs/p50Ms/p95Ms/p99Ms/qps/exitCode`，以及客户端耗时拆分字段 `timing.prepare|untilHeaders|bodyRead|parse|clientTotal` 的 min/max/p50/p95/p99，便于复制到集成测试记录或脚本解析。

如果启动时报：

```text
UnsupportedClassVersionError: ... class file version 65.0 ... recognizes class file versions up to 52.0
```

说明当前实际执行的是 Java 8。`65.0` 是 Java 21 字节码，`52.0` 是 Java 8 运行时。处理方式是在部署机安装 JDK 21，然后在 `config/a39-simulator.env` 中设置：

```bash
export CARD_SHOP_TRADE_JAVA_HOME='/opt/java/jdk-21.0.11'
```

如果 token 阶段报 `getToken failed: code=401, msg=凭证错误`，说明请求已经到达服务端，但 `app_id + app_secret` 在目标环境的 `mc_ext_depart` 中没有匹配到启用商户。优先检查本地 env 是否仍是模板值 `***`、运行脚本是否加载当前包下的 `config/a39-simulator.env`、请求路径是否为 `/v1/cardapi/auth/getToken`，以及服务端目标环境商户是否启用。

如果下单阶段报 `Create response: code=500, msg=商户配置缺失或版本不一致`，说明请求已经通过 token、SM4 解密和 SM3 验签，进入 Redis 受理 Lua 后因为商户或商品配置基线缺失而 fail closed。优先检查目标 Redis 是否存在：

```text
cardshop:trade:recharge:{A39}:merchant:config
cardshop:trade:recharge:{A39}:product:<selected_product_id>:config
```

`--product-id` 传入的是 MySQL `mc_depart_product.parent_id` 对应的父产品号，不是最终派单子商品号。服务端会从该父产品下选择子商品，并在 Lua 前按 `selected_product_id` 原子补热缺失配置；已有 key 不覆盖，Lua 仍校验 `version/status/price/product_type`。例如数据库若为父号 `80100022`、子号 `80100023`，模拟器应传 `--product-id 80100022`，Redis key 应检查 `product:80100023:config`。

## 部署打包

生成可部署 zip 包：

```bash
./scripts/package.sh
```

脚本会强制检测 JDK 21，并在 `dist/` 下生成 `cardshop-trade-engine-<version>.zip`。zip 内包含应用 jar、`bin/start.sh`、`config/env.example` 和 `STARTUP.md`。服务器解压后复制模板为对应的 `config/.<profile>.env`，从部署密钥系统填写 Nacos、RabbitMQ、端口、JDK、旧卡管代理地址、V1 JWT secret 和 JVM 参数；`.env` 会被 Bash `source` 读取，密码、JWT secret、API key 等同类密钥值必须用单引号包起来，尤其当值包含 `$`、`!`、空格、反引号、`&`、`|`、`;`、`<`、`>` 时。启动脚本只校验必填环境变量并导出给 Spring placeholder 读取，不把密钥写入 JVM argv，然后执行：

```bash
./bin/start.sh start dev
./bin/start.sh status dev
./bin/start.sh stop dev
```

`scripts/publish-dev-nacos.sh` 用于从本机忽略文件 `config/env.dev` 发布 `cardshop-trade-engine-dev.yaml` 到 dev Nacos。该文件可以包含真实 Nacos/MySQL 密码，以及 `CARD_SHOP_REDIS_HOST`、`CARD_SHOP_REDIS_PORT`、`CARD_SHOP_REDIS_DATABASE`、`CARD_SHOP_REDIS_PASSWORD` 等 Redis 发布变量，但受 `.gitignore` 保护，不能提交。旧卡管 dev Nacos 只作为可选辅助来源；确需临时读取时显式设置 `CARD_SHOP_REDIS_LOAD_FROM_CARDSHOP_NACOS=true` 和对应 `CARD_SHOP_DEV_NACOS_*` 凭据。

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

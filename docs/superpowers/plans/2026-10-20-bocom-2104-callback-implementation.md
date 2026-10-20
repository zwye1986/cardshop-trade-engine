# Bocom 2104 Callback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将订单、核销和作废通知通过官方 SDK 可靠投递到交行 2104，保留直连兼容且不访问真实银行。

**Architecture:** 统一通知准备模块将可信订单事实与显式路由合成不可变任务，银行正文不依赖商户 HMAC。发送 seam 保留 `MerchantNotifyClientPort`，用冻结通道选择银行/直连 adapter；任务持久化提供领取代次 CAS，逐次记录网关请求。

**Tech Stack:** Java 21、Maven 3.9、Spring Boot 3.3.6、MyBatis、Liquibase、JUnit 5/AssertJ、官方 bocom-openapi-sdk 2.2.6、项目受管 Jackson/Bouncy Castle、Python 3（SDK 安装脚本测试）。

---

## 执行约束与文件职责

在 `/Users/mac/codespace/cardshop-trade-engine/.worktrees/feat/bocom-callback` 执行全部命令。每次 Maven 命令前设置：

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PATH="$JAVA_HOME/bin:$PATH"
```

已批准设计：`docs/superpowers/specs/2026-10-20-bocom-2104-callback-design.md`。不修改原工作区的用户文档，不运行远端联调、部署或 Nacos 发布。没有子代理工具，使用本会话顺序执行；完成每项记录红/绿测试证据及提交。

文件分工：

- domain `notify/MerchantNotifyOperation.java`：1/2/3 的唯一业务枚举。
- application `recharge/Bocom2104PayloadCodec.java`：纯 Java 固定字段格式、长度和字符验证，无 Spring/SDK/IO。
- application `recharge/NotifyDeliveryChannel.java`、`MerchantNotifyRoute.java`、`MerchantNotifyRoutePort.java`：通道及冻结路由契约。
- application `recharge/MerchantNotifyPreparationService.java`：可信订单/事件到银行或直连任务，保留首次正文、事件时间与 nonce。
- infrastructure `notify/bocom/`：SDK 请求响应 DTO、client wrapper、发送 adapter、路由配置 adapter。
- infrastructure persistence 的现有任务实体/Mapper/adapter：冻结数据、领取代次、CAS 和逐次尝试记录。
- simulator `bocom/`：独立离线银行网关桩，使用运行时生成的测试密钥。
- scripts `install-bocom-sdk.sh`、`bocom-openapi-sdk.pom.xml`：校验并安装用户自备官方 jar，固定坐标与受管依赖相容。

## Task 1: 协议枚举与冻结正文编码

**Files:**
- Create: `cardshop-trade-engine-domain/src/main/java/com/cardshop/trade/engine/domain/notify/MerchantNotifyOperation.java`
- Create: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/Bocom2104PayloadCodec.java`
- Create: `cardshop-trade-engine-application/src/test/java/com/cardshop/trade/engine/application/recharge/Bocom2104PayloadCodecTest.java`

- [x] 1. 编写测试，先以反射断言 codec 类型存在，取得编译成功但断言失败的红灯；加入三类通知、固定字段顺序、空值、时间中冒号与机密字段拒绝断言。公开接口为 `String encode(MerchantNotifyOperation operation, Map<String, ?> fields)` 与 `void validate(String encoded)`。

```java
@Test void codecMustExist() {
    assertThatCode(() -> Class.forName(
        "com.cardshop.trade.engine.application.recharge.Bocom2104PayloadCodec"))
        .doesNotThrowAnyException();
}
```

- [x] 2. 执行 `mvn -pl cardshop-trade-engine-application -am -Dtest=Bocom2104PayloadCodecTest -Dsurefire.failIfNoSpecifiedTests=false test`，确认失败原因为缺少 codec。
- [x] 3. 实现枚举 `ORDER_STATUS("1")/REDEEMED("2")/CANCELLED("3")`，编码固定白名单并拒绝缺字段、未知字段、null、逗号与业务 sign/keyId。金额只接受非负整数，不接受小数/科学记数法。字符校验核心为：

```java
for (int i = 0; i < text.length(); i++) {
    char c = text.charAt(i);
    if (Character.isHighSurrogate(c)) {
        if (++i >= text.length() || !Character.isLowSurrogate(text.charAt(i))) {
            throw new IllegalArgumentException("invalid Unicode in bank payload");
        }
    } else if (Character.isLowSurrogate(c)) {
        throw new IllegalArgumentException("invalid Unicode in bank payload");
    }
}
if (text.codePointCount(0, text.length()) > 2500) {
    throw new IllegalArgumentException("bank payload exceeds character limit");
}
```

- [x] 4. 增加中英文/补充字符及 2499/2500/2501 边界、不可表示字符、意外逗号、整数金额测试，运行同一命令至绿；运行 application 全模块测试。
- [x] 5. 提交 `feat: add bocom notification operations and payload codec`。

执行证据：`/tmp/bocom-codec-red.log` 首次缺类型为 1 项断言失败；`/tmp/bocom-codec-behavior-red.log` 为未实现行为红灯；`/tmp/bocom-codec-green.log` 为 domain/application 全量绿灯，其中编码器 15 项全部通过。临时日志非长期归档保证。

## Task 2: SDK 可重复安装及离线兼容

**当前阻塞（2026-10-20）：** 安装前只读预检发现 SDK `WebUtils` 静态初始化使用 `DefaultTrustManager`，其 `checkServerTrusted` 直接 return。JDK 21 离线反射探针对该方法传入空 X509Certificate 数组仍正常返回，输出 `SDK_DEFAULT_TRUST_MANAGER_ACCEPTED_EMPTY_SERVER_CERTIFICATE_CHAIN`；未建立网络连接。SDK 默认保留的主机名检查不等于服务器证书信任链校验。官方提供 `DefaultBocomClient.openCertVerify(String certificatePath)`，但实现修改 `WebUtils` 静态信任配置，采用时须增加受控 TLS 证书配置及全局初始化约束。先由用户确认采用官方显式证书校验还是更换传输 seam；在确认前不安装/集成 SDK、不削弱 TLS、不中途改为自研协议。当前 Task 2 尚未完成。

复核方式：对接入包中的 `lib/bocom-openapi-sdk-2.2.6.jar` 执行 `javap -classpath "$SDK" -c -p 'com.bocom.api.utils.WebUtils$DefaultTrustManager'`，可见 `checkServerTrusted` 只有 return；对 `WebUtils` 的 static 初始化核对其默认安装此 TrustManager。离线探针只反射构造该默认 TrustManager 并调用 `checkServerTrusted(new X509Certificate[0], "RSA")`，未调用业务 execute。临时源码 `/tmp/BocomSdkTrustProbe.java`、字节码证据 `/tmp/bocom-webutils-bytecode.txt` 不保证长期留存。

本阶段最终回归：显式 JDK 21，`mvn -B test` 根工程 690 项（跳过 19 项），`mvn -B -f cardshop-trade-engine-simulator/pom.xml test` 模拟器 77 项，均零失败/错误。日志 `/tmp/bocom-stage1-reactor.log`、`/tmp/bocom-stage1-simulator.log`。这是阶段回归，不代表 SDK 或完整银行通知已通过验收。

**Files:**
- Create: `scripts/install-bocom-sdk.sh`
- Create: `scripts/bocom-openapi-sdk.pom.xml`
- Create: `scripts/tests/test_install_bocom_sdk.py`
- Modify: `cardshop-trade-engine-infrastructure/pom.xml`
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/notify/bocom/Bocom2104Request.java`
- Create: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/notify/bocom/Bocom2104Response.java`
- Create: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/notify/bocom/BocomSdkCompatibilityTest.java`

- [ ] 1. Python 测试先断言安装脚本存在，运行 `python3 -m unittest discover -s scripts/tests -p 'test_install_bocom_sdk.py'`，确认缺脚本红灯。随后覆盖缺文件和 SHA 不符必须非零退出、成功时固定 Maven 坐标且不输出 jar 内容。
- [ ] 2. 实现安装脚本，仅接受自备 jar 或接入包目录。校验固定 SHA `8df82ae2931dd5cc52fcbbea5643cd6819412f27f9cf64478b476c2e53120fad`；不匹配时绝不执行 Maven 安装。使用脚本所在目录解析 POM，不依赖 cwd。

```sh
mvn install:install-file -Dfile="$sdk_jar" \
  -DpomFile="$script_dir/bocom-openapi-sdk.pom.xml"
```

POM 使用 `com.bocom:bocom-openapi-sdk:2.2.6`，依赖 Apache HttpClient 4.5.14、Commons IO 2.17.0、Commons Logging 1.3.4、Jackson、SLF4J 和 BC；项目侧保留 Boot 管理的版本。先用 `jdeps` 确认 SDK 实际需要的包，只有确有引用才加依赖，不复制整个 lib 目录。
- [ ] 3. Python 测试至绿，然后用用户给的 jar 安装本地 Maven 制品。基础设施增加该固定依赖，DTO 继承 SDK AbstractBocomRequest/BocomResponse，结构严格为 head/body，`isNeedEncrypt=false`、`getMethod=POST`，body 中 trx_data_imprt/protocol 都是字符串。
- [ ] 4. SDK 测试通过实际 `DefaultBocomClient` 构造业务 JSON（测试子类只暴露 protected buildBizContentStr），断言外层编码保留字符串，并用 SDK 原生签名实现与生成密钥验证 RSA/SM2。测试先以缺类型/缺行为红灯，后补 DTO；禁止以 mock 成功代替密码兼容证据。
- [ ] 5. 运行 `mvn -pl cardshop-trade-engine-infrastructure -am -Dtest=BocomSdkCompatibilityTest -Dsurefire.failIfNoSpecifiedTests=false test` 和 `mvn -pl cardshop-trade-engine-infrastructure -am dependency:tree`，核对未降级受管依赖；提交 `build: integrate verified bocom sdk artifact`。

## Task 3: 显式路由与业务事实准备

**Files:**
- Create application `recharge/NotifyDeliveryChannel.java`, `MerchantNotifyRoute.java`, `MerchantNotifyRoutePort.java`, `MerchantNotifyPreparationService.java`
- Modify application `recharge/RechargeNotifyTaskDraft.java`
- Create test `cardshop-trade-engine-application/src/test/java/com/cardshop/trade/engine/application/recharge/MerchantNotifyPreparationServiceTest.java`
- Create infrastructure `notify/bocom/ConfiguredMerchantNotifyRouteAdapter.java`
- Modify `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineProperties.java`

- [ ] 1. 编写不带商户 HMAC 密钥的银行准备测试：空下单 URL 仍得到 `BOCOM_OPENAPI` 任务，冻结 protocol/网关，正文无 sign/keyId；直连空地址仍跳过。运行精确测试观察红灯。
- [ ] 2. 定义 route record，含 channel/orgCode/routeVersion/gatewayUrl/protocol/appId/signatureAlgorithm/privateKeyRef/bankPublicKeyRef。route port 用可信 orgCode 与下单 URL 解析 Optional route，注册银行商户但缺配置时抛出可恢复配置异常，不返回 Optional.empty。enabled 只控制发送，不控制分类。
- [ ] 3. 准备模块接受 route port、旧直连 signer、Clock；订单/作废/核销统一从可信订单事实生成 draft。银行分支使用 codec，使用 SecureRandom 16 字节 nonce，订单金额以 long 保存，核销实际时间/作废事实时间保持原语义。旧直连 signer 只在 DIRECT_JSON 分支调用。draft 新字段为 channel/routeSnapshot/encodedPayload，旧构造方法明确默认 DIRECT_JSON，避免历史测试和调用方被静默改通道。
- [ ] 4. 增加重复 prepare 后写库幂等由原 taskId 保证的断言，以及缺路由、跨商户 route、含逗号、直连签名行为测试。运行准备模块及原 `V1MerchantNotifyPayloadSignerTest`；提交 `feat: prepare frozen bank notifications by merchant route`。

## Task 4: 数据库、领取代次与尝试审计

**Files:**
- Modify: `cardshop-trade-engine-migration/src/main/resources/db/changelog/db.changelog-master.yaml`
- Modify application `recharge/RechargeNotifyTask.java`, `RechargeNotifyWorkerTaskPort.java`
- Modify infrastructure persistence `entity/RechargeNotifyTaskDO.java`, `mapper/RechargeNotifyTaskMapper.java`, `MybatisRechargeNotifyTaskAdapter.java`
- Create infrastructure persistence `entity/RechargeNotifyAttemptDO.java`, `mapper/RechargeNotifyAttemptMapper.java`
- Create test `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/persistence/RechargeNotifyClaimCasTest.java`

- [ ] 1. 先写 H2/MyBatis 真实 SQL 竞争测试：Worker A 租约过期、B 重领后，A 的 SUCCESS/FAILED/MANUAL_REQUIRED 均更新零行；B 正常成功。增加银行空目标不得被旧清理成功的失败测试。
- [ ] 2. 追加 changeset 增加 delivery_channel（DIRECT_JSON 默认）、route_snapshot、encoded_payload、claim_version（0 默认）及 recharge_notify_attempt 表。新增 DTO/DDL 注释完全一致。attempt 以 msg_id 唯一，按 task_id+created_at 查询。
- [ ] 3. claim 原子更新 `claim_version=claim_version+1`，返回持有者、截止时间与新版本；结果端口返回 boolean 表示 CAS 命中。每次结果更新 where 条件为：

```sql
where task_id = :taskId and status = 'PROCESSING'
  and claim_version = :claimVersion and locked_by = :workerId
  and locked_until > :now
```

尝试登记必须使用同一领取条件核验后写入；网络返回仅修改自己的 msgId 记录。bank 空目标转故障恢复，不执行直连清理规则。
- [ ] 4. 运行 CAS 测试、迁移测试及现有 notify persistence 测试；验证过期领取不误报成功、计数不重复递增；提交 `feat: persist notify routes and fence worker results`。

## Task 5: 接入三个事实生成入口

**Files:**
- Modify application `recharge/RechargeOutboxWorkerService.java`
- Modify infrastructure persistence `MybatisMinisoWriteOffStore.java`
- Modify infrastructure config `TradeEngineInfrastructureConfiguration.java`, `TradeEngineFulfillmentAutoConfiguration.java`, `MinisoWriteOffAutoConfiguration.java`
- Extend existing tests `RechargeOutboxWorkerServiceTest`, Miniso write-off persistence tests

- [ ] 1. 三个入口分别新增空 notify_url 的银行场景红灯；缺路由时可靠 Outbox/核销事件仍可恢复；无商户 HMAC 密钥不阻止银行通知。
- [ ] 2. 注入 Task 3 准备模块，取代入口中的全局 URL 跳过及银行签名依赖；保留直接构造调用的兼容构造方法。名创任务、凭据和事件完成仍在同一事务提交。已有任务比对可信事实与冻结目标，不因重试重建 nonce 或改正文。
- [ ] 3. 覆盖重复事件、不重复供应商履约/退款、名创 3/2 与 3/3 和事实时间一致性；原直连用例不变。运行精确测试至绿；提交 `feat: enqueue all bocom callback operations reliably`。

## Task 6: 银行投递、结果分类与 Worker

**Files:**
- Create infrastructure `notify/bocom/BocomMerchantNotifyClientAdapter.java`, `notify/RoutingMerchantNotifyClientAdapter.java`
- Modify application `recharge/MerchantNotifyRequest.java`, `MerchantNotifySendResult.java`, `RechargeNotifyWorkerService.java`
- Modify infrastructure config wiring and bootstrap logging config
- Create infrastructure test `notify/bocom/BocomMerchantNotifyClientAdapterTest.java`
- Extend application `recharge/RechargeNotifyWorkerServiceTest.java`

- [ ] 1. 先写 SDK 响应判定、发送暂停、缺凭据版本、不可识别通道和 CAS 未命中不算成功的失败测试。
- [ ] 2. result 分类为 SUCCESS/RETRYABLE/DEFERRED/MANUAL_REQUIRED，兼容旧静态 success/failed 工厂。DEFERRED 释放租约并推迟下次扫描，不消耗物理发送次数。网络超时和 P 走 RETRYABLE，验签异常/报文错误走 MANUAL_REQUIRED；不把异常原文落 last_error。
- [ ] 3. 银行 adapter 复核可信任务身份和 route 后，先保存新的 msgId 关联记录，再调用 SDK；验签通过且 S+200 才 SUCCESS。各次请求保持 frozen encodedPayload/protocol。路由 adapter 只看冻结 channel，不按当前商户配置换通道。
- [ ] 4. Worker 用新 CAS 返回值统计，不把未持有租约的回写算成功。校验连接+读取总预算小于租约；SDK/Apache wire 报文日志默认禁用，业务异常日志使用 LogStormGuard 并脱敏。
- [ ] 5. 运行新增 adapter/worker 与原直连 guard 测试，审查所有 catch 是否有日志或立即重抛；提交 `feat: deliver bocom notifications with verified acknowledgements`。

## Task 7: 离线银行桩、集成证据与回归

**Files:**
- Create simulator `src/main/java/com/cardshop/trade/engine/simulator/bocom/Bocom2104GatewayServer.java`
- Create simulator `src/test/java/com/cardshop/trade/engine/simulator/bocom/Bocom2104GatewayServerTest.java`
- Create infrastructure test `notify/bocom/Bocom2104DeliveryIntegrationTest.java`
- Create `scripts/bocom-2104-example.py`, `docs/交行2104回调接入指引.md`
- Modify `docs/集成测试.md`, both main design docs, this plan

- [ ] 1. 先写本地生成 RSA/SM2 测试密钥的 SDK↔loopback 桩闭环测试。断言 form 和 SDK 网关签名，通过可验签响应完成 S+200；覆盖业务失败、P、空响应、非 2xx、响应篡改、错误公钥和超时。
- [ ] 2. simulator 支持选定的成功/失败响应模式、首冒号解析、业务事件去重测试与请求关联查询。不把测试银行私钥写入仓库；使用随机 loopback 端口，不配置可访问真实银行的默认地址。
- [ ] 3. 可执行示例只生成合成业务正文并校验长度，不默认发网路请求。指引写明 SDK 校验/安装、冻结路由、私钥安全注入、仅 dev/test、实际 protocol 由银行提供与旧直连验收不等同银行验收。
- [ ] 4. 在《集成测试.md》追加本地集成的日期、profile、Nacos DataIds（未使用）、中间件（无外部）、合成订单范围、命令、结果和局限。更新两个主设计的“尚未实现”表述，仅在实际通过验证后更新，不声称部署或真实银行通过。
- [ ] 5. 执行 `mvn test`、`mvn -f cardshop-trade-engine-simulator/pom.xml test`、Python 脚本测试、`git diff --check`。逐条核对 spec 第 8 节测试清单，未覆盖项目明确列为剩余而不宣称完成。
- [ ] 6. 自审金额/时间/密钥/路由/CAS/日志，记录测试数量、跳过项与证据。提交 `test: verify bocom callbacks offline and document integration`，在不自动合并或推送的前提下向用户报告。

## 计划自审

- 协议、字符长度及业务安全字段：Task 1；SDK 制品和密码兼容：Task 2。
- 明确商户通道、冻结正文及目标：Task 3；数据库、尝试审计与租约：Task 4。
- 三入口、可靠事务与无密钥生成：Task 5；ACK、重试分类与日志：Task 6。
- 实际 SDK 传输闭环、模拟器、操作示例和执行证据：Task 7。
- 不部署、不使用生产数据、不覆盖用户未提交文件的约束适用于所有任务。

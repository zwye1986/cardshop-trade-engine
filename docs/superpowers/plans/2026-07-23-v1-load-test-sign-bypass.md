# V1 Load-Test Sign Bypass Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 当请求头 `X-Load-Test` 等于配置口令时，下单预检仍执行 SM3 计算但不比对客户端 `sign`，方便 JMeter 压测。

**Architecture:** 在 `V1ProtocolPrecheckService.precheckCreate` 中始终调用 `V1Sm3SignatureVerifier.verifyCreate`；仅当配置了非空 `trade.engine.v1.load-test-token` 且 Header 常量时间匹配时，忽略 `valid=false`。Controller 把 Header 经 `V1MerchantCommand.loadTestHeader` 传入用例，查单/作废路径不启用旁路。

**Tech Stack:** Java 21、Spring WebFlux、JUnit 5、AssertJ、`TradeEngineProperties` / Nacos 绑定配置。

**Spec:** `docs/superpowers/specs/2026-07-23-v1-load-test-sign-bypass-design.md`

---

## File map

| File | Responsibility |
| --- | --- |
| `docs/交行直充业务系统详细设计.md` | 记录旁路语义与配置项 |
| `docs/交行直充业务系统高可用方案设计.md` | 记录口令旁路风险与运维约束 |
| `TradeEngineProperties.V1` | 绑定 `load-test-token` |
| `V1ProtocolSettings` | 向 API 模块传递 `requestSm4Enabled` + `loadTestToken` |
| `V1CardApiConfiguration` | 从 properties 构造 settings |
| `V1ProtocolPrecheckService` | create 路径签名旁路 + rate-limited warn |
| `V1MerchantCommand` | 携带可选 `loadTestHeader` |
| `V1RechargeOrderController` | 读取 `X-Load-Test` 写入 command |
| `V1CreateRechargeOrderUseCaseService` | 把 `loadTestHeader` 传给 `precheckCreate` |
| 相关单测 | 旁路矩阵与回归 |

本期不做：算签 HTTP 接口、查单/作废旁路、Micrometer 计数器（spec 可选；首期仅 rate-limited warn）。

---

### Task 1: 更新两份设计文档

**Files:**
- Modify: `docs/交行直充业务系统详细设计.md`（「3. API/V1 接入层」V1 协议基线表附近）
- Modify: `docs/交行直充业务系统高可用方案设计.md`（「21. 容量规划与压测基线」附近）

- [ ] **Step 1: 更新详细设计**

在「V1 协议基线」表增加一行：

```markdown
| 压测签名旁路 | 请求头 `X-Load-Test` 与 `trade.engine.v1.load-test-token` 常量时间相等时，`createRechargeOrder` 预检仍执行 SM3，但不比对客户端 `sign`；口令为空时旁路永不生效。查单/作废不启用。口令禁止写入日志与仓库。 |
```

在创建订单协议处理流程第 3 步附近补充：旁路只影响签名结果比对，不影响 token、timestamp、SM4 与业务校验。

在 `trade.engine.v1.*` 配置表增加：

```markdown
| `trade.engine.v1.load-test-token` | `TRADE_ENGINE_V1_LOAD_TEST_TOKEN` | 敏感，默认空。非空且 Header 匹配时启用下单签名结果旁路。 |
```

- [ ] **Step 2: 更新高可用方案设计**

在「21. 容量规划与压测基线」增加：

```markdown
### 压测签名结果旁路

JMeter 可通过 `X-Load-Test: <load-test-token>` 跳过 `createRechargeOrder` 的签名结果比对，但服务端仍计算 SM3。口令未配置时旁路关闭。持有口令即可在任意已部署环境绕过签名完整性校验，因此口令必须仅存 Nacos/密钥系统；压测窗口外应清空配置。该旁路不替代 token 鉴权，也不免除业务幂等与余额规则。
```

- [ ] **Step 3: Commit**

```bash
git add docs/交行直充业务系统详细设计.md docs/交行直充业务系统高可用方案设计.md
git commit -m "$(cat <<'EOF'
docs: document V1 load-test sign bypass

EOF
)"
```

---

### Task 2: 配置属性与 `V1ProtocolSettings`

**Files:**
- Modify: `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineProperties.java`
- Modify: `cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/config/TradeEnginePropertiesTest.java`
- Modify: `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/protocol/V1ProtocolSettings.java`
- Modify: `cardshop-trade-engine-bootstrap/src/main/java/com/cardshop/trade/engine/bootstrap/config/V1CardApiConfiguration.java`

- [ ] **Step 1: 写失败的 properties 测试**

在 `TradeEnginePropertiesTest` 增加（沿用现有 bind helper）：

```java
@Test
void bindsV1LoadTestToken() {
    TradeEngineProperties properties = bind(
            "trade.engine.v1.load-test-token=load-secret");
    assertThat(properties.getV1().getLoadTestToken()).isEqualTo("load-secret");
}
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
mvn -pl cardshop-trade-engine-infrastructure -Dtest=TradeEnginePropertiesTest#bindsV1LoadTestToken test
```

Expected: 编译失败或断言失败（缺少 `loadTestToken`）。

- [ ] **Step 3: 实现配置字段**

在 `TradeEngineProperties.V1` 增加：

```java
/**
 * JMeter 压测签名结果旁路口令。空字符串表示旁路关闭。
 * 敏感配置，禁止写入日志。参见详细设计「3. API/V1 接入层」。
 */
private String loadTestToken = "";

public String getLoadTestToken() {
    return loadTestToken;
}

public void setLoadTestToken(String loadTestToken) {
    this.loadTestToken = loadTestToken == null ? "" : loadTestToken;
}
```

- [ ] **Step 4: 扩展 `V1ProtocolSettings`**

```java
/**
 * `/v1/cardapi` 协议预校验配置。
 *
 * <p>对应 {@code docs/交行直充业务系统详细设计.md} 的「3. API/V1 接入层」。
 *
 * @param requestSm4Enabled 是否启用请求 SM4 解密
 * @param loadTestToken 压测签名结果旁路口令；空白表示旁路关闭
 */
public record V1ProtocolSettings(boolean requestSm4Enabled, String loadTestToken) {
    public V1ProtocolSettings {
        loadTestToken = loadTestToken == null ? "" : loadTestToken;
    }

    /** 兼容旧单参构造，旁路默认关闭。 */
    public V1ProtocolSettings(boolean requestSm4Enabled) {
        this(requestSm4Enabled, "");
    }
}
```

- [ ] **Step 5: 装配**

`V1CardApiConfiguration.v1ProtocolSettings`：

```java
return new V1ProtocolSettings(
        properties.getV1().isRequestSm4Enabled(),
        properties.getV1().getLoadTestToken());
```

- [ ] **Step 6: 跑通相关测试**

Run:

```bash
mvn -pl cardshop-trade-engine-infrastructure,cardshop-trade-engine-api,cardshop-trade-engine-bootstrap -am \
  -Dtest=TradeEnginePropertiesTest,V1ProtocolPrecheckServiceTest,V1CardApiConfigurationTest,V1CreateRechargeOrderUseCaseServiceTest,V1QueryAndInvalidRechargeOrderUseCaseServiceTest \
  test
```

Expected: PASS（旁路行为尚未实现，兼容构造仍可用）。

- [ ] **Step 7: Commit**

```bash
git add cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineProperties.java \
  cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/config/TradeEnginePropertiesTest.java \
  cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/protocol/V1ProtocolSettings.java \
  cardshop-trade-engine-bootstrap/src/main/java/com/cardshop/trade/engine/bootstrap/config/V1CardApiConfiguration.java
git commit -m "$(cat <<'EOF'
feat(v1): add load-test-token protocol setting

EOF
)"
```

---

### Task 3: 预检旁路（TDD 核心）

**Files:**
- Modify: `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/protocol/V1ProtocolPrecheckService.java`
- Modify: `cardshop-trade-engine-api/src/test/java/com/cardshop/trade/engine/api/v1/protocol/V1ProtocolPrecheckServiceTest.java`

- [ ] **Step 1: 写失败测试**

在 `V1ProtocolPrecheckServiceTest` 增加：

```java
@Test
void bypassesCreateSignatureMismatchWhenLoadTestHeaderMatchesConfiguredToken() {
    V1ProtocolPrecheckService bypassService = new V1ProtocolPrecheckService(
            new V1RequestBodyCodec(),
            new V1TimestampValidator(
                    new V1TimestampSettings(Duration.ofSeconds(300)),
                    Clock.fixed(Instant.parse("2026-07-05T04:15:58Z"), ZoneOffset.UTC)
            ),
            new V1Sm3SignatureVerifier(),
            new V1ProtocolSettings(false, "load-secret")
    );
    String rawBody = """
            {
              "product_id": "73301280",
              "out_order_sn": "OUT-1",
              "account": "18186222336",
              "timestamp": "1783224958",
              "nonce_str": "nonce-32",
              "notify_url": "http://notify",
              "num": 1,
              "order_price": 5200,
              "product_type": 1,
              "sign": "bad-sign"
            }
            """;

    V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> result = bypassService.precheckCreate(
            rawBody,
            new V1MerchantSecurityProfile("app-gray", "A36", "AES-KEY"),
            "load-secret"
    );

    assertThat(result.valid()).isTrue();
    assertThat(result.payload()).isNotNull();
}

@Test
void doesNotBypassCreateSignatureWhenLoadTestTokenBlank() {
    V1ProtocolPrecheckService bypassService = new V1ProtocolPrecheckService(
            new V1RequestBodyCodec(),
            new V1TimestampValidator(
                    new V1TimestampSettings(Duration.ofSeconds(300)),
                    Clock.fixed(Instant.parse("2026-07-05T04:15:58Z"), ZoneOffset.UTC)
            ),
            new V1Sm3SignatureVerifier(),
            new V1ProtocolSettings(false, "")
    );
    String rawBody = """
            {
              "product_id": "73301280",
              "out_order_sn": "OUT-1",
              "account": "18186222336",
              "timestamp": "1783224958",
              "nonce_str": "nonce-32",
              "notify_url": "http://notify",
              "num": 1,
              "order_price": 5200,
              "product_type": 1,
              "sign": "bad-sign"
            }
            """;

    V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> result = bypassService.precheckCreate(
            rawBody,
            new V1MerchantSecurityProfile("app-gray", "A36", "AES-KEY"),
            "load-secret"
    );

    assertThat(result.valid()).isFalse();
    assertThat(result.reason()).isEqualTo("SIGN_FAILED");
}

@Test
void doesNotBypassCreateSignatureWhenHeaderMismatches() {
    V1ProtocolPrecheckService bypassService = new V1ProtocolPrecheckService(
            new V1RequestBodyCodec(),
            new V1TimestampValidator(
                    new V1TimestampSettings(Duration.ofSeconds(300)),
                    Clock.fixed(Instant.parse("2026-07-05T04:15:58Z"), ZoneOffset.UTC)
            ),
            new V1Sm3SignatureVerifier(),
            new V1ProtocolSettings(false, "load-secret")
    );
    String rawBody = """
            {
              "product_id": "73301280",
              "out_order_sn": "OUT-1",
              "account": "18186222336",
              "timestamp": "1783224958",
              "nonce_str": "nonce-32",
              "notify_url": "http://notify",
              "num": 1,
              "order_price": 5200,
              "product_type": 1,
              "sign": "bad-sign"
            }
            """;

    V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> result = bypassService.precheckCreate(
            rawBody,
            new V1MerchantSecurityProfile("app-gray", "A36", "AES-KEY"),
            "wrong"
    );

    assertThat(result.valid()).isFalse();
    assertThat(result.reason()).isEqualTo("SIGN_FAILED");
}

@Test
void stillComputesSm3WhenBypassingSignatureMismatch() {
    CountingVerifier verifier = new CountingVerifier();
    V1ProtocolPrecheckService bypassService = new V1ProtocolPrecheckService(
            new V1RequestBodyCodec(),
            new V1TimestampValidator(
                    new V1TimestampSettings(Duration.ofSeconds(300)),
                    Clock.fixed(Instant.parse("2026-07-05T04:15:58Z"), ZoneOffset.UTC)
            ),
            verifier,
            new V1ProtocolSettings(false, "load-secret")
    );
    String rawBody = """
            {
              "product_id": "73301280",
              "out_order_sn": "OUT-1",
              "account": "18186222336",
              "timestamp": "1783224958",
              "nonce_str": "nonce-32",
              "notify_url": "http://notify",
              "num": 1,
              "order_price": 5200,
              "product_type": 1,
              "sign": "bad-sign"
            }
            """;

    assertThat(bypassService.precheckCreate(
            rawBody,
            new V1MerchantSecurityProfile("app-gray", "A36", "AES-KEY"),
            "load-secret").valid()).isTrue();
    assertThat(verifier.createCalls).isEqualTo(1);
}

private static final class CountingVerifier extends V1Sm3SignatureVerifier {
    private int createCalls;

    @Override
    public V1SignatureVerifyResult verifyCreate(V1RechargeCreateOrderRequest request, String aesKey) {
        createCalls++;
        return super.verifyCreate(request, aesKey);
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
mvn -pl cardshop-trade-engine-api -Dtest=V1ProtocolPrecheckServiceTest test
```

Expected: 编译失败（尚无三参 `precheckCreate`）或新用例 FAIL。

- [ ] **Step 3: 实现旁路逻辑**

改造 `V1ProtocolPrecheckService`：

1. 增加常量 `LOG_EVENT_SIGN_BYPASS = "v1-protocol-precheck.sign-bypass"`。
2. `precheckCreate` 增加三参重载；两参方法委托 `precheckCreate(rawBody, merchant, null)`。
3. create 路径走可感知 header 的内部逻辑；query/invalid 仍走原无旁路 `precheck`。
4. 签名失败时：

```java
if (!signatureResult.valid()) {
    if (allowCreateSignBypass(loadTestHeader)) {
        LogDecision decision = logStormGuard.shouldLog(LOG_EVENT_SIGN_BYPASS);
        if (decision.shouldLog()) {
            log.warn("V1压测签名结果旁路生效 appId={}, orgCode={}, occurrence={}, sampled={}",
                    merchant.appId(),
                    merchant.orgCode(),
                    decision.occurrence(),
                    decision.sampledOccurrence());
        }
        return V1ProtocolPrecheckResult.ok(payload);
    }
    return V1ProtocolPrecheckResult.invalid(V1ProtocolFailureReason.SIGN_FAILED);
}
```

5. 匹配方法：

```java
private boolean allowCreateSignBypass(String loadTestHeader) {
    String configured = settings.loadTestToken();
    if (configured == null || configured.isBlank()) {
        return false;
    }
    if (loadTestHeader == null || loadTestHeader.isBlank()) {
        return false;
    }
    byte[] expected = configured.getBytes(StandardCharsets.UTF_8);
    byte[] actual = loadTestHeader.getBytes(StandardCharsets.UTF_8);
    return MessageDigest.isEqual(expected, actual);
}
```

`V1Sm3SignatureVerifier` 不感知 Header。注释链接详细设计「3. API/V1 接入层」与 `docs/superpowers/specs/2026-07-23-v1-load-test-sign-bypass-design.md`。

- [ ] **Step 4: 运行测试确认通过**

Run:

```bash
mvn -pl cardshop-trade-engine-api -Dtest=V1ProtocolPrecheckServiceTest test
```

Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/protocol/V1ProtocolPrecheckService.java \
  cardshop-trade-engine-api/src/test/java/com/cardshop/trade/engine/api/v1/protocol/V1ProtocolPrecheckServiceTest.java
git commit -m "$(cat <<'EOF'
feat(v1): bypass create sign compare on load-test header

EOF
)"
```

---

### Task 4: 贯通 Controller → Command → UseCase

**Files:**
- Modify: `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/V1MerchantCommand.java`
- Modify: `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/controller/V1RechargeOrderController.java`
- Modify: `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/usecase/V1CreateRechargeOrderUseCaseService.java`

- [ ] **Step 1: 扩展命令对象**

```java
/**
 * @param appId 从 token 或认证上下文解析出的商户 appId
 * @param orgCode 从 token 或认证上下文解析出的商户编码，不信任请求体自带值
 * @param rawBody 原始请求体，用于协议预校验、解密和验签
 * @param loadTestHeader 请求头 {@code X-Load-Test} 原值；空白表示未启用压测旁路
 */
public record V1MerchantCommand(String appId, String orgCode, String rawBody, String loadTestHeader) {
    public V1MerchantCommand(String appId, String orgCode, String rawBody) {
        this(appId, orgCode, rawBody, "");
    }

    public V1MerchantCommand {
        loadTestHeader = loadTestHeader == null ? "" : loadTestHeader;
    }
}
```

- [ ] **Step 2: Controller 读取 Header**

常量：`private static final String LOAD_TEST_HEADER = "X-Load-Test";`

两处构造 command 改为：

```java
new V1MerchantCommand(
        principal.appId(),
        principal.orgCode(),
        rawBody,
        request.getHeaders().getFirst(LOAD_TEST_HEADER))
```

- [ ] **Step 3: UseCase 传 Header**

`prepare` 中：

```java
V1ProtocolPrecheckResult<V1RechargeCreateOrderRequest> precheck = precheckService.precheckCreate(
        command.rawBody(),
        new V1MerchantSecurityProfile(merchant.appId(), merchant.orgCode(), merchant.aesKey()),
        command.loadTestHeader());
```

查单/作废仍用两参 precheck，不旁路。

- [ ] **Step 4: 编译与相关测试**

Run:

```bash
mvn -pl cardshop-trade-engine-api,cardshop-trade-engine-application,cardshop-trade-engine-bootstrap -am \
  -Dtest=V1ProtocolPrecheckServiceTest,V1CreateRechargeOrderUseCaseServiceTest,V1QueryAndInvalidRechargeOrderUseCaseServiceTest,V1ControllerBlockingBoundaryTest,V1CardApiConfigurationTest \
  test
```

Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/V1MerchantCommand.java \
  cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/controller/V1RechargeOrderController.java \
  cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/usecase/V1CreateRechargeOrderUseCaseService.java
git commit -m "$(cat <<'EOF'
feat(v1): plumb X-Load-Test header into create precheck

EOF
)"
```

---

### Task 5: 最终回归

- [ ] **Step 1: 跑相关模块测试**

```bash
mvn -pl cardshop-trade-engine-api,cardshop-trade-engine-application,cardshop-trade-engine-infrastructure,cardshop-trade-engine-bootstrap -am \
  -Dtest=V1ProtocolPrecheckServiceTest,TradeEnginePropertiesTest,V1CreateRechargeOrderUseCaseServiceTest,V1QueryAndInvalidRechargeOrderUseCaseServiceTest,V1ControllerBlockingBoundaryTest,V1CardApiConfigurationTest \
  test
```

Expected: PASS。

- [ ] **Step 2: 确认 working tree clean（或提交残留改动）**

---

## JMeter 交付摘要（实现完成后可复制给测试）

1. Nacos：`trade.engine.v1.load-test-token: '<口令>'`（或 env `TRADE_ENGINE_V1_LOAD_TEST_TOKEN`）
2. `POST http://apitest.kalaiwang.com/v1/cardapi/auth/getToken`
3. `POST http://apitest.kalaiwang.com/v1/cardapi/order/createRechargeOrder`  
   Header：`Authorization: <token>`、`X-Load-Test: <口令>`  
   Body：V1.1 字段；`sign` 可为任意非空占位；每笔独立 `out_order_sn`/`nonce_str`/`timestamp`；压测 `notify_url` 建议 `""`
4. 压测结束后清空 `load-test-token`

---

## Spec coverage checklist

| Spec 要求 | Task |
| --- | --- |
| 仍执行 SM3 | Task 3 CountingVerifier |
| 口令匹配时忽略比对 | Task 3 |
| 口令空则永不旁路 | Task 3 |
| Header 不匹配失败 | Task 3 |
| 仅 create | Task 3/4 |
| 配置项 | Task 2 |
| 设计文档 | Task 1 |
| rate-limited warn | Task 3 |
| 不算签接口 | 全计划未引入 |
| 可选 metrics | YAGNI 跳过 |

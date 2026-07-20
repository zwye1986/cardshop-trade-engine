# Supplier Mock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `cardshop-trade-engine-simulator` 中实现 `--mode=supplier-mock`，提供可配置终态延迟的供应商下单/查单 HTTP mock。

**Architecture:** 入口先识别 mode；`supplier-mock` 走独立 `SupplierMockOptions`（不要求商户密钥）。JDK `HttpServer` 同时挂通用路径与 MINISO 兼容路径；内存 `SupplierMockOrderStore` 在下单时抽样 `readyAt` 与终态，查单按时间返回 PROCESSING 或终态。

**Tech Stack:** JDK 21 `HttpServer`、Jackson、JUnit 5、AssertJ；无 Spring。

**Spec:** `docs/superpowers/specs/2026-07-20-supplier-mock-design.md`

---

## File map

| File | Responsibility |
| --- | --- |
| `.../simulator/a39/A39SimulatorMode.java` | 增加 `SUPPLIER_MOCK` 与解析 |
| `.../simulator/supplier/SupplierMockOptions.java` | CLI/env 解析与校验 |
| `.../simulator/supplier/SupplierMockOrderStore.java` | 内存订单状态机 |
| `.../simulator/supplier/SupplierMockServer.java` | HTTP 路由与 JSON 响应 |
| `.../simulator/a39/A39BusinessSimulatorApplication.java` | mode 分流启动 mock |
| `.../simulator/a39/A39SimulatorOptions.java` | usage 文案补充 supplier-mock |
| `README.md` | 启动示例与 Nacos 指向说明 |
| 对应 `src/test/java/.../supplier/*Test.java` | 单测 |

---

### Task 1: Mode enum + Options

**Files:**
- Modify: `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/A39SimulatorMode.java`
- Create: `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/supplier/SupplierMockOptions.java`
- Create: `cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/supplier/SupplierMockOptionsTest.java`

- [ ] **Step 1: 扩展 mode 解析**

在 `A39SimulatorMode` 增加 `SUPPLIER_MOCK`，`parse` 接受 `supplier-mock` / `supplier_mock`；错误信息改为 `mode must be scenario, load, or supplier-mock`。

- [ ] **Step 2: 写 Options 失败用例（先写测）**

```java
@Test
void rejectsWhenMinGreaterThanMax() {
    assertThatThrownBy(() -> SupplierMockOptions.parse(new String[]{
            "--final-result-delay-ms-min", "9000",
            "--final-result-delay-ms-max", "1000"
    }, Map.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("final-result-delay");
}

@Test
void rejectsInvalidSuccessRate() {
    assertThatThrownBy(() -> SupplierMockOptions.parse(new String[]{
            "--success-rate", "1.5"
    }, Map.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("success-rate");
}

@Test
void parsesDefaultsAndOverrides() {
    SupplierMockOptions options = SupplierMockOptions.parse(new String[]{
            "--bind-host", "127.0.0.1",
            "--port", "18083",
            "--final-result-delay-ms-min", "100",
            "--final-result-delay-ms-max", "200",
            "--success-rate", "0.5"
    }, Map.of());
    assertThat(options.bindHost()).isEqualTo("127.0.0.1");
    assertThat(options.port()).isEqualTo(18083);
    assertThat(options.finalResultDelayMsMin()).isEqualTo(100);
    assertThat(options.finalResultDelayMsMax()).isEqualTo(200);
    assertThat(options.successRate()).isEqualTo(0.5);
}
```

- [ ] **Step 3: 实现 `SupplierMockOptions`**

```java
public record SupplierMockOptions(
        String bindHost,
        int port,
        long finalResultDelayMsMin,
        long finalResultDelayMsMax,
        double successRate) {

    public static final String DEFAULT_BIND_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 18082;
    public static final long DEFAULT_DELAY_MIN_MS = 1200L;
    public static final long DEFAULT_DELAY_MAX_MS = 9800L;
    public static final double DEFAULT_SUCCESS_RATE = 1.0;

    public static SupplierMockOptions parse(String[] args, Map<String, String> env) {
        // 解析 --bind-host/--port/--final-result-delay-ms-min/max/--success-rate
        // env: SUPPLIER_MOCK_BIND_HOST, SUPPLIER_MOCK_PORT,
        //      SUPPLIER_MOCK_FINAL_RESULT_DELAY_MS_MIN/MAX, SUPPLIER_MOCK_SUCCESS_RATE
        // 命令行优先；校验 min<=max、0<=rate<=1、port>0
    }

    public String sanitizedSummary() { /* 无密钥，打印全部配置 */ }

    public static String usage() { /* 仅 supplier-mock 相关帮助 */ }
}
```

- [ ] **Step 4: 跑测**

Run: `mvn -pl cardshop-trade-engine-simulator -Dtest=SupplierMockOptionsTest,A39SimulatorMode* test`

若现有 mode 单测断言错误文案，同步更新。

- [ ] **Step 5: Commit**（仅当用户要求提交时执行；否则跳过）

---

### Task 2: Order store 状态机

**Files:**
- Create: `.../supplier/SupplierMockOrderStore.java`
- Create: `.../supplier/SupplierMockOrderStoreTest.java`

- [ ] **Step 1: 写失败测（核心行为）**

覆盖：
1. `registerDispatch` 首次返回 PROCESSING，并写入 readyAt/终态
2. `query` 在 readyAt 前 PROCESSING，之后返回预定终态
3. `successRate=0` / `1` 边界（注入 `DoubleSupplier` / `LongSupplier` 可测随机）
4. 同一 orderId 重复 dispatch 不重置 readyAt/终态
5. 未知订单 query → FAILED + `E_MOCK_NOT_FOUND`

建议签名：

```java
public final class SupplierMockOrderStore {
    public record MockResponse(String status, String code, String message, String supplierOrderId) {}

    public SupplierMockOrderStore(
            long delayMsMin,
            long delayMsMax,
            double successRate,
            Clock clock,
            LongUnaryOperator delaySampler,   // 输入 maxInclusiveDelta，返回 [0, delta]
            DoubleSupplier successSampler);   // 返回 [0,1)

    public MockResponse registerDispatch(String orderKey);
    public MockResponse query(String orderKey);
}
```

订单 key：调用方传入已解析的 `order_id` 或 fallback `supplier_request_id`。

- [ ] **Step 2: 实现 store 使测试通过**

- [ ] **Step 3: 跑测**

Run: `mvn -pl cardshop-trade-engine-simulator -Dtest=SupplierMockOrderStoreTest test`

---

### Task 3: HTTP server

**Files:**
- Create: `.../supplier/SupplierMockServer.java`
- Create: `.../supplier/SupplierMockServerTest.java`

- [ ] **Step 1: 写 HTTP 联调测**

用 `HttpClient` 打本机随机端口：
- `POST /supplier/dispatch` 与 `POST /tripartite/coupon/batchGrandCoupon` 均返回 PROCESSING
- 把 store 的 clock/`readyAt` 推到终态后，`/supplier/query` 与 `/tripartite/coupon/queryGrand` 返回 SUCCESS
- body 解析 `order_id`；缺失时用 `supplier_request_id`
- 未知路径 404

响应 JSON 字段：`status`/`code`/`message`/`supplier_order_id`。

- [ ] **Step 2: 实现 `SupplierMockServer`**

```java
public final class SupplierMockServer implements AutoCloseable {
    public static SupplierMockServer start(SupplierMockOptions options, SupplierMockOrderStore store) throws IOException;
    public URI baseUri();
    @Override public void close(); // server.stop(0)
}
```

四条 path 注册到同一 handler 分发。线程池用 `Executors.newCachedThreadPool()`（或固定小池），`server.setExecutor(...)`。

- [ ] **Step 3: 跑测**

Run: `mvn -pl cardshop-trade-engine-simulator -Dtest=SupplierMockServerTest test`

---

### Task 4: 入口分流 + README

**Files:**
- Modify: `.../a39/A39BusinessSimulatorApplication.java`
- Modify: `.../a39/A39SimulatorOptions.java`（usage 增加 supplier-mock 一行）
- Modify: `.../a39/A39BusinessSimulatorApplicationTest.java`（若有）
- Create/Modify: 入口测覆盖 `mode=supplier-mock` 不要求密钥即可启动（可测到 server 起来并 GET/POST 后 close）
- Modify: `README.md` simulator 章节

- [ ] **Step 1: 入口先解析 mode**

```java
static int run(String[] args) throws Exception {
    if (hasHelp(args)) { ... print combined or mode-specific usage ... }
    A39SimulatorMode mode = resolveMode(args, System.getenv());
    if (mode == A39SimulatorMode.SUPPLIER_MOCK) {
        return runSupplierMock(SupplierMockOptions.parse(args, System.getenv()), System.out::println);
    }
    return run(A39SimulatorOptions.parse(args), System.out::println);
}
```

`runSupplierMock`：构造 store（真实 Random）、start server、打印 `sanitizedSummary` 与 listening URL，然后 `Thread.currentThread().join()` 或 `CountDownLatch` 阻塞；shutdown hook close server。

- [ ] **Step 2: README 补充**

说明：
- `--mode=supplier-mock` 启动命令
- 与 A39 同机双进程
- Nacos 示例指向 `127.0.0.1:18082/tripartite/coupon/*` 或 `/supplier/*`
- 仅限 dev/test

- [ ] **Step 3: 全模块测试**

Run: `mvn -pl cardshop-trade-engine-simulator test`

- [ ] **Step 4: Commit**（仅当用户要求时）

---

## Spec coverage checklist

| Spec 项 | Task |
| --- | --- |
| mode=supplier-mock 常驻 | Task 4 |
| 无商户密钥 | Task 1/4 |
| 双路径 dispatch/query | Task 3 |
| 延迟区间 + success-rate | Task 1/2 |
| 幂等不重置 | Task 2 |
| 未知订单失败 | Task 2 |
| README | Task 4 |
| 不作废/不持久化 | 刻意不做 |

---

## Self-review notes

- 无 TBD；类型名在 Task 间一致（`MockResponse` / `SupplierMockOrderStore`）。
- Commit 步骤默认跳过，遵守仓库「用户未要求不提交」规则；实现完成后由用户决定是否提交。

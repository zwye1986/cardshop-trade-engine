# A39 Business Simulator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Java simulator that behaves like an A39 merchant: token, signed create order, conditional order query, and callback receive/verify.

**Architecture:** Add a dedicated Maven module that avoids Spring Boot runtime dependencies. Keep protocol code in small units: options parsing, SM3 signing, HTTP client, scenario runner, and callback server.

**Tech Stack:** Java 21, Maven, JDK `HttpClient`, JDK `HttpServer`, Jackson, BouncyCastle SM3, JUnit 5, AssertJ.

---

## File Structure

- Create `cardshop-trade-engine-simulator/pom.xml`: module dependencies and test setup.
- Keep root `pom.xml` free of the simulator module so the main application package does not build or ship the simulator.
- Create `V1Sm3Signer`: canonical source, SM3 digest, sign, verify.
- Create `A39SimulatorOptions`: parse command-line options and environment fallbacks.
- Create `V1CardApiClient`: send token/create/query HTTP JSON requests.
- Create `A39BusinessScenarioRunner`: orchestrate token, create, conditional query loop.
- Create `A39CallbackServer`: receive callback JSON, verify sign, return legacy response JSON.
- Create `A39BusinessSimulatorApplication`: main entry point.
- Create `scripts/package-a39-simulator.sh`: standalone simulator zip packaging.
- Create `deploy/bin/run-a39-simulator.sh`: packaged simulator runner.
- Create focused tests under `cardshop-trade-engine-simulator/src/test/java`.

### Task 1: Module and Signing Core

**Files:**
- Modify: `pom.xml`
- Create: `cardshop-trade-engine-simulator/pom.xml`
- Create: `cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/V1Sm3SignerTest.java`
- Create: `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/V1Sm3Signer.java`

- [ ] **Step 1: Write the failing signer tests**

Tests assert the known SM3 vector, create-order source string, and query-order source string.

- [ ] **Step 2: Run signer tests red**

Run: `mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=V1Sm3SignerTest`

Expected: compilation failure because `V1Sm3Signer` does not exist.

- [ ] **Step 3: Implement `V1Sm3Signer`**

Use a `TreeMap`, skip `sign`, include `aesKey`, preserve empty callback values when the caller asks for callback verification, and calculate SM3 hex with BouncyCastle.

- [ ] **Step 4: Run signer tests green**

Run: `mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=V1Sm3SignerTest`

Expected: tests pass.

### Task 2: Request Building and Query Decision

**Files:**
- Create: `A39BusinessScenarioRunnerTest.java`
- Create: `A39SimulatorOptions.java`
- Create: `V1CardApiClient.java`
- Create: `A39BusinessScenarioRunner.java`

- [ ] **Step 1: Write failing scenario tests**

Tests assert `base-url` is treated as a request prefix even with path segments, HTTP requests log sanitized curl plus raw responses, `product_id` defaults to `1011` but can be overridden by parameter, create payload includes `order_price/product_type` but excludes them from sign, query runs for create codes `200/510` only, and status-change logs are emitted only when `order_status` changes.

- [ ] **Step 2: Run scenario tests red**

Run: `mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=A39BusinessScenarioRunnerTest`

Expected: compilation failure because scenario classes do not exist.

- [ ] **Step 3: Implement minimal options, client DTOs, and runner**

Use constructor injection so tests can fake the client without network calls.

- [ ] **Step 4: Run scenario tests green**

Run: `mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=A39BusinessScenarioRunnerTest`

Expected: tests pass.

### Task 3: Callback Server and CLI

**Files:**
- Create: `A39CallbackServerTest.java`
- Create: `A39CallbackServer.java`
- Create: `A39BusinessSimulatorApplication.java`

- [ ] **Step 1: Write failing callback tests**

Tests start the callback server on an ephemeral port, POST a signed callback body, and assert success JSON; then POST a tampered body and assert retry JSON.

- [ ] **Step 2: Run callback tests red**

Run: `mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=A39CallbackServerTest`

Expected: compilation failure because callback server does not exist.

- [ ] **Step 3: Implement callback server and CLI entry point**

Use JDK `HttpServer`; CLI reads `--key value` options plus `A39_*` environment fallbacks and prints sanitized progress.

- [ ] **Step 4: Run callback tests green**

Run: `mvn -f cardshop-trade-engine-simulator/pom.xml test -Dtest=A39CallbackServerTest`

Expected: tests pass.

### Task 4: Documentation and Verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document simulator usage**

Add a short section with Maven run command and environment variable names. Use placeholders for `app_secret` and `aesKey`.

- [ ] **Step 2: Run targeted module tests**

Run: `mvn -f cardshop-trade-engine-simulator/pom.xml test`

Expected: simulator module tests pass.

- [ ] **Step 3: Run compile impact check**

Run: `mvn -f cardshop-trade-engine-simulator/pom.xml test`

Expected: upstream modules plus simulator tests pass.

## Self Review

The plan covers token, sign, create, query, callback, CLI configuration, documentation, and tests from the approved design. No placeholders or unresolved file names remain.

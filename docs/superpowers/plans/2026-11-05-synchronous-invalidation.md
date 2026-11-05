# 交行作废接口同步化实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. 默认在持有改动的当前会话内执行；不因本计划自动创建线程或委派实现。

**Goal:** 将V1作废从“入队即返回200/2”改为“请求内推进原事件，完成返回200，未决返回202”，保持不重复作废、不误退款与可恢复性。

**Architecture:** 沿用唯一`INVALID_ORDER`、现有`processEvent`和持久尝试。扩展作废端口提供主库受理/结果读取，应用服务复用Worker；HTTP响应以独立截止时间观察已提交事实，响应取消不截断已开始的作废。保留2104完成通知与资金恢复。

**Tech Stack:** Java 21、Maven 3.9+、Spring Boot 3.3/WebFlux/Reactor、MyBatis、MySQL、Redis、JUnit 5、AssertJ、JDK本机HTTP替身；不升级依赖。

---

## 1. 计划状态、输入与执行边界

- 日期：2026-11-05，Asia/Shanghai。**Task 0—7本地实现已落地，Task 8的三项P0故障补证已通过；Task 9未执行。**P0证据及替身边界见《集成测试.md》同日SINV-P0记录，不代表容量、现场配置或银行验收通过。
- 方案来源：[原thread](https://ampcode.com/threads/T-01a1a5d3-2584-7531-95d5-e489a0afcb9d)。实现依据：[详细设计9.3](../../交行直充业务系统详细设计.md#93-作废设计)、[高可用设计19.1](../../交行直充业务系统高可用方案设计.md#191-作废请求内执行与异常恢复2026-11-05本地实现未部署)。不依赖被忽略的`.amp/in/`方案才能执行本计划。
- **2026-11-05用户已批准推荐口径并授权本地实施，暂不部署。**采用“券终态＋合法资金释放许可/无需释放裁决持久提交即200”，余额回补/Delta刷账不阻断；两份主设计已同步。
- 2104作废通知按既有已确认合同保留，不另行停发；如用户要求取消，先单独修订协议和设计。原方案把这一点列为待定，但仓库文末2104用户确认已明确三类通知均生成，执行时以该现有合同为准。
- 5秒/202为我方制定的目标合同，不代表交行已完成适配。供应商3秒为现有代码默认，现场Nacos、调用方与代理超时仍需核验。
- 本地实施已获授权；不包含commit/push、Nacos发布、共享数据库写入、部署、重置人工任务或真实供应商/银行请求。共享环境须另行授权。
- 工作区开始时已有migration、两份主设计、部署/集成文档等未提交改动；只增量合并。不要stash/reset覆盖，也不将其归入同步改造。已经删除的旧测试前端不在范围。

## 2. 文件责任与改动清单

以下路径均相对仓库根目录。生产Java包基准为`com/cardshop/trade/engine`。

| 路径 | 操作/责任 |
| --- | --- |
| `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/RechargeOrderInvalidationService.java` | 新增；受理、原事件即时推进及业务结果协调。不复制Gateway或资金状态机，不依赖Reactor/Spring。 |
| `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/RechargeOrderInvalidationState.java` | 新增；主库观察/受理结果，四类状态及安全原因。 |
| `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/InvalidRechargeOrderOutboxPort.java` | 扩展为`prepareInvalidOrder`/`findInvalidOrder`合同；替换仅入队的公开调用，正常装配不使用noop。 |
| `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/InvalidRechargeOrderUseCase.java` | 增加作废专用截止/受理上下文参数；保持创建/查单公共命令不变。 |
| `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/InvalidRechargeOrderContext.java` | 新增；单调截止时间、请求局部受理竞争及最后可安全响应事实。不是持久业务状态，不携带密钥/券码。 |
| `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/v1/V1CardApiResultStatus.java` | 新增`INVALIDATION_PENDING("202", "作废结果待确认，请查询订单状态")`，不改其他码值。 |
| `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeDispatchOutboxAdapter.java` | 扩展作废受理短事务与观察实现，复用唯一事件，不改变DISPATCH/QUERY的写入合同。 |
| `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/mapper/RechargeDispatchOutboxMapper.java` | 新增不按due筛选的受商户订单约束读取，显式字段、参数绑定、归属哨兵；保留已有claim语义。 |
| `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeFulfillmentOrderAdapter.java` | 核对并复用现有资金裁决读取规则；仅为一致事实读取确有必要时调整，不移动终态/资金写入所有权。 |
| `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisMinisoWriteOffProtection.java`、`MinisoWriteOffOrderLocks.java` | 复用统一锁序；仅为受理期不修改原任务的证据检查提供必要入口，不改变核销授权优先级。 |
| `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/usecase/V1InvalidRechargeOrderUseCaseService.java` | 保留协议检查，调用编排、映射200/202/503；删除200/2受理及只看券状态的成功快捷路径。 |
| `cardshop-trade-engine-api/src/main/java/com/cardshop/trade/engine/api/v1/controller/V1RechargeOrderController.java` | 作废专用完整body起点、非取消共享执行、独立计时与观察；创建/查单保持原实现。 |
| `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineProperties.java` | 在V1新增`invalidationWaitTimeoutMs=5000`及清晰单位/来源注释。 |
| `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/config/TradeEngineInfrastructureConfiguration.java`、`TradeEngineMybatisPersistenceAutoConfiguration.java` | 装配应用编排及真实受理依赖；核验晚装配顺序，不产生双Bean。 |
| `cardshop-trade-engine-bootstrap/src/main/java/com/cardshop/trade/engine/bootstrap/config/V1CardApiConfiguration.java` | 更新作废用例真实/不可用装配，缺Worker或保护端口不可用，不用noop掩盖。 |
| `cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/a39/V1CardApiClient.java`、`HttpV1CardApiClient.java` | 新增invalid HTTP调用，复用现有HTTPS、SM4及请求超时；无自动作废重试。 |
| 同目录`A39SimulatorMode.java`、`A39SimulatorOptions.java`、`A39BusinessSimulatorApplication.java` | 新增显式`invalidate`模式，只操作指定已存在订单；不在scenario/load默认下单后自动作废。 |
| 同目录`A39InvalidationScenarioRunner.java` | 新增；签名作废、处理202、有限查原单，只按券状态＋首次时间终结，和原下单runner职责分开。 |
| `deploy/nacos/cardshop-trade-engine-dev.example.yaml`、`deploy/nacos/cardshop-trade-engine-prod.preprod.example.yaml` | 只更新仓库样例的新等待配置，不发布环境。 |
| 两份主设计、`docs/交行直充接口技术文档-V1.2-商户级签名密钥修订稿.docx`、`docs/交行2104回调接入指引.md`、`docs/模拟器使用手册.md`、`docs/部署手册.md` | 同步接口合同、调用方分支、时间预算、通知幂等和切换/回退步骤；不改V1.1历史协议。 |
| `scripts/v1-invalidation-example.py`、`scripts/tests/test_v1_invalidation_example.py` | 新增离线合成200/202/503示例与消费判定，不加载凭据、不发网络。 |
| `docs/集成测试.md` | 实际dev/test联调时追加本次执行记录，不提前写“通过”。 |

Worker及MINISO客户端/适配器是复用边界，原则上不改。实施过程中若发现必须改变安全状态机或锁协议，先更新两份主设计并确认差异，不能借同步化扩大重构范围。

## 3. 执行顺序与检查点

| 阶段 | 任务 | 退出条件 |
| --- | --- | --- |
| 合同冻结 | 0 | 资金口径明确，两个主设计无冲突，用户批准开始实现。 |
| 持久事实与编排 | 1—3 | 无HTTP依赖下证明唯一事件、归属门禁、统一完成谓词、指定事件即时执行。 |
| HTTP与装配 | 4—5 | 五秒独立计时、取消不截断已开始执行、晚装配与缺依赖规则通过。 |
| 调用方和协议 | 6—7 | 模拟器/离线示例正确区分200/202，协议不再宣称异步200/2。 |
| 本地证明 | 8 | 全量及真实本机HTTP/隔离数据库故障验证通过；记录尚未验证的环境边界。 |
| 授权联调/切换 | 9 | 具体dev/test验收证据与银行消费规则具备；发布另行授权。 |

依赖为0→1→2→3→4→5→6/7→8→9；测试按每个任务先红后绿执行。共享环境阶段不与代码阶段混在一次“完成”声明中。

### Task 0：冻结资金成功口径及实施范围

**Files:** 两份主设计9.3/19.1、本计划第1节。

- [x] 2026-11-05用户确认推荐口径，已写入两份设计。
- [x] 保留既有2104完成通知合同，不要求额外确认。
- [x] 已保存工作区与设计基线；JDK21/Maven3.9.16定向133项通过。证据`.amp/in/artifacts/sinv-baseline-*`。
- [x] 已获本地实施授权；无共享环境/部署/真实调用授权。
- [x] 改实现前保留旧行为红灯：未落库仍200而非503，慢认证5500ms仍无响应；定向旧测试记录入队即200/2。证据`.amp/in/artifacts/sinv-red-baseline.log`。此为用例/Controller基线，尚非真实HTTP端到端证明。

### Task 1：定义可观察的作废受理与结果合同

**Files:** application的`RechargeOrderInvalidationState.java`、`InvalidRechargeOrderOutboxPort.java`。本任务只定义类型/端口；公共行为测试分别在Task 2真实事务、Task 3应用编排之前先写，不为无逻辑record单独添加测试。

- [x] 定义以下类型（不在应用层暴露数据库字段或Reactor）：

```java
public record RechargeOrderInvalidationState(
        RechargeOrderSnapshot order,
        String eventId,
        Status status,
        String reason) {
    public enum Status { COMPLETED, PENDING, NOT_ACCEPTED, MANUAL_REQUIRED }
}
```

- [x] 扩展原作废端口为详细设计9.3.2的`prepareInvalidOrder(command)`及`findInvalidOrder(orgCode,outOrderSn)`；受理返回主库判定，读取empty只表示无订单事实，存储失败抛异常。订单不存在/缺主库但有Redis受理事实由API按9.3.6区分，不能用SQL异常伪装505。
- [x] 更新所有API调用后移除原公开`enqueueInvalidOrder`及生产noop，原写入逻辑仅作适配器私有实现保留。稳定ID仍只由适配器生成；测试使用真实行为夹具而非noop假成功。
- [x] 写明status与safe reason合同：NOT_ACCEPTED必定未创建首次事件；PENDING关联原操作；MANUAL_REQUIRED不等于失败；COMPLETED使用统一资金口径。平台单号缺失时不得生成`invalid-null`。

**Check:** `mvn -pl cardshop-trade-engine-application -am test`，预期现有应用/领域测试通过。端口增加抽象方法后，其基础设施实现和API内旧测试桩在后续任务同步更新；Task 1—5为一个尚不可发布的改造批次，中途不提交不可构建的跨层接口状态。

### Task 2：实现受理短事务与一致主库观察

**Files:** 表中infrastructure持久化文件；扩展现有`MybatisRechargeOutboxAdapterTest.java`、`MybatisRechargeFulfillmentOrderAdapterTest.java`；真实SQL/事务行为放新增`cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/persistence/RechargeOrderInvalidationTransactionTest.java`，复用现有隔离数据库夹具。

- [x] 已有SINV-02/03/04/13真实Mapper回归：两个不同商户的相同外单号、缺扩展旧单、未到期FAILED、MANUAL及缺时间终态；另补同稳定ID异身份冲突红/绿回归。H2仅证明SQL与本机事务行为，MySQL验证见Task 8。
- [x] 在原适配器实现`prepareInvalidOrder`主数据源短事务：锁主单/同身份扩展→先原操作/完整终态→只对首次检查本地凭据与核销证据→唯一事件insert→读回稳定ID及身份。复用`MinisoWriteOffOrderLocks`与现有事务中证据检查，不调用会停止任务的`blockInvalidation`预检，不把HTTP/Redis放事务里。
- [x] 实现不受due/status筛选的同商户订单观察，显式选所需列并绑定参数；多次读使用独立REPEATABLE_READ只读快照，即使连接默认RC也不能拼接不同视图。不在100ms观察中取订单写锁。
- [x] 完成谓词同时校验12/CANCELLED、券状态/时间与合法资金裁决；复用现有资金裁决校验规则的源逻辑，不复制宽松版本。裁决不完整/互斥冲突、缺时间的历史CANCELLED不能200。
- [x] H2及真实MySQL并发验证同单仅一行原事件；受理/核销共用锁协议；真实故障触发器证明首次写入回滚无事件、无供应商调用，明确回滚返回业务500。只读观察不触发供应商或资金写入。
- [x] 扩展构造器的真实Mapper/事务依赖已同步Bean工厂及测试，派单/查单原适配器回归通过；未新增DDL或宽化旧单归属。

**Check:** `mvn -pl cardshop-trade-engine-infrastructure -am -Dtest=MybatisRechargeOutboxAdapterTest,MybatisRechargeFulfillmentOrderAdapterTest,RechargeOrderInvalidationTransactionTest -Dsurefire.failIfNoSpecifiedTests=false test`。H2不能替代MySQL锁/隔离验收；Task 8补真实隔离MySQL。

### Task 3：请求内即时推进原事件，不复制Worker

**Files:** `RechargeOrderInvalidationService.java`；新增`cardshop-trade-engine-application/src/test/java/com/cardshop/trade/engine/application/recharge/RechargeOrderInvalidationServiceTest.java`，扩展同目录既有`RechargeOutboxWorkerServiceTest.java`。

- [x] 公开编排/持久观察测试覆盖原完成时间、未决原事件、人工状态；保存旧200/2行为及新门禁/截止红灯，不把新类未编译当唯一失败证据。
- [x] SINV-01/05/11通过：关闭所有后台扫描首次完成、同事件CAS竞争及资金收尾闸门期间可信完成观察；真实HTTP覆盖见Task 8。
- [x] 实现公开编排：准备受理→提交后对返回eventId执行一次`processEvent(eventId, INVALID_ORDER, instanceWorkerId)`→观察主库。COMPLETED/NOT_ACCEPTED无需执行；MANUAL不触发执行；PENDING是否due/是否可抢占由原Worker决定，不在API层复制due算法。
- [x] 不依据`Outcome.FAILED/SKIPPED`直接构造503；执行异常与业务结果分离，读取已提交事实后按统一合同决定200/202。已开始受理后的未知异常保留202，不宣称明确未受理；P0的提交后执行拒绝注入及受理前真实调度拒绝见Task 8，未扩大为饱和压测结论。
- [x] 保持Worker状态机不变：`handleClaimed`的已授权资金优先恢复、调用前持久尝试、核销前后复核、终态裁决事务与资金/通知幂等。未提取透传执行wrapper或重构Worker。
- [x] 持久attempt及queryOnly现有回归通过，真实HTTP丢回包/终态回滚恢复未再次abandon；P0另补独立JVM SIGKILL后自然租约过期恢复，未将`queryGrand`原生0/1/2或FAILED文本升级成确定作废失败。

**Check:** `mvn -pl cardshop-trade-engine-application -am -Dtest=RechargeOrderInvalidationServiceTest,RechargeOutboxWorkerServiceTest -Dsurefire.failIfNoSpecifiedTests=false test`。

### Task 4：实现独立五秒响应、协议映射与取消边界

**Files:** `InvalidRechargeOrderContext.java`、`InvalidRechargeOrderUseCase.java`、`V1CardApiResultStatus.java`；API用例/Controller；扩展现有`V1QueryAndInvalidRechargeOrderUseCaseServiceTest.java`、`V1ControllerBlockingBoundaryTest.java`，新增同controller测试包`V1InvalidationDeadlineTest.java`。

- [x] 闸门及真实调度验证慢认证、受理中截止、提前完成观察和受理后取消；真实HTTP覆盖4秒/6秒提交，旧5500ms慢认证用例已保留红灯证据。没有仅用正常路径短sleep冒充截止验证。
- [x] P0通过JDK `LongSupplier`注入单调纳秒时间源，确定性验证截止前1ns/同刻/后1ns的首次受理与完成发布，并用闸门验证关闭与受理CAS竞争、已受理执行不被关闭截断。默认仍为`System::nanoTime`，无新框架/依赖；同刻remaining=0即超期。证明时间采样边界与状态竞争，不宣称物理网络或CAS具备纳秒级实时保证。
- [x] 将作废Controller改为body完整后创建上下文再认证，截止使用单调时间。保持认证、商户读取、验签所需阻塞与主库读在`boundedElastic`；不改变创建/查单方法的起点及共享命令。
- [x] 请求局部协调采用下列状态转换，状态与最后可信响应事实用一个原子快照发布，不使用多个独立布尔值：

```text
PREPARING --已验签/归属且允许开始受理--> ADMITTING
PREPARING --到期/断开--> CLOSED（之后禁止首次受理）
ADMITTING --原操作/提交成功--> OBSERVING
ADMITTING --确定回滚/拒绝--> CLOSED（返回相应拒绝，不执行）
ADMITTING/OBSERVING --响应到期/断开--> 结束等待，执行不被取消
OBSERVING --已观察到可信完成--> 200并停止观察
```

- [x] use case增加作废专用上下文参数，所有直接调用方/配置lambda/测试同步更新；认证失败返回401，验签/参数码保持，不在上下文预先存请求伪造身份。到期前未完成认证或未开始受理时关闭首次受理，回500且无订单数据；已开始事务但结果未知时202，文案不能声称已受理。
- [x] API全部改用受理/观察新合同后，删除原`enqueueInvalidOrder`公开入口及noop，更新适配器和所有测试桩；保留其原稳定事件写入逻辑在受理事务内部，不以临时兼容入口继续提供200/2异步受理。
- [x] 采用请求局部`cache()`非取消执行源，独立计时与每次完成后100ms串行观察竞争响应。截止/断开停止观察但不取消ADMITTING后的执行；计时器不做JDBC，Netty不等待future，执行与观察不共享单个串行线程。
- [x] 映射详细设计9.3.6的200/202/503，保持HTTP200；202清空无可信结论的券字段，保留已验证主单状态。已完成重复请求经过同一个持久完成谓词，不刷新首次时间。截止不强行追加读库。
- [x] 真实TCP断连验证已登记attempt后的执行仍提交终态/资金Delta；Controller取消测试通过。响应结束停止观察，执行结束释放请求局部订阅，不保留永久future。
- [x] P0真实TCP在认证闸门处断开；等服务端连接关闭后放开认证，观察无作废事件、无供应商调用，并以正常重复请求为控制。RC/RR均通过，不再用慢认证超时冒充断连场景。

**Check:** `mvn -pl cardshop-trade-engine-api -am -Dtest=V1QueryAndInvalidRechargeOrderUseCaseServiceTest,V1ControllerBlockingBoundaryTest,V1InvalidationDeadlineTest,RechargeOrderInvalidationServiceTest -Dsurefire.failIfNoSpecifiedTests=false test`。包含9项新增确定性纳秒/竞争用例、既有真实调度与用例回归；Task 8另有真实HTTP/MySQL证据，二者不混称同一种验证。

### Task 5：真实装配、配置与最小观测

**Files:** 表中infrastructure/bootstrap配置、Nacos样例；扩展`TradeEnginePropertiesTest.java`、`TradeEngineInfrastructureConfigurationTest.java`、`TradeEngineMybatisPersistenceAutoConfigurationTest.java`及`cardshop-trade-engine-bootstrap/src/test/java/com/cardshop/trade/engine/bootstrap/config/V1CardApiConfigurationTest.java`。

- [x] SINV-15装配回归通过：缺Worker或MINISO保护依赖必须不可用；完整后置MyBatis上下文最终只有一个真实作废用例。
- [x] 加V1等待属性，默认5000ms，拒绝非正值，保留现有配置命名/绑定风格；不机械钳制错误值伪装有效。供应商3秒和租约120秒保持既有默认，不改全局HTTP客户端或定时任务开关。
- [x] 装配编排服务和作废用例时晚解析真实依赖，覆盖基础/后置配置，缺所需依赖用现有不可用响应；不注册noop受理或只写事件的假成功实现。
- [x] 在HTTP响应观测边界记录详细设计9.3.8两项响应指标，复用现有Micrometer，无新框架或Application监控依赖；固定低基数结果标签，`LogStormGuard`限制超时warn及异常error频率，不输出敏感报文。
- [x] 两个仓库Nacos样例写入新等待属性，文档注明只影响新JAR。未发布Nacos、未改实际秘密配置。

**Check:** `mvn -pl cardshop-trade-engine-bootstrap -am -Dtest=TradeEnginePropertiesTest,TradeEngineInfrastructureConfigurationTest,TradeEngineMybatisPersistenceAutoConfigurationTest,V1CardApiConfigurationTest -Dsurefire.failIfNoSpecifiedTests=false test`。

### Task 6：显式作废模拟器与待确认消费验证

**Files:** 第2节模拟器文件；扩展`cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/a39/HttpV1CardApiClientTest.java`、`A39SimulatorOptionsTest.java`、`A39BusinessSimulatorApplicationTest.java`；新增同目录`A39InvalidationScenarioRunnerTest.java`。

- [x] SINV-16通过：202→12/1继续等待→12/3/3＋时间才确认；另以6/3/3矛盾主单保留红/绿回归，不只检查券字段。
- [x] client新增`invalidRechargeOrder(token,payload,sm4Enabled,aesKey)`，调用`/v1/cardapi/order/invalid`，复用post，不内置重试。请求字段仍out_order_sn/timestamp/nonce_str/sign，沿用现有`signQueryOrder`字段白名单，不将reason纳入签名。
- [x] 新增显式`--mode=invalidate`：必须显式提供既有out-order-sn，获取凭证后发一次签名作废，再按现有query-attempts/query-interval有限查单。不新建订单、不读取默认合成外单号执行真实作废，非invalidate模式行为不变。每次请求生成新timestamp/nonce/sign。
- [x] runner区分200完成、202确认中、503明确未受理；超时/断连查询原单。尝试用尽输出“仍待确认/需核查”且退出2，不自动重发invalid。日志不输出token/signKey/手机号/券码。
- [x] 保留已有10秒客户端默认并校验invalidate模式读取超时不小于10秒；本机HTTP替身验证路径、明文/SM4与签名保留、503及缺券时间场景。独立模拟器89项测试通过。

**Check:** `mvn -f cardshop-trade-engine-simulator/pom.xml test`。模拟器不在根聚合modules内，根`mvn test`不能替代此命令。

### Task 7：修订对接协议、离线示例与运维交接

**Files:** 第2节文档、`scripts/v1-invalidation-example.py`及其测试。

- [x] 已读取原§2.4及码表，使用OOXML增量编辑V1.2：200/202/503、HTTP200封装、5/3/10秒、重试/查单消费；保留密钥章节及其他ZIP成员，不替换权威协议形式。
- [x] 离线脚本仅生成合成completed/pending/not-accepted响应及消费判定，无凭据、无网络。覆盖12/1、缺时间、完整3/3及不明码；另补矛盾主单6的红/绿回归。
- [x] 2104指引补同步200与operate_type=3乱序幂等、202不触发最终失败通知；签验/银行ACK规则不变。模拟器手册补显式invalidate模式及原订单范围。
- [x] 部署手册补新配置与切换/回退：暂停作废新入口、登记在途、统一版本、核验202消费；保留事件/attempt/裁决/首次时间，继续可靠恢复，不向202操作宣判最终失败。
- [x] DOCX解包语义核对通过，无旧异步200/2合同，原商户密钥章节保留；V1.1历史文件未改，其他历史说明保留日期/边界。协议仅本地修订，未交付银行验收。

**Check:** `python3 -m unittest discover -s scripts/tests -p 'test_v1_invalidation_example.py'`；`git diff --check`；离线执行脚本确认只有合成标识且不发网络。正式命令行参数与帮助文本一并由脚本测试固定。

### Task 8：本地端到端故障证明与全量回归

**Files:** 新增`V1SynchronousInvalidationIntegrationTest.java`及可复用的`scripts/test-v1-synchronous-invalidation.sh`，组装真实HTTP入口、Worker、隔离数据库和本机MINISO替身。脚本只新建本机无卷随机schema容器并自行清理，不接受共享数据库；一次性编辑脚本删除，保留脱敏证据。

- [x] 旧用例/Controller保留未落库200及5500ms慢认证红灯，新用例对应门禁和截止测试通过；没有将此基线冒充旧版真实TCP证据。真实TCP只对新实现执行。
- [x] timer/XXL关闭；真实HTTP＋有效签名证明首次200前已有主库终态及资金裁决，资金释放暂停时也可完成；abandon一次、重复首次时间不变、唯一Delta。
- [x] 远端替身丢回包→约5秒202→原attempt只queryGrand恢复200，未第二次abandon；终态事务触发器回滚→202→只查证恢复200；真实TCP断连后继续完成。P0另以MySQL触发器拒绝供应商审计UPDATE，确认流水仍new/响应为空、无资金许可/Delta、202，解除故障后仅查证恢复200。
- [x] MySQL8.0.28分别RC/RR、真实独立连接验证并发受理/核销冲突；另跑61项本机隔离Redis合同及两JVM资金恢复。同步HTTP资金释放使用闸门替身，不能称为HTTP→Redis全栈测试。没有写共享test库。
- [x] 真实受理事务4秒提交返回200、6秒提交约5秒返回202后持久完成；本次待确认墙钟5009—5018ms，慢认证5012—5017ms。秒精度900ms首次due回归先红后绿。
- [x] 首轮根`mvn test`及`mvn clean package`各975项实际执行、76项条件跳过、0失败/错误；独立模拟器89项、Redis61项、Python11项通过。P0最终根`mvn clean package`984项实际执行、76项跳过（新增9项边界测试），定向34项通过；RC/RR隔离脚本8次显式合同报告通过。默认跳过的同步HTTP/桥接资金合同通过隔离脚本另跑，其他可选中间件测试未冒称通过。本轮未改模拟器/Python，未将首轮独立结果改记为新运行。
- [x] 同步改造无新依赖/DDL/广域锁；未改Worker/Gateway状态机；清理本轮缓存/临时编辑脚本，保留可复用验证脚本及脱敏日志。既有migration工作不归入本次。
- [x] 三项P0补证：精确截止采样/CAS竞争与受理前真实断连；attempt持久提交且供应商收到abandon后SIGKILL独立JVM（137），不改事件，等待夹具1秒租约自然过期，新JVM仅queryGrand恢复，事件/Delta各1、首次时间不刷新；真实SQL审计更新失败与执行拒绝。RC/RR各通过，生产默认120秒租约未改。
- [x] 执行拒绝分两层：真实Controller的boundedElastic替换为已关闭JDK执行器，实际调度拒绝→500且无data/事件，恢复调度器后正常200；提交后在`processEvent`入口注入JDK拒绝异常→202、原事件INIT/attempt=0，解除后200且abandon/Delta各1。当前`processEvent`直接同步执行，后者是执行边界注入，不是声称存在另一个生产异步提交器或完成线程池饱和压测。
- [x] 反向校验：临时将截止受理`>0`改为`>=0`，同刻/后1ns两例失败；临时让子JVM忽略持久attempt，恢复后abandon总数13而非12，合同测试失败。均已移除临时错误实现，保留原始失败证据，非将红灯计为通过。

### Task 9：授权后的dev/test联调、压力与切换核验

**Files:** `docs/集成测试.md`追加`SINV-DEVTEST-执行日期`记录；相关部署文档仅记录真实已完成事实。

- [ ] 先取得具体环境、节点、Nacos变更、测试订单/券、次数与金额授权。真实MINISO作废及可能触发的2104发送分别明确；不得沿用2026-10-26旧单授权，也不得因profile叫prod就将非授权生产数据纳入测试。
- [ ] 只读核验实际JAR/commit、schema、迁移、profile、主/供应商Nacos DataId及namespace/group、商户路由、3/5/10秒、租约、代理超时及银行发送开关。无确认的字段作为门禁，不填猜测值；操作范围仅dev/test，生产另行安排。
- [ ] 分别验证：新入口停后台扫描首次完成；既存任务API/Worker竞争；慢回包202及查询收敛；银行界面“确认中”；同步结果/通知乱序幂等；失败不重复退款。故障注入以隔离替身为主，真实券不做无授权重放。
- [ ] 在授权负载范围记录请求量/并发、P50/P95/P99、5秒超差、202比例、SQL次数/延迟、连接池/隔离线程、INVALID及人工积压，估算100ms观察在实测流量下的负担；未达到双方既有容量目标或出现资源饱和不得放行，不以历史260ms单样本替代压测。
- [ ] 仅在共享写入/部署获具体授权后执行已评审切换；回退演练保留未决操作和事实。银行未支持202、代理超时未核实、旧接流节点未下线、既有桥接/资金发布门禁未满足时停止切换。
- [ ] 记录脱敏请求/响应、主库前后事实、attempt与queryOnly计数、资金/通知状态、完整命令和结论；只有该范围实际执行并通过才能宣布dev/test验收，不能据本计划宣称部署完成。

## 4. 验收矩阵：每项都要能区分一种错误实现

| 编号 | 输入/故障 | 必须观察到的结果 | 可击穿的错误 |
| --- | --- | --- | --- |
| SINV-01 | 完整未使用单券，XXL与timer关闭 | 首次响应200、12/3/3及时间，主库完成/裁决已提交，abandon=1 | 只是入队或仅等扫描。 |
| SINV-02 | A/B商户同外单号、请求属于A | 只操作A；B事件/券/资金不变 | 只按外单号/平台ID读写。 |
| SINV-03 | Redis未落库、主单缺扩展旧单、未发券、缺凭据 | 首次503/明确未受理，无新INVALID/abandon | 查单兼容默认DB_PERSISTED或延迟作废。 |
| SINV-04 | 原PROCESSING/未到期FAILED/人工事件且新快照不满足首次条件 | 原事件不变，202/原完成事实，无重建/提前重试 | due空就视为不存在、复活人工。 |
| SINV-05 | 两API节点及Worker争同一原事件 | 稳定ID一行、正常abandon一次、资金一次、原时间不变 | 多执行器各自裸调Gateway。 |
| SINV-06 | 认证/隔离排队持续超过5秒，尚未受理 | 约5秒500无未认证数据；放开闸门后仍不入队 | 认证后才起计时或超时后晚受理。 |
| SINV-07 | 受理事务已开始，提交确认持续超过5秒 | 202带已验证身份，不声称已受理；提交后继续恢复 | 把提交未知归为明确未受理。 |
| SINV-08 | 已登记attempt，HTTP迟回或客户端断开 | 响应结束不取消结果持久化；恢复不重复abandon | timeout中断整条业务链。 |
| SINV-09 | 4秒/6秒提交；截止前后及同刻竞争 | 早已观察完成可200，晚提交202后查询成功，无伪失败 | 调用结束后事后判断超时。 |
| SINV-10 | 未认证、验签失败、跨商户猜订单 | 原错误码、不泄露身份/券、不写作废事件 | 全链路超时fallback返回带数据202。 |
| SINV-11 | 终态/资金裁决已提交，Redis释放暂停/失败 | 推荐口径200；选择实时回补口径则202；首次/重入一致 | 直接用Worker FAILED或重复请求快捷成功。 |
| SINV-12 | 通知准备/银行ACK/markSuccess失败，或合法无需释放裁决 | 合法券事实按同口径；任务继续恢复，时间不刷新 | 把收尾失败反向报券失败或提前结束任务。 |
| SINV-13 | 缺时间/损坏或缺失资金裁决的历史CANCELLED | 不200、不自动补裁决/时间/退款 | 仅券状态3或balance_reserved=0即成功。 |
| SINV-14 | 核销先提交、远端成功后冲突、授权先提交迟到核销 | 前两者无首次退款；后者留证人工且不撤销原授权 | API绕过保护或重裁决截断资金恢复。 |
| SINV-15 | 缺Worker/保护/事务依赖；完整后置自动配置 | 缺依赖不可用，完整上下文单Bean可同步执行 | 早注册fallback/noop假成功。 |
| SINV-16 | 202→查单200/12/1→200/12/3/3＋时间 | 不在12/1终结，不显示最终失败，后续确认完成 | 复用下单runner以订单12终结。 |
| SINV-17 | 空/错/重复succeed、部分failed、仅外层200 | 202，不退款、不完成，原事件可恢复 | 放宽供应商严格成功解析。 |
| SINV-18 | 远端执行但回包丢失、attempt后宕机/审计失败 | 后续只有INVALID_QUERY；同券同批次原生status=3才完成 | 不查持久尝试、盲重发。 |
| SINV-19 | 终态事务回滚、事件提交后执行器拒绝 | 不提前200，原任务保留；明确回滚与提交未知分开 | 无恢复载体或把入队当完成。 |
| SINV-20 | 其他供应商返回PROCESSING；现有创建/查单/通知回归 | 作废202，原任务恢复；其他入口合同不变 | 用MINISO同步能力推定全渠道，扩大公共接口修改。 |

## 5. 验证命令与证据要求

先设置本机JDK21；以下命令已于2026-11-05本地执行，计数与限制见本节和《集成测试.md》同日记录。原133项仅为改造前定向基线，不能代替最终根回归。

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PATH="$JAVA_HOME/bin:$PATH"
java -version
mvn -version

# 保留的供应商、Worker与V1现有行为基线
mvn -pl cardshop-trade-engine-api,cardshop-trade-engine-infrastructure -am \
  -Dtest=MinisoHttpClientTest,MinisoRechargeSupplierGatewayAdapterTest,RechargeOutboxWorkerServiceTest,V1QueryAndInvalidRechargeOrderUseCaseServiceTest,V1ControllerBlockingBoundaryTest \
  -Dsurefire.failIfNoSpecifiedTests=false test

# 实施后全量编译/测试/打包；模拟器不在根聚合中，必须另跑
mvn test
mvn clean package
mvn -f cardshop-trade-engine-simulator/pom.xml clean package
# 本机系统Python3.9缺少既有脚本需要的SM3；使用已安装的3.12，不升级依赖
/opt/homebrew/bin/python3.12 -B -m unittest discover -s scripts/tests -p 'test_*.py'
bash scripts/test-v1-synchronous-invalidation.sh
mvn -pl cardshop-trade-engine-infrastructure -am \
  -Dbalance.localRedisTests=true -Dtest=BalanceReservationLocalRedisContractTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
bash -n scripts/test-v1-synchronous-invalidation.sh
git diff --check
```

数据库/Redis依赖用现有隔离夹具并明确版本和跳过条件，不写共享环境。跳过的真实MySQL、跨连接或Redis用例须另外执行才能宣布对应结论；没有运行条件则诚实标阻断，不把H2通过等价为MySQL锁验证。银行SDK缺失时按`docs/交行2104回调接入指引.md`使用已批准的官方包安装，不改版本或跳银行测试。

本地证据放`.amp/in/artifacts/`前确认`.git/info/exclude`已有`/.amp/in/`，留脱敏日志/响应/SQL结果，不保存凭据、手机号、完整券码或原始敏感回包。永久测试优先覆盖公共接口与可观察业务结果，不保留只断言私有helper/调用顺序的测试。临时复现脚本在验证后清理，只有实际集成执行时才更新`docs/集成测试.md`执行结论。

## 6. 设计覆盖与交付判定

| 设计条款 | 实施任务 | 主要验收 |
| --- | --- | --- |
| 9.3.1目标/决策 | 0、7、9 | 资金选择、202消费、现有通知合同。 |
| 9.3.2端口/事务 | 1、2、5 | SINV-02/03/04/13/15。 |
| 9.3.3即时执行/观察 | 3、4、8 | SINV-01/05/11/19。 |
| 9.3.4deadline/取消 | 4、5、8 | SINV-06/07/08/09/10。 |
| 9.3.5资金/通知 | 0、2、3、7、8 | SINV-11/12/13/14。 |
| 9.3.6协议/调用方 | 4、6、7、9 | SINV-10/16/20。 |
| 9.3.7供应商/故障 | 3、8 | SINV-05/14/17/18/19。 |
| 9.3.8配置/观测/数据门禁 | 5、7、8、9 | 无新DDL、低基数指标、全量/独立模拟器回归及环境授权。 |
| HA19.1切换/回退 | 7、9 | 统一版本、真实消费验证、事实保留及旧200/2回退协调。 |

交付须分别报告文档、本地实现/验证、dev/test联调及提交/推送/部署状态。**文档和Task 0—7本地实现完成，Task 8三项P0故障补证及最终全量构建通过；容量及Task 9现场联调/切换未执行，未提交/推送/部署。**代码完成不表示DOCX已交付银行，现场JAR更新不表示202消费/银行ACK已验收。下一优先任务为本地容量/资源验证，不自动取得任何部署或共享环境写入授权。

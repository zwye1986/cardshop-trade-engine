# MINISO Shared Callback Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use executing-plans to implement this plan task-by-task. If the user explicitly chooses delegation, use subagent-driven-development for independently owned tasks. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在保留旧名创唯一回调入口及Java8/Java21运行边界的条件下，实现可靠共库分流，并保证新旧退款隔离、核销与首次释放裁决互斥、已授权释放可完整恢复。

**Architecture:** 旧入口提交共享inbox后确认；后台在主数据源同连接事务中投递新事件，或提交旧核销事实及冻结通知任务。新旧凭据发布、核销及资金裁决统一使用券码实体guard；新MINISO终态与持久释放授权原子提交，锁外Redis释放后再原子提交RELEASE delta及释放标记。保护不依赖入口或Job开关。

**Tech Stack:** 新引擎Java21、Spring Boot3、MyBatis、Liquibase、JUnit5/AssertJ；旧卡管Java8、Spring Boot2、MyBatis、Fastjson2及已有JUnit Jupiter测试；可丢弃本地MySQL/InnoDB、真实本地Redis、Maven3.9、离线HTTP桩。

---

## 0. 执行状态、授权与不可变合同

2026-10-21用户已整稿评审通过。已批准规格为`docs/superpowers/specs/2026-10-20-miniso-shared-callback-bridge-design.md`，契约`MINISO-SHARED-CALLBACK-V1`；主详细设计9.8及高可用设计同名章节同步。用户随后授权在当前会话按任务顺序实施、分阶段验收，并单独授权只读核验test188所连MySQL版本及默认隔离级别。**下列勾选只表示对应本地步骤完成，不代表整套实现或发布完成。** 远端仅执行该项获授权的只读核验，不运行共享DDL、不改Nacos、不部署、不push/PR/merge。

执行位置：

- `E=/Users/mac/codespace/cardshop-trade-engine-1020_MINISO_SHARE_DB`：当前新引擎隔离工作区，分支`1020_MINISO_SHARE_DB`，计划编写时基线为`5271fcf`。
- 旧卡管计划编写时位于`/Users/mac/Documents/codeSpace/cardshop`，本地`dev`基线`34eadb29ed00a10e528915dc32982cff674fa58e`。用户同意独立工作树后，已从该本地基线创建`L=/Users/mac/Documents/codeSpace/cardshop/.worktrees/miniso-shared-callback`，分支`miniso-shared-callback`；后续旧端执行及脚本参数均使用此`L`。原`dev`不切换，用户未跟踪接入说明保留原处，不复制或覆盖。
- 上述是本地基线，不声称等于`origin`或远端运行版本。不能跨工作树覆盖未提交文件。

先后依赖：任务1—8交付新端持久保护；任务9—16交付旧桥接及所有旧写入者保护；任务17做跨JVM并发和恢复验收；任务18—19交付调度、文档、回归及待授权发布检查。全部属于同一资金合同，不能只部署“能收回调”的半套实现。顺序实施，默认不分派同一文件给多个代理。

必须保持：

1. 已提交授权不因迟到核销撤销，包含Redis尚未执行的窗口；核销转人工不能终止该授权的资金恢复。
2. 可能已发券但缺券码/凭据：原订单号查单，仍不明则人工，不自动退款；具体“明确未发券”失败码必须另有供应商合同证据，不能猜测。
3. 主单→扩展归属→凭据→guard→新事件→inbox→任务/结果CAS。未知接收只锁guard及事件/inbox，不持guard再找主单。
4. 旧端排除任何存在`recharge_order_ext`的订单，不限渠道或persist_status。新端领域层不引用旧表或桥接持久化类型。
5. 所有重要代码中文注释链接详细设计9.8；DTO字段注释逐项对应DDL COMMENT。异常直接抛给事务边界；处理/退避处安全采样记录栈及event/order/org，不记录完整券码、手机号、payload、密钥或签名原文。

### 命令约定

以下函数用于本机执行测试，不写入生产启动脚本。旧项目已有Jupiter测试，不能仅看到junit4依赖就改用JUnit4。每次核对Surefire实际执行数量，避免把零测试或跳过当绿灯。

```bash
export E=/Users/mac/codespace/cardshop-trade-engine-1020_MINISO_SHARE_DB
export L=/Users/mac/Documents/codeSpace/cardshop/.worktrees/miniso-shared-callback
engine_test() (
  export JAVA_HOME=$(/usr/libexec/java_home -v 21)
  export PATH="$JAVA_HOME/bin:$PATH"
  mvn -B -f "$E/pom.xml" -pl cardshop-trade-engine-infrastructure -am \
    -Dtest="$1" -Dsurefire.failIfNoSpecifiedTests=false test
)
legacy_test() (
  # 本机java_home -v 1.8会优先选中JavaAppletPlugin JRE，必须显式使用带javac的JDK8。
  export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-1.8.jdk/Contents/Home
  export PATH="$JAVA_HOME/bin:$PATH"
  mvn -B -f "$L/pom.xml" -pl cardShop-admin -am \
    -Dmaven.test.skip=false -DskipTests=false \
    -Dtest="$1" -Dsurefire.failIfNoSpecifiedTests=false test
)
```

每个行为任务按“加失败测试→运行确认失败原因→最小实现→同一测试转绿→关联回归→本地小提交”执行。编译/环境失败不能冒充行为红灯。下文代码块给出接口、关键SQL和判别断言；测试放进列出的真实事务夹具，不为每个内部方法新建测试。提交时只stage本任务文件，禁止`git add .`。本地提交不授权push或部署。

### 2026-10-21第一阶段执行记录

- 新端基线为本地`07ec488`，当前隔离工作树无他人未提交修改；旧端仍为`dev`、`34eadb29`，保留用户未跟踪接入说明，未创建旧工作树或改旧代码。相关目录未发现额外AGENTS。
- 基线真实运行：新端三组持久化测试29项、旧端两组订单测试36项，均失败0、错误0、跳过0。旧端首次因JRE缺编译器失败，指定上面JDK8后通过，不将环境失败计为TDD红灯。
- 写入者复核：除原列出的两条MINISO发券和通用/V2/支付宝退款外，`ExtCommonApi.monisoAbandon`可绕过订单作废边界；`McVoucherListOutServiceImpl.syncState`手工BATCH会话提交且按券码覆盖状态；奈雪回调更新SQL缺渠道限制。任务10/12必须消除这些可写MINISO凭据的旁路。386品牌库存路径需先证明channel/state_type及可信凭据来源，不能仅凭品牌名当作MINISO通道。
- `MovieLxOrderService.refundLocal`虽属于独立电影业务，却实际写共享mc_order、企业余额及流水，因此任务11必须纳入新归属最终门禁，不重做电影退款金额/售后模型。独立采购总单、充值及转账不据同名余额SQL扩大改造。`singleRefundMethod`调用均已注释，但保留为待保护残留；V2公有入口同样不因标注“未用”豁免。
- 已读MCUtils.queryGrand、新MINISO适配及固定测试；本机未找到供应商V1.5原始PDF，现有准备报告只注明文件名/正文版本差异。无权威“明确未发券”失败码证据，任务7不实现无券码自动退款例外。
- 新增0023合同测试先出现1项预期断言失败（独立变更不存在），追加结构后转绿。关联新端模块共528项：509通过、19项既有环境依赖测试跳过，失败0、错误0；其中迁移合同4项全通过。实体仅增加映射；三授权字段一致性由任务5的实际读写入口校验，当前无资金授权实现。
- **任务3前置条件未满足**：Docker daemon可用，本机已有mysql:8.4镜像，但未核实test188所连MySQL具体版本和默认隔离级别；未设置MINISO_MYSQL_IMAGE，未创建容器或执行MySQL迁移，不能把本地镜像当作现场版本。第一阶段仅验收源码基线及DDL解析合同，等待版本证据后继续任务3，不跳到资金实现。
- **同日10:21 CST只读核验补充，解除上述版本未知阻断**：用户单独授权后，按JVM PID18924的本地数据库端口49250匹配PROCESSLIST会话12637338，确认实际schema仍为cardshop_test_new。SELECT VERSION()=8.0.28、@@version_comment=Source distribution、@@GLOBAL.transaction_isolation=READ-COMMITTED，本次新会话亦为RC。performance_schema会话变量查询因1142权限不足，未直接测得旧JVM既有会话隔离级别；不扩大权限或混同全局默认。任务3选择MySQL8.0.28基线（镜像/平台可用性仍待验证），显式测试RC/RR，不用本机8.4替代。尚未创建容器或执行迁移，完整证据见《集成测试.md》同日补充。
- 详细命令与实际测试结果统一追加`docs/集成测试.md`；真实MySQL双跑、RC/RR并发、Redis恢复及发布均未验收。

## 1. 文件职责图

路径缩写仅用于本计划，均相对上面的`E`或`L`：

- `EA=cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge`
- `EI=cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure`
- `ET=cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure`
- `LB=cardShop-admin/src/main/java/com/ruoyi/web/mc/miniso/bridge`
- `LT=cardShop-admin/src/test/java/com/ruoyi/web/mc/miniso/bridge`
- `LM=cardShop-admin/src/main/java/com/ruoyi/web/mc`

新端以既有所有者为主：

- `EA/RechargeFulfillmentOrderPort.java`及`EA/MinisoBalanceReleaseAuthorization.java`：授权读取、终态/首次授权、释放收尾的应用合同；不新增通用资金框架。
- `EI/persistence/MybatisRechargeFulfillmentOrderAdapter.java`、`entity/RechargeOrderExtDO.java`：主单/扩展锁、授权不可变、终态原子性、delta/标记原子性。
- `EI/persistence/MinisoReceiptStore.java`、`MinisoWriteOffOrderLocks.java`、`MybatisMinisoWriteOffStore.java`、`MybatisMinisoWriteOffProtection.java`：复用现有凭据及核销边界接入guard、共享证据和迟到授权冲突。
- 新`EI/persistence/mapper/MinisoCouponGuardMapper.java`、`MinisoSharedCallbackInboxMapper.java`：只负责互斥行和共享接收证据的单表SQL；不把旧业务表暴露给application。
- `EI/persistence/mapper/MinisoReceiptMapper.java`、`MinisoWriteOffEventMapper.java`、`RechargeBalanceDeltaOutboxMapper.java`：锁后当前读、版本CAS、既有delta一致性读取。
- `EI/config/MinisoWriteOffAutoConfiguration.java`、`TradeEngineMybatisPersistenceAutoConfiguration.java`：真实主连接装配及结构核验。专用outbox写连接不可用于授权收尾事务。
- `EA/RechargeOutboxWorkerService.java`：先恢复已有授权，再首次裁决；HTTP/Redis保持锁外；非MINISO行为不变。

旧端新增一个内聚桥接包，不新增Maven模块、共享运行时JAR或匿名手工接口：

- `LB/MinisoCallbackBridgeProperties.java`：模式、入口/Job开关、独立key2及重试策略。
- `LB/MinisoCallbackAuthenticator.java`：有界UTF-8、严格JSON/头、Java8兼容签名及事件ID。
- `LB/MinisoCallbackFact.java`、`MinisoBridgeLease.java`：最小已认证事实和owner/version租约；禁止默认toString暴露券码。
- `LB/MinisoBridgeConfiguration.java`、`MinisoCallbackBridgeStore.java`：绑定`masterDataSource`的JdbcTemplate/TransactionTemplate及结构核验，接收与路由SQL均使用这一个连接；不使用旧动态/no-binlog路径。
- `LB/MinisoCallbackRouteService.java`：候选查询、归属锁后复核、新事件投递、旧事实应用及最终CAS。
- `LB/MinisoLegacyOrderTransactions.java`：旧MINISO凭据发布、作废前后复核、退款写前归属与证据复核；现有余额/流水Mapper只有经证明参与同一主事务才复用。
- `LB/MinisoLegacyNotificationService.java`：冻结旧AES正文、锁外发送、HTTP层结果及租约CAS。
- `LB/MinisoCallbackBridgeJobs.java`：两个受控XXL handler及扫描/积压摘要，不新增第三套调度器。
- `LM/api/ExtCommonApi.java`：保留URL，BRIDGE模式读取原始有界正文；LEGACY模式保留首次兼容行为，不承担SQL。
- `LM/api/service/impl/MINISOSupplierTradeServiceImpl.java`、`LM/service/impl/McOrderServiceImpl.java`、`McOrderV2ServiceImpl.java`：全部名创凭据发布/作废/退款调用转入上述事务所有者，不整体重构大类。
- `LM/mapper/McOrderMapper.java`及`cardShop-admin/src/main/resources/mapper.system/McOrderMapper.xml`：候选排除及严格条件更新，保留其他供应商原语义。

测试/协议文件：扩展既有持久化、Worker、验签、配置测试；新增`ET/persistence/MinisoBridgeMysqlContractTest.java`、`LT/MinisoBridgeMysqlContractTest.java`用于各自实际生产边界的InnoDB验证；`scripts/test-miniso-shared-callback.sh`仅编排可丢弃本地测试。共享的是迁移和固定离线向量，不共享Spring上下文或Java版本依赖。

## Task 1: 核对源码、协议与执行基线

**Files:** Read `E/AGENTS.md`、`L/AGENTS.md`、已批准规格、两份主设计；执行记录更新目标为`E/docs/集成测试.md`，只在实际测试发生后追加。

- [x] 1. 在两个仓库分别运行`git status --short`、`git branch --show-current`、`git log -3 --oneline`；记录原有变化。确认JDK8/21和Maven3.9，读取子目录AGENTS。旧端`docs/名创单入口共库核销分流接入说明.md`已只读检查，不覆盖。
- [x] 2. 用以下精确搜索建立“全部相关写入者→实际调用入口→主数据源/事务→后续改造任务”的清单（补充见第一阶段执行记录）；不能靠任务开关排除可调用路径。只扩大到按共享mc_order执行资金或MINISO事实写入的路径。

```bash
rg -n 'refundExtDepartBalance|singleRefundMethod|void orderRefund|insertMcBatchRecord|upBatchRecord|updateMcBatchRecordByOrderIdAndCode|extProductRechargeMc' "$L/cardShop-admin/src/main"
rg -n 'markBalanceReleased|updateFulfillmentStatus|terminalBalanceReleasePending|blockInvalidation|insertIfAbsent' "$E/cardshop-trade-engine-application/src/main" "$E/cardshop-trade-engine-infrastructure/src/main"
rg -n 'id: 002[0-9]' "$E/cardshop-trade-engine-migration/src/main/resources/db/changelog/db.changelog-master.yaml"
```

- [ ] 3. 原始供应商PDF尚缺，未确认任何可证明“未发券”的失败码。已核对`MCUtils.queryGrand`、新端响应分类和固定测试；无权威证据时保持查单/人工，不实现自动放行例外，不阻塞其他任务。
- [x] 4. 运行基线：`engine_test 'MinisoWriteOffPersistenceTest,MinisoReceiptPersistenceTest,RechargeFulfillmentVoucherStatePersistenceTest'`及`legacy_test 'McOrderServiceImplTest,McOrderV2ServiceImplTest'`。记录失败/缺依赖，先区分既有故障，不修改无关业务制造绿灯。
- [x] 5. 不做独立业务提交；基线证据与首个结构实现一起记录。非测试的只读发现不写成联调通过。

## Task 2: 新增桥接结构，冻结DDL合同

**Files:** Modify `E/cardshop-trade-engine-migration/src/main/resources/db/changelog/db.changelog-master.yaml`、`E/cardshop-trade-engine-migration/src/test/java/com/cardshop/trade/engine/migration/RechargeD2ChangelogContractTest.java`；Modify `EI/persistence/entity/RechargeOrderExtDO.java`。

- [x] 1. 扩展既有迁移合同测试，读取新增changeSet并按表断言主键/唯一键/重试索引、binary券码、order_id长度100和全部中文COMMENT。新增ext三个字段必须可空；不能给历史数据自动回填授权。先运行：

```java
@Test
void declaresBridgeEvidenceAndNullableReleaseAuthorization() throws IOException {
    String sql = Files.readString(Path.of("src/main/resources/db/changelog/db.changelog-master.yaml"));
    assertThat(sql).contains("mc_miniso_callback_inbox", "mc_miniso_legacy_notify_task",
            "mc_miniso_coupon_guard", "balance_release_authorized_at",
            "balance_release_event_id", "balance_release_amount");
}
```

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -B -f "$E/pom.xml" \
  -pl cardshop-trade-engine-migration -am -Dtest=RechargeD2ChangelogContractTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

预期红灯：桥接结构不存在；不是修改旧changeSet断言放行。
- [x] 2. 核验编号仍空闲后追加`0023-miniso-shared-callback-bridge`；完整列/默认值/索引按规格5、6.1、8.3、9节逐项落实。关键DDL如下，其余接收/通知列使用规格的明确清单，不修改0020—0022：

```sql
CREATE TABLE mc_miniso_coupon_guard (
  coupon_code varchar(128) COLLATE utf8mb4_bin NOT NULL COMMENT '名创券码',
  created_at datetime(3) NOT NULL COMMENT '创建时间',
  PRIMARY KEY (coupon_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='名创券码事务互斥表';
ALTER TABLE recharge_order_ext
  ADD COLUMN balance_release_authorized_at datetime(3) NULL COMMENT '余额释放授权时间',
  ADD COLUMN balance_release_event_id varchar(100) NULL COMMENT '余额释放授权事件编号',
  ADD COLUMN balance_release_amount bigint NULL COMMENT '余额释放授权金额，单位分';
```

- [x] 3. 实体追加对应`LocalDateTime`/`String`/`Long`字段，注释与DDL一致。迁移遇已有结构HALT，不用MARK_RAN掩盖；无自动删表/删列回滚，只允许保留证据的前向修复。授权三字段全空或全非空的读写校验属于任务5的生产事务边界，当前尚未实现，不由逐字段实体setter承担。
- [x] 4. 同命令转绿；真实MySQL迁移两遍的验收在紧接的任务3执行，未执行前本任务只有解析/合同证据。第二遍须无新增执行且数据保留，核对information_schema。解析不是发布证明。
- [x] 5. 本地提交`feat(migration): add miniso bridge evidence and release authorization`，包含本阶段源码核对及实际测试记录；不代表MySQL验收或可单独部署。

## Task 3: 建立可丢弃MySQL测试环境

**Files:** Create `E/scripts/test-miniso-shared-callback.sh`、`ET/persistence/MinisoBridgeMysqlContractTest.java`、`LT/MinisoBridgeMysqlContractTest.java`；Create `E/cardshop-trade-engine-infrastructure/src/test/resources/miniso-bridge/legacy-baseline.sql`，只供可丢弃schema初始化。旧端测试通过脚本提供的同一schema消费结构，不复制发布DDL。不新增共享运行时模块。

2026-10-21执行结果：用户同意后创建上述旧工作树，基线36项全通过；旧端消费合同已在Java8.0_491及项目既有Connector/J8.0.33上运行。新端脚本以MySQL8.0.28（linux/amd64，本机仿真）分别在RC/RR执行真实master全部25项及无变更重跑，随后旧JVM只读消费同一schema中的guard、接收事实、冻结通知及历史空授权。两级隔离×新旧两端共4次显式MySQL合同全通过、无跳过；两个schema分别为`miniso_bridge_it_34a0737db382c5b5`、`miniso_bridge_it_79a9f9926a547edc`，容器均已清理。缺配置/无法连接显式非零；默认回归新端509通过/20跳过，旧端36通过/1跳过，失败/错误均0。旧端本地提交`d5a5b5bfa3f6f95318a05b8f450adf7947b7b66f`仅含Java8测试；新端源码/执行记录随本任务一起本地提交，不push或部署。任务3只交付结构夹具，未实现生产路由/资金事务，后续实际mapper/adapter、Spring事务代理及锁竞争仍按任务4—17验证；本次停在阶段验收，不直接执行任务4。完整命令、checksum、反例及限制见《集成测试.md》`MINISO-BRIDGE-MYSQL-SCHEMA-LOCAL-20261021`补充。

- [x] 1. 脚本先只支持`--schema`：要求显式`MINISO_MYSQL_IMAGE`（先核验测试目标版本再选镜像，不把任意8.x冒充现场），仅使用Docker新建容器、随机回环端口和本次随机凭据。数据库名固定前缀`miniso_bridge_it_`加随机后缀；不接受任意远端JDBC URL。无Docker、镜像或JDK直接非零，不报告跳过为通过。
- [x] 2. 用测试资源建最小legacy表前置结构，再通过真实Liquibase加载新端master changelog；旧端只消费迁移后的结构。记录`SELECT VERSION(), DATABASE(), @@transaction_isolation`及迁移checksum。给测试用户仅本次schema权限。脚本退出仅清理自己创建的容器/临时文件，不执行共享schema DROP。

```sql
SELECT VERSION(), DATABASE(), @@transaction_isolation;
SELECT table_name, engine FROM information_schema.tables
WHERE table_schema=DATABASE()
  AND table_name IN ('mc_miniso_callback_inbox','mc_miniso_coupon_guard',
                    'mc_miniso_legacy_notify_task');
```

- [x] 3. `MinisoBridgeMysqlContractTest`显式开启时必须连接成功；普通单测默认不连接外部。参数为`-Dminiso.mysqlTests=true`，连接由脚本注入本次本地测试环境；缺配置是测试失败。本任务只验证结构；后续业务事务测试必须使用实际生产mapper/adapter及Spring事务代理，不能用重写生产算法的JDBC假实现取代。
- [x] 4. 执行`MINISO_MYSQL_IMAGE="$MINISO_MYSQL_IMAGE" bash "$E/scripts/test-miniso-shared-callback.sh" --schema`；预期三表为InnoDB、迁移幂等、无历史授权回填。执行前如果变量为空则明确失败。
- [x] 5. 本地提交`test: add disposable mysql bridge contract fixture`；此提交交付数据库验证能力，不声称并发合同已通过。旧端测试独立提交到旧工作树分支，两端均不推送。

## Task 4: 新端统一guard及共享证据当前读

**Files:** Create `EI/persistence/mapper/MinisoCouponGuardMapper.java`、`MinisoSharedCallbackInboxMapper.java`；Modify `MinisoReceiptStore.java`、`MinisoWriteOffOrderLocks.java`、`MybatisMinisoWriteOffProtection.java`、`mapper/MinisoReceiptMapper.java`；Test `ET/persistence/MinisoReceiptPersistenceTest.java`、`MinisoWriteOffPersistenceTest.java`。统一guard还须同步`MybatisMinisoWriteOffStore.java`的直接接收/处理调用，以及`MinisoWriteOffAutoConfiguration.java`、`TradeEngineInfrastructureConfiguration.java`和既有装配测试，避免构造器兼容路径绕过保护；不提前实现任务8的释放授权优先级。

2026-10-21执行结果：基线22项通过；补齐两套H2最小schema后，先观察共享证据/首次凭据/缺guard等11项行为失败，再补直接接收/处理入口反例得到37项中14项预期失败、错误0。接入同主事务guard与共享证据当前读后，同两组37项全通过；关联模块544项中524通过/20跳过，全根reactor707项中687通过/20跳过，均失败0、错误0。只完成本地Task4；共享inbox六状态均阻止作废，guard自身/不同券不阻止，失败时凭据、主单、扩展及新建guard随调用事务回滚。首次作废凭据在guard后临时insert再按订单复核，冲突时整体回滚；不跳过缺凭据时的同券证据。RC/RR当前读和缺行锁竞争仍待任务17使用真实InnoDB验证；任务5—8授权及恢复尚未实现，不构成可发布半套方案。执行证据见《集成测试.md》`MINISO-BRIDGE-GUARD-LOCAL-20261021`；本任务按下面标题本地提交，不push、部署或改共享配置。

- [x] 1. 在真实事务测试中插入任意状态共享inbox（包括MANUAL_REQUIRED、TRANSFERRED_NEW），通过现有作废写入口断言不能提交；只有guard无inbox应不阻止。用不同券码及不同状态作反例，避免测成“有guard就一律拦截”。运行`engine_test 'MinisoWriteOffPersistenceTest,MinisoReceiptPersistenceTest'`得到红灯。

先将两个H2 fixture的最小schema增加本任务实际查询的guard/inbox及授权列，mapper加入同一SqlSession配置，再加入下面测试。这样红灯来自未保护，而非缺表异常；InnoDB语义仍在任务17验证。`seed/receipt/notice/status/USED`均复用现有`MinisoWriteOffPersistenceTest`成员。

```java
@Test
void sharedManualEvidenceBlocksVoidEvenWithoutNewEngineEvent() {
    seed(0, "");
    jdbc.update("insert into mc_miniso_callback_inbox "
            + "(event_id,coupon_code,use_time,status,retry_count,next_retry_time,version,received_at,updated_at) "
            + "values(?,?,?,'MANUAL_REQUIRED',0,?,1,?,?)",
            notice().eventId(), "C1", USED, USED, USED, USED);
    assertThatThrownBy(() -> tx.executeWithoutResult(s -> receipts.save(receipt(3))))
            .isInstanceOf(MinisoWriteOffException.class)
            .hasMessage("REDEEM_VOID_CONFLICT");
    assertThat(status()).isZero();
    assertThat(count("recharge_miniso_writeoff_event")).isZero();
}
```

- [x] 2. 实体guard只在调用方实际事务中幂等建行并锁定；用下面精确键语句，不用`INSERT IGNORE`吞其他错误。新端首次凭据insert前锁guard，更新已有凭据先锁凭据再锁guard；未知接收不能反向拿主单锁。

```sql
INSERT INTO mc_miniso_coupon_guard(coupon_code,created_at)
VALUES(?,?) ON DUPLICATE KEY UPDATE coupon_code=coupon_code;
SELECT coupon_code FROM mc_miniso_coupon_guard WHERE coupon_code=? FOR UPDATE;
SELECT event_id,use_time,status FROM recharge_miniso_writeoff_event
WHERE coupon_code=? FOR UPDATE;
SELECT event_id,use_time,status FROM mc_miniso_callback_inbox
WHERE coupon_code=? FOR UPDATE;
```

- [x] 3. 调用方必须先锁主单/扩展/凭据；guard后不用早先快照当“无核销”。`blockInvalidation`只作前置提示，不能授予资金许可；最终保护参与终态调用方事务。读/锁异常直接抛出，绝不返回无证据。
- [x] 4. 同命令转绿，扩展事务中断断言凭据/主单全部回滚；任务17再以RC/RR证明缺行竞争。
- [x] 5. 本地提交`fix(miniso): serialize receipt publication and callback evidence`。

## Task 5: 新端终态与首次持久授权原子提交

**Files:** Create `EA/MinisoBalanceReleaseAuthorization.java`；Modify `EA/RechargeFulfillmentOrderPort.java`、`EI/persistence/MybatisRechargeFulfillmentOrderAdapter.java`、`EI/persistence/entity/RechargeOrderExtDO.java`及现有mapper；Test `ET/persistence/RechargeFulfillmentVoucherStatePersistenceTest.java`。

2026-10-21执行结果：在本地Task4提交`0138744`之后，仅实施Task5。基线两组15项全通过；原终态旁路先出现2项预期断言失败，新增许可接口尚未实现时41项中30项断言失败、4项因明确拒绝的占位接口报错，未把编译/缺表故障当作行为红灯。补充历史终态重开及两套状态错配反例后45项中4项预期失败，再转绿；最后补充仅修改主单6/11却伪装派单SUCCESS的2项旁路反例，先红后绿。最终五组目标测试99项全通过；根reactor747项中727通过/20项既有环境依赖测试跳过，失败/错误均0。

生产晚装配工厂＋真实主SqlSession Mapper＋Spring事务代理证明：终态、凭据及首次授权整体提交/回滚，任意核销证据阻止首次许可；event-A/137分/首次毫秒时间冻结，event-B和迟到证据不重写，历史无授权终态不能补认或被SUCCESS重新打开。金额核对锁定的`mc_depart_record(type=1,正数value)`原预扣事实，源事件校验本订单现存履约outbox身份/类型。实体三列已在Task2映射，本轮无新增DDL或字段；正常/晚装配均传入主Mapper并延迟解析凭据Store，未改专用outbox写连接。仅可信作废凭据可证明已发券路径具备释放资格，无券码/未使用券及未经核验的失败码不放行。

端口`completeMinisoBalanceRelease`明确TODO并拒绝执行，留Task6；Worker接入及恢复留Task7—8，InnoDB RC/RR并发留Task17。证据见《集成测试.md》`MINISO-BRIDGE-AUTHORIZATION-LOCAL-20261021`；仅本地提交，不push、部署或改共享配置。

- [x] 1. 在现有生产事务代理测试中加入“CANCELLED更新不带receipt仍有inbox”“FAILED已收到核销”“授权字段写失败”的三个判别输入；断言分别无授权/不覆盖核销、无授权、终态及授权整体回滚。运行`engine_test RechargeFulfillmentVoucherStatePersistenceTest`，确认现有边界会失败。
- [x] 2. 扩展原履约端口，不新增通用退款接口。新增类型/方法如下；实现类中文注释说明仅MINISO使用，原非MINISO端口语义不变。

```java
public record MinisoBalanceReleaseAuthorization(
        String orderId, String orgCode, String outOrderSn,
        String sourceEventId, long amount, java.time.Instant authorizedAt) {}

// RechargeFulfillmentOrderPort新增：
Optional<MinisoBalanceReleaseAuthorization> findMinisoReleaseAuthorization(String orderId);
Optional<MinisoBalanceReleaseAuthorization> authorizeMinisoTerminalRelease(
        OrderFulfillmentStatusUpdate update, String sourceEventId);
void completeMinisoBalanceRelease(MinisoBalanceReleaseAuthorization authorization);
```

`findMinisoReleaseAuthorization`无授权返回empty，三字段部分为空必须抛不一致异常。`authorizeMinisoTerminalRelease`返回empty只表示可信订单原本未预扣，不制造退款；身份损坏、缺证据或核销冲突必须分类抛出，不与“无需退款”混同。新端MINISO所有CANCELLED/可退款FAILED写入由该事务入口承接；原`updateFulfillmentStatus`也必须识别已授权及终态门禁，不能让另一调用者绕过。扩展现有fixture的MINISO专用数据及mapper装配，不把原GENERIC订单改成MINISO导致非MINISO旧测试语义变化。
- [x] 3. 首次授权锁主单/扩展，核验DB_PERSISTED、预扣身份/金额、可信receipt或已核验未发券事实；锁guard后当前读两种证据，无核销才同时写终态及三个授权字段。金额从锁定原预扣事实核对，首次sourceEventId必须对应本订单现存履约outbox；不是HTTP请求自报金额。已有完整授权只核对并返回冻结值，不重写时间、eventId或金额；历史终态无授权不自动补认。
- [x] 4. 用源event-A/重试event-B、137分/错误139分的非对称输入断言冻结值仍为A/137；部分授权、商户/外部单号错配均失败。已有授权后迟到SUCCESS只能形成冲突，不能把主单恢复可派单。运行同一测试及`MinisoReceiptPersistenceTest`转绿。
- [x] 5. 本地提交`feat(miniso): persist terminal release authorization atomically`。

## Task 6: 新端RELEASE delta与释放标记原子收尾

**Files:** Modify `EI/persistence/MybatisRechargeFulfillmentOrderAdapter.java`、`mapper/RechargeBalanceDeltaOutboxMapper.java`、`config/TradeEngineMybatisPersistenceAutoConfiguration.java`；Test `ET/persistence/RechargeFulfillmentVoucherStatePersistenceTest.java`、`ET/config/TradeEngineMybatisPersistenceAutoConfigurationTest.java`。

- [x] 1. 在真实事务fixture以已有授权调用新增`completeMinisoBalanceRelease`：故意让ext更新失败，断言不存在孤立delta；反向让delta插入失败，断言balance_reserved仍1。预置错误金额同ID delta时也必须失败。运行`engine_test 'RechargeFulfillmentVoucherStatePersistenceTest,TradeEngineMybatisPersistenceAutoConfigurationTest'`确认红灯。
- [x] 2. 实现收尾主事务：锁主单/扩展，校验传入授权与持久字段及可信身份完全一致；按稳定`balance-flush-release-{orderId}`插入/锁读核对delta，最后置balance_reserved=0。不得调用绑定专用outbox DataSource的writer实现本事务；使用同主SqlSession mapper。重复delta不重置任务状态/次数/版本。

```sql
SELECT event_id,org_code,order_id,out_order_sn,delta_type,delta_amount
FROM recharge_balance_delta_outbox WHERE event_id=? FOR UPDATE;
UPDATE recharge_order_ext SET balance_reserved=0,updated_at=?
WHERE order_id=? AND org_code=? AND balance_release_event_id=?
  AND balance_release_amount=? AND balance_release_authorized_at IS NOT NULL;
```

- [x] 3. 只有Worker确认Redis RELEASED/ALREADY_RELEASED后才能调用收尾；重放已完成授权时仍验证delta存在且一致，不能凭balance_reserved=0跳过损坏事实。完成只表示delta可靠入库，不能等待或假装已经刷到旧余额。
- [x] 4. 同命令转绿，装配测试配置独立writer数据源为失败桩，授权收尾仍使用主连接成功；实际数据库中间状态以事务外连接核对，不只verify调用次数。
- [x] 5. 本地提交`fix(miniso): atomically persist release delta and completion`。

2026-10-21本地执行：两组基线57项通过；加入收尾反例及真实装配测试后83项中19项断言失败、6项因端口尚未实现报错，原有用例通过；实现后同命令83项全部通过。额外关联七组131项全通过，根reactor773项中753通过/20环境依赖跳过，失败及错误0。独立JDBC核验真实CHECK故障双向回滚、重试补齐、稳定ID事实冲突失败关闭及重复不改刷账进度。MINISO旧分步标记入口拒绝执行，非MINISO语义保留；普通及晚装配均传入主Mapper。第3项的Redis确认是收尾端口调用前置契约，Worker接入仍在Task7，本轮未调用Redis或认定其已释放；已完成标记但缺delta报不一致，不重建可能已刷账的损坏事实。详细证据见《集成测试.md》`MINISO-BRIDGE-RELEASE-COMPLETION-LOCAL-20261021`；真实MySQL/Redis竞争与断点仍待Task17，未push或部署。

## Task 7: Worker先恢复授权，无凭据不明结果不释放

**Files:** Modify `EA/RechargeOutboxWorkerService.java`及`E/cardshop-trade-engine-application/src/test/java/com/cardshop/trade/engine/application/recharge/RechargeOutboxWorkerServiceTest.java`；必要时在现有供应商响应分类中保留明确证据，不凭status=FAILED增加白名单。

- [x] 1. 扩展既有Worker公开批处理测试：DISPATCH/QUERY/INVALID三种任务遇已有授权＋迟到核销，均完成137分一次释放及delta；当前event-B必须使用冻结event-A。原任务不能因核销人工或通知创建失败被提前SUCCESS。运行：

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -B -f "$E/pom.xml" \
  -pl cardshop-trade-engine-application -am -Dtest=RechargeOutboxWorkerServiceTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

预期红灯：当前先核销门禁/当前eventId/分步收尾不符合合同。
- [x] 2. 在所有三种终态恢复分支先读取授权，存在则从冻结字段构造既有释放命令；先执行资金恢复，再完成其余可重试通知与原任务。无授权不能依据CANCELLED/FAILED直接释放，也不能被普通非派单状态分支标SUCCESS。新终态走任务5的事务授权入口；前后`blockInvalidation`不是授权依据。

```java
var command = new BalanceReservationReleaseCommand(
        authorization.orgCode(), authorization.orderId(), authorization.outOrderSn(),
        authorization.amount(), "RELEASE", authorization.sourceEventId(), reason);
var result = balanceReservationReleasePort.release(command);
if (result.succeeded()) {
    orderPort.completeMinisoBalanceRelease(authorization);
}
```

- [x] 3. 加入MINISO可能已发券但本地无receipt、查询null/空data/网络失败的测试：先用原orderId查单、未确认保持重试/人工，余额及delta不变；次数耗尽也不授权。只有任务1已核验的“未发券”证据才能走例外，若尚无该证据则不实现例外。非MINISO FAILED及原频控回归保持不变。
- [x] 4. 同命令转绿；对授权前、授权后未Redis、Redis响应丢失、delta失败、任务收尾失败注入故障并重跑，业务断言为余额只增加137、仅一条RELEASE且身份正确、授权不变。真实Redis验证留任务17，不用mock结果声称Lua通过。
- [x] 5. 本地提交`fix(miniso): recover authorized releases before redemption arbitration`。

2026-10-21本地执行：基线32项通过；首轮42项出现10项预期断言失败、错误0，接入后转绿。随后完成标记/持久凭据反例48项中5项、授权后快照反例49项中1项、作废补凭据反例51项中2项、非履约事件边界反例54项中3项均先红后绿。最终Worker54项及关联六组195项全部通过；根reactor795项中775通过、20环境依赖跳过，失败/错误0。三类履约任务先读冻结授权，恢复137分及同主事务收尾后才创建通知/关闭任务；资金已完成时仍核对delta，不重新依赖Redis。无授权历史终态转人工，不从FAILED猜未发券；派单缺证据转原订单号查单，作废缺凭据/归属未确认只在原INVALID任务内查单补证据并退避，不绕过门禁作废，也不重开已完成的稳定查单事件。查询确认发券不等于确认可退款，仍须后续作废及事务授权。补齐读授权后重载稳定终态，避免旧SUCCESS快照误发失败通知。所有金额/幂等断点结果均为应用端口合同模拟；真实MySQL/Redis组合验收仍待Task17，迟到核销接收/处理修改仍待Task8。证据见《集成测试.md》`MINISO-BRIDGE-WORKER-RECOVERY-LOCAL-20261021`；本地提交，不push、部署或改共享配置。

## Task 8: 新端直接回调及核销处理遵守授权优先级

**Files:** Modify `EI/persistence/MybatisMinisoWriteOffStore.java`、`MinisoWriteOffOrderLocks.java`、`mapper/MinisoWriteOffEventMapper.java`；Test `ET/persistence/MinisoWriteOffPersistenceTest.java`。

- [x] 1. 在真实事务fixture加入授权后直接accept/共享事件process，以及未知券首次接收后再发凭据场景；断言迟到事实保留、转MANUAL_REQUIRED，不改已授权终态/凭据、不建正常核销通知，不改资金授权或原履约任务。运行`engine_test MinisoWriteOffPersistenceTest`红灯。
- [x] 2. 已知订单接收从主单锁开始、未知仅guard→事件；候选查询后发现凭据改变仍不得锁序倒置。处理Job取得可信归属和guard后检查持久授权；冲突保存人工事实，不把该状态传播为资金任务人工。禁止重投重置已完成/人工事件进度。
- [x] 3. 共享及直接事件ID同算法，首次received_at保持首次接收时间。现有事件version=0保持不变；需要分离receivedAt与updatedAt时扩展mapper参数，不用投递时间覆盖年龄。
- [x] 4. 同命令及`engine_test RechargeFulfillmentVoucherStatePersistenceTest`转绿；原已核销/已作废/重复券冲突测试不删不改弱。
- [x] 5. 本地提交`fix(miniso): retain late redemption as manual evidence`。

2026-10-21本地执行：基线两组101项通过；新增授权优先级反例后核销37项中4项预期断言失败，再增加锁后候选变化反例，38项中5项预期断言失败，错误均0。实现后核销38＋履约事务72共110项通过，最终关联六组188项通过；根reactor804项中784通过、20环境依赖跳过，失败/错误0。已知归属锁主单→扩展→凭据→guard后当前读授权，任一授权字段存在即禁止自动核销；新接收直接插入MANUAL_REQUIRED/version0，已有待处理事件按T3版本CAS转人工，不改资金许可、终态、凭据或原履约任务。重投保留INIT/RETRY及PROCESSED/MANUAL_REQUIRED进度和首次时间，旧共享事件received_at早于投递一小时仍保持；当前直接入口没有异步转投，未无需求扩展receivedAt参数，旧桥接独立写入留Task14。H2两连接受控竞争确认候选C1在等主单时变为C2后，不绑定旧订单、不锁C2 guard；真实MySQL RC/RR证明仍归Task17。共享Job尚未实现，本轮按表合同构造输入，不冒称已跑旧端。完整记录见《集成测试.md》`MINISO-BRIDGE-LATE-REDEMPTION-LOCAL-20261021`；仅本地提交，不push、部署或写共享环境。

## Task 9: 旧端独立配置、主连接与严格验签

**Files:** Create `LB/MinisoCallbackBridgeProperties.java`、`MinisoBridgeConfiguration.java`、`MinisoCallbackAuthenticator.java`、`MinisoCallbackFact.java`；Create `LT/MinisoCallbackAuthenticatorTest.java`、`MinisoBridgeConfigurationTest.java`；Read旧`MCUtils`，不借此修改主动发券密钥。

- [x] 1. 定义Java8公开合同：`new MinisoCallbackAuthenticator(String key2)`，方法`MinisoCallbackFact authenticate(byte[] utf8Body, Map<String,List<String>> headers)`；事实是不可变普通Java8类，提供`getCouponCode()/getUseTime()/getEventId()`，useTime为LocalDateTime。先加可编译但抛UnsupportedOperationException的最小骨架，再写下面行为测试；编译缺类型不算行为红灯。运行`legacy_test 'MinisoCallbackAuthenticatorTest,MinisoBridgeConfigurationTest'`。

```java
@Test
void acceptsFixedLegacyVectorAndKeepsStableEventIdentity() {
    Map<String, List<String>> headers = new HashMap<String, List<String>>();
    headers.put("nonce", Collections.singletonList("0123456789abcdef0123456789abcdef"));
    headers.put("time", Collections.singletonList("1792200600000"));
    headers.put("sign", Collections.singletonList("62088F92CF0CB4452F9A95098E60E350"));
    byte[] body = ("{\"couponCode\":\"TEST-MINISO-0001\","
            + "\"useTime\":\"2026-10-17 09:29:00\"}").getBytes(StandardCharsets.UTF_8);
    MinisoCallbackFact fact = new MinisoCallbackAuthenticator("miniso-callback-local-test-key")
            .authenticate(body, headers);
    assertEquals("TEST-MINISO-0001", fact.getCouponCode());
    assertEquals(LocalDateTime.of(2026, 10, 17, 9, 29), fact.getUseTime());
    assertEquals("miniso-redeemed-ff08b00be72babc1a123564e3e203072731fd9811650618cec05951824fc3465",
            fact.getEventId());
}
```

- [x] 2. 使用现有Jackson严格流读取完成重复键/尾随token检查，再由Fastjson2序列化TreeMap签名；不能先绑定JSONObject丢掉重复键。UTF-8用REPORT拒绝坏字节；仅两字符串、不trim券码、严格日期和Unicode、4096字节、唯一头、大小写规范，tenant/version如给须匹配，常量时间比较MD5。独立key2不回退旧配置；不调用会打印签名原文的`MCUtils.getSign2`。

```java
// Java8；MessageDigest/UTF_8从JDK获取，摘要固定64位小写hex，含前导零。
String source = "MINISO:REDEEMED:" + couponCode;
byte[] digest = MessageDigest.getInstance("SHA-256")
        .digest(source.getBytes(StandardCharsets.UTF_8));
StringBuilder hex = new StringBuilder(64);
for (byte b : digest) hex.append(String.format(Locale.ROOT, "%02x", b & 0xff));
String eventId = "miniso-redeemed-" + hex;
```

- [x] 3. 绑定`@Qualifier("masterDataSource")`创建桥接JdbcTemplate和同DataSource的TransactionTemplate；不修改全局DruidConfig。保护相关SQL必须同连接；旧Mapper若仍绑定dynamicDataSource不能混入该模板宣称原子，需要在本服务用主JdbcTemplate完成受影响SQL或验证同一连接绑定后复用。
- [x] 4. 配置实现规格10所有默认值及非法组合校验，含LEGACY模式下routing/notify不得开启，BRIDGE接收暂停为503；保护bean不以receive/routing开关为条件。缺表/列结构不兼容在升级保护装配时失败关闭。加入4096/4097、重复头/键、数字couponCode、错签名/错tenant、错误UTF-8及密钥toString脱敏断言，同命令转绿。
- [x] 5. 本地提交`feat(miniso): add isolated legacy bridge configuration and authentication`。旧端本地分支中提交，不修改生产配置。

2026-10-21本地执行：旧端既有三组38项基线通过；可编译骨架下60项出现42项断言失败及7项明确缺实现的UnsupportedOperationException，随后同组60项转绿。另加装配反例复现新增桥接模板导致Boot不再创建默认transactionTemplate（22项中1项断言失败）；显式保留全局TM的Primary默认模板，桥接模板只经限定名称使用，不改DruidConfig或旧Mapper。最终旧端12组144项全部通过（验签39、装配23、既有订单/滴滴/电影等82），新端验签/核销两组42项通过，失败/错误/跳过均0。生产只读探针已加入旧端同schema消费测试；真实MySQL8.0.28首轮暴露Connector/J8.0.33对DATETIME(3)返回getScale=0，改为同连接读取information_schema.DATETIME_PRECISION后，RC/RR各跑新端迁移1项＋旧端消费1项全部通过，本次容器均已删除。H2额外验证毫秒精度降级、缺表/列、错类型/宽度和缺唯一键在全部开关关闭时仍阻止启动。HTTP暂停503保留为Task13明确TODO，本轮仅验证BRIDGE接收关闭时Job配置仍可开启；旧资金写入保护按Task10—12接入，不能将基础设施bean已装配宣称为旧保护已完成。完整命令和证据见《集成测试.md》`MINISO-BRIDGE-LEGACY-AUTH-CONFIG-LOCAL-20261021`；仅两仓库本地提交，不push、部署或写共享DDL/Nacos，Task10未开始。

## Task 10: 旧端凭据首次发布及相关事实写入统一锁序

**Files:** Create `LB/MinisoLegacyOrderTransactions.java`；Modify `LM/api/service/impl/MINISOSupplierTradeServiceImpl.java`、`LM/service/impl/McOrderServiceImpl.java`中`extProductRechargeMc`及任务1发现的MINISO首次/状态写入路径；Create `LT/MinisoLegacyOrderTransactionsTest.java`，Extend `LT/MinisoBridgeMysqlContractTest.java`。 实际调用链同时修改`LM/api/ExtCommonApi.java`、`LM/service/impl/McVoucherListOutServiceImpl.java`、`LM/service/impl/McOrderCustomerReissueServiceImpl.java`及其单测、旧`McOrderMapper.xml`的奈雪渠道条件；移除`MCUtils`及`HttpUtils`本次链路的敏感日志。新端仅扩可丢弃MySQL旧表投影，不改发布迁移。

2026-10-21范围确认：用户明确386人工库存券不使用本次MINISO核销入口，`ownCartOrder`及对应人工库存写入保持原行为。两条MINISO远端发券入口请求前短事务记处理中13并冻结商品/渠道，重入只查原订单号；可能已发券或保存失败先查单补齐全部凭据，仍不明转存疑18，不恢复库存、不自动退款、不继续候选渠道。完整凭据、主单成功和真实库存扣减同主连接事务提交，查询幂等不重复扣库存；历史成功单凭据不全时不猜测库存断点。该实施细化已同步主详细设计9.8及高可用设计同名章节。原Amp会话中断时，两条真实入口的第二券插入失败测试均为预期断言失败；此记录不代表实现或后续并发验收已完成。

归属探测按两主设计的Task10细化：本服务复制专属主库模板并设置RC，不改Task9及旧业务默认模板；主单→扩展→本单全凭据→全guard后，跨单旧/新凭据归属使用RC已提交读复查，不持guard反向锁其他订单凭据。事件/inbox/历史核销证据仍当前锁定读。等待guard期间的新发布归属必须被发现，不能只在guard之前探测一次或依赖缺行gap锁。删除本次实际调用链既有签名原文/签名及完整供应商响应日志，不改变主动签名协议或密钥来源。

- [x] 1. 针对两个实际发券入口分别模拟远端成功返回两个券、第二券insert失败：断言主单成功状态和全部新凭据一起回滚，不能吞异常后返回发券成功。同原订单号查单补证据必须保存所有券，不能再次发券。运行`legacy_test MinisoLegacyOrderTransactionsTest`确认红灯。
- [x] 2. 远端调用保持锁外；从可信响应构造完整券集合后进入`MinisoLegacyOrderTransactions`主事务，锁主单、复核无新归属、锁已有凭据，按券码固定顺序建/锁全部guard，再提交本次凭据/主单事实。库存业务规则不重做，但原有相关数据库写入不能在凭据失败后独立成功。多券先锁完全部凭据再锁guard，不逐券交替两种锁。
- [x] 3. 删除本次MINISO凭据写入处的吞错成功路径；已可能发券且保存失败/结果未知，回原orderNo查询，仍不明转人工，不改成可自动退款的“明确失败”。核销/作废/查询补状态所有可达写入同样遵守guard协议；遇其他供应商路径保持原行为。
- [x] 4. 同测试转绿，任务17以真实MySQL＋已认证未知inbox的相反顺序竞争证明发布不会漏保护。若发现无法证明完整券/金额对应的旧历史单，保守人工，不按首券放行整单。
- [x] 5. 本地提交`fix(miniso): publish legacy receipts under coupon guards`。

2026-10-21本地执行完成：沿用Amp中断草稿恢复两个实际发券入口红灯（2断言失败/0错误），逐步修复并补全主事务49项、一键补发13项；最终旧端15组208项全部通过，失败/错误/跳过0。MySQL8.0.28真实生产publisher首轮RC暴露Connector/J通用DATETIME返回LocalDateTime，显式getTimestamp后RC/RR各跑新端迁移1项＋旧端探针/生产publisher2项全部通过，三次运行容器均清理。原单重入、查询不二扣库存、第二券失败连同主单/流水回滚、历史损坏转18、guard后跨单归属、旧retry最终CAS、限频及一键补发等待快照、实际回调/同步/作废旁路和日志脱敏均纳入测试。386人工库存按stock主单精确同步，不依赖批次默认值且不建guard；ownCartOrder不改。独立规格及代码质量审查均通过；旧端本地提交`3d66d0bb`，新端仅同步两主设计/计划/集成测试记录及可丢弃schema旧表投影。完整证据见《集成测试.md》`MINISO-BRIDGE-LEGACY-PUBLISH-LOCAL-20261021`。H2两连接仅证明本地可见性，跨JVM未知inbox相反顺序竞争及Redis断点仍归Task17；Task11未开始，不push/部署/共享DDL或Nacos，不可单独启用BRIDGE。

## Task 11: 所有旧退款入口排除新引擎所有权

**Files:** Modify `LM/service/impl/McOrderServiceImpl.java`、`McOrderV2ServiceImpl.java`、`LM/mapper/McOrderMapper.java`、`L/cardShop-admin/src/main/resources/mapper.system/McOrderMapper.xml`、`LB/MinisoLegacyOrderTransactions.java`；Extend旧`McOrderServiceImplTest.java`、`McOrderV2ServiceImplTest.java`及`LT/MinisoBridgeMysqlContractTest.java`。

实际清单补充：旧`web/movie/service/MovieLxOrderService.java`及`MovieLxOrderRefundTest`、`MovieFailedRefundCompatibilityTest`；新增`LT/LegacyRefundFixture.java`和`LegacyRefundSqlTest.java`。新端扩充`ET/persistence/MinisoBridgeMysqlContractTest.java`及`src/test/resources/miniso-bridge/legacy-baseline.sql`，仅用于真实迁移后的资金/首发事务验证。

- [x] 1. 从任务1清单逐个覆盖定时、单笔、V2、内部可调用入口；新单FAILED、非MINISO新单、损坏persist_status、有ext但balance_reserved=0均应不改余额/流水/退款状态。运行`legacy_test 'McOrderServiceImplTest,McOrderV2ServiceImplTest'`，确认旧行为红灯。
- [x] 2. 候选查询排除新扩展，不能仅在MINISO分支排除；最终任何资金/退款状态写入前仍在主单锁内复核扩展存在。SQL使用参数绑定，排除无额外状态条件：

```sql
SELECT o.order_Id FROM mc_order o
WHERE o.order_status=?
  AND NOT EXISTS (SELECT 1 FROM recharge_order_ext e WHERE e.order_id=o.order_Id);
SELECT order_Id FROM mc_order WHERE order_Id=? FOR UPDATE;
SELECT order_id FROM recharge_order_ext WHERE order_id=? FOR UPDATE;
```

- [x] 3. 外层批量不持整批事务；逐单最终事务拒绝新归属，无支付记录的状态更新也不能绕过。独立采购总单/充值/转账不泛化改造；对于`zfbOrderRefund`等按共享mc_order退款的可达分支同样加归属最终门禁，但不改其金额/部分退款协议。
- [x] 4. 加入候选读取后ext归属发布的真实事务竞争：最终写前检查必须阻止旧回补；并验证新引擎主单/扩展首次发布同事务。不能用只verify候选filter代替最终状态断言。两组单测转绿，MySQL用例纳入任务17。
- [x] 5. 本地提交`fix(refund): exclude engine-owned orders from legacy writers`。

2026-10-21本地执行：旧端两组基线36项通过；新增实际入口资金断言后46项中10项预期失败，随后转绿。候选真实XML过滤和最终主连接主单/扩展锁覆盖通用、V2、内部单笔、支付宝及电影售后；外层无整批事务，逐单REQUIRES_NEW原子回补。数字状态及流水列兼容测试另出现2项预期失败，文本语义归一后通过。最终11组201项，失败/错误/跳过均0。真实MySQL8.0.28在RC/RR下分别跑新端1项＋旧端3项：候选后归属发布及主单锁等待后，最终余额100/流水1/状态6不变；新端实际落库事务代理与真实Mapper验证主单/扩展同连接提交、半单外部不可见、异常整体回滚。旧端本地提交`f390a59e`；新端仅追加测试夹具、原子性验证及记录。证据见《集成测试.md》`MINISO-BRIDGE-LEGACY-REFUND-LOCAL-20261021`。Task12及Task17整体验收仍未完成；不push、部署或写共享DDL/Nacos。

## Task 12: 旧MINISO作废及退款按单原子保护

**Files:** Modify `LB/MinisoLegacyOrderTransactions.java`、`LM/api/service/impl/MINISOSupplierTradeServiceImpl.java`、`LM/service/impl/McOrderV2ServiceImpl.java`、`McOrderServiceImpl.java`及相关mapper；Extend `LT/MinisoLegacyOrderTransactionsTest.java`、`LT/MinisoBridgeMysqlContractTest.java`。

实际清单补充：`LM/api/ExtCommonApi.java`、`LM/api/utils/MCUtils.java`、`LB/MinisoCallbackAuthenticator.java`，以及旁路`LM/service/impl/McVoucherListOutServiceImpl.java`、`McOrderCustomerReissueServiceImpl.java`。Task11的`McOrderServiceImpl`已统一委派主库退款服务，本任务不为重复接线改大类；生产受影响SQL均在同一JdbcTemplate内，不混用动态Mapper。新端仅更新两主设计、计划及集成记录。

- [x] 1. 写旧端行为红灯：两券仅第二券已核销、共享inbox已接收但未路由、远端作废成功但本券不在succeed、批次更新0行、退款流水insert异常。期望整单不回补；最后一项余额/流水/退款状态全回滚。运行`legacy_test MinisoLegacyOrderTransactionsTest`。
- [x] 2. MINISO adapter只检查本次请求券确在远端succeed并返回结果，移除adapter直接批次UPDATE。作废前短事务复核→锁外远端→最终短事务再复核，固定先锁完整凭据再全部guard再事件/inbox。最终条件更新同时限制order/channel/state_type/原未使用状态；主单按旧FAILED语义更新，不套新CANCELLED。

```sql
UPDATE mc_batch_record SET state='3',invalid_time=?
WHERE order_no=? AND coupon_code=? AND channel='MINISO'
  AND state_type='3' AND state='1';
```

- [x] 3. 旧退款逐单主事务内重新锁主单、归属、全部凭据/guard及核销证据，验证完整金额对应，再同时写mc_ext_depart余额、mc_depart_record退款流水及主单退款状态。查不到凭据/任一已核销/证据不一致均不放行。去掉受影响逐单事务的`noRollbackFor`和吞流水异常，外层循环只在事务已回滚后记录脱敏采样错误并继续下一单；不能在catch里再提交半套退款状态。
- [x] 4. 同测试转绿；加入先退款完成再来核销转人工及其他旧供应商金额语义回归。SQL异常必须保留安全栈，不把含券码的数据库原始message直接写last_error。
- [x] 5. 本地提交`fix(miniso): atomically guard legacy invalidation and refunds`。

2026-10-22本地执行：基线4组102项通过。首次新增作废/退款场景59项中9项预期失败；随后主单最终写0行、HTTP期间金额变化各1项红灯，出库/专用补发旁路2项红灯，补发旧券被误当原单可退凭据1项红灯，均修复转绿。最终12组241项通过；加强外层补发事务真实回滚断言后，凭据事务76项另行通过，均无失败/错误/跳过。真实MySQL8.0.28在RC/RR下各新端1项＋旧端4项通过：NOWAIT证明模拟HTTP期间主单及guard可由第二连接取得，两券作废条件写失败整体回滚，真实退款流水SQL错误回滚余额/状态，第二券guard竞争后当前读inbox阻止退款，已退款后迟到核销保存MANUAL_REQUIRED并保留首次时间，补发标记阻止旧券再次用于退款。首轮真实MySQL发现无时区useTime经Timestamp绑定产生8小时偏移，改严格日期文本写入后两轮通过。V2/V3、直接券码、出库直充及专用补发均复用完整协议；人工库存及其他供应商保留金额模型。旧端本地提交`22c5dc50`；详见《集成测试.md》`MINISO-BRIDGE-LEGACY-VOID-REFUND-LOCAL-20261022`。Task13及Task17完整验收未完成，不单独启用BRIDGE；未push、部署或改共享配置。

## Task 13: 旧唯一HTTP入口持久接收后确认

**Files:** Create `LB/MinisoCallbackBridgeStore.java`、`LB/MinisoBridgeLease.java`；Modify `LM/api/ExtCommonApi.java`；Create `LT/MinisoCallbackBridgeReceiveTest.java`。

实际清单补充：`LB/MinisoBridgeConfiguration.java`、`LT/MinisoBridgeConfigurationTest.java`（共用完整DDL夹具）及`LT/MinisoBridgeMysqlContractTest.java`；旧`ruoyi-common`新增`filter/MinisoRawCallbackRequest.java`，在`RepeatableFilter`及`XssFilter`仅对该POST映射跳过正文预处理，避免全量预读/改写先于控制器。新端仅更新两主设计、计划及集成记录。

- [x] 1. 用MockMvc及真实事务store测试提交前阻塞、提交失败、提交后回执丢失重投、同券异时冲突、未知归属。合法首次请求只有提交后得HTTP200/code数值200；提交未知返回503，重投不重置状态/首次时间/版本。运行`legacy_test MinisoCallbackBridgeReceiveTest`红灯。
- [x] 2. 保留`POST /api/common/mcNotify`，从ServletInputStream有界读4097字节以判超限，不先`@RequestBody JSONObject`；Content-Type不符415、超限413、业务参数/验签冲突HTTP200 code500、存储/资源不可用503。BRIDGE暂停503，不能回退旧同步；LEGACY仅首次兼容验证使用。删除此入口输出完整正文/请求头/签名的日志。
- [x] 3. 验签通过后才建guard/inbox。已知归属从主单开始复查，未知只guard→新事件→inbox；不能先锁inbox再等主单。幂等保留首次事实，MANUAL_REQUIRED相同事实仍确认接收，不自动恢复。最小落库不含nonce/time/sign/raw/key2。

```sql
INSERT INTO mc_miniso_callback_inbox
 (event_id,coupon_code,use_time,status,retry_count,next_retry_time,version,received_at,updated_at)
VALUES(?,?,?,'INIT',0,?,1,?,?)
ON DUPLICATE KEY UPDATE event_id=event_id;
SELECT event_id,coupon_code,use_time,status,version,received_at
FROM mc_miniso_callback_inbox WHERE coupon_code=? FOR UPDATE;
```

- [x] 4. insert幂等之后必须读回比对eventId/券码/useTime，不把重复主键当相同事实。提交结果不明也不补发成功；后续重投由唯一事实收敛。相同HTTP测试及严格验签测试转绿。
- [x] 5. 本地提交`feat(miniso): durably accept bridge callbacks before acknowledgement`。

2026-10-22本地执行：基线3组138项通过。可编译接收骨架下25项中23项预期断言失败；扩展后LEGACY坏JSON的3项返回码反例修复，真实过滤器链再出现5项预期红灯（超限被trim后接受、坏UTF-8被替换后接受、控制器前全量读取），仅对该POST映射及Spring等价路径绕过正文包装后转绿。最终13组287项全部通过，另加强UTF-8响应msg及提交故障注入断言后46项接收测试通过，均失败/错误/跳过0。真实MySQL8.0.28 RC/RR各新端1项＋旧端5项通过，验证提交前不可见、提交结果未知503后重投保持首次时间/人工进度、上海DATETIME、已知先等主单、未知等guard后不反向定位新发布主单；过滤器链参与真实HTTP调用。最小接收不写可信归属或投递事件，不提前执行Task14。旧端本地提交`6abf92e0`，日志及配置见《集成测试.md》`MINISO-BRIDGE-DURABLE-RECEIVE-LOCAL-20261022`。新端仅设计/计划/执行记录；未push、部署或改共享配置，Task14及Task17完整验收未完成，不单独启用BRIDGE。

## Task 14: 租约路由与同连接新事件投递

**Files:** Create `LB/MinisoCallbackRouteService.java`、`LT/MinisoCallbackRouteServiceTest.java`；Extend `LB/MinisoCallbackBridgeStore.java`、`LT/MinisoBridgeMysqlContractTest.java`。

实际清单补充：`LT/MinisoBridgeConfigurationTest.java`验证主连接store/service真实装配及默认不开扫描；新端同步两主设计、本计划与`docs/集成测试.md`，不改新端生产代码或共享DDL。

- [x] 1. 写真实事务测试：新单不建旧批次；新事件insert后最终inbox CAS失效必须二者全回滚；重复投递保留PROCESSED进度；未知后凭据补齐；双归属/多条/批次或手机号不符转人工。运行`legacy_test MinisoCallbackRouteServiceTest`红灯。
- [x] 2. 短事务领取INIT/RETRY或过期PROCESSING，提交owner/version/60秒租约后结束；业务事务重新按锁序复核归属、事件，最后CAS。路由枚举仅NEW_ENGINE/LEGACY；已固定归属遇矛盾转人工，不改投。数据库错/锁错不推断为旧单。

```sql
UPDATE mc_miniso_callback_inbox
SET status='TRANSFERRED_NEW',route_target='NEW_ENGINE',order_id=?,org_code=?,
    routed_at=?,updated_at=?,lock_owner=NULL,locked_until=NULL,version=version+1
WHERE event_id=? AND status='PROCESSING' AND lock_owner=? AND version=?
  AND locked_until>?;
```

CAS更新行数不是1时抛异常回滚整业务事务，包括已插新事件；失败重试回写也需owner/version，不让迟到Worker改新代次。
- [x] 3. 按0021列合同写新事件INIT/retry0/version0，received_at=inbox首次时间，next_retry_time/updated_at=本次时间；现存相同事实不改进度。只有INIT/RETRY且非空归属一致才CAS补空order/org；PROCESSED要求完整匹配，MANUAL_REQUIRED不恢复。使用同主JdbcTemplate/事务，不调用HTTP或独立outbox writer。
- [x] 4. 测试orderId长度80、源event version0/inbox version1、首次received_at比投递早一小时、空归属RETRY且已有retry_count=7，断言不截断/重置。60秒退避、最多100次失败、耗尽人工；单批100、owner/version及DB异常回滚均转绿。
- [x] 5. 本地提交`feat(miniso): route shared inbox to engine events atomically`。

2026-10-22本地执行：145项基线通过；可编译路由空骨架下28项中19项预期断言失败，8项因缺少投递事件的后续读回报错，不将其当独立行为红灯。首轮28项及扩展37项转绿；另加旧表大小写命中、零批次及空商户订单号反例，40项中3项预期断言失败后修正，最终路由41项全通过。最终14组329项全部通过，失败/错误/跳过0。真实MySQL8.0.28 RC/RR每轮新端1项、旧端6项全通过，确认80位订单号、首次接收年龄、事件version0/inbox version1、RETRY计数7/version9补归属且不改ON UPDATE时间、PROCESSED全字段幂等、最终CAS/真实SQL/插入后租约到期整事务回滚、主单等待期间新Worker独立接管inbox。一次收尾复跑误改既有Task11同步屏障导致超时，恢复观察点后完整RC/RR转绿，未改该生产退款路径。旧端本地提交`5bc67e40`；日志/随机schema见《集成测试.md》`MINISO-BRIDGE-LEASE-ROUTE-LOCAL-20261022`。可信旧归属目前仅固定LEGACY并RETRY，TODO留Task15，不写旧核销/通知或报告完成；Task18调度及Task17整套跨JVM/Redis验收未交付，不单独启用BRIDGE。两仓库仅本地提交，未push/部署/改共享配置。

## Task 15: 旧核销事实与冻结通知任务原子提交

**Files:** Modify `LB/MinisoCallbackRouteService.java`；Create `LB/MinisoLegacyNotificationService.java`、`LT/MinisoLegacyNotificationServiceTest.java`；Extend `LT/MinisoCallbackRouteServiceTest.java`。

实际清单补充：复用Task14既有事务所有者`LB/MinisoCallbackBridgeStore.java`应用旧分支，扩展`LT/MinisoBridgeMysqlContractTest.java`；新端仅扩展可丢弃MySQL夹具`E/cardshop-trade-engine-infrastructure/src/test/resources/miniso-bridge/legacy-baseline.sql`的旧商户AES/通知配置投影，以及两主设计、计划和集成记录，不改共享迁移或新端生产代码。

- [x] 1. 写旧分支红灯：唯一可信旧券核销→一条history/状态2/一条冻结task/LEGACY_APPLIED；任务insert失败则三种副作用全部回滚；已作废/已退款/异时历史核销只转人工。运行`legacy_test 'MinisoCallbackRouteServiceTest,MinisoLegacyNotificationServiceTest'`。
- [x] 2. 锁主单/无ext复核/匹配MINISO state_type3全部凭据/目标guard/新事件/inbox，在当前锁定读下幂等保存history及目标批次条件更新。别的券不变，主单充值状态和余额不变。历史已核销且同时间只登记接收幂等事实，不创建新的历史补发task；同券多条矛盾历史人工，不取MAX。
- [x] 3. 新事实需通知时从可信order/本券/useTime构造旧TreeMap字段，保留数字timestamp、原8位nonce格式、state_type/use_state原类型及AESUtils.Encrypt流程；只从mc_config.writeOffNotifyUrl取URL。冻结URL/完整正文一次，移除aesKey后落库；无配置只提交事实，不回退新notify_url。

```java
Map<String, Object> signed = new TreeMap<String, Object>();
signed.putAll(fieldsFromLockedFacts);
signed.put("aesKey", aesKey);
String signature = AESUtils.Encrypt(signed, aesKey);
signed.put("sign", signature);
signed.remove("aesKey");
String frozenPayload = JSONObject.toJSONString(signed);
```

`fieldsFromLockedFacts`是本方法内由已锁主单/目标券构造的局部Map，字段明确为order_id/out_order_sn/account/state_type/use_state/use_time/timestamp/nonce_str；不复用`getOrderInvalid`的首券/MAX JOIN。固定本地aes测试向量独立验密，不在日志输出正文。
- [x] 4. taskId=`miniso-legacy-notify-`加同券64位摘要，event_id唯一，初值INIT/version1/retry0/max10；任务/事实/inbox最终CAS同主事务提交。双券不同useTime非对称输入验证只冻结目标券，重投仍同一payload/URL/sign。上述测试转绿。
- [x] 5. 本地提交`feat(miniso): apply legacy redemption with frozen notification task`。

2026-10-22本地执行：141项基线通过；新增旧分支红灯58项中16项预期断言失败，1项因未生成任务的后续读取错误，不把它当独立行为红灯；首轮58项转绿，补充固定OpenSSL向量/独立JCE验密、双券不同useTime、异渠道历史、新事件冲突、重复task键及提交结果未知后重投后64项通过。最终15组352项全通过，失败/错误/跳过0。真实MySQL8.0.28 RC/RR每轮新端1项、旧端7项均通过；Task15逐连接显式隔离级别，验证非首券history/状态2/冻结task/LEGACY_APPLIED、task真实SQL/批次CAS/最终CAS/插入后租约到期整事务回滚、重投冻结与历史不补发、余额137及主单不变。旧端本地提交`143b72ea`；完整配置/命令/随机schema/日志见《集成测试.md》`MINISO-BRIDGE-LEGACY-APPLY-LOCAL-20261022`。Task15只创建通知任务，HTTP仍TODO留Task16；Task17整套跨JVM/Redis及Task18调度未验收。两仓库仅本地提交，未push、部署或改共享配置，不单独启用BRIDGE。

## Task 16: 旧通知锁外HTTP与租约防迟到回写

**Files:** Modify `LB/MinisoLegacyNotificationService.java`、`MinisoCallbackBridgeStore.java`、`LT/MinisoLegacyNotificationServiceTest.java`。

实际清单补充：扩展`LT/MinisoBridgeMysqlContractTest.java`验证真实InnoDB下锁外HTTP、9/10次失败边界、租约接管和冻结UTF-8正文恢复；新端仅同步两主设计、本计划及`docs/集成测试.md`，不改共享DDL或新端生产代码。

- [x] 1. 离线HTTP服务器返回200且正文`{"code":500}`，预期仍DELIVERED_HTTP；500/超时则FAILED退避，不改冻结正文。过期Worker返回200时不得覆盖新Worker的人工/成功结果。运行`legacy_test MinisoLegacyNotificationServiceTest`红灯。
- [x] 2. 短事务claim INIT/FAILED及过期SENDING，HTTP在事务外，3秒超时、60秒租约；只用冻结URL/payload，不读取当前配置重新签名。默认禁止重定向泄露冻结正文；HTTP2xx仅标网络层投递，非2xx/网络失败30秒退避、最多10次失败后人工。对实际发送次数不承诺物理恰好一次。
- [x] 3. 所有完成/失败结果写入owner/version CAS；过期租约后失败CAS无权修改新代次。对网络响应体不记录、不按业务code猜合同；任务HTTP调用不得持有任何订单/guard/任务行事务锁。

```sql
UPDATE mc_miniso_legacy_notify_task SET status=?,retry_count=?,next_retry_time=?,
    last_error=?,lock_owner=NULL,locked_until=NULL,updated_at=?,version=version+1
WHERE task_id=? AND status='SENDING' AND lock_owner=? AND version=?
  AND locked_until>?;
```

- [x] 4. 同测试转绿，加受控时钟测试第9/10次失败边界、不同URL配置变更不重签、宕机恢复原正文；不扫描旧历史订单补task。
- [x] 5. 本地提交`feat(miniso): deliver legacy notifications with lease fencing`（旧端`c0bcded7`）。

2026-10-22本地执行：88项基线通过。可编译发送空骨架下46项中21项预期断言失败；实现后修正测试夹具字符集大小写/204响应，再修复LONGTEXT映射Clob误发描述文本及断开响应后close误判网络失败，核心46项全通过。扩展57项中2项干净红灯证明任务行等待会缩短领取租约、完成CAS使用等待前时间会放行过期结果；取得任务锁后重新取时后关联122项转绿。最终15组387项全部通过，失败/错误/跳过0，其中通知58项。真实MySQL8.0.28 RC/RR每轮新端1项、旧端8项全通过，确认HTTP200/business500仅网络投递、HTTP500第9/10次失败与30秒退避、HTTP期间第二连接NOWAIT取得主单/guard/任务锁、过期接管拦截迟到200、宕机恢复原UTF-8正文。每个新增MySQL连接显式设置隔离级别；仅消费专用任务，不补扫历史订单。逐笔领取避免串行批量消耗租约；结果写库失败不额外计网络失败。完整配置/命令/随机schema/日志见《集成测试.md》`MINISO-BRIDGE-LEGACY-NOTIFY-LOCAL-20261022`。旧端本地提交`c0bcded7`。本任务不交付Task17整套跨JVM/Redis验收或Task18 Job装配；仅本地提交，不push、部署或修改共享配置，不单独启用BRIDGE。

## Task 17: 真实MySQL双隔离级别、跨JVM及Redis断点验收

**Files:** Extend `E/scripts/test-miniso-shared-callback.sh`、`ET/persistence/MinisoBridgeMysqlContractTest.java`、`LT/MinisoBridgeMysqlContractTest.java`、`ET/redis/MinisoLocalRedisContractTest.java`；新增测试专用`ET/persistence/MinisoMysqlArbitrationFixture.java`、`MinisoMysqlReleaseRecovery.java`并补充离线`legacy-baseline.sql`的id投影；Append `E/docs/集成测试.md`。

- [x] 1. 脚本增加`--all --legacy-repo "$L"`，校验JDK8/21和实际执行测试数；共享本次Docker schema只给两个测试JVM，不启动Nacos/MQ/XXL/真实供应商。旧测试调用真实桥接store/路由/事务服务，新测试调用真实新端adapter/Worker。两JVM屏障只在测试schema内，使用独立控制连接和有界等待；生产事务不写控制屏障表。每次连接显式设置RC或RR，不只改全局变量。
- [x] 2. 构造确定性竞争，不用sleep猜时间：未知接收候选无凭据后暂停→另事务发布凭据→分别放行接收或首次授权。测试MyBatis/JDBC拦截器可暂停准确语句，但不增加生产暂停钩子。接收先提交：无授权/无Redis释放；授权先提交：授权不变/核销人工/恢复完成。RC和RR均执行，保留屏障顺序、事务提交及最终SQL结果。
- [x] 3. 覆盖旧端整单多券仅非首券核销、候选后新归属出现、批次CAS失败、退款流水失败；以及旧路由租约过期最终CAS失败，分别断言无资金越权、无半套退款、无孤立新事件/旧事实/task。加入不同券并行控制，不能把全局串行误当同券正确。
- [x] 4. 扩展现有真实redis-server夹具执行生产release Lua，金额137分；在授权提交前回滚、提交后未Redis、Redis已生效响应丢失、delta写前、delta/标记事务失败、原任务SUCCESS前分别中断。每个断点恢复时插入迟到核销，核对余额只增加137、RELEASE唯一且正确、balance_reserved与delta一致、sourceEventId不变。原Lua身份错配、金额139及重复release是负控，不只断言ALREADY_RELEASED文本。

```bash
MINISO_MYSQL_IMAGE="$MINISO_MYSQL_IMAGE" \
  bash "$E/scripts/test-miniso-shared-callback.sh" --all --legacy-repo "$L"
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -B -f "$E/pom.xml" \
  -pl cardshop-trade-engine-infrastructure -am -Dtest=MinisoLocalRedisContractTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Dminiso.localRedisTests=true test
```

- [x] 5. 预期：RC/RR和两个实际JVM边界全部通过，Redis及最终数据库值满足上述独立期望。无Docker/redis-server、真实版本不符或任一强制用例跳过就是该门禁未通过，不能降级为H2后声明完成。
- [x] 6. 将执行日期、profile、本地容器版本/schema、禁用远端Nacos说明、连接隔离级别、测试数据范围、命令、Surefire实际数量、屏障及最终余额/delta证据追加《集成测试.md》；仅记录真实结果。删除临时容器、可丢弃schema及scratch；有长期回归价值的合同测试保留。本地提交`test(miniso): prove shared callback arbitration and release recovery`。

2026-10-22本地执行：`--all --legacy-repo`强制RC/RR，每级新端结构1、旧端合同9、跨JVM两端各1、组合恢复1、独立Redis5，共36次执行全部通过，无失败/错误/跳过。跨JVM准确暂停未知候选查询后，分别证明接收先提交阻断首次授权，以及授权先提交后迟到核销转人工且真实Worker恢复137分。七个恢复窗口覆盖提交前回滚、提交后未Redis、回复丢失、delta写前/插入后/标记SQL后提交失败及原SUCCESS前；新增事件B恢复冻结A许可，比较完整delta身份和独立余额。旧多券非首券、归属后出现、CAS/流水回滚、租约末CAS和不同券并行均实测。每条连接显式RC/RR；旧生产发布/作废专属RC保留。新端关联192项、旧端291项全通过。审查补齐Worker负控/不同事件source及构造失败/中断清理，实际Redis启动失败和SIGTERM反例通过；正常退出也逐轮核对容器/scratch清理。旧端本地提交`2c78660d`。完整证据见《集成测试.md》`MINISO-BRIDGE-CROSS-JVM-RECOVERY-LOCAL-20261022`。生产代码及共享DDL未改；仅本地提交，不执行Task18—19、push或部署，不单独启用BRIDGE。

授权提交前回滚窗口补充判据：后来核销胜出时必须无授权、无Redis释放、无delta，并由真实Worker阻断；“只增加137”仅适用于已提交许可的恢复窗口，不能给未提交许可制造退款。

## Task 18: 默认关闭、受控Job与低频观测

**Files:** Create `LB/MinisoCallbackBridgeJobs.java`、`LT/MinisoCallbackBridgeJobsTest.java`；Modify `EI/config/MinisoWriteOffAutoConfiguration.java`及`ET/config/MinisoWriteOffAutoConfigurationTest.java`、旧`MinisoBridgeConfigurationTest.java`。复用新端既有`MinisoWriteOffXxlHandler`，不新建核销Job。

- [ ] 1. 测试所有bridge开关默认关闭、BRIDGE入口停收503、停route/notify不丢已接收事实、处理关闭仍阻止首次退款、缺表/读异常失败关闭。运行`legacy_test 'MinisoBridgeConfigurationTest,MinisoCallbackBridgeJobsTest'`及`engine_test MinisoWriteOffAutoConfigurationTest`确认缺行为红灯。
- [ ] 2. 注册`minisoCallbackBridgeRoute`、`minisoCallbackBridgeLegacyNotify`；只通过现有XXL executor运行，默认limit100，非法参数拒绝、不暴露公网手工接口。新端处理仍复用`minisoWriteOffProcessJob`，与接收分开装配。开关不能禁用归属和资金保护，LEGACY初次兼容不是混写许可。

```java
@XxlJob("minisoCallbackBridgeRoute")
public void route() {
    routeService.processBatch(properties.getBatchSize());
}
@XxlJob("minisoCallbackBridgeLegacyNotify")
public void notifyLegacy() {
    notificationService.processBatch(properties.getBatchSize());
}
```

`processBatch(int limit)`在对应服务公开定义，内部自行检查routing/legacy-notify开关并校验limit；禁用时返回无副作用摘要。
- [ ] 3. 接入当前可用指标设施或结构化低频批末摘要：首次/重复/冲突/存储失败、各状态待处理数及最老年龄、新投递/旧应用/HTTP通知结果、授权未收尾数、最后成功扫描时间。每类异常由已有采样工具或服务内时间窗限频，不引入通用监控平台。所有日志仅event/order/org及安全错误码，保护路径异常保留抽样栈。
- [ ] 4. 同测试转绿；测试服务真实执行一批后才更新最后成功扫描，不把handler注册/开关true当已扫描证据。每批失败摘要和耗尽人工均可观测。
- [ ] 5. 本地提交`feat(miniso): expose controlled bridge jobs and backlog signals`。

## Task 19: 协议示例、两仓库回归与发布门禁交接

**Files:** Modify `E/scripts/miniso-writeoff-example.py`、`E/cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/supplier/SupplierMockServer.java`及其现有测试、`E/README.md`、两份主设计、已批准规格、`E/docs/集成测试.md`；旧端接入说明先读取现有内容并保留他人编辑。

- [ ] 1. 复用既有离线签名向量更新可执行示例：仍发送旧唯一路径；明确200接收不代表核销完成，增加同券同时间/异时冲突、BRIDGE暂停503及新旧路由结果说明。模拟器只补本方案确需的MINISO成功/不明发券/查单/作废桩，不加银行2104假成功或生产地址。先跑现有模拟器测试，再加会区别错误实现的行为测试至红。
- [ ] 2. 实现最小离线场景，继续限制脚本显式本机URL及环境变量key2、不打印正文；执行`python3 "$E/scripts/miniso-writeoff-example.py"`，预期全部固定向量PASS；模拟器新场景测试转绿。已删除旧前端不在交付中，不声称更新它。
- [ ] 3. 显式JDK运行完整回归，旧端不跳过测试：

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -B -f "$E/pom.xml" test
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -B -f "$E/cardshop-trade-engine-simulator/pom.xml" test
JAVA_HOME=$(/usr/libexec/java_home -v 1.8) mvn -B -f "$L/pom.xml" \
  -pl cardShop-admin -am -Dmaven.test.skip=false -DskipTests=false test
```

记录各次实际数量、失败及跳过原因，不复用历史690/77或其他结果。新端领域模块无Spring/MyBatis/旧表依赖；`git diff --check`、秘密扫描和两仓库修改清单一并核对。
- [ ] 4. 更新设计为“已实现、离线/本地验收状态及未通过门禁”，不能笼统写“上线完成”。接口/DDL/默认配置/依赖/凭据和授权不可变语义与文档一致，所有集成证据仍在《集成测试.md》。本地提交`docs(miniso): record bridge protocol and verification evidence`。
- [ ] 5. **在需要任何共享状态写入之前停下，申请具体test188发布授权。** 交接需列出两个实际commit/JAR校验和、新0023迁移checksum、Nacos待改键名但不含密钥、待暂停的测试任务/实例、唯一URL/TLS/接入身份及供应商重试证据。无证据项逐项标阻断，不自行补写结论。
- [ ] 6. 获得授权后才另行执行测试发布：盘点全部写入者及历史无授权终态→受控暂停相关测试入口/资金写入→新端迁移/永久保护→旧端关闭开关版本→核验所有实例/schema/主连接→配置并证实三个Job实际扫描→切BRIDGE并启用接收→恢复受控测试。停收返回503，不能回LEGACY混写。pro140完全排除，不能触发生产部署或修改全租户供应商回调。
- [ ] 7. 真实测试使用不同券分别核销/作废；未获独立范围授权不发真实请求、不补历史通知。银行2104 SDK TLS及银行通知验收独立，桥接通过不代表银行通过。回滚保留inbox/event/task/guard/授权；退回不理解合同的旧JAR前必须停相关资金写入并排空/登记未完成事项，禁止清表抹证据。

## 2. 规格覆盖与完成判据

- 规格1—3（范围/架构/模块）：任务1、2、9、19；旧Java8与新Java21、唯一URL及非目标不变。
- 规格4—5（认证/可靠接收/状态）：任务9、13、17；收到HTTP200必须已提交，重投不改进度。
- 规格6（租约/归属/guard/多券）：任务4、8、10、11、12、14、17；RC/RR均证明同券竞争，不依赖缺行gap lock。
- 规格7（新事件）：任务8、14；version0、首次时间、空归属受控补齐、同主连接事务和最终CAS回滚。
- 规格8.1—8.2（旧资金隔离）：任务11、12、17；全部可达入口，候选及最终锁后双重排除新ext。
- 规格8.3（新授权/恢复）：任务5、6、7、8、17；终态与授权原子、冻结sourceEventId/amount、迟到核销人工但资金恢复不断、RELEASE delta/标记原子。
- 规格8.4—8.5（无凭据/旧作废/已批准取舍）：任务1、7、10、12；不明先查单且不退款，不虚构失败码证据。
- 规格9（旧事实/通知）：任务15、16；原AES、冻结URL/正文、只确认HTTP层、不自动历史重放。
- 规格10（默认/Job/发布/回滚）：任务9、18、19；永久保护不受bridge开关控制，实际发布单独授权。
- 规格11—12（验证与证据）：任务3、17、19；真实本地MySQL/Redis、两仓库回归、实际结果记录，无测试即不宣称通过。

执行完成必须同时报告：本地实现/测试结果、两个仓库实际提交状态、哪些门禁仍阻断、是否已push/部署。源码和H2通过不能替代InnoDB/Redis证据，更不能替代真实名创或交行验收。

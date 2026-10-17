# MINISO 请求鉴权与履约接入实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. 不提交、不推送、不创建worktree、不操作线上。

**Goal:** 按已确认PDF与旧卡管实现四个名创主动调用接口，冻结批次并可靠保存直充履约凭据。

**Architecture:** infrastructure内独立协议客户端与名创Gateway；application保持通用履约端口，通过带类型的凭据结果与MySQL事务接入。默认关闭真实名创，mock路径保留。

**Tech Stack:** Java21、Jackson、JDK HttpClient、MyBatis、Liquibase、JUnit5、H2。

### Task 1 协议客户端

- [x] 新增supplier/miniso/MinisoHttpClient、MinisoSettings、MinisoApiResponse及对应JUnit测试。
- [x] 先用本机HttpServer捕获四个请求，独立MD5校验实际body，覆盖32位nonce/毫秒时间/UTF-8/<=10张/重复空项/HTTP5xx/超时。
- [x] 观察测试红灯，再实现一次序列化与请求头鉴权，不记录敏感正文；校验tenant/version和HTTPS，测试仅回环例外。
- [x] 命令：JAVA_HOME=$(/usr/libexec/java_home -v21) mvn -pl cardshop-trade-engine-infrastructure -am test -Dtest=MinisoHttpClientTest -Dsurefire.failIfNoSpecifiedTests=false；预期协议用例通过。

### Task 2 冻结批次快照

- [x] V1ProductSelection追加String supplierBatchId，并提供原签名构造器默认空值；fingerprint加入批次。McProductDO/Mapper读取external_product_id，selector传入冻结值。
- [x] Redis选品缓存末尾追加supplier_batch_id；AcceptRechargeOrderCommand、accept Lua末尾参数、API MQ snapshot、PersistOrderSnapshot、Redis snapshot adapter、order_ext及dispatch outbox payload完整传递，不改变旧参数位置。
- [x] 测试批次变更改变指纹、缓存读写、Redis/MQ两路落库保持同一batchId；缺失批次不能触发真实名创。

### Task 3 凭据与Gateway

- [x] 新增application MinisoFulfillmentReceipt及SupplierGatewayResponse可选凭据，保持旧工厂/构造兼容。
- [x] 新增0020 changeSet、凭据Mapper与事务保存；凭据和SUCCESS订单状态同事务，校验可信org/order归属、唯一券码及幂等。
- [x] 新增MinisoRechargeSupplierGatewayAdapter：从订单ext快照读取batch/num，调用客户端并校验完整明细；只允许本订单券码作废，完整succeed或查单已明确作废方可成功。未知响应返回RETRYABLE_FAILED。
- [x] 补充Gateway的真实本地HTTP测试与H2事务回滚测试；覆盖status0/1/2/3、未知码、缺失明细、部分作废、重复/跨订单券码、迟到状态不回退。

### Task 4 配置与装配

- [x] supplier.miniso默认关闭，启用时MINISO路由专属Gateway、其它渠道保留通用Gateway；启用而凭据存储未装配须失败，不能静默降级mock。
- [x] TradeEngineProperties绑定测试、Spring装配路由测试、配置模板仅写环境变量占位；启动说明写清本地验证与真实验收边界。

### Task 5 全量验证与审查

- [x] JDK21执行mvn test，单独执行mvn -f cardshop-trade-engine-simulator/pom.xml test。
- [x] 按测试结果检查数据库迁移和合同边界；补docs/集成测试.md本地fake/H2证据（不使用线上profile和密钥）。
- [x] 先做规格审查，再做代码质量审查，修复发现项并复测相关用例；git diff --check通过后交付，保持未提交。


## 完成证据

主工程JDK21全量测试607个，失败0、错误0，19个按环境条件跳过；模拟器74个全部通过。显式本机Redis合同测试另行通过，H2凭据5用例通过（含生产事务代理回滚）。四接口客户端14用例、Gateway21用例通过。两轮规格/质量审查发现项已修复复核；未执行真实名创请求、迁移发布、线上配置变更、提交或推送。执行记录见docs/集成测试.md中MINISO-LOCAL-20261017。

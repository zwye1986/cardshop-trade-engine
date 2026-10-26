# 交行2104 JSON字符串正文实施计划

> **For agentic workers:** 按本计划在当前会话顺序执行，测试先行；不委派、不提交或部署。

**Goal:** 按用户已确认要求，将三类通知的`trx_data_imprt`从逗号键值文本改为JSON字符串。

**Architecture:** 只修改application中的唯一编码源，继续冻结编码文本并由SDK当作String发送；不修改外层DTO、签验、HTTP、租约或数据库。发送前原有编码一致性校验拒绝历史旧格式，不静默转换或补发。模拟器与Python示例同步独立JSON解码。

**Tech Stack:** Java21、Maven3.9、现有受管Jackson、JUnit5/AssertJ、Python3 unittest、真实SDK2.2.6与本机HTTPS合成桩。

设计以两份主设计「本次交行回调对接确认」2026-10-26修订为准。用户已确认实际发送改JSON字符串；不新增配置开关、迁移、重发维护工具或远端操作。

### 1. 先同步设计与红灯测试

- [x] 在修改代码前同步`docs/交行直充业务系统详细设计.md`及`docs/交行直充业务系统高可用方案设计.md`。
- [x] 修改`cardshop-trade-engine-application/src/test/java/com/cardshop/trade/engine/application/recharge/Bocom2104PayloadCodecTest.java`：用独立JSON解析检查三操作、全部字段和类型、金额1950/1877不混淆、中文及逗号/引号/反斜杠/控制字符往返、空时间、非法Unicode与2500码点边界。上限针对序列化文本而非解码字段长度。
- [x] 修改`cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/notify/bocom/Bocom2104DeliveryIntegrationTest.java`：在HTTPS接收端要求外层字段为text，独立解析内层JSON，检查预期字段类型与值、重试原文、双层ACK，覆盖RSA/SM2。
- [x] 扩展同目录`BocomMerchantNotifyClientAdapterTest.java`，用旧格式冻结请求检查转人工且不登记或发送；现有发送试验改为检查JSON正文。
- [x] 修改`cardshop-trade-engine-simulator/src/test/java/com/cardshop/trade/engine/simulator/bocom/Bocom2104GatewayServerTest.java`及`scripts/tests/test_bocom_2104_example.py`，要求JSON字符串并独立解码，拒绝旧格式/重复字段/非对象。
- [x] 使用显式`JAVA_HOME=$(/usr/libexec/java_home -v 21)`运行定向Maven与Python测试，确认失败来自旧编码或解析行为，而不是编译/环境错误。日志保存`.amp/in/artifacts/bocom-json-red-20261026.log`等。

### 2. 最小实现与调用方断言更新

- [x] 在`cardshop-trade-engine-application/pom.xml`将已有jackson-databind依赖从test提升到compile，domain不引入JSON库。
- [x] 修改`cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/Bocom2104PayloadCodec.java`，保留字段白名单、金额和文本校验；用Jackson ObjectNode按协议顺序写String与整数Number，输出紧凑JSON；原始文本先校验Unicode，序列化后校验2500码点。
- [x] 同步application中`MerchantNotifyPreparationServiceTest`、`RechargeOutboxWorkerServiceTest`和infrastructure中`MinisoWriteOffPersistenceTest`的旧格式断言，保留三入口、事务及资金保护测试。
- [x] 修改`cardshop-trade-engine-simulator/src/main/java/com/cardshop/trade/engine/simulator/bocom/Bocom2104GatewayServer.java`，独立严格解析JSON对象，拒绝重复键、尾随文本及非标量类型，保留观测API与网关验签。
- [x] 修改`scripts/bocom-2104-example.py`，使用`json.dumps(fields,ensure_ascii=False,separators=(',',':'))`生成内层字符串，不再拒绝合法逗号。
- [x] 修改`BocomSdkCompatibilityTest`用JSON字符串，验证真实SDK form往返不二次编码或改变数值。
- [x] 复跑定向测试，确认JSON格式、特殊字符、历史拒发以及三类通知通过。

### 3. 文档、全量验证与交付状态

- [x] 同步`docs/交行2104回调接入指引.md`与`docs/superpowers/specs/2026-10-20-bocom-2104-callback-design.md`，明确JSON字符串示例、旧格式任务边界及银行尚未验收。
- [x] 执行显式JDK21下`mvn -B test`、`mvn -B -f cardshop-trade-engine-simulator/pom.xml test`及Python3.12下`-m unittest discover -s scripts/tests`；记录实际Surefire数量，不沿用旧结果。
- [x] 在`docs/集成测试.md`追加本机HTTPS执行范围、无Nacos/远端中间件、合成数据、命令、结果和未部署状态；检查最终diff与`git diff --check`。
- [x] 准备向用户交付的合成JSON字符串包装及验证结论，明确未提交、推送、部署或补发，不能宣称已解决银行ADPTSY0002。

执行结果：定向146项通过；主工程1031项中960执行通过、71条件测试跳过；独立模拟器85项和Python9项通过，均无最终失败/错误。首次全量发现bootstrap交付物测试遗留旧格式断言，已同步修订；系统Python不支持SM3，使用已安装Python3.12完成全套。详细命令、失败与成功证据见《集成测试.md》本次14:49–14:54记录。

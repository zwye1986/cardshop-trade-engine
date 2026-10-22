# MINISO No-release Terminal Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 恢复合法未预扣MINISO终态的通知与原任务，保持资金许可、历史失败关闭及核销顺序安全；修复旧端离线测试门禁。

**Architecture:** 在既有主事务中冻结无需释放的首次事件及时间；Worker先恢复资金许可，再识别无需释放裁决。两类事实互斥，不制造零额退款，不回填历史。新端核销接收及处理按任一终态裁决留存迟到人工证据。

**Tech Stack:** Java21/Maven/Spring/MyBatis/Liquibase、H2、独立Redis、可丢弃MySQL8 RC/RR；旧端Java8与现有JUnit/mock夹具。

---

### Task 1: 设计及失败用例

- [x] 在代码修改前更新两主设计9.8及资金恢复边界，记录用户批准的迟到核销优先级。
- [x] 扩展`RechargeOutboxWorkerServiceTest`未预扣场景，覆盖通知创建失败、任务完成失败、重建Worker及三类履约事件。保持历史无许可终态负控。
- [x] 执行`JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -B -o -pl cardshop-trade-engine-application -am -Dtest=RechargeOutboxWorkerServiceTest -Dsurefire.failIfNoSpecifiedTests=false test`，确认恢复断言为红灯（63项、3个预期失败，随后63项全绿）。

### Task 2: 持久裁决及前向迁移

- [x] 新增`db/changelog/0025-miniso-no-release-decision.yaml`，增加nullable `balance_no_release_decided_at`及`balance_no_release_event_id`，在master的0024之后include；不改历史changeSet。
- [x] 扩展`RechargeOrderExtDO`及`RechargeOrderExtMapper.lockFulfillmentOrder`投影；原授权查询验证裁决互斥及完整性。履约端口增加锁内读取已核实无需释放裁决的语义，缺实现默认拒绝。
- [x] 在`MybatisRechargeFulfillmentOrderAdapter`首次未预扣分支原子提交裁决/终态；重放保留首次来源/时间。拒绝错身份、错source、部分字段、相互冲突及已有扣款流水。
- [x] 扩展既有`RechargeFulfillmentVoucherStatePersistenceTest`，验证裁决重放、字段损坏及事务回滚；新迁移测试经真实Liquibase/MySQL执行，核对历史NULL、二次update/checksum及回滚保留裁决/通知。

### Task 3: Worker及核销保护

- [x] `RechargeOutboxWorkerService`在资金许可之后读取无需释放裁决；重读稳定订单并共用终态通知/频控/任务收尾，不进入退款或供应商分支。
- [x] 扩展`MinisoWriteOffEventMapper`的终态裁决查询及新端接收/处理调用，任一部分裁决均禁止自动核销；保持首次裁决前guard和证据锁序。
- [x] 更新受影响H2结构/显式列及配置结构探针测试，不给历史无裁决终态补标记；重跑应用与基础设施相关测试至绿。

### Task 4: 真实组合回归

- [x] 扩展`MinisoMysqlReleaseRecovery`，使用真实持久端口/Worker/通知Mapper验证无需释放通知失败及任务完成失败后跨独立JVM恢复，资金端口不可调用、delta为0、原事件与作废时间冻结。覆盖CANCELLED/FAILED、事件B恢复A及裁决回滚后核销先提交负控。
- [x] 执行`JAVA21_HOME=$(/usr/libexec/java_home -v 21) MINISO_MYSQL_IMAGE=mysql:8.0.28 bash scripts/test-miniso-shared-callback.sh --all --legacy-repo /Users/mac/codespace/cardshop`，RC/RR两轮38次强制执行，无失败/错误/跳过；证据见《集成测试.md》本日修复节及`.amp/in/artifacts/miniso-no-release-mysql-redis.log`。
- [x] 执行新端`mvn -B -o test`（1023项、952通过/71既有条件跳过）、模拟器85项、`scripts/tests`9项及显式本机资金Redis组合110项；后面三组无跳过，均无失败/错误。

### Task 5: 独立旧端测试修复及交付

- [x] 旧端两失败mock测试补MyBatis实体元数据，应用测试建立本地日志、独占回环Redis及替身边界后再执行；六类61项连续4轮通过，主线程独立再跑61项通过。不新增skip，不连接dev共享服务。
- [x] 复核旧端修改并在显式JDK8、清除继承代理且限定回环绕过规则后运行根工程`clean test`：852项、842通过、10既有真实MySQL条件跳过、失败/错误0；主线程核对77份新XML及生效属性。按《集成测试.md》追加命令、环境、代理零网络红绿探针及证据；MOVIE异常ACK/PROCESSING风险独立记录，不擅改业务语义，不冒充真实MQ或新建订单验收。
- [x] 两端diff检查、临时探针清理及进程检查完成。用户随后授权中文提交并推送两个现有dev，提交前全量重跑结果不变，Git交付回执见本次会话；不包含部署、共享DDL、真实供应商验收或MOVIE生产语义修复，现场发布门禁保留。

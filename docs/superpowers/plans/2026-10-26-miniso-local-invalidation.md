# MINISO本地凭据作废 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 取消正常MINISO作废前查单/远端phone限制，未知结果只查证、不重发、不误退款。

**Architecture:** 正常作废从冻结订单与本地完整单券凭据构造请求。Worker以原事件的持久供应商流水决定queryOnly恢复模式，网关仅对同券码/批次且状态3恢复成功。保持现有核销、释放许可和通知事务边界，不加DDL。

**Tech Stack:** Java 21、Maven、JUnit 5、AssertJ、MyBatis、真实本机HTTP替身。

---

### Task 1: 固定合同与网关回归

Files: 两份主设计文档；`cardshop-trade-engine-infrastructure/src/{main,test}/java/com/cardshop/trade/engine/infrastructure/supplier/miniso/MinisoRechargeSupplierGatewayAdapter{,Test}.java`；`cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/SupplierInvalidGatewayRequest.java`。

- [x] 两份主设计追加2026-10-26修订，保留已有未提交内容。
- [x] 修改回归：仅提供完整abandon应答，断言直接成功且真实HTTP只有abandonUserCoupon；先运行观察旧实现失败。
- [x] 请求增加显式queryOnly且保留旧构造器；首次检查本地归属后直接作废，恢复只queryGrand；远端phone仅恢复时允许空，本地时间不变。
- [x] 扩展恢复phone缺失/null/空白、非空冲突、严格状态/批次/券码、未知回包及本地身份冲突用例；普通发券/查单不放宽。

### Task 2: 持久尝试与Worker恢复

Files: `cardshop-trade-engine-application/src/{main,test}/java/com/cardshop/trade/engine/application/recharge/RechargeOutboxWorkerService{,Test}.java`；同目录`RechargeFulfillmentOrderPort.java`；`cardshop-trade-engine-infrastructure/src/{main,test}/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeFulfillmentOrderAdapter{,Test}.java`及已有数据库测试夹具。

- [x] 增加结果未知/崩溃重入回归：不普通查单、不退款，持久attempt使后续queryOnly；核销冲突仍拒绝，成功仅一次释放/通知。
- [x] 端口按orderId和原eventId查询作废尝试；流水request_param写operation与supplier_request_id，调用前落库，恢复流水区分INVALID_QUERY。
- [x] 使用真实H2数据库Mapper及生产事务代理覆盖事件精确匹配、非最新尝试及恢复重读，不依赖mock验证持久安全合同。

### Task 3: 验证与交付

- [x] 用JDK21运行定向回归，再运行全量`mvn -B -o test`；命令输出留在`.amp/in/artifacts/`。
- [x] 更新`docs/集成测试.md`，记录188项定向（含独立真实Redis5项）、全量959通过/71条件跳过、真实HTTP超时及本地隔离边界。
- [x] 检查diff与敏感日志；仅交付本地修改和验证结果，不提交、不推送、不部署、不恢复历史任务。

定向命令：`JAVA_HOME=$(/usr/libexec/java_home -v 21) PATH="$(/usr/libexec/java_home -v 21)/bin:$PATH" mvn -B -o -pl cardshop-trade-engine-infrastructure -am -Dtest=MinisoRechargeSupplierGatewayAdapterTest,RechargeOutboxWorkerServiceTest -Dsurefire.failIfNoSpecifiedTests=false test`。

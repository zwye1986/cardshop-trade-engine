# 历史已作废订单退款最小兼容实施计划

> 本轮按用户已批准方案在当前会话逐项执行，使用TDD；保留工作区原有JMeter手册改动。

**Goal:** 已扣款且已作废的旧无资金标记订单能够退款，并保留现有订单级防重复及收尾。

**Architecture:** 可信MySQL资金入口产生内部授权，四个既有Java文件透传，现有Lua仅在旧无标记分支回补available。普通新协议、事务、Worker和自动配置不变。

**Tech Stack:** Java21、Maven3.9、JUnit5、H2/MyBatis、独立本机Redis。

## 1. 同步合同与基线

- [x] 更新两份主设计，新增`docs/superpowers/specs/2026-10-21-legacy-cancelled-refund-design.md`，覆盖此前缺标记一律拒绝退款的规则。
- [x] 基线：运行下列命令，旧资金测试全部通过。

```sh
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home PATH=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin:$PATH mvn -B -o -pl cardshop-trade-engine-infrastructure -am -Dbalance.localRedisTests=true -Dtest=BalanceReservationLocalRedisContractTest,RechargeOrderFundsTransactionTest,RedisBalanceReservationReleaseAdapterTest,RechargeOutboxWorkerServiceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

## 2. RED：旧记录回补与MySQL资格

- [x] 修改`cardshop-trade-engine-infrastructure/src/test/java/com/cardshop/trade/engine/infrastructure/redis/BalanceReservationLocalRedisContractTest.java`，构造旧无标记、PENDING_MQ、无业务快照数据，真实Lua追加第11参数1；先断言RELEASED/available1000/frozen原值，重放ALREADY_RELEASED且金额不变。

```java
redis.opsForHash().remove(KEYS.reservationKey("A35", "O1"), "funds_status", "org_code").block();
redis.opsForHash().put(KEYS.reservationKey("A35", "O1"), "status", "PENDING_MQ").block();
redis.delete(KEYS.orderKey("A35", "OUT1")).block();
```

- [x] 修改`RechargeOrderFundsTransactionTest.java`，基于真实H2/MyBatis记录捕获命令，要求内部授权；原PRE_DEDUCT改为INIT或FAILED时不授权，非作废成功不允许退款。
- [x] 先运行上述定向命令，确认旧Lua返回RESERVATION_FUNDS_STATUS_MISSING构成有效RED；新增参数访问器在生产字段引入前不使用，避免将编译失败算作RED。

## 3. GREEN：最小实现

- [x] `cardshop-trade-engine-application/src/main/java/com/cardshop/trade/engine/application/recharge/BalanceReservationReleaseCommand.java`追加`boolean allowLegacyCancelledRefund`，保留7/9参数构造器默认false。
- [x] `RechargeOrderFundsService.java`增加内部四参数release重载，既有重载关闭兼容，传递授权给命令。
- [x] `cardshop-trade-engine-infrastructure/src/main/java/com/cardshop/trade/engine/infrastructure/persistence/MybatisRechargeOrderFundsAdapter.java`保存已验证PRE_DEDUCT，调用`service.release(order, reason, previous != null, cancelled && TaskStatus.SUCCESS.name().equals(preDeduct.getStatus()))`。
- [x] `RedisBalanceReservationReleaseAdapter.java`追加`command.allowLegacyCancelledRefund() ? "1" : "0"`到Lua参数。
- [x] `cardshop-trade-engine-infrastructure/src/main/resources/redis/lua/release_balance_reservation.lua`受限兼容分支：授权、RELEASE、期望CONSUMED、缺资金标记；跳过旧持久化/消费凭证前提，保留身份/金额/资金基线/原退款保护，只加available并记录LEGACY_CANCELLED。
- [x] 在两种授权布尔值下检查命令透传和Lua参数，运行相同定向命令验证GREEN。

## 4. 完成证据

- [x] 补验回包丢失、不同事件并发、MySQL回滚、同订单退款凭证冲突、核销冲突与最终RELEASE刷账；复用现有Worker无重复供应商调用回归。
- [x] 全量：沿用JDK21，运行`mvn -B -o -Dbalance.localRedisTests=true -Dminiso.localRedisTests=true clean package`，检查全部测试汇总和真实Redis用例未跳过。
- [x] 更新`docs/集成测试.md`，记录环境、数据范围、RED/GREEN和全量证据；执行`git diff --check`及完整差异审阅。只交付修改，不自动提交/推送/部署或执行真实退款。

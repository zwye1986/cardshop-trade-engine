# 成功预扣收尾最小修复计划

> 当前工作区串行执行，保留已有改动，不提交/推送/部署，不写共享资金。使用TDD和真实本机临时Redis验证。

**Goal:** 补齐成功消费确认，保持已有扣款、发券和退款正常语义，并停止缓存任务覆盖业务余额。

**Architecture:** 复用现有余额释放端口、Lua执行器和DISPATCH/QUERY重试。只新增Redis资金标记及确认动作，缓存任务收窄为无历史新商户的安全初始化；不实现通用资金重建。

**Tech Stack:** Java21、现有MyBatis/Spring Redis、Lua、JUnit5、临时本机Redis。

- [x] 检查当前未提交工作，先修订两份主设计9.2/14.7，明确四处必要行为及历史兼容边界。
- [x] 新增本机Redis资金合同及Worker恢复回归，验证旧代码失败：首批Lua6例中4例断言失败、Worker2例失败；缓存已有余额覆盖回归失败。后补资金标记冲突及余额丢失的退款拒绝回归，也先验证失败。
- [x] 复用现有端口/适配器及release Lua完成确认/已消费退款分支；接单只增加funds_status，落库Lua只追加reservation key及单调状态同步。Worker成功及成功终态恢复都确认后才结束事件，不重派。
- [x] 测试驱动收窄RedisRechargeCacheRepairAdapter：已有余额不写；无pending后再读取新商户无历史SQL，再以owner令牌条件Lua初始化；释放锁原子比较删除。真实Redis验证已有余额/pending/历史/owner变更拒绝，H2真实SQL证明完成历史也阻止自动初始化。
- [x] `mvn -Dbalance.localRedisTests=true -Dminiso.localRedisTests=true clean package`740例，722通过/18可选中间件跳过；其中本机资金Lua/缓存合同12例全通过。模拟器77例通过，diff检查通过。生产代码仅9个既有文件，无新增表/接口/任务，历史资金及已部署901c应用未动。

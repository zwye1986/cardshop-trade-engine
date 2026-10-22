# A35名创真实参数接入与测试资金基线变更执行计划

> **For agentic workers:** 使用 executing-plans 在当前会话串行执行。本计划只变更用户已授权的联调部署配置和A35测试资金，不开发功能、不发券、不改旧卡管、不提交Git。

**Goal:** 在三节点连接cardshop_test_new的前提下，复用旧卡管MINISO接入参数、补齐A35缓存，并把MySQL余额和Redis可用余额统一为99,999,999元（9,999,999,900分）。

**Architecture:** 供应商参数从旧Nacos只读获取；敏感值仅写各节点权限600的部署环境文件，新Nacos引用环境变量。共享MINISO由mock切真实前按商户维修锁阻断受影响的新单并确认无遗留任务；保留其他配置。余额在维修锁、MySQL行锁、旧值条件和Redis版本条件下变更。缓存通过当前部署JAR的选品与编码逻辑及生产Lua构建，不手工猜指纹。

**Tech Stack:** SSH、Nacos、MySQL只读检查及受控事务、Redis Sentinel/Lua、JDK21、现有Spring Boot部署脚本。

---

## 边界与失败处理

- 目标仅为现有联调三节点pro143/pro140/pro120、cardshop_test_new及既有Redis前缀；部署profile虽名为prod，不修改旧卡管cardshop生产库或供应商配置。
- 不向名创发券/查单/作废，不向银行发送通知，不改名创全局回调URL。只准备独立key2，启用核销处理但不开放核销接收；接收须另行确认新旧订单回调隔离和HTTPS入口。
- 变更前备份Nacos原文、各节点环境文件、目标缓存及A35资金旧值到远端权限700的变更目录，秘密不得进入仓库或普通日志。
- 必须检查受影响商户的Redis pending/frozen、MINISO主订单及未完成派单/资金任务；有积压或锁竞争立即停止，不能删除任务或券凭据。
- 发布使用原文摘要作为并发变更校验；重启每次只处理一台，readiness/liveness与配置存在性核对成功后再进行下一台。
- 失败时保留维修锁及诊断证据。未产生真实订单前可以按旧值条件回滚本次配置/资金；若已出现订单，不允许盲目切回mock或覆盖余额，停止并报告。

## Task 1：前置复核与备份

- [x] 重新读取两份主设计、STARTUP及最新集成/部署记录；记录工作区已有修改。
- [x] 从旧卡管生产Nacos读取mingchuang并验证tenant=MINISO、version=tripartite1.0、HTTPS源站、tripartite/key/key2非空。只输出一致性布尔值。
- [x] 核对新库48条迁移、当前JAR所需表/列/唯一键、A35商户及唯一商品授权；列出MINISO受影响商户，确认并发任务和Redis状态。
- [x] 建立受控备份目录并保存Nacos及目标缓存原值；获取9个受影响商户维修锁，检查锁持有者与TTL。

## Task 2：部署秘密与共享配置

- [x] 三台分别原子更新config/.prod.env中的MINISO_BASE_URL、MINISO_TRIPARTITE、MINISO_API_KEY、MINISO_CALLBACK_KEY2，保留属主及600权限；输出与源配置相等的布尔值，不输出值/摘要。
- [x] Nacos供应商配置保留原channels及其他字段，显式启用http-enabled/miniso.enabled，引用部署环境变量；核销processing-enabled=true、enabled=false。
- [x] Nacos主配置在现有merchant-config-preheat名单追加A35，保留A39及其他成员；不改数据源或其他业务参数。
- [x] 再读Nacos并在各节点解析环境变量，验证目标参数完整、旧配置无非目标字段漂移。

## Task 3：A35资金与缓存

- [x] 持有A35维修锁，SQL SELECT FOR UPDATE确认唯一商户、余额旧值及无资金任务；Redis确认frozen=0及version旧值。目标整数为9999999900分。
- [x] MySQL按id与余额旧值条件更新并提交；Redis通过校验维修锁、旧available/frozen/version的Lua修改available并递增version，不改其他商户。
- [x] 准备跨存储失败的旧值CAS补偿逻辑，本次两端成功、未触发补偿；未将准备工作误记为实际回滚。
- [x] 预热merchant:config为数据库资料对应的ENABLED/version，使用部署JAR中选择器、快照编码器和生产Lua预热80100392→80100139→106552；读回验证指纹、批次、售价及TTL。

## Task 4：滚动重启与最终验证

- [x] 按pro143→pro140→pro120滚动。发现历史root进程与PID文件权限问题后，改为root按真实PID优雅停旧进程、修正目标PID文件属主、由cardshop执行现有start脚本；每台双健康UP、进程环境与源配置一致、A35预热成功后再处理下一台。问题与恢复过程完整记录在集成测试文档。
- [x] 逐台读取JVM实际MySQL socket，与RDS PROCESSLIST HOST端口精确关联到cardshop_test_new；比较JAR SHA-256，三台各9条连接全部匹配。
- [x] 复核48项迁移、24个准确新引擎ID、11张表、关键列及唯一键、商品授权及三台配置；余额均9999999900、frozen=0，批次106552。
- [x] 确认无新单/任务后按owner释放9把维修锁；释放后只读复查通过。尚未真实发券及核销/银行通知验收。
- [x] 更新docs/集成测试.md及docs/高可用环境部署方案.md，只记录非敏感证据，并执行git diff --check。

完成时间：2026-10-20 12:01 CST。最终新PID：pro143=37578、pro140=30508、pro120=3410021；均cardshop用户，双健康UP。详细执行与后续核销/银行边界见集成测试`ENABLE-A35-MINISO-20261020`章节。

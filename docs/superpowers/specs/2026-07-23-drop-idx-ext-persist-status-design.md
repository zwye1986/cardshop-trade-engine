# 删除 recharge_order_ext.idx_ext_persist_status 设计

## 背景

pro143 压测窗口显示批事务 phase `recharge_order_ext` 可达约 11s。该 phase 对应 `insertExtBatchOrUpdatePersistState`（multi-row `INSERT … ON DUPLICATE KEY UPDATE`）。表数据量仅约千行量级，排除全表扫描；主要瓶颈为并发 Consumer 下唯一键争用，以及低基数二级索引 `idx_ext_persist_status (persist_status, updated_at)` 在几乎全为 `DB_PERSISTED` 写入时的右端叶页热点维护。

本变更只处理二级索引热点；双唯一键争用不在本期范围。

## 目标与边界

**目标**：通过 Liquibase 正式删除 `recharge_order_ext.idx_ext_persist_status`，并同步两份主设计文档与迁移契约测试，使写路径不再维护该索引。

**本期做**：

1. 新增 changeset `0016-d2-drop-recharge-order-ext-persist-status-index`：`ALTER TABLE recharge_order_ext DROP INDEX idx_ext_persist_status`。
2. rollback：`ADD INDEX idx_ext_persist_status (persist_status, updated_at)`。
3. 更新 `docs/交行直充业务系统高可用方案设计.md` 建议约束，去掉该索引。
4. 更新 `docs/交行直充业务系统详细设计.md` 字段字典：`persist_status` 索引列改为无该二级索引（仅保留字段语义，索引/约束列标注为无或「无二级索引」）。
5. 更新 `RechargeD2ChangelogContractTest`：断言存在 DROP changeset；不再把「最终必须保留该索引」作为契约。

**本期不做**：

- 不改 `0005-d2-create-recharge-order-ext` 历史 CREATE（已执行环境以追加 DROP 为准；新环境先建后删，可接受）。
- 不改 Mapper / 业务 Java（现有查询均走 `uk_ext_order_id` / `uk_ext_org_out_order` 或 outbox 侧条件）。
- 不引入条件索引、生成列或替代扫描索引。
- 不调整 persist consumer 并发、batch size、Hikari。
- 不处理 `uk_ext_org_out_order` 单商户热点。

## 依赖结论（为何可删）

生产代码中 `persist_status` 用法：

| 路径 | 访问方式 | 是否依赖本索引 |
| --- | --- | --- |
| `selectByOrderIds` / `selectByOrderId` | `order_id` IN / 等值 | 否（唯一键） |
| `selectByOrgAndOutOrder` | `(org_code, out_order_sn)` | 否（唯一键） |
| outbox 归属哨兵 | 按订单三元组关联 + `persist_status='DB_PERSISTED'` | 否（先唯一定位） |
| 批写 ODKU | 插入/更新 `persist_status`、`updated_at` | 删除后少一次二级索引维护 |

未完成落库补偿仍以 Redis pending 为主路径，不存在按 `(persist_status, updated_at)` 扫 MySQL 的正式任务。

## Liquibase 约定

| 项 | 约定 |
| --- | --- |
| changeSet id | `0016-d2-drop-recharge-order-ext-persist-status-index` |
| SQL | `ALTER TABLE recharge_order_ext DROP INDEX idx_ext_persist_status` |
| rollback | `ALTER TABLE recharge_order_ext ADD INDEX idx_ext_persist_status (persist_status, updated_at)` |
| 幂等 | 标准 Liquibase 一次执行；若手工已删，部署前需确认索引存在或改用 precondition（可选，首期不强制） |

历史 `0005` 的 CREATE 仍可包含该 `key` 行，契约测试允许其作为「曾创建」痕迹存在，但必须同时断言 `0016` 的 DROP。

## 文档约定

高可用方案「建议约束」最终形态：

```sql
unique key uk_ext_order_id (order_id)
unique key uk_ext_org_out_order (org_code, out_order_sn)
key idx_ext_msg_id (persist_msg_id)
```

并加一句说明：曾规划的 `idx_ext_persist_status (persist_status, updated_at)` 因低基数 + 高频批写热点已删除；排障勿依赖该索引扫描。

详细设计 14.3：`persist_status` 的「索引/约束」列不再写 `idx_ext_persist_status`。

## 验证

1. `mvn -pl cardshop-trade-engine-migration test`：契约与 Liquibase parse 通过。
2. 目标库执行 changeset 后：`SHOW INDEX FROM recharge_order_ext` 无 `idx_ext_persist_status`。
3. 复压（可选）：对比 `trade.engine.persist.batch.mysql.phase.duration{phase="recharge_order_ext"}` 窗口最慢值；预期有改善，但不保证单独消除双 UK 争用导致的长尾。

## 风险与回滚

| 风险 | 缓解 |
| --- | --- |
| 运维手工按状态+时间扫表变慢 | 文档声明；需要时按 `order_id`/`org+out` 查，或临时加回索引 |
| 索引已手工删除导致 DROP 失败 | 部署前 `SHOW INDEX`；必要时加 `preConditions onFail:MARK_RAN` |

回滚：执行 changeset rollback 或手工 `ADD INDEX`，并恢复两份设计文档对应段落。

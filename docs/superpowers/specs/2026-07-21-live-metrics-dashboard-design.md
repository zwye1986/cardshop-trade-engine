# 交易引擎实时指标页设计

> 日期：2026-07-21  
> 状态：已评审，待实现  
> 交付位置：测试控制台 `/Users/mac/Documents/codeSpace/frontend/cardshop-trade-engine-test-console`  
> 关联：`deploy/monitoring/grafana/cardshop-trade-engine-overview.json`、`docs/交行直充业务系统详细设计.md` §11–12、前端现有 `ObservationDashboard`

## 1. 背景

联调与压测时，需要边打单边观察 Prometheus / MySQL（应用侧）/ RabbitMQ / Redis 相关指标。当前能力分散：

- 后端 `/actuator/prometheus` 暴露 `trade_engine_*`、Hikari、Spring AMQP 客户端指标；Redis/MySQL 积压 Gauge 来自 15s TTL 快照。
- 队列 depth 不在应用内采集，依赖 RabbitMQ Management / Exporter。
- 测试控制台 `ObservationDashboard` 只支持粘贴证据后聚合，不能实时轮询。
- `deploy/monitoring` 有 Grafana 模板，但联调门槛高，不适合「边打单边看」。

因此需要在测试控制台新增独立实时页，指标口径对齐 Grafana，便于后续运维复用。

## 2. 目标

1. 在测试控制台新增独立路由 `/live-metrics`（实时指标页），与 Observation 粘贴取证工作流分离。
2. 双源实时轮询：应用 Actuator（health + prometheus）+ RabbitMQ Management（队列 depth）。
3. 展示形态：当前值仪表盘 + 近 N 分钟 sparkline（前端本地环形缓冲）。
4. V1 覆盖七组：总览健康、Redis、MySQL 落库、Outbox/异常、RabbitMQ、V1 下单阶段、缓存。
5. 指标映射与 Grafana 模板同名对照，不引入 Prometheus 时序库或后端聚合 API。

## 3. 非目标

- 不新增后端 `/internal/live-metrics` 或类似生产聚合接口。
- 不合并或替代 `ObservationDashboard`。
- 不在本轮导入/部署 Grafana、Alertmanager。
- 浏览器不直连 Redis / MySQL；不持久化跨会话 history。
- 不把 `local` 环境加入集成测试环境列表（沿用前端 AGENTS 约束）。
- 不修改交易引擎生产业务代码路径；后端仓库仅在需要时补充交叉文档说明。

## 4. 方案结论

采用 **测试控制台纯前端轮询**：

| 数据源 | 访问路径 | 用途 |
| --- | --- | --- |
| Actuator | 现有 `/trade-api/{env}` → `/actuator/health/liveness`、`/actuator/health/readiness`、`/actuator/prometheus` | 健康态 + 业务/客户端指标 |
| Rabbit Management | 新增 `/rabbit-api/{env}` proxy → `/api/queues/{vhost}/{name}` | persist / retry / DLQ 的 `messages_ready` / `messages_unacknowledged` |

默认刷新间隔 5s（可选 2/5/10）；history 窗口默认 10 分钟；刷新页清空缓冲。

## 5. 页面信息架构

自上而下：

1. **控制条**：环境（复用现有 env state）、刷新间隔、LIVE/DEGRADED/PAUSED、上次刷新时间、立即刷新、暂停。
2. **健康总览条**：Liveness、Readiness、`environment` 标签、MQ breaker（`trade_engine_mq_publish_breaker_open`）、本轮 poll 状态/耗时。
3. **2×3 分区**：
   - Redis：WAIT 结果、Lua 耗时/慢调用、pending / due / maxAge + sparkline
   - MySQL 落库：batch size/duration、phase duration、orders success/failed/skipped、Hikari active/pending + sparkline
   - Outbox / 异常：dispatch pending/maxAge、balance delta pending/maxAge、accept anomaly open + sparkline
   - RabbitMQ：各队列 ready/unacked、DLQ、publish confirm、connections/channels + sparkline
   - V1 下单阶段：prepare / accept / wait / mq_confirm 等 stage duration
   - 缓存：商品/商户 hit、refresh、MySQL 回源

## 6. 组件边界

| 单元 | 职责 | 依赖 |
| --- | --- | --- |
| `LiveMetricsView` | 页面壳：控制条、分区编排、暂停/立即刷新 | service + 子组件 |
| `HealthStrip` / `MetricPanel` | 当前值 + sparkline 展示 | 归一化 snapshot 字段 |
| `liveMetricsService` | 并发拉取双源、汇总本轮状态 | axios / 环境 state |
| `prometheusParser` | Prometheus 文本 → typed snapshot | `liveMetricsMap` |
| `rabbitQueueClient` | Management JSON → 队列 depth | rabbit proxy |
| `metricsHistoryStore` | 环形缓冲、速率差分 | 纯内存 |
| `liveMetricsMap.js` | 指标名 → 面板字段映射（对齐 Grafana） | 无 |

解析与 history 必须可单测；View 不解析原始文本。

## 7. 指标映射（V1）

与 Grafana / 应用口径对齐的核心指标（Prometheus 名）：

**Redis**

- `trade_engine_redis_pending` / `trade_engine_redis_pending_due` / `trade_engine_redis_pending_max_age_seconds`
- `trade_engine_redis_wait_total`（按 `result` / `fail_code`）
- `trade_engine_redis_lua_duration_seconds_*`、`trade_engine_redis_lua_slow_total`

**MySQL / Hikari**

- `trade_engine_persist_batch_size`、`trade_engine_persist_batch_mysql_duration_seconds_*`
- `trade_engine_persist_batch_mysql_phase_duration_seconds_*`（`phase` 白名单）
- `trade_engine_persist_batch_orders_total`（`result`）
- `hikaricp_connections_active` / `hikaricp_connections_pending`

**Outbox / 异常**

- `trade_engine_dispatch_outbox_pending_total` / `trade_engine_dispatch_outbox_max_age_seconds`
- `trade_engine_balance_delta_pending_total` / `trade_engine_balance_delta_max_age_seconds`
- 兼容读取旧名 `trade_engine_outbox_pending` / `trade_engine_outbox_max_age_seconds`（仅派单语义）
- `trade_engine_accept_anomaly_open`

**Rabbit（应用侧 + Management）**

- 应用：`trade_engine_mq_publish_confirm_total`、`trade_engine_mq_publish_breaker_open`、`rabbitmq_connections` / `rabbitmq_channels` / `rabbitmq_published_total` / `rabbitmq_consumed_total`
- Management：`recharge.persist.mysql.queue`、`.retry.queue`、`.dlq` 的 messages_ready / messages_unacknowledged

**V1 stage / 缓存**

- `trade_engine_v1_create_stage_duration_seconds_*`（`stage`）
- `trade_engine_product_cache_*`、`trade_engine_merchant_profile_cache_hit_total`

Counter 类展示优先用相邻采样差分得到的近似速率；Gauge 直接画当前值趋势。

## 8. 配置

仅测试控制台：

1. `vue.config.js` 新增：
   - `/rabbit-api/dev` → `VUE_APP_RABBIT_DEV_PROXY_TARGET`
   - `/rabbit-api/test` → `VUE_APP_RABBIT_TEST_PROXY_TARGET`
2. 环境 state 扩展：Rabbit 基址、目标队列名列表、可选 Management basicAuth（仅本地 env / 忽略文件，不入库、不提交）。
3. 页面默认：refresh=5s，historyWindow=10min。
4. 同步更新前端 `AGENTS.md` / `README.md`：说明实时页、proxy 变量、与 Grafana 口径对齐关系。

后端仓库可选补充一句交叉说明（实时查看入口在测试控制台），不改 Actuator 暴露面。

## 9. 错误处理

| 情况 | 行为 |
| --- | --- |
| Actuator 失败 | 健康条标红；分区保留上一快照并标 `stale` |
| 仅 Rabbit 失败 | 队列 depth 显示「不可用」；其它分区继续刷新 |
| 解析失败 | 控制台 warn 摘要（不刷屏）；该轮不写入 history |
| 连续失败 ≥3 | LIVE → DEGRADED；不自动停止轮询 |
| 用户暂停 | 停止定时器；状态 PAUSED |

不做全局 modal、不做失败上报后端、不做跨会话持久化。

## 10. 测试计划

1. **单元**：prometheus 样例文本 → 期望 snapshot（含 `trade_engine_*` / Hikari / rabbit 客户端字段）。
2. **单元**：history ring buffer 截断、Counter 差分速率。
3. **手工联调**：dev/test 打开 LIVE，对照 Observation 粘贴结果与 Grafana 模板同名指标。
4. **文档**：前端 README/AGENTS；如需，后端 docs 增加交叉入口一句。

## 11. 实现范围清单

前端仓库：

- 路由 `/live-metrics` + 导航入口
- `LiveMetricsView` 与子组件
- `liveMetricsService` / `prometheusParser` / `rabbitQueueClient` / `metricsHistoryStore` / `liveMetricsMap`
- `vue.config.js` Rabbit proxy
- 单元测试 + README/AGENTS

后端仓库（本设计落地时）：

- 可选：docs 交叉引用；不强制改代码
- `.gitignore` 忽略 `.superpowers/`（brainstorm 会话产物）

## 12. 决策记录

- 用途：联调优先，口径对齐 Grafana（方案 C）。
- 数据源：Actuator + Rabbit Management（不含粘贴证据流）。
- 页面：独立 `/live-metrics`，不并入 Observation。
- 展示：当前值 + 本地 sparkline。
- 架构：纯前端轮询，不新增后端聚合 API。

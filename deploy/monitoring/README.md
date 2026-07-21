# cardshop-trade-engine 监控模板

本目录把 `docs/集成测试.md` 第 18 节的“应用薄业务指标 + RabbitMQ 外部队列探针”固化成可导入模板：

- `prometheus-rules/cardshop-trade-engine-alerts.yml`：Prometheus/Alertmanager 告警规则模板。
- `grafana/cardshop-trade-engine-overview.json`：Grafana 看板模板。

这些文件是仓库内的运维模板，不表示真实 Alertmanager 或 Grafana 已经上线。发布前需要运维在目标 Prometheus/Grafana 环境导入、调整外部 RabbitMQ exporter 的 label 名称，并完成告警通道、静默策略和升级联系人配置。

## 接入原则

1. 不新增独立 MySQL schema。指标读取继续按设计面向旧卡管共享库和新服务扩展表。
2. 应用只暴露低基数业务指标；`orderId`、`outOrderSn`、`orgCode`、手机号、账号、密钥不得进入 Prometheus label。
3. RabbitMQ 主队列、retry、DLQ depth 使用 RabbitMQ management Prometheus/exporter 或受控运维探针采集，不把 RabbitMQ management 凭据放进业务应用。
4. `dev`/`test` 共享旧卡管测试库时，outbox/XXL worker 可能互抢样本。联调取证必须临时停另一个 profile、关闭对应 worker，或使用明确测试窗口。

## 验证规则文件

如果部署机有 `promtool`：

```bash
promtool check rules deploy/monitoring/prometheus-rules/cardshop-trade-engine-alerts.yml
```

只有 Ruby 可用时，可先做 YAML 语法检查：

```bash
ruby -e 'require "yaml"; YAML.load_file("deploy/monitoring/prometheus-rules/cardshop-trade-engine-alerts.yml")'
```

## 导入 Prometheus

裸 Prometheus 可在 `prometheus.yml` 中引用：

```yaml
rule_files:
  - /etc/prometheus/rules/cardshop-trade-engine-alerts.yml
```

Kubernetes/Prometheus Operator 环境可把 `groups` 转为 `PrometheusRule.spec.groups`，并按集群要求补充 `metadata.labels`。

## 导入 Grafana

在 Grafana 导入 `grafana/cardshop-trade-engine-overview.json` 后，选择 Prometheus datasource，并确认 `environment` 变量能看到 `dev/test/prod`。

RabbitMQ 面板默认使用常见 exporter 指标：

- `rabbitmq_queue_messages_ready`
- `rabbitmq_queue_messages_unacked`

如果现网 exporter 使用不同指标名或标签名，需要在面板和告警规则中同步替换。

## Redis pending 告警

触发 `CardshopTradeRedisPendingDue` 或 `CardshopTradeRedisPendingTooOld` 时：

1. 先确认 `/actuator/health/readiness`、RabbitMQ 主队列/retry/DLQ depth、Consumer 日志和 MySQL 写入是否异常。
2. 使用固定人工入口或 XXL-Job 触发 `rechargePersistMqRepublishJob`，不要调用泛化 Bean/方法。
3. 共享旧卡管测试库下执行时，先隔离 dev/test worker，避免误判样本被另一个 profile 抢占。
4. 超过保护窗口时暂停扩大切流；生产环境按变更流程关闭新服务接单写入开关。

## Outbox 告警

触发 `CardshopTradeOutboxFailedOrManual` 或 `CardshopTradeOutboxTooOld` 时：

1. 这两条规则盯的是**派单履约**积压。Prometheus 名 `trade_engine_outbox_pending` 仅为派单兼容视图；目标指标优先读 `trade_engine_dispatch_outbox_pending_total{event_type,status}`。
2. 按 `event_type` 区分 `SUPPLIER_DISPATCH` / `SUPPLIER_QUERY` / `INVALID_ORDER`，核查对应固定 job、下游供应商与 `locked_until`。
3. **余额刷账积压**不在本规则范围；见下方「余额 Delta 刷账告警」与 `trade_engine_balance_delta_pending_total{status}`。
4. 需要人工关闭事件时，先保留 MySQL 证据和日志上下文，再按执行记录回填 `docs/集成测试.md` 或生产事件单。

## 余额 Delta 刷账告警

触发 `CardshopTradeBalanceDeltaFailedOrManual` 或 `CardshopTradeBalanceDeltaTooOld` 时：

1. 只看 `trade_engine_balance_delta_pending_total` / `trade_engine_balance_delta_max_age_seconds`，不要再用旧 `trade_engine_outbox_pending{event_type=BALANCE_DELTA_FLUSH}`（拆表后已不再暴露）。
2. 核查 `rechargeBalanceDeltaFlushJob`、`recharge_balance_delta_outbox`、`mc_ext_depart` 行锁，以及 `trade.engine.outbox.balance-delta-*` 租约/退避配置。
3. 非法退避 CSV 会导致应用启动 fail-fast；先核对 Nacos `TRADE_ENGINE_OUTBOX_BALANCE_DELTA_RETRY_BACKOFF_SECONDS`。

## 接单异常告警

触发 `CardshopTradeAcceptAnomalyOpen` 时：

1. 按 `anomaly_type` 核查 Redis Lua timeout、Redis BUSY、WAIT 未确认或状态未知。
2. 对同一订单核对 Redis order/idem/reservation/pending、RabbitMQ confirm/consumer 轨迹和 MySQL 四表事实。
3. 不要只凭单侧事实释放余额或重建订单。

## 余额对账告警

触发 `CardshopTradeBalanceDiffDetected` 时：

1. 暂停扩大灰度范围，核查 `mc_ext_depart.balance`、Redis available、未完成 `recharge_balance_delta_outbox`（指标 `trade_engine_balance_delta_pending_total`）和 balance owner。
2. 新旧系统共享旧库期间，确认同一商户同一交易类型没有双写余额。
3. 差异修复必须保留审计记录，不能直接覆盖 Redis 或 MySQL。

## Redis WAIT/Lua 告警

触发 `CardshopTradeRedisWaitNotConfirmed` 或 `CardshopTradeRedisLuaSlow` 时：

1. `WAIT >= 1` 只在具备副本确认的 Redis 环境中作为强验收；单机 test 环境应确认 `wait-replicas` 配置口径。
2. 核查 Redis CPU、slowlog、主从延迟、网络抖动、热点商户和 pending org_index 热点。
3. 生产环境 WAIT 未确认持续出现时，应按高可用方案关闭新写入并启动修复核查。

## RabbitMQ publish 告警

触发 `CardshopTradeMqPublishConfirmFailed` 或 `CardshopTradeMqPublishBreakerOpen` 时：

1. 核查 publisher confirm、mandatory return、exchange/binding、broker 磁盘/内存水位和网络。
2. 确认 Redis pending 是否保留，补投 job 是否能恢复。
3. 发布熔断打开时，应阻断新下单进入 Redis Lua，避免继续扩大“Redis 已接单但 MQ 未确认”的窗口。

## RabbitMQ depth 外部探针

触发 RabbitMQ depth 相关告警时：

1. 主队列堆积：核查 Consumer 数、ack、应用 readiness、MySQL 写入和连接池。
2. retry 队列持续有消息：结合应用日志判断是否为可恢复失败，避免重复人工补投同一订单。
3. DLQ 非空：先保留消息和业务上下文，禁止直接清空队列；核查 poison message、幂等冲突、Redis snapshot 兜底和落库异常。

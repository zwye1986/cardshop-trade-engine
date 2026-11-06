# MINISO 共库桥接测试环境配置指南

更新日期：2026-10-27。依据现有生产代码及当日只读现场核验编写；本文是待执行指南，不代表已发布配置或启用任务。

对应《交行直充业务系统详细设计》9.8、`MINISO-SHARED-CALLBACK-V1` 及实施计划 Task 19。这里只列测试环境操作，不授权生产操作、供应商全租户回调切换、历史退款或历史通知重放。

## 1. 先分清两个调度中心

当前两端使用不同 XXL-Job Admin，不能只按执行器名称或组 ID 操作：

- **旧卡管**：`http://47.117.130.100:9090/xxl-job-admin/`，test01。
  - 执行器 AppName：`cardShop-test`，大小写必须一致。
  - 执行器组：ID **9**，名称「卡管admin测试」。
  - 当前手动注册地址：`http://47.100.11.188:9130/`，指向 test188；9130 是 XXL 端口，不是业务 HTTP 端口 9191。
  - 此中心已核验没有 `minisoCallbackBridgeRoute`、`minisoCallbackBridgeLegacyNotify` 任务。
- **新引擎**：`http://47.117.129.235:8090/xxl-job-admin`，pro235。
  - 执行器 AppName：`cardshop-trade-engine`。
  - 执行器组：ID **15**，三个应用节点注册在 19999 端口。
  - 任务 176 是既有一次性核销测试任务，`schedule_type=NONE`，处于停用状态。

旧中心也有一个 `cardshop-trade-engine` 组（ID 18），但当前没有注册地址，不要把新端任务误建到这里。新中心另有生产卡管 `cardshop-admin` 组，不要把旧端测试桥接任务误建到那里。

两端现有执行器已经配置 Admin 地址和 access token。本次不需要更换这些配置，也不要把 access token 写进任务参数。

## 2. 执行前门禁

1. 确认具体测试切换窗口及授权范围，盘点并受控暂停相关入口和资金/凭据写入者；不是暂停全部无关业务。
2. 复核所有相关实例均已运行理解桥接合同的代码，连接同一共享主库，0023/0025 及永久保护已就绪。配置指向同一数据库不等于全部写入者已证明使用正确事务连接。
3. 复核 inbox、旧通知、新核销事件及待恢复授权的实时积压；以下启用的扫描不是“只读探针”，有待处理数据就可能推进业务或发送通知。
4. 登记两笔历史人工作废订单 `202610200826272563779785`、`202610201139049714510365`，不得因本次配置启用而补认授权、自动退款或重置任务。
5. 核对供应商实际登记的唯一回调 URL、TLS、签名身份及 HTTP 503/200 重试合同。磁盘 Nginx 配置只能证明代理配置，不能代替供应商登记证据或公网验证。
6. 旧订单需要通知时，核对该商户 `mc_config.config_key=writeOffNotifyUrl` 和有效商户 AES 配置。桥接不回退使用新订单通知 URL；缺少旧通知 URL 时可能只提交核销事实而不创建通知任务。

## 3. 旧端 Nacos 配置

### 3.1 配置位置

- Namespace：`cardshop-dev`
- Group：`dev`
- DataId：`cardshop-admin-dev.yml`
- 属性前缀：**`mingchuang.callback-bridge`**，不是 `miniso.callback-bridge`。

在现有 YAML 上按键合并，已有 `mingchuang` 节点则在其下追加 `callback-bridge`，不要制造同名顶层键，也不要覆盖主动发券、数据库、Redis、XXL 等已有配置。

### 3.2 第一阶段：BRIDGE 模式、暂停接收、允许后台扫描

以下阶段只能在上述门禁满足且获授权后实施。它会让旧唯一回调入口返回 **HTTP 503**，必须事先安排好停收窗口。

```yaml copy Copy Legacy Bridge Config
mingchuang:
  callback-bridge:
    mode: BRIDGE
    receive-enabled: false
    routing-enabled: true
    legacy-notify-enabled: true
    key2: "${MINISO_CALLBACK_KEY2}"
    batch-size: 100
    lease-seconds: 60
    route-retry-backoff-seconds: 60
    max-route-retry-count: 100
    legacy-notify-max-failures: 10
    legacy-notify-retry-backoff-seconds: 30
    legacy-notify-timeout-ms: 3000
```

密钥要求：

- `MINISO_CALLBACK_KEY2` 是本指南建议的环境变量名称，代码实际绑定的是 `mingchuang.callback-bridge.key2`。
- 通过受控环境或部署密钥注入 test188 的 Java 进程；重启前确认变量可用。不能让未解析的 `${MINISO_CALLBACK_KEY2}` 被当成密钥原文使用。
- 值必须是供应商用于该测试回调身份的 **key2**。不要填主动发券 key、商户 AES key 或任意新生成值，不要在聊天、普通日志或 Git 中记录。
- 新端仅消费共享事件时不负责验证这一路旧入口签名；保持其已有独立配置，不需要为桥接开放新入口。

注意：`mode=LEGACY` 且任一路由/旧通知开关为 true 会导致配置校验失败。不要为了先验证扫描而在 LEGACY 下打开这两个开关。

### 3.3 第二阶段：开放接收

扫描核验通过、供应商入口门禁解除后，仅将同一配置中的 `receive-enabled` 改为 `true`，其余保持第一阶段配置。

**发布 Nacos 后按受控流程重启旧卡管并验证启动。** 当前验签 Bean 使用 `@ConditionalOnProperty(receive-enabled=true)` 在装配时创建，不能仅凭 Nacos 热更新就断言暂停/恢复已完整生效。第一阶段到第二阶段同样需要考虑该装配边界。

旧服务保留 `POST /api/common/mcNotify`；从现有管理端 `/prod-api/` 代理进入时路径为 `/prod-api/api/common/mcNotify`。必须与供应商登记 URL 逐项核对，不在本次指南中假定供应商已经登记该地址。

接收成功的 HTTP 200、数值 `code=200` 只代表持久接收完成，不代表核销或通知已经完成。

## 4. 新端处理配置

在现有供应商配置上合并或核对以下键：

- Namespace：`494ce583-7775-43e5-abed-cd0cf4802137`
- Group：`cardshop-trade-engine-prod`
- DataId：`cardshop-trade-engine-supplier.yaml`

这里虽然使用 prod profile/Group，现场连接仍是测试库；不能据名称推断操作授权范围。

```yaml copy Copy Engine Processing Config
trade:
  engine:
    supplier:
      miniso:
        callback:
          enabled: false
          processing-enabled: true
          batch-size: 100
          retry-backoff-seconds: 60
          max-retry-count: 100
```

当前现场已经是接收关闭、处理开启。不要为桥接打开 `enabled`：唯一供应商入口应保留在旧卡管。已有 key2、供应商地址及其他配置均保留，以上不是整份 DataId 的替换内容。若确需改变处理开关，按新端受控重启流程核验 Handler 装配。

## 5. XXL-Job 具体配置

### 5.1 三个任务的共同字段

下面的 Cron 是本指南建议的测试环境初始值，不是协议强制值。任务创建后先保持停止，验收后再启动定时调度。

- 运行模式：`BEAN`
- 路由策略：`FIRST`（第一个）；本轮不使用分片广播。
- 阻塞处理策略：`SERIAL_EXECUTION`（单机串行）
- 调度过期策略：`DO_NOTHING`（忽略）
- 失败重试次数：`0`，业务重试由持久化任务、租约及退避管理，避免叠加 XXL 重试。
- 任务超时时间：`0`，不设置额外的整批调度超时；旧通知单次 HTTP 自有 3000ms 超时，整批串行 100 笔可能超过 60 秒租约，不能把单笔租约当成整批超时。
- 子任务 ID：留空；不要依赖子任务串联三个流程。
- 配置负责人及失败告警接收人，不填真实密钥或业务报文。

### 5.2 旧端任务一：共享接收路由

在 **旧中心 9090 → 任务管理 → 新增**：

- 执行器：`cardShop-test` / 「卡管admin测试」/ 组 9。
- 任务描述：`MINISO共享回调路由-测试`
- JobHandler：`minisoCallbackBridgeRoute`
- 调度类型：`CRON`
- Cron：`0/5 * * * * ?`（每 5 秒）
- 执行参数：`limit=100`
- 初始状态：停止。

### 5.3 旧端任务二：旧订单核销通知投递

仍在 **旧中心 9090** 新增：

- 执行器：`cardShop-test` / 组 9。
- 任务描述：`MINISO旧订单核销通知-测试`
- JobHandler：`minisoCallbackBridgeLegacyNotify`
- 调度类型：`CRON`
- Cron：`1/5 * * * * ?`（每 5 秒，与路由错开 1 秒）
- 执行参数：`limit=100`
- 初始状态：停止。

**两个旧端任务只接受空参数或单一 `limit=正整数`，不要加 `workerId`、`parallelism` 或分片参数。** 请求 limit 可为 1—1000，但实际单批最多处理 100；首次受控手工验证可临时用 `limit=1`，仍不是只读执行。

旧通知 HTTP 2xx 只记为 `DELIVERED_HTTP`，不检查响应正文业务码；它不代表交行 2104 业务确认。

### 5.4 新端任务：核销事件处理

在 **新中心 8090 → 任务管理 → 新增**：

- 执行器：`cardshop-trade-engine` / 组 15，核对注册地址为现有三节点的 19999。
- 任务描述：`MINISO新引擎核销处理-测试`
- JobHandler：`minisoWriteOffProcessJob`
- 调度类型：`CRON`
- Cron：`2/5 * * * * ?`（每 5 秒）
- 执行参数：`limit=100,workerId=xxl-miniso-bridge-test`
- 初始状态：停止。

建议保留任务 176 为停用的一次性测试记录，另建上述常驻任务；不要同时启动两份重复的常驻扫描。

新端只接受 `limit`、`workerId`；不要加 `parallelism`。`limit` 范围 1—1000，workerId 为 1—80 位字母、数字、点、下划线、冒号或连字符。

新端核销处理负责更新凭据并创建通知任务；交行实际发送仍依赖已有 `rechargeNotifyRetryJob` 及银行配置。本次只检查它正常工作，不重复创建第四个银行发送任务。

## 6. 推荐执行顺序与验收

1. 保存两份相关 Nacos 配置和两个调度中心的任务基线；核对并登记全部积压、历史人工任务，完成第 2 节门禁。
2. 在两个正确的中心创建三个任务，均保持停止；确认执行器地址不是生产卡管或空执行器组。
3. 在授权的停收窗口发布旧端第一阶段配置，受控重启；确认健康、主连接和结构探针正常。核对新端处理已装配，直接接收仍关闭。
4. 明确允许推进的实时积压范围后，对三个任务分别手工执行一次；无积压时也应完成真实扫描。单看触发成功或 Handler 注册成功不够：
   - 旧端日志应包含 `MinisoBridgeBatchResult[enabled=true,...]`。
   - `lastSuccessfulScanEpochSeconds` 应在真实扫描后更新，`failures=0`；不能把 `enabled=false` 的空跑当通过。
   - 新端有本次「名创核销处理」批次结果；数据库无异常，人工/失败结果按实际原因记录。
   - 旧端人工积压存在时 Handler 可以报失败，即使触发 HTTP 成功；不要关闭告警制造绿灯。
5. 先启动下游新端处理及旧通知任务，再启动旧端路由任务，观察扫描和积压；Cron 的秒偏移只是分散负载，不是事务顺序保证。
6. 所有门禁满足后发布旧端第二阶段 `receive-enabled=true`，受控重启并验证。仅对获授权订单发送签名回调，不用未授权真实券作探针。
7. 不同券分别覆盖新订单核销、旧订单核销、重复/冲突及作废路径；新单检查 `TRANSFERRED_NEW → PROCESSED → 银行业务ACK`，旧单检查 `LEGACY_APPLIED → DELIVERED_HTTP`。管理端核销详情作为独立读模型一起复验。
8. 持续观察 INIT/RETRY/PROCESSING、INIT/FAILED/SENDING 的数量和最老年龄，以及人工积压、授权未收尾数。测试步骤、配置变更、任务 ID、请求范围及脱敏证据统一追加《集成测试.md》。

## 7. 暂停与回退

- 暂停新接收：保持 BRIDGE，将 `receive-enabled=false`，按受控流程使其生效，确认入口返回 503；允许既有后台任务继续收尾，除非另有明确的暂停范围。
- 暂停指定扫描：停对应 XXL 任务；如需关闭服务开关，再受控发布对应 `routing-enabled` 或 `legacy-notify-enabled`。停止调度不会清除已接收事实。
- **不要切回 LEGACY 作为停收方式，不清 inbox/event/task/guard，不删除授权或裁决，不重置历史人工任务。** 回退不理解资金合同的旧 JAR 前必须先停止相关资金写入并盘点未完事项。
- 文中所有 Nacos 发布、任务新增/启动、重启和真实请求都是待授权的共享状态操作。本次提供指南没有执行它们。

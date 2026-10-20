# 名创单入口、共库核销分流设计

日期：2026-10-20。契约版本：`MINISO-SHARED-CALLBACK-V1`。
状态：用户已确认“旧入口持久接收＋共库事务投递新引擎事件”的总体方案及test188测试部署范围；本文为待复核的详细设计，尚未实现、迁移或部署。旧项目保持Java8，新引擎保持Java21。

## 1. 范围与现场证据

- 改造旧卡管测试实例：test188，`/mnt/bin/cardShop-admin.jar`，当前PID18924，profile=dev，HTTP9191。Nacos namespace=`cardshop-dev`，group=`dev`，DataId包含`cardshop-database.yml`、`cardshop-admin-dev.yml`。
- 新旧测试系统实际共用`cardshop_test_new`。旧JVM持有3306连接的本地端口45156，与RDS PROCESSLIST的`172.19.64.43:45156`精确对应该schema；新引擎配置同schema。dev配置还存在指向同schema的no-binlog数据源，但桥接只能使用主数据源，不得在同一业务事务内切换数据源。
- pro140 `/home/kalaiwang/bin`连接cardshop的旧生产实例不是本次改造/发布目标，不改其JAR、数据源、Nacos、回调地址或生产数据。
- 旧测试JAR SHA-256为`778c320f0477f561c3b4523c5aef41afb936760c7becaa14cfeeecdc886755a7`。旧代码基线位于`/Users/mac/Documents/codeSpace/cardshop`，当前dev分支、工作区干净；新引擎已有文档修改必须保留。
- test188 Nginx中存在apitest.kalaiwang.com默认代理9191、managetest.kalaiwang.com/prod-api代理9191的候选路径；本机配置不证明当前DNS、公网TLS或名创登记地址。真实供应商核销验收前必须单独核实名创唯一URL及其实际路由；不能为了测试改动生产全租户回调。
- 旧测试mingchuang配置与新引擎真实MINISO配置的baseUrl/key/key2不相等，tripartite未配置。桥接不能直接依赖其旧key2，也不能将旧主动发券参数静默切成真实通道。

## 2. 方案及非目标

保留名创既有对外URL和旧接口`POST /api/common/mcNotify`。旧端完成验证与可靠接收，后台在共享数据库内根据可信券归属处理或投递；新单不使用HTTP转发、不另设新公开地址，不要求新引擎callback.enabled=true。

未采用：

1. HTTP转发至新引擎公开回调：增加网络不确定性、原签名留存与密钥轮换期间重试合同，当前共库不需要。
2. 向新旧两端广播或未知直接走旧：可能造成重复通知、资金保护缺口和归属错误。
3. 给新单补造mc_batch_record：把两套履约/状态/通知模型混合，不能替代新引擎事件与退款保护。

本次不修改发券、扣款、批次选择、库存模型；不改新旧商户通知协议，不实现银行2104；不自动迁移或补发历史商户通知；不对真实名创压测。

## 3. 模块与契约

旧项目增加一个桥接模块，入口接口仅暴露“接收已签名核销请求”和“执行一批路由任务”。内部使用验签、接收仓储、归属解析、旧业务适配、新事件投递及旧通知任务适配。Controller不包含跨表SQL或网络投递。

旧端不得引入Java21模块、Spring Boot3依赖；新旧复用的是版本化SQL/事件契约及固定离线向量，不是共享Spring上下文或JAR。新引擎领域层不依赖旧表；公共接收表的读取与保护位于infrastructure。

新引擎是桥接DDL唯一发布者：新增0023迁移，建立下述两表；旧项目只做启用时的结构版本核验，避免两套Liquibase争夺同一表。桥接关闭时旧实例无需新表；开启时缺表/关键列必须启动失败，不能当作“未找到新订单”回退旧处理。

## 4. 持久接收与确认

旧端桥接模式采用新引擎既定入站合同：

- 正文有界读取，UTF-8 JSON不超过4096字节，仅couponCode/useTime两个字符串；拒绝重复JSON键、额外字段、隐式类型转换和非法字符/日期。
- nonce为32位小写字母数字，time为13位毫秒文本，sign为32位大写MD5；每个请求头必须唯一。tenant/version如果携带须匹配可信配置MINISO/tripartite1.0。
- 按TreeMap两字段紧凑JSON构建旧协议签名，使用桥接专用key2配置和常量时间比较。Java8/Fastjson2与新端固定向量逐字节交叉验证。
- 不设置短时效窗口拒绝合法迟到重投；以券码唯一事件及首次核销时间防重。同券同时间重投幂等，同券不同时间是冲突，禁止MAX覆盖。
- `event_id = 'miniso-redeemed-' + lowerHex(SHA256(UTF8('MINISO:REDEEMED:' + couponCode)))`，与现有新事件ID完全一致。
- 成功确认仅表示共享接收事件已提交，HTTP200、数值code=200、msg=接收成功。未知归属也可可靠接收，不表示核销事实或商户通知已完成。
- 验签/参数失败HTTP200、code=500；冲突code=500；存储提交未知、池拒绝/超时HTTP503；超限413、类型错误415。异常不返回原始SQL、请求正文或密钥。

不持久化key2、签名原文、nonce/time/sign或完整原始HTTP正文：共库投递不需再次验签。仅保存已认证的最小业务事实，后续密钥轮换不影响已接收事件。

## 5. 接收表与状态

新增`mc_miniso_callback_inbox`，重要字段/DDL COMMENT如下，Java DTO注释须保持一致：

- event_id varchar(96)：核销事件编号，主键。
- coupon_code varchar(128) utf8mb4_bin：名创券码，唯一。
- use_time datetime：核销时间，秒级上海时间。
- status varchar(32)：接收事件状态。
- route_target varchar(16) NULL：可信处理归属；仅NEW_ENGINE、LEGACY，未知为空。
- order_id varchar(64) NULL：可信平台订单号。
- org_code varchar(64) NULL：可信商户编码。
- retry_count int：失败处理次数，默认0。
- next_retry_time datetime(3)：下次处理时间。
- lock_owner varchar(128) NULL：任务租约持有者。
- locked_until datetime(3) NULL：任务租约截止时间。
- version bigint：事件版本，默认1。
- last_error varchar(500) NULL：脱敏失败摘要。
- received_at datetime(3)：首次接收时间。
- routed_at datetime(3) NULL：归属处理完成时间。
- updated_at datetime(3)：更新时间。

状态为INIT、PROCESSING、RETRY、TRANSFERRED_NEW、LEGACY_APPLIED、MANUAL_REQUIRED。TRANSFERRED_NEW仅表示新事件已可靠入库；LEGACY_APPLIED仅表示旧核销事实及应建通知任务已提交，不表示商户收到通知。

除显式NULL字段外均NOT NULL；status初值INIT、retry_count=0、version=1，接收/重试/更新时间由应用显式传入，不依赖数据库会话时区。索引：status/next_retry_time、status/locked_until、order_id、org_code；事件和券码唯一约束不得省略。DDL使用InnoDB、utf8mb4；券码不trim、不变更大小写。

重复接收只读回首次事实，不重置重试次数、租约、完成状态或首次时间。MANUAL_REQUIRED相同事实重投仍返回接收成功，不通过供应商重投绕过人工状态。

## 6. 租约、锁顺序及归属判定

路由Job采用短事务抢占＋独立业务事务，先提交租约，不持接收行锁等待主订单。执行及回写都校验owner/version，租约60秒，单批100，失败退避60秒，默认最多100次失败处理（配置化，不删除耗尽事件）。数据库不可用不得判为旧订单；未知归属等待，耗尽转人工。

先进行无锁候选查询，再按统一顺序拿锁复查：

`mc_order → recharge_order_ext（新单）→ 凭据（recharge_miniso_receipt或mc_batch_record）→ recharge_miniso_writeoff_event（新单）→ mc_miniso_callback_inbox → 通知任务`

入口已知新归属时也先锁主订单/扩展/凭据，再插入接收事件，使接收与作废本地资金事务串行。完全无凭据时入口只锁接收事件，提交后等待归属，禁止持接收锁再升级获取主单锁。

归属规则：

- NEW_ENGINE：唯一新凭据、主订单、DB_PERSISTED扩展行的订单/商户一致；主单渠道MINISO、数量1，凭据批次/手机号与冻结订单一致。不能只因同一mc_order存在就确认新单。
- LEGACY：唯一MINISO/state_type=3的mc_batch_record能关联可信主单，且无对应新凭据、无该订单的新引擎归属扩展记录。旧历史主单supplier_channel可能为空，不强套新单字段完整性要求。
- 无匹配：RETRY。新扩展已存在但凭据迟到，不回退LEGACY。
- 多条匹配、双归属、身份/批次矛盾：MANUAL_REQUIRED，不猜归属、不广播。已确认route_target/order_id不因后续另一条脏记录自动切换；矛盾转人工。

## 7. 新事件事务投递

在上述锁序内，按0021合同插入recharge_miniso_writeoff_event：eventId/couponCode/useTime与接收表相同，可信org/order来自锁后校验，status=INIT，retry_count=0，version=1，next_retry_time取当前时间。保留接收事件首次received_at用于滞留观测，处理时间使用既有上海时区约定。

新事件已存在且事实完全一致时视为幂等投递，不改其状态、重试或时间；同ID/券码不同核销时间、不同可信归属必须冲突，不覆盖。

插入/确认新事件和接收表TRANSFERRED_NEW以**同一主数据源连接、同一数据库事务**提交；任一步失败全部回滚。旧端不直接更新新凭据、voucher_*、余额或新通知任务。新引擎既有minisoWriteOffProcessJob及通知Worker继续各负其责。

不得使用no-binlog数据源；不得在事务内异步切线程、另开数据源连接或依赖跨事务HTTP确认。

## 8. 资金保护：接收后、分流前也必须生效

只接收到旧inbox、尚未投递新事件期间，同样存在可信核销证据。新引擎MybatisMinisoWriteOffProtection必须在已有本地凭据的券码上检查共享接收表，不能只检查recharge_miniso_writeoff_event。

- 已认证接收行无论INIT/PROCESSING/RETRY/MANUAL_REQUIRED/TRANSFERRED_NEW等状态，都阻止该券自动作废/退款；归属冲突不按“不是新单”放行。
- 保护随0023和持久化适配装配，不依赖旧接收开关、路由Job、新HTTP接收开关或新处理开关。缺表、读取异常失败关闭。
- 接收/最终退款事务的主单行锁确定先后：先提交的核销阻止后续自动退款；若作废/退款已先终结，迟到核销转人工，不伪造撤销已发生资金动作。
- 新引擎本地直接回调产生的已有事件保护保持不变；桥接不移除原有证据检查。

这是本次必须覆盖的新端改动，不能只改旧Controller而留下退款竞态窗口。

## 9. 旧分支与商户通知边界

旧核销只处理可信旧订单。在主单/批次/接收锁保护下，幂等保存mc_use_history、将旧批次未使用状态推进到已核销；已作废或核销时间矛盾转人工，不回退状态，不改订单充值终态或余额。

同一新接收事件最多创建一条旧通知任务，核销事实、通知快照与LEGACY_APPLIED原子提交。既有历史券已核销且历史时间相同，只登记幂等事实，不自动补发未知是否成功的历史通知；需要补发走明确人工审计，不以迁移自动重放旧商户副作用。

新增`mc_miniso_legacy_notify_task`：task_id varchar(96)主键、event_id varchar(96)唯一、order_id varchar(64)、org_code varchar(64)、notify_url varchar(1024)、payload longtext、status varchar(32)、retry_count int默认0、max_retry_count int默认10、next_retry_time datetime(3)、lock_owner varchar(128)可空、locked_until datetime(3)可空、version bigint默认1、last_error varchar(500)可空、created_at/updated_at datetime(3)。其余字段非空，使用InnoDB/utf8mb4。字段中文注释依次为任务编号、核销事件编号、平台订单号、商户编码、核销通知地址、冻结通知报文、通知任务状态、失败处理次数、最大失败处理次数、下次发送时间、任务租约持有者、任务租约截止时间、任务版本、脱敏失败摘要、创建时间、更新时间。

任务ID为`miniso-legacy-notify-`加与事件相同的64位SHA-256摘要；状态仅INIT、SENDING、FAILED、DELIVERED_HTTP、MANUAL_REQUIRED。建立status/next_retry_time及status/locked_until索引。旧事实提交时只插一次INIT任务，HTTP不在数据库事务内执行；租约持有者对所有结果回写校验owner/version，已完成任务不因超时旧Worker回写而回退。

旧通知保持mc_config.writeOffNotifyUrl、原AES签名、字段类型和原nonce格式；首次任务创建时冻结URL/正文/timestamp/nonce/sign。无配置仍按旧行为只保存事实，不回退新订单notify_url；不持久化aesKey，不输出payload或签名原文。其他供应商原writeOffNotify调用不在本次重构范围。

旧代码目前不校验商户业务code，不能擅自把新引擎“HTTP2xx且code=200”套给旧商户。本次旧任务仅将HTTP2xx记为DELIVERED_HTTP，明确表示网络层投递，不宣称商户业务确认；非2xx/网络失败按30秒退避，最多10次失败处理，耗尽转人工，超时3秒、租约60秒。配置化策略适用于桥接新建任务，不扫描或重建历史通知。网络不确定或租约恢复可能重复HTTP，商户须按可信订单及核销事实幂等，不承诺物理恰好一次。

如用户要求旧商户也按业务响应确认，必须先确认其实际应答合同并另行修订本节，不在实现中猜测。

## 10. 配置与部署

旧端配置命名空间为`mingchuang.callback-bridge.*`：

- mode=LEGACY为初次发布兼容默认，另一合法值为BRIDGE。仅BRIDGE模式使用本设计接收与任务路径；routing/legacy-notify开启而mode非BRIDGE是配置错误。
- receive-enabled=false、routing-enabled=false、legacy-notify-enabled=false默认关闭，便于分阶段启用。BRIDGE模式下receive-enabled=false必须返回HTTP503，不得回退旧同步处理，路由/通知Job仍可独立恢复。
- key2为空默认；接收开启必须通过部署secret/env显式注入与本次名创接入身份一致的独立key2，禁止回退旧mingchuang.key2、主动key或商户密钥，禁止轮询多把key。不修改旧主动baseUrl/tripartite/key。
- batch-size=100、route-retry-backoff-seconds=60、max-route-retry-count=100、lease-seconds=60。
- legacy-notify-max-failures=10、legacy-notify-retry-backoff-seconds=30、legacy-notify-timeout-ms=3000。

旧端新增XXL handler `minisoCallbackBridgeRoute`与`minisoCallbackBridgeLegacyNotify`，分别建议5秒一次、参数limit=100；新端复用`minisoWriteOffProcessJob`。只注册受控Job，不新增匿名公网手工触发接口。处理开关打开不代表调度已配置，发布时必须留存实际扫描证据。

mode=LEGACY时旧接口才走原旧模式，仅移除敏感日志；BRIDGE模式的接收暂停与兼容模式不是一回事。BRIDGE模式停路由/通知任务不影响已开启的可靠接收。暂停/回滚优先保持BRIDGE、关闭receive-enabled，返回503并清点待处理事件；不得用切回LEGACY代替暂停而造成双处理。退出BRIDGE或回滚到不理解本契约的旧JAR，必须先停止相关入口流量、排空/登记接收事件与通知任务，并证明没有仍需新路径处理的新券回调；无法证明时禁止回滚。

发布顺序：

1. 新引擎发布0023及共享证据资金保护，新HTTP接收保持关闭；验证结构、处理Job及空队列健康。
2. 旧测试系统发布Java8代码，所有桥接开关关闭；核对主数据源、事务、权限和原模式兼容。
3. 先配置/验证新核销处理Job和旧路由/旧通知Job，再开启旧桥接接收；新端无需开放HTTP回调。
4. 先本机离线签名向量及受控数据库/HTTP桩验证，再检查唯一真实回调URL、外部HTTPS、DNS和Nginx确实进入test188测试入口。不能用生产地址成功探针代替测试路由证据。
5. 只有独立测试数据范围和供应商路由明确后才安排一笔新单及一笔旧单核销。若供应商唯一地址实际属于生产旧实例，本次不自动切换，须先单独确认隔离/路由范围。

回滚应用不删除新事件、共享inbox、旧通知任务或资金保护；已有可靠接收事件必须排空/登记，不通过清表或降级到旧同步入口掩盖积压。

## 11. 测试与观测

- Java8验签固定向量与Java21交叉：正常、UTF-8转义、重复键、额外字段、头重复、超限、错误签名、同券同时间重投/异时冲突；失败日志无秘密。
- 接收事务提交失败/结果未知、回执丢失、服务重启及重复投递收敛；接收提交后无新事件前的资金保护。
- 新旧归属、未知后凭据补齐、双归属/多行、缺表/数据库故障不回退、批次/手机号/商户/订单错配。
- H2/MyBatis/Spring真实事务证明新事件插入与TRANSFERRED_NEW整事务回滚、重复新事件不回退终态、owner/version失效不提交副作用。
- 并发接收、路由租约过期、作废/退款与接收锁顺序；新端处理关闭时桥接证据仍保护资金。
- 旧核销事实与通知任务原子提交、已核销历史券不自动补历史通知、旧协议/空URL/HTTP层确认、超时/重投/耗尽、CAS防迟到回写。
- 两项目回归及迁移解析。旧项目显式使用JDK8及-Dmaven.test.skip=false；新工程JDK21，独立simulator单独运行，不把675/77历史结果当作本次通过。
- 指标至少区分首次接收/重复/冲突/存储失败、未知与人工积压、最老事件年龄、新事件投递、旧事实应用、旧通知HTTP投递与失败/耗尽、最后成功扫描时间。高频异常采用采样/限频，记录eventId及可信order/org，不记录完整券码、手机号或payload。

所有集成/部署证据追加本仓库docs/集成测试.md；旧项目说明链接本契约版本。接口和调度新增项在实现处写中文注释并链接主详细设计共享分流章节；暂未实现的边界必须明确TODO，不能用成功回执占位。

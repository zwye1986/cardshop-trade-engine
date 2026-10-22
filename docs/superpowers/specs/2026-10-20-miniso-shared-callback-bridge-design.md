# 名创单入口、共库核销分流设计

日期：2026-10-20。契约版本：`MINISO-SHARED-CALLBACK-V1`。
状态：2026-10-21用户已明确“评审通过”，本详细设计获批，Task1—18已本地实现，Task19协议/回归/发布交接已执行，旧端完整回归仍有阻断；尚未共享迁移或部署。旧项目保持Java8，新引擎保持Java21。设计通过不替代具体测试发布授权。

本稿从原会话工作区恢复至`1020_MINISO_SHARE_DB`工作区，继续依据当前代码复核。用户已分别明确确认两项资金规则：**释放授权一旦提交，即使Redis尚未执行，之后到达的核销也只转人工，不自动撤销授权**；**名创可能已发券但本地没有券码或履约凭据时，先按原订单号查单补证据，仍无法确认则转人工，期间不自动退款**。两项规则不再重复征询。随后用户整稿评审通过，包含第6节券码互斥表及第8节持久授权细化。主详细设计9.8及高可用设计的同名章节同步记录此状态，实施步骤见`docs/superpowers/plans/2026-10-21-miniso-shared-callback-bridge-implementation.md`；本地实现及可丢弃MySQL迁移已完成，未执行共享DDL发布。

## 1. 范围与现场证据

- 以下远端信息来自前次会话的历史核验，本轮仅做本地静态评估，未重新连接主机、Nacos或数据库。目标为test188，`/mnt/bin/cardShop-admin.jar`，当时PID18924，profile=dev，HTTP9191。Nacos namespace=`cardshop-dev`，group=`dev`，DataId包含`cardshop-database.yml`、`cardshop-admin-dev.yml`；PID、端口及路由须发布前重新核验。
- 新旧测试系统实际共用`cardshop_test_new`。旧JVM持有3306连接的本地端口45156，与RDS PROCESSLIST的`172.19.64.43:45156`精确对应该schema；新引擎配置同schema。dev配置还存在指向同schema的no-binlog数据源，但桥接只能使用主数据源，不得在同一业务事务内切换数据源。
- pro140 `/home/kalaiwang/bin`连接cardshop的旧生产实例不是本次改造/发布目标，不改其JAR、数据源、Nacos、回调地址或生产数据。
- 当时旧测试JAR SHA-256为`778c320f0477f561c3b4523c5aef41afb936760c7becaa14cfeeecdc886755a7`。旧代码基线位于`/Users/mac/Documents/codeSpace/cardshop`；本轮该仓库已有未跟踪的接入说明，不修改或覆盖。当前新引擎工作区分支为`1020_MINISO_SHARE_DB`，从已合入交行通知协议枚举/编码器的基线继续，不覆盖其他工作区的并行修改。
- test188 Nginx中存在apitest.kalaiwang.com默认代理9191、managetest.kalaiwang.com/prod-api代理9191的候选路径；本机配置不证明当前DNS、公网TLS或名创登记地址。真实供应商核销验收前必须单独核实名创唯一URL及其实际路由；不能为了测试改动生产全租户回调。
- 旧测试mingchuang配置与新引擎真实MINISO配置的baseUrl/key/key2不相等，tripartite未配置。桥接不能直接依赖其旧key2，也不能将旧主动发券参数静默切成真实通道。

## 2. 方案及非目标

保留名创既有对外URL和旧接口`POST /api/common/mcNotify`。旧端完成验证与可靠接收，后台在共享数据库内根据可信券归属处理或投递；新单不使用HTTP转发、不另设新公开地址，不要求新引擎callback.enabled=true。

未采用：

1. HTTP转发至新引擎公开回调：增加网络不确定性、原签名留存与密钥轮换期间重试合同，当前共库不需要。
2. 向新旧两端广播或未知直接走旧：可能造成重复通知、资金保护缺口和归属错误。
3. 给新单补造mc_batch_record：把两套履约/状态/通知模型混合，不能替代新引擎事件与退款保护。

本次不改变发券、扣款、批次选择、库存的业务模型，但必须修改名创凭据首次发布、作废终态和退款的事务保护，否则回调先到、凭据后到仍会漏保护。旧退款的订单归属排除适用于所有新引擎订单，不限MINISO；不借此重构其他供应商的退款模型。

本桥接任务不改新旧商户通知协议、不独立实现银行2104；新单投递后按既定通道处理，不能为A35退回旧AES或以直连HMAC通知冒充银行验收。合并dev后独立2104实现及其SDK/TLS验证记录一并保留，以对应银行章节和执行日期为准；桥接通过不代表银行通知通过。不自动迁移或补发历史商户通知，不对真实名创压测。

资金裁决候选方案：

1. **已批准采用：持久释放授权＋券码实体互斥行。** 在订单事务内唯一裁决，锁外执行Redis；未知归属回调与已知订单可锁同一券码，不依赖不存在行的gap lock。代价为新增一张小型互斥表和扩展表授权字段。授权提交后迟到核销不自动撤销授权的业务边界、表结构及事务实现细化均已获用户整稿批准。
2. 固定MySQL隔离级别并依赖事件/inbox缺失键的gap/next-key锁：表少，但强依赖索引、隔离级别及所有写入者的语句和事务边界，不能解决独立检查事务返回后锁已释放的问题，不作为推荐合同。
3. 仅补几次核销查询、或仅把CANCELLED当释放许可：改动少，但不能统一FAILED退款，也不能解决Redis已释放而恢复重新被迟到核销挡住的问题，不接受作为完整修复。

## 3. 模块与契约

旧项目在现有admin模块内增加内聚的桥接包，不新增Maven模块或共享运行时JAR。入口仅暴露“接收已签名核销请求”和“执行一批任务”；Controller不包含跨表SQL或网络投递。验签、归属、事务投递、旧通知各依现有边界实现，不要求为每个步骤新增一层接口。

旧端不得引入Java21模块、Spring Boot3依赖；新旧复用的是版本化SQL/事件契约及固定离线向量，不是共享Spring上下文或JAR。新引擎领域层不依赖旧表；公共接收表的读取与保护位于infrastructure。

新引擎是桥接DDL唯一发布者：已追加0023迁移，建立接收、旧通知、券码互斥三表，并为recharge_order_ext追加释放授权字段（见8.3）；不修改已发布0020—0022。实际ID为0023-miniso-shared-callback-bridge，未修改0020—0022。旧项目只做结构版本核验，避免两套Liquibase争夺同一表。未部署该版本的旧实例不要求新表；部署新保护代码后，旧退款归属隔离及资金保护不得随桥接开关关闭，相关表缺失或关键列不兼容须失败关闭，不能回退旧处理。

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
- order_id varchar(100) utf8mb4_bin NULL：可信平台订单号，与现有凭据/事件表长度一致。
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

路由Job采用短事务抢占＋独立业务事务，先提交租约，不持接收行锁等待主订单。执行及回写都校验owner/version及有效租约；业务事务最后CAS失效则全部副作用回滚，不能只跳过inbox回写却保留新事件/旧核销。租约60秒，单批100，失败退避60秒，默认最多100次失败处理（配置化，不删除耗尽事件）。数据库不可用不得判为旧订单；未知归属等待，耗尽转人工。死锁/锁超时整事务回滚后退避，不作为无核销证据或已处理。

先进行无锁候选查询，再按统一顺序拿锁复查：

`mc_order → recharge_order_ext归属复核 → 凭据（recharge_miniso_receipt或mc_batch_record）→ mc_miniso_coupon_guard → recharge_miniso_writeoff_event（如有）→ mc_miniso_callback_inbox → 任务/结果CAS`

入口已知新单或旧单归属时均从主单开始；旧单也须复核不存在该订单的新引擎扩展记录。候选查询仅作定位，不能将普通快照查询的“无记录”直接用作资金裁决。

### 6.1 券码互斥覆盖凭据尚未落库的接收

已实现`mc_miniso_coupon_guard`，仅作事务互斥，不保存核销、归属或资金状态：coupon_code varchar(128) utf8mb4_bin NOT NULL主键，COMMENT为“名创券码”；created_at datetime(3) NOT NULL，COMMENT为“创建时间”。InnoDB/utf8mb4，表COMMENT为“名创券码事务互斥表”。通过精确重复主键的幂等建行及`SELECT ... FOR UPDATE`锁住实体行，持有至业务事务提交/回滚；不能将其他数据库错误吞成重复键成功，也不能依赖不存在行锁或独立事务提前释放的锁。

- 已认证但未知归属：只按`guard → 新事件（新端直接入口如适用）→ inbox（旧桥接入口）`接收证据并提交，不在持guard时再锁主单或凭据。即使此时凭据已经出现，也留给下一事务从主单重新定位，避免“凭据发布持凭据等guard，接收持guard等凭据”的死锁。
- 新`recharge_miniso_receipt`、旧MINISO `mc_batch_record`的首次发布，以及后续会改变核销/作废事实的路径，都须遵守相同锁序和guard协议；首次发布时从本次可信供应商响应取得券码，在写入凭据前锁guard。已有订单已有券码的授权事务也锁同一guard。
- guard后对事件/inbox进行当前锁定读，不复用事务早先的普通快照读。guard确保即使事件/inbox尚无行，同券接收与裁决也不能交错提交；既有证据不得删除或迁移成短暂无证据的状态。
- 新端直接入口即使当前关闭，也须纳入协议；旧LEGACY回调或凭据发布若仍绕过guard，不能宣称已具备并发安全。开通前须证明全部相关实例已升级或停止写入。
- guard行不是已认证证据，单独存在不阻止退款；未验签请求不得创建guard或inbox。暂不自动清理guard，清理需另行证明无凭据/迟到回调/资金任务仍引用，不能按TTL随意删除锁载体。

guard只解决双方均能定位同一券码的竞争。订单无券码时不能凭空关联未知inbox；无凭据退款资格的限制见8.4，不用guard表掩盖此边界。

旧订单可能多券：核销接收仍逐券判归属，不强加新引擎num=1限制。整单作废/退款须证明全部券与订单及金额的完整对应，并在同事务先锁全部凭据、再按券码固定顺序锁全部guard、最后读取事件/inbox；任一券有核销证据即阻止整单自动退款。无法证明完整对应则补查/人工，不从现有getOrderInvalid首券结果推出整单可退；本轮不另行定义部分券退款协议。

归属规则：

- NEW_ENGINE：唯一新凭据、主订单、DB_PERSISTED扩展行的订单/商户一致；主单渠道MINISO、数量1，凭据批次/手机号与冻结订单一致。不能只因同一mc_order存在就确认新单。
- LEGACY：唯一MINISO/state_type=3的mc_batch_record能关联可信主单，且无对应新凭据、无该订单的新引擎归属扩展记录。旧历史主单supplier_channel可能为空，不强套新单字段完整性要求。
- 无匹配：RETRY。新扩展已存在但凭据迟到，不回退LEGACY。
- 多条匹配、双归属、身份/批次矛盾：MANUAL_REQUIRED，不猜归属、不广播。已确认route_target/order_id不因后续另一条脏记录自动切换；矛盾转人工。

## 7. 新事件事务投递

在上述锁序内，按0021合同插入recharge_miniso_writeoff_event：eventId/couponCode/useTime与接收表相同，可信org/order来自锁后校验，status=INIT，retry_count=0，version=0（与现有DDL及mapper一致），next_retry_time取当前时间。新事件的received_at取共享inbox首次received_at，updated_at为本次写入时间；不能用异步投递时间重置接收年龄。共享inbox自身version初值仍为1，二者不是同一个版本号。

新事件已存在且事实完全一致时视为幂等投递，不改其状态、重试或时间；同ID/券码不同核销时间、不同可信归属必须冲突，不覆盖。

若既有事件的order_id/org_code为空：仅在INIT/RETRY且非空字段均与本次可信归属一致时，按锁定version CAS补齐缺失字段；不重置计数、到期时间、received_at或事实。PROCESSED必须已有完整匹配归属；MANUAL_REQUIRED不自动恢复或补成已处理，转人工核查。已投递不等于新端已核销，不能以新事件存在绕过人工状态。

插入/确认新事件和接收表TRANSFERRED_NEW以**同一主数据源连接、同一数据库事务**提交；任一步失败全部回滚。旧端不直接更新新凭据、voucher_*、余额或新通知任务。新引擎既有minisoWriteOffProcessJob及通知Worker继续各负其责。

不得使用no-binlog数据源；不得在事务内异步切线程、另开数据源连接或依赖跨事务HTTP确认。

## 8. 资金保护与唯一释放裁决（已实现，本地恢复验收通过，未发布）

### 8.1 复核发现及现有边界

本轮静态核对未证明资金事故已经发生，也未确认远端任务启停。下列是代码可达风险，不是线上复现结论：

- 旧`McOrderServiceImpl.orderRefund`按order_status=6扫描且不排除新扩展，依据共享mc_depart_record向mc_ext_depart回补；旧`singleRefundMethod`和V2退款方法也有直接资金写入。只关闭一个Job不能保护其他调用入口。
- 旧`McOrderV2ServiceImpl.orderInvalidCore`普通读后调用远端，MINISO供应商adapter随后直接更新批次为作废，SQL未限制原状态，主单再单独改6。已接收核销可被覆盖，批次与主单可能只成功一半。
- 新`MybatisMinisoWriteOffProtection.blockInvalidation`为REQUIRES_NEW，返回时锁已释放；Worker随后分步执行Redis、RELEASE delta和释放标记。作废终态事务的复核只在携带MINISO凭据时触发，不能视作所有退款入口的最终授权。派单/查单FAILED分支也直接进入公共释放方法。

两个必须被实现测试打破的旧序列：

1. 门禁返回无证据 → 回调提交 → Redis回补。只再加一次查询，仍会在查询后留下窗口。
2. Redis已回补 → delta未入库时宕机 → 核销提交 → 恢复重新门禁被挡 → delta无法补齐。正常路径和恢复路径不能重新作不同资金裁决。

### 8.2 新旧资金所有权隔离

旧退款候选查询先排除存在recharge_order_ext的订单；最终任何旧资金/退款流水/订单退款状态写入前，在主单锁内重新复核。**扩展行存在即禁止旧端退款**，不以DB_PERSISTED、渠道或balance_reserved筛除；扩展损坏或身份矛盾转人工，不能误降为旧单。新引擎订单主单与扩展的首次发布必须保持同事务，不允许先提交主单、以后再补归属。

所有能处理该类mc_order的旧入口均执行上述限制：定时和内部任务、直接/单笔退款、V2实现、名创造废入口。不能因注释“未用”、目前任务关闭或当前无订单而豁免可调用代码。单纯充值/商户转账及独立批量采购总单不是本桥接退款范围，但实施前须确认没有通过它们按新单身份退款的入口。不能根据所有refundExtDepartBalance调用同名就全盘重构余额服务。

旧订单保留原资金模型：在单笔主数据源事务内锁主单、复核无新归属、锁凭据/guard并当前读核销证据，再原子完成余额回补、退款流水及退款状态。任一步异常必须回滚该订单，不沿用noRollbackFor或吞掉流水失败后提交余额的处理；批量任务逐订单短事务，不持全批锁。

### 8.3 新引擎持久释放授权

本节授权合同限定MINISO，其他供应商维持既有失败/释放语义，不在本轮迁移其状态机。拟在recharge_order_ext新增：

- balance_release_authorized_at datetime(3) NULL，COMMENT“余额释放授权时间”。
- balance_release_event_id varchar(100) NULL，COMMENT“余额释放授权事件编号”；冻结首次获授权的现有履约outbox事件ID，后续其他事件也必须复用，不因重试入口改变Redis去重键。
- balance_release_amount bigint NULL，COMMENT“余额释放授权金额，单位分”；来自锁定可信订单的原预扣金额，不取新请求金额。

三字段全空表示未授权，均非空且金额大于0表示已授权，部分为空属于不一致而非可放行状态。仅对DB_PERSISTED、balance_reserved=1且预扣身份/金额证据一致的订单首次授权；原本未预扣的订单不制造退款许可。授权只有首次可写，身份依所属扩展/主单商户、订单、outOrderSn绑定。复用balance_reserved：已授权且为1表示执行/恢复中，为0表示Redis释放已确认、RELEASE delta已可靠入库；这不表示delta已刷至mc_ext_depart。无需再增加与balance_reserved重复的COMPLETED状态。

首次作废完成/明确失败事务按主单→扩展→凭据→guard→证据顺序当前读复核；只要存在凭据已使用/use_time、新核销事件或任意状态的已认证共享inbox，就不能产生授权。未知/人工/已转投状态的证据同样有效，不以归属冲突放行。没有证据且具备退款资格时，**终态与授权同事务提交**；所有MINISO CANCELLED写入必须执行门禁，即使调用者未携带凭据对象，也不能跳过。

唯一裁决顺序取guard保护下的事务提交顺序，不取HTTP到达时间、应用时间戳或供应商useTime：

- 核销接收先提交：禁止后续自动授权；作废/退款矛盾转人工。
- 释放授权先提交：后到核销仍可靠保存，但转人工；不自动撤回授权、不回退终态、不阻断该授权的剩余资金步骤，也不自动二次扣款。**2026-10-20用户已确认：即使Redis尚未执行，迟到核销也不自动撤销已提交授权。**
- 授权事务回滚：没有资金许可；不能据内存检查结果、远端作废成功或孤立CANCELLED字段调用Redis。

授权后锁外执行：读取并校验已提交授权 → 用冻结eventId/amount执行既有幂等Redis release → 确认RELEASED/ALREADY_RELEASED → 在同一MySQL事务幂等插入/核对`balance-flush-release-{orderId}`的RELEASE delta并将balance_reserved置0 → 完成原履约事件。已存在delta须比对订单/商户/金额/类型，不将“insert ignored”当作一致证明。HTTP和Redis均不在上述MySQL事务锁内执行，不试图以本地事务实现跨库原子性。

派单FAILED、查单FAILED、作废成功和三类终态恢复全部走同一授权入口；**先识别已有授权，再判断是否需要首次核销门禁**，不能像当前handleInvalidOrder/completeTerminalBalanceRelease那样先因迟到核销阻断已授权恢复。保留原履约outbox为恢复载体，不新增释放调度体系；授权引用的任务未完成前不能被普通终态分支提前标SUCCESS，耗尽转人工仍保留授权/余额待处理清单。授权后的通知创建失败也不能使资金恢复入口丢失。

迟到核销的人工状态属于核销事件/inbox，不得据此把已授权资金恢复任务转人工而截断后续步骤；资金任务仍可因自身Redis/存储错误退避或耗尽转人工。其他迟到查单/终态写入也必须在主单锁内识别已有授权，矛盾转人工，不把已授权订单改回可派单/正常成功并覆盖退款事实。

断点恢复的预期：授权后未调Redis则执行；Redis已执行但响应丢失/delta未写则幂等重放补齐；delta及释放标记事务失败则整体重试；已完成但原任务未SUCCESS仅收尾。各点遇到迟到核销均保存冲突并按同一既有授权继续，不重新裁决。

保护随结构与真实持久化适配装配，不依赖旧接收开关、路由/处理Job或新HTTP开关。缺表、读异常、锁超时失败关闭；新端已有本地事件保护保持。历史已有Redis释放或CANCELLED/FAILED但无授权字段的订单不能自动补认授权，发布前盘点并人工核验，不盲目回填时间或重新执行余额回补。

### 8.4 无券码/凭据与旧作废的边界

有可信券码的路径使用guard；没有券码就无法证明某个未知inbox属于本订单。**用户已接受：MINISO可能已发券但本地没有券码或履约凭据时，先按原orderNo查单补证据，仍无法确认则转人工，期间不自动退款**。不得把“查不到凭据”等同“从未发券”，也不得因为查单次数耗尽、时间超过阈值或转人工就自动授权退款。

只有供应商合同明确证明未发券的业务失败才可进入无券码授权路径；具体可用失败码/证据属于实施前协议核验项，未明确核验的失败码不作自动放行依据，不在实现中猜码。其他供应商维持其既有失败合同。此项接受仅确定不明结果的保守处理规则，不代表任何具体失败码已被核验。

旧MINISO作废按三段处理：短事务前置复核 → 锁外远端调用 → 短事务最终复核及提交。MINISO供应商adapter只返回已核验本券的远端结果，不直接改批次；最终事务从主单/归属开始、经凭据/guard当前读证据，按原状态“未使用”条件更新批次作废时间并更新主单为旧端既定失败语义。更新行数或事实不一致转人工，不覆盖核销，不在旧端套用新端12/CANCELLED状态模型。

旧作废终态不等于旧退款完成：随后旧退款事务仍在相同guard下检查核销，余额/流水/退款状态同事务提交；先已退款再来的核销保存冲突并转人工。旧端在远端调用期间发生核销而远端又报告作废成功时，保留矛盾证据，不自动退款。

### 8.5 已批准规则与实施门禁

用户已分别确认“释放授权提交后不因迟到核销自动撤销”和“可能已发券但缺券码/凭据时先按原订单号查单，仍不明则人工，期间不自动退款”，并于2026-10-21整稿评审通过。本轮资金取舍及互斥/授权设计不再列为未决问题，现进入实施计划；失败码证据核验、唯一供应商URL及旧任务启停仍属于实施/发布门禁，不能用设计确认或本轮静态核对替代。

## 9. 旧分支与商户通知边界

旧核销只处理可信旧订单。在主单/归属/批次/guard/接收锁保护下，幂等保存mc_use_history、将旧批次未使用状态推进到已核销；已作废、已退款或核销时间矛盾转人工，不回退状态，不改订单充值终态或余额。旧表无本事件唯一约束时，由唯一inbox及同券guard实现桥接幂等；历史同券多条矛盾记录不能用MAX取一条掩盖。

同一新接收事件最多创建一条旧通知任务，核销事实、通知快照与LEGACY_APPLIED原子提交。既有历史券已核销且历史时间相同，只登记幂等事实，不自动补发未知是否成功的历史通知；需要补发走明确人工审计，不以迁移自动重放旧商户副作用。

新增`mc_miniso_legacy_notify_task`：task_id varchar(96)主键、event_id varchar(96)唯一、order_id varchar(100) utf8mb4_bin、org_code varchar(64)、notify_url varchar(1024)、payload longtext、status varchar(32)、retry_count int默认0、max_retry_count int默认10、next_retry_time datetime(3)、lock_owner varchar(128)可空、locked_until datetime(3)可空、version bigint默认1、last_error varchar(500)可空、created_at/updated_at datetime(3)。其余字段非空，使用InnoDB/utf8mb4。字段中文注释依次为任务编号、核销事件编号、平台订单号、商户编码、核销通知地址、冻结通知报文、通知任务状态、失败处理次数、最大失败处理次数、下次发送时间、任务租约持有者、任务租约截止时间、任务版本、脱敏失败摘要、创建时间、更新时间。

任务ID为`miniso-legacy-notify-`加与事件相同的64位SHA-256摘要；状态仅INIT、SENDING、FAILED、DELIVERED_HTTP、MANUAL_REQUIRED。建立status/next_retry_time及status/locked_until索引。旧事实提交时只插一次INIT任务，HTTP不在数据库事务内执行；租约持有者对所有结果回写校验owner/version，已完成任务不因超时旧Worker回写而回退。

旧通知保持mc_config.writeOffNotifyUrl、原AES签名、字段类型和原nonce格式；首次任务创建时冻结URL/正文/timestamp/nonce/sign。从锁定的可信订单、券与本次useTime组装，不复用现有getOrderInvalid的首券JSON/MAX(use_time)/未限定channel的JOIN查询；不把另一渠道或另一张券的时间带入。无配置仍按旧行为只保存事实，不回退新订单notify_url；不持久化aesKey，不输出payload或签名原文。其他供应商原writeOffNotify调用不在本次重构范围。

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

mode=LEGACY仅供首次兼容发布核验原入口，不代表具备本方案资金安全保证；完成新旧全部写入者升级、切入BRIDGE后不能再有LEGACY入口并发写名创事实。退款归属隔离和已装配资金保护不随mode关闭。BRIDGE模式的接收暂停与兼容模式不是一回事。BRIDGE模式停路由/通知任务不影响已开启的可靠接收。暂停/回滚优先保持BRIDGE、关闭receive-enabled，返回503并清点待处理事件；不得用切回LEGACY代替暂停而造成双处理。退出BRIDGE或回滚到不理解本契约的旧JAR，必须先停止相关入口流量、排空/登记接收事件与通知任务及未完成释放授权，并证明没有仍需新路径处理的新券回调；无法证明时禁止回滚。

发布顺序：

1. 本轮资金规则确认、两项目离线及真实MySQL并发验证通过后，申请具体测试发布授权；本轮设计请求不视为环境写入许可。核验唯一URL、HTTPS、DNS、Nginx及供应商接入身份，若实际指向生产或身份会覆盖生产流量则停止，不自行切换。
2. 盘点共库所有名创写入者、旧退款调用入口、运行版本、旧队列及无授权历史终态，制定受控暂停窗口；仅限测试名创及受影响退款任务，不停共享生产服务。未经另行授权不执行暂停或发布。
3. 新引擎发布0023、guard与释放授权/共享证据保护，新HTTP接收保持关闭；旧测试系统发布Java8代码，桥接开关仍关闭。滚动期间不安排新名创测试订单或并发回调，未升级实例不得继续相关事实写入。
4. 两端完成结构、主数据源、事务、权限及全写入者协议验证，配置并证实新处理/旧路由/旧通知Job实际扫描；切入BRIDGE后开启旧接收，再按顺序恢复受控任务。切换期无法可靠接收时返回503，不能成功丢弃；供应商实际重试能力须预先确认。
5. 只有隔离数据和路由都明确后才安排新单、旧单真实核销；另用不同券验证作废，不对同一券同时要求核销与作废成功。银行2104单独验收，不能以旧通知或本地桩替代。

回滚应用不删除新事件、共享inbox、旧通知任务、guard或释放授权，不移除已接收证据的资金保护；已有可靠接收事件及未完成授权必须排空/登记，不通过清表或降级到旧同步入口掩盖积压。回滚至无guard/授权认识的JAR前必须停掉相关资金写入者，不能一面回滚一面宣称保护持续有效。

## 11. 测试与观测

- Java8验签固定向量与Java21交叉：正常、UTF-8转义、重复键、额外字段、头重复、超限、错误签名、同券同时间重投/异时冲突；失败日志无秘密。
- 接收事务提交失败/结果未知、回执丢失、服务重启及重复投递收敛；接收提交后无新事件前的资金保护。
- 新旧归属、未知后凭据补齐、双归属/多行、缺表/数据库故障不回退、批次/手机号/商户/订单错配。
- H2/MyBatis/Spring真实事务证明新事件插入与TRANSFERRED_NEW整事务回滚、重复新事件不回退终态、owner/version失效不提交副作用。
- 同生产候选版本及隔离级别的**可丢弃本地MySQL**验证并发与锁，H2不替代InnoDB锁证据。分别覆盖READ COMMITTED、REPEATABLE READ，实体guard正确性不能只在有gap lock的环境通过；保存真实版本/隔离级别和事务屏障顺序。
- 用可控屏障构造：未知接收查无凭据后暂停 → 凭据发布 → 释放授权竞争；分别让接收先提交和授权先提交，断言前者无Redis回补、后者保留授权且迟到事件转人工。不用sleep概率或仅断言无异常代替顺序验证。
- 作废终态携带/不携带凭据均执行最终门禁；MINISO派单FAILED、查单FAILED、CANCELLED正常及恢复都不能绕过授权。无券码但结果不可确认不自动回补；明确未发券的业务拒绝与不明结果输入必须得到不同结论，部分为空的授权及金额错配均不得执行。
- 授权提交前回滚、提交后未调用Redis、Redis成功但响应丢失、Redis成功但delta未入库、delta/标记事务中断、原任务完成前中断各点注入崩溃；恢复期间插入迟到核销，验证只回补一次、仅一条正确RELEASE delta、标记一致、授权不变。
- 旧Job候选出现新引擎FAILED单、候选查询后归属发生变化、单笔/V2绕过Job调用、批次条件更新失败和退款流水插入失败；断言旧端不修改新单资金，旧单整笔回滚。存在新扩展但状态/身份损坏也不得按旧单退款。
- 旧单两券中仅非首券已核销，整单自动退款必须被阻止；凭据数量/金额对应不完整不得仅按第一张券释放整单金额。
- 路由租约过期后旧Worker仍持有执行结果：最终CAS失败必须整事务回滚，无孤立新事件、旧核销或通知；新端处理关闭时桥接证据仍保护首次授权。
- 旧核销事实与通知任务原子提交、已核销历史券不自动补历史通知、旧协议/空URL/HTTP层确认、超时/重投/耗尽、CAS防迟到回写。
- 两项目回归及迁移解析。旧项目显式使用JDK8及-Dmaven.test.skip=false；新工程JDK21，独立simulator单独运行，不把675/77历史结果当作本次通过。
- 指标至少区分首次接收/重复/冲突/存储失败、未知与人工积压、最老事件年龄、新事件投递、旧事实应用、旧通知HTTP投递与失败/耗尽、最后成功扫描时间。高频异常采用采样/限频，记录eventId及可信order/org，不记录完整券码、手机号或payload。

所有集成/部署证据追加本仓库docs/集成测试.md；旧项目说明链接本契约版本。接口和调度新增项在实现处写中文注释并链接主详细设计共享分流章节；暂未实现的边界必须明确TODO，不能用成功回执占位。

## 12. 本轮结论与进入实施的门禁

本轮仅恢复、修订方案及核对新旧源代码，没有执行Maven、MySQL并发测试、共享迁移、远端核验或真实核销。第11节是**待执行验收要求**，不是通过记录；本轮不向《集成测试.md》新增虚构执行结果。

总体架构保留，原三项资金风险以“旧退款所有权隔离、旧作废/退款单笔事务保护、新端持久释放授权”分别覆盖；另外补上未知归属的券码互斥、事件version/空归属及订单号长度兼容。第8.3节授权不自动撤销规则与第8.4节无凭据不明结果处理规则均已获用户确认；2026-10-21完整详细稿评审通过，进入桥接实施计划。实际部署还须失败码证据、URL/接入身份、全部写入者升级、供应商重试与独立测试范围核验。业务方案确认不等于授权部署。

## 13. Task19本地交付与未通过门禁（2026-10-22）

本节只更新实施状态，不改变MINISO-SHARED-CALLBACK-V1合同及两项已批准资金规则。唯一接收URL仍在旧测试端；新端公开callback默认关闭，processing独立控制。旧端LEGACY及三开关false为初次兼容发布默认值，切BRIDGE必须完成全部升级和切换门禁，停收返回503而不能回LEGACY混写。独立key2不回退主动key或旧测试配置；DTO/DDL合同、永久保护及授权不可变语义未改变。

可执行示例沿用固定MD5/HMAC-SM3向量，新增同时间重投、异时冲突签名和稳定event-id，固定向量只证明密码学合同；真实接收/租约/路由/资金由已有两仓库测试证明。新增Python回环HTTP测试验证期望状态与脱敏。模拟器原生MINISO仅是本地成功、不明后查单及精确作废桩；不生成银行2104假成功，不证明供应商认证或接入地址正确。

本轮完整回归：新端815项（789通过/26按既有外部设施条件跳过），模拟器83项及Python5项通过；旧端784项（727通过/2失败/45错误/10条件跳过），clean后复现相同结果。325项桥接及资金关联回归通过不能覆盖旧端全量失败。Task17—18已验收MySQL8.0.28 RC/RR、跨JVM和Redis恢复，本轮未新执行该组合；不把本地证据写成现场验收。

发布交接及唯一集成证据入口为`docs/集成测试.md`的`MINISO-BRIDGE-TASK19-LOCAL-20261022`：两候选JAR校验和和源commit、0023 checksum及master SHA、Nacos键名与计划值（无秘密）、待暂停实例/写入入口/Job、三个实际扫描、唯一URL/TLS/身份与供应商重试证据。旧端全量未绿、现场完整盘点及供应商合同证据缺失、具体test188发布授权缺失均标阻断。未push/PR/merge/部署，不改pro140，不修改全租户回调，不发真实名创请求，不补历史通知。银行2104 SDK TLS/通知验收仍独立。旧未跟踪说明保留，旧隔离工作树仅新增交接补充；已删除前端不在交付范围。

Task19发布顺序待澄清（2026-10-22）：旧配置校验只允许在mode=BRIDGE时启用routing/legacy-notify，故计划所述“先证实三个Job实际扫描再切BRIDGE”不能按字面在LEGACY下完成。已向用户提出候选细化：全部相关写入者暂停、升级及主连接核验完成后，以BRIDGE/receive-enabled=false验证扫描，最后启用接收。确认前该语义冲突维持发布阻断，不修改远端或把候选顺序写成已批准；顺序确认也不等于具体test188发布授权。

## 14. 合并dev兼容验收（2026-10-22）

合并父版本为本分支479e4c2与远端dev 2b81378。共库桥接已批准合同保留，dev早期桥接草稿不覆盖持久授权/guard/version0/100位订单号等最终语义；dev独立银行通知、成功消费确认、原始流水恢复及非MINISO快照失效/历史作废退款一并保留。普通资金入口在锁定真实归属后拒绝MINISO释放，防止以已作废或旧核销检查替代许可；MINISO仍只由Worker冻结授权路径锁外释放，再原子收尾。银行准备器与guard同时传入持久store，接收独立于处理，银行处理不因商户HMAC缺失而误拒绝。

0023原文及checksum不变，0024原文件和identity不变，master顺序先0023后0024。主工程1006项（935通过/71既有条件跳过），模拟器85项、Python9项通过；另显式开启的独立资金Redis测试及MySQL8.0.28 RC/RR、跨JVM和Redis恢复36次强制执行通过，含CONSUMED后快照删除、迟到核销、事件B恢复冻结A许可。完整证据统一见《集成测试.md》MINISO-DEV-MERGE-20261022。本次只提交/推送特性分支，不改dev，不代表新的共享迁移、部署或BRIDGE启用授权；原发布门禁继续保留。

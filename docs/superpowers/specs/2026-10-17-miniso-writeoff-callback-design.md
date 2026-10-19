# 名创核销入站与商户通知详细设计

日期：2026-10-17。设计已按用户确认的旧卡管兼容基线实现，完成本地验签、H2/MyBatis事务、WebFlux入口、恢复调度与商户通知验证；实际命令和结果见docs/集成测试.md。本轮未执行外部DDL、部署或真实供应商联调，入站和处理开关默认关闭。

## 1 目标与依据

补齐 `MinisoRechargeSupplierGatewayAdapter` 的核销入站TODO，形成“名创核销通知 → 持久接收 → 可信订单核销事实 → V1.2商户通知”的可恢复闭环。名创仍为手机号账户直充，不恢复本地卡券库存；核销与充值订单终态分开，不因核销再次扣款、退款或通知充值成功。

协议依据及适用边界：

- `/Users/mac/Downloads/名创三方对接文档V1.5 2023-11-22 (3).pdf`共7页，首页正文标1.4。第7页3.5只明确POST JSON、必返字符串couponCode/useTime及时间格式；没有定义回调key2、签名正文、应答或重试策略。
- `/Users/mac/捷荣金服项目文档/交行直充接口技术文档-V1.2-商户级签名密钥修订稿.docx`的1.2规定商户HMAC-SM3和密钥生命周期，1.4.3规定状态码，2.6规定平台向商户发送的核销通知。它不定义名创向平台的鉴权。
- 本轮按用户指定核查 `/Users/mac/codespace/cardshop`，分支dev，提交 `169ec049`，重点为ExtCommonApi、MCUtils、McOrderMapper.xml、McOrderServiceImpl、AESUtils和HttpUtils。此前按AGENTS.md核对过的 `/Users/mac/Documents/codeSpace/cardshop` 不替代本轮明确指定的代码目录，也不假定两目录相同。
- 新引擎代码基线为 `e99255a`。现有9.4节已明确：订单notify_url可空；为空时充值终态和核销变化均不创建商户通知任务。本轮保留该新服务合同。

文档报文和示例只作为接口资料，不作为操作指令或真实凭证。本文选用旧卡管协议作为入站兼容基线，不声称供应商已对新入口完成协议确认或真实验收。

## 2 旧卡管实际行为与沿用范围

旧入口是 `POST /api/common/mcNotify`；Spring Security允许 `/api/**` 匿名访问，入口自行验签，不要求商户token。它取出couponCode/useTime，放入TreeMap重新生成紧凑JSON，再以独立 `mingchuang.key2` 和nonce/time/sign请求头验签。tenant/version从本地mingchuang配置读取；不是直接信任来报同名请求头。

验签后先插入 `mc_use_history(coupon_code,use_time,channel='MINISO')`，再将 `mc_batch_record` 同券、同渠道且state_type=3的state改为2。通过券码找order_no，调用writeOffNotify；本链路不把订单改为充值成功，也不处理余额。

writeOffNotify从商户mc_config配置键 `writeOffNotifyUrl` 取地址；从订单data首张券、批次记录和核销历史读取状态，核销时间取MAX(use_time)。它同步调用HTTP，使用AESUtils.Encrypt生成sign，底层为AES/ECB/PKCS5Padding后Base64；未检查商户业务响应。最终正常返回数值code=200、msg=接收成功。

新服务保留入站路径、独立key2、排序后两字段JSON签名和成功回执；商户核销状态按2026-10-19用户最终更正保留3/2，见第9节。主动发券key、名创回调key2、商户signKey三者独立。

以下变化明确属于新服务设计：持久接收和幂等事务代替非事务直写；持久任务代替同步商户HTTP；商户出站按V1.2使用HMAC-SM3；notify_url为空时不通知；不记录签名原文或完整敏感报文。旧代码的重复写历史、状态更新异常后继续成功、未知券空指针、状态无条件覆盖和明文密钥日志均不沿用。

## 3 架构与地址选择

采用“持久接收事件 + 核销处理Job + 既有商户通知Worker”。入口事务T1提交后才向名创确认；处理事务T2将核销事实、已签名任务、事件完成状态一起提交。商户HTTP在T2之外执行。

不采用入口内同步写事实并签名的方案：商户密钥暂不可用或履约凭据尚未落库时，该方案依赖供应商重投，而资料未提供重试承诺。不增加只投MQ的方案，避免为本需求另建MQ确认与接收确认的恢复合同。

名创接收地址是新引擎的 `/api/common/mcNotify`，与商户出站地址是两种配置。核销商户通知地址按新引擎9.4节使用下单冻结的notify_url，即mc_order.callback_url；非空时必须是已校验的http/https地址。为空时只保存核销事实并完成事件，不查商户签名密钥、不生成通知任务。

本轮不读取旧卡管mc_config.writeOffNotifyUrl，不做“商户地址优先、订单地址兜底”或反向兜底，避免改变已约定的空地址行为。以后若要求恢复独立核销地址，应先单独修订9.4节、商户配置/冻结时点及验收范围，不能在实现中静默引入。

## 4 名创入站协议

### 4.1 路径与字段

请求为 `POST /api/common/mcNotify`，Content-Type为application/json，字符集UTF-8。正文最多4096字节，只接受JSON对象及couponCode、useTime两个字符串字段；拒绝缺失、JSON null、空字符串、重复JSON键、未知字段、数组、嵌套对象和隐式数字转字符串。

couponCode长度1至128字符，拒绝首尾空白和控制字符，不自动trim、改大小写或URL解码。useTime必须是19字符的严格 `uuuu-MM-dd HH:mm:ss`，按Asia/Shanghai解释，拒绝不存在的日期；不能只用正则后宽松解析。是否处于本单有效期在可信凭据就绪后判断，不在入口根据当前时间猜测。

请求头nonce/time/sign各只能出现一次。nonce限定32位小写字母数字；time限定13位毫秒时间戳字符串；sign限定32位大写十六进制。tenant/version如携带必须唯一且与本地MINISO/tripartite1.0相同；未携带时仍使用可信本地值。tripartite不参与验签。

这些格式校验是新服务的明确边界，旧代码没有完整执行；实际供应商样本在真实联调时必须覆盖，不能把源码检查称为对端已通过兼容验收。

### 4.2 签名正文

以旧卡管TreeMap及Fastjson2的输出为兼容目标，只按couponCode、useTime的顺序生成紧凑JSON。不得直接签原始HTTP字节，也不得加入未知业务字段；输入JSON键顺序和无意义空白不影响签名。

`source = key2 + "nonce" + nonce + "tenant" + "MINISO" + "time" + time + "version" + "tripartite1.0" + canonicalBody`

`sign = UpperHex(MD5(UTF8(source)))`

序列化必须显式固定UTF-8、字段顺序、空白和字符串转义方式，不依赖全局ObjectMapper配置。实施时用固定向量并交叉核对旧Fastjson2对引号、反斜线和非ASCII字符的行为；若使用Jackson，不得仅凭两者都输出JSON就认定字节相同。签名解码后使用MessageDigest.isEqual或等效常量时间比较，不能用String.equals。所有失败都不回退主动请求key、商户signKey、aesKey或JWT密钥。

### 4.3 防重放与确认

不套用商户下单的短时间窗拒绝历史核销重投；核销只允许单向生效，依靠已认证券码事件的持久唯一性防重。相同券码及useTime，无论nonce/time/sign是否变化，都只形成一次业务事件和一次商户任务。不同useTime视为冲突，不能用MAX或最新值覆盖首次事实。

格式和验签通过不等于接收成功。T1真正提交后返回HTTP 200及 `{"code":200,"msg":"接收成功"}`；完全相同的已接收事件重投也返回该结果。数据库提交结果未知、接收执行池拒绝或入口超时返回HTTP 503及 `{"code":500,"msg":"接收失败，请重试"}`，即使后台随后提交，也靠重投幂等收敛。

参数错误及验签失败保持旧接口的HTTP 200、业务code=500，固定msg分别为参数不合规、验签失败；不泄露期望签名或具体密钥信息。同券不同时间冲突返回HTTP 200、code=500、msg=核销信息冲突。超大正文返回HTTP 413，错误Content-Type返回HTTP 415，业务code均为500。入口未启用时不注册该路径，返回404，不能装配成功确认的noop。

名创重试周期、最长补发期限、真实key2及实际入口配置未由所给资料确定；不据此承诺入口故障前的通知必然不丢失。key2变更应协调供应商切换及历史重投，不能轮询尝试多把密钥。已提交的事件不因后续key2轮换而重新验签。

### 4.4 离线固定向量

以下全部为本地测试数据，无真实券或真实密钥。固定key2为 `miniso-callback-local-test-key`，nonce为 `0123456789abcdef0123456789abcdef`，time为 `1792200600000`，对应2026-10-17 09:30:00（Asia/Shanghai）。

canonicalBody为 `{"couponCode":"TEST-MINISO-0001","useTime":"2026-10-17 09:29:00"}`，预期sign为 `62088F92CF0CB4452F9A95098E60E350`。

event_id的计算规则见下一节，该券预期值为 `miniso-redeemed-ff08b00be72babc1a123564e3e203072731fd9811650618cec05951824fc3465`。本轮仅离线计算并复核这两个向量，未调用供应商、入口或商户HTTP。

## 5 数据模型与迁移

新增Liquibase changeset `0021-miniso-writeoff-events`，不重写0020。迁移只创建接收表并为履约凭据增加use_time；不创建库存表，不向旧mc_batch_record/mc_use_history盲写。新事件表承担新引擎核销接收历史和恢复记录。

以下DDL已落实到0021 changeset，作为持久化合同；本轮未在共享或外部数据库执行：

```sql
CREATE TABLE recharge_miniso_writeoff_event (
  event_id varchar(80) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '核销事件唯一ID',
  coupon_code varchar(128) COLLATE utf8mb4_bin NOT NULL COMMENT '名创券码',
  use_time datetime NOT NULL COMMENT '核销时间',
  order_id varchar(100) COLLATE utf8mb4_bin NULL COMMENT '平台订单号',
  org_code varchar(64) COLLATE utf8mb4_bin NULL COMMENT '商户编码',
  status varchar(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'INIT' COMMENT '处理状态',
  retry_count int unsigned NOT NULL DEFAULT 0 COMMENT '重试次数',
  next_retry_time datetime(3) NOT NULL COMMENT '下次处理时间',
  last_error varchar(128) NOT NULL DEFAULT '' COMMENT '最近处理错误',
  version bigint unsigned NOT NULL DEFAULT 0 COMMENT '记录版本号',
  received_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '接收时间',
  processed_at datetime(3) NULL COMMENT '处理完成时间',
  updated_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (event_id),
  UNIQUE KEY uk_miniso_writeoff_coupon (coupon_code),
  KEY idx_miniso_writeoff_due (status, next_retry_time, received_at, event_id),
  KEY idx_miniso_writeoff_order (org_code, order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='名创核销接收事件表';

ALTER TABLE recharge_miniso_receipt
  ADD COLUMN use_time datetime NULL COMMENT '核销时间';
```

DTO和record的重要类/方法写中文注释并链接两份主设计对应章节；字段注释与上述DDL COMMENT逐字一致。入口couponCode、useTime分别对应“名创券码”“核销时间”。status限定INIT、RETRY、PROCESSED、MANUAL_REQUIRED，禁止任意字符串作为业务状态。

event_id固定为 `"miniso-redeemed-" + LowerHex(SHA256(UTF8("MINISO:REDEEMED:" + couponCode)))`。useTime不进入事件身份，因为同券只允许一次不可逆核销。哈希ID只用于幂等与脱敏定位，不承担验签。碰撞或唯一键冲突后仍必须比较原券码和use_time，不能单凭哈希相同承认同一事件。

数据库datetime保存上海本地时间，所有now从注入Clock的Instant显式转换，不能混用服务器默认时区。use_time秒精度，调度和审计时间毫秒精度。order_id/org_code可在接收时为空，可信归属确认后填充；不设指向凭据的外键，允许先回调后落库。

回滚应用不删除接收事件或use_time。清理只能另立归档策略；不能因已通知或超过重试次数就删除，破坏历史重放幂等。当前不自动迁移旧系统核销历史。

## 6 接收事务T1与全局锁序

### 6.1 全局锁序

所有涉及同一名创订单的发券结果、查单、作废和核销处理，统一按“mc_order主单 → recharge_order_ext扩展行 → recharge_miniso_receipt凭据 → recharge_miniso_writeoff_event事件 → recharge_notify_task任务”获取行锁。初始按券码/事件读取只用于定位候选，不作为可信结果；拿锁后重新核对归属。

禁止处理器先锁事件再等待主单，同时入口先锁主单再等待事件，否则会形成死锁环。没有凭据的接收/退避事务只锁事件，之后不得再升级为锁主单；需要处理订单时必须从新事务按完整顺序开始。SQL保持现有单表读取约定，不新增多表JOIN。

### 6.2 接收过程

1. 在事务外完成报文解析、格式校验、验签和event_id计算，不访问供应商。
2. 若只读定位能发现凭据，T1按全局顺序锁主单、扩展行和凭据，重新核对券码映射，再处理事件。这样已接收核销与作废最终确认以相同订单锁串行；本步骤不把入口升级为业务处理或商户签名路径。
3. 若定位不到凭据，T1只接收事件，order_id/org_code先为空，不猜测商户或平台单号。后续重新定位由T2完成。该事件仍可可靠确认接收，但不代表已归属于新服务订单。
4. 使用唯一键upsert，发生重复时只做自赋值，不修改已有时间、状态、关联或重试计数；随后以锁定读取比较原coupon_code/use_time，精确一致才确认成功。禁止INSERT IGNORE吞掉截断、约束等非重复错误。
5. 首次接收设置INIT、retry_count=0、next_retry_time=now、version=0；无额外商户通知或余额副作用。外层事务代理/TransactionTemplate返回且提交成功后才生成成功响应。

相同事件正在处理时，重复回调等待有限时间的行锁；超时返回可重试失败，不提前成功。同券不同时间保留首个记录并返回冲突，不覆盖、重置或重启已完成事件。冲突留采样日志及计数，人工以脱敏eventId定位；当前不为每次非法或冲突请求新建无限审计表。

## 7 处理事务T2与重试事务T3

### 7.1 到期扫描与互斥

新增framework-free门面 `MinisoWriteOffProcessJob.runOnce(shardTotal, shardIndex, limit, workerId)`，并接入XXL handler `minisoWriteOffProcessJob`。扫描INIT/RETRY且next_retry_time到期的记录，按next_retry_time、received_at、event_id升序；分片固定使用 `MOD(CRC32(event_id), shardTotal)=shardIndex`。参数约束1≤shardTotal≤1024、0≤shardIndex<shardTotal、1≤limit≤1000，默认limit=100。

初始每节点串行处理，避免将数据库锁压力放大；多个节点仍可同时扫描，靠事务锁和拿锁后的状态/version复核保证同事件只生效一次。扫描不设持久PROCESSING租约：事务内没有HTTP，进程崩溃即由数据库回滚及释放锁，下轮扫描恢复，避免另一套租约续期合同。事务上限5秒，SQL与锁等待超时不得大于该上限；锁竞争计deferred，不消耗业务重试次数。

### 7.2 归属核验及事实推进

按券码初读候选凭据；若存在，按全局锁序锁主单、扩展、凭据，再锁事件，重新检查事件是否仍到期、状态及version。核对主单/扩展的order_id、org_code、out_order_sn完全一致，扩展DB_PERSISTED，主单MINISO、num=1，冻结supplier_batch_id数值与凭据batch_id一致，凭据org_code/order_id/phone与可信订单一致。

正常充值成功order_status=12才推进核销。2026-10-19修订后作废中保持order_status=12、dispatch_status=CANCEL_REQUESTED，持久核销证据应阻止作废并收敛中间态；历史13中间态且本地凭据仍完整时保留事件并退避；不得据此当作重新发券或充值失败。该作废流程必须按第8节检查待处理核销并退出/人工收敛，不能无限以中间态互相等待。其它处理中状态等待原履约链路补齐。已失败6/退款11与核销冲突转人工，不把终态重写为成功。

没有凭据时，只锁事件确认状态/version，返回RECEIPT_PENDING，由外层在T2结束后通过T3登记退避；本事务不获取订单锁。不能按手机号、商品、最新配置或旧卡管表猜测归属，也不能为了补证据再次发券。

凭据状态采用queryGrand的0未使用、1已使用、2已过期、3已作废；不能套用selectUserCouponList的1/2/3码表。状态处理为：

- 状态0：确认useTime在可信begin_time/end_time闭区间后改为1并保存首次use_time。
- 状态1且use_time为空：用同一有效核销事件补时间并生成一次通知；查单只有“已使用”而无时间不能伪造时间。
- 状态1且use_time相同：幂等完成，不修改时间或重复建任务；不同时间转人工。
- 状态2：允许有效期内的迟到真实核销推进为1；不根据当前已过期拒绝历史合法使用。
- 状态3：记录REDEEM_VOID_CONFLICT转人工，保留原状态，不静默采用3优先覆盖冲突。
- 时间超出凭据有效期、不可解析的旧凭据时间、未知状态或归属矛盾：保留证据并转人工。

核销事实、已签名商户任务（非空notify_url时）、事件PROCESSED/processed_at/version+1在同一T2提交。任务签名或写库失败时全部回滚。PROCESSED表示本地事实及通知交付责任已持久化，不表示商户已确认收到。

### 7.3 重试与并发保护

T2异常由外层应用服务捕获，先确认其已回滚，再用REQUIRES_NEW的T3记录固定错误类别、retry_count+1、next_retry_time=now+60秒、version+1。T3必须以捕获的event_id/version及INIT/RETRY状态作CAS更新，避免旧失败回写覆盖另一个Worker刚完成的PROCESSED或人工操作。

归属/时间/作废等业务冲突须在任何事实和任务变更前校验：只更新事件为MANUAL_REQUIRED并提交，不改核销事实或通知。若冲突在已有写操作之后才暴露，必须抛出分类冲突使T2全部回滚，再由T3按version CAS转人工，不能把半成事实和人工状态一起提交。RECEIPT_PENDING、ORDER_PENDING、MERCHANT_KEY_UNAVAILABLE及暂时数据库异常可重试；达到100次转MANUAL_REQUIRED。接收重投不清零计数；锁竞争不计一次业务失败。T3本身数据库不可用时，事件仍保留原状态，后续Job可再扫描，输出采样安全error，不能假称重试已登记。

人工重置只能针对eventId，以已知version作CAS将MANUAL_REQUIRED改为RETRY并明确重试计数策略；保留coupon_code/use_time不变。修改原核销时间、迁移旧订单、跨商户归属修复和新密钥补发另有授权流程，本设计不提供无鉴权公网重放入口。

## 8 核销与作废并发及退款防护

当前MinisoReceiptStore存在状态3优先合并，INVALID_ORDER在远端成功后改本地失败并释放预扣。接入核销时必须同时收紧这两个路径，不能只新增入口。

作废开始前按同订单锁检查：凭据coupon_status=1、use_time非空，或该券已经存在经过验签并提交的核销事件（包括INIT/RETRY/MANUAL_REQUIRED），都不能继续自动作废或退款。事件存在即代表需要先解释的供应商核销证据，不能因密钥暂不可用导致T2未完成而放行退款。

供应商HTTP在本地事务外执行，不跨网络持有订单锁。远端作废成功后的本地最终确认事务必须再次按全局锁序复核凭据和事件，使用锁定读取看最新已提交事件，不能用旧快照。如果有核销证据，回滚本地CANCELLED及券作废状态更新，作废outbox转人工，不创建作废通知、不调用Redis release、不写RELEASE余额Delta。

若作废Worker先写了CANCEL_REQUESTED/13，之后发现核销事件：未发HTTP时从明确成功凭据恢复原充值成功视图并将作废事件转人工；已发HTTP且返回不明确/矛盾时，按原orderNo查证并转人工，不能恢复时顺带发出新的充值成功通知，也不能释放预扣。T2随后在有效订单视图下处理核销事实。恢复动作与outbox人工状态在订单事务内完成，避免两边永久等待。

终态失败补偿分支 `terminalFailureAwaitingBalanceRelease → completeTerminalBalanceRelease` 也要检查同一核销证据；不能仅加在首次远端调用分支。名创释放资格检查在已持久化失败/作废事实之后进行：该终态阻止之后的T2自动推进；失败终态之前已提交的核销证据必须先由最终事务锁定读发现并阻止进入释放分支。若释放前发现新冲突，停止释放并转人工。

本地锁只能决定本地提交顺序，无法把远端核销、远端作废、Redis与MySQL变成分布式原子事务。若作废和余额释放已先完成，之后才收到迟到核销，保留失败/退款终态和核销接收事件，报警转人工对账，不自动再扣款或冲回退款。不能承诺消除供应商侧互相矛盾的结果。

MinisoReceiptStore不得把已核销/有use_time的凭据从1回退0/2，也不得以3优先无条件覆盖；查单不能清空use_time。改动仅落在MINISO凭据与作废防护分支，不改变其它渠道履约模型。

## 9 V1.2商户核销通知

新增独立 `V12RedemptionNotifyPayloadSigner`，明确输入可信订单、核销时间和任务创建时刻，不借用现有signWriteOff(order, invalidTime)。2026-10-19用户最终更正：交行侧名创核销成功保留state_type="3"、use_state="2"，作废采用3/3；use_time为事件核销时间，invalid_time=""。供应商原生凭据状态不变，历史签名任务不在正常重试中修改，见详细设计9.6节。

报文必须包含order_id、out_order_sn、timestamp、nonce_str、account、use_time、state_type、use_state、invalid_time、keyId、sign。全部业务值从可信订单和已验签事件生成；timestamp为任务生成时13位毫秒文本，nonce_str以16字节安全随机数编码成32位小写十六进制。原名创couponCode不发送给商户。

按可信orgCode加载商户当前未撤销signKey，Base64URL无填充解码后完整32字节参与HMAC-SM3；使用现有V12MessageSignatures.WRITEOFF_CALLBACK字段集合，ASCII排序并以逗号连接。invalid_time空字符串保留参与签名，sign输出64位小写十六进制，keyId参与签名。不绑定订单创建时密钥，不使用token有效期限制回调。

任务notify_type=VOUCHER_STATE、biz_status=REDEEMED、max_retry_count=100、首次next_retry_time为当前上海时间。task_id为 `"notify-miniso-redeemed-" + LowerHex(SHA256(UTF8(orderId + ":" + eventId)))`，长度小于现有varchar(100)，避免超长平台订单号拼接超限。

写任务前先按taskId查已有记录：若身份和核销业务字段一致，沿用原keyId、payload和URL，不重新签名；如身份或use_time等冲突，事务回滚并转NOTIFY_TASK_CONFLICT人工。已有原任务不因轮换而与当前新签名比较相等。正常路径由T2原子提交保障事件与任务一致，唯一键upsert只是并发兜底，不能吞掉冲突。

商户通知发送复用现有Worker和HTTP客户端：发送前按任务orgCode及原keyId检查历史密钥、签名和订单身份；撤销后禁止HTTP发送，轮换后的正常重试不重签、不改任务URL。只有HTTP 2xx且响应JSON业务code为200才认为成功；超时、非JSON或其它业务码均失败。按既有客户端兼容数值200/字符串200，文档标准响应仍为数值200。

本轮已补齐RechargeNotifyWorkerService的max_retry_count上限判断及端口markManualRequired：发送前检查retry_count>=max_retry_count，达到上限转MANUAL_REQUIRED、清理租约且不再发HTTP；claim同时核对retry_count，拒绝旧扫描快照绕过次数上限。失败次数不足上限才继续原退避，人工态不再进入到期扫描。通知失败不撤销已完成核销，PROCESSED与通知任务发送状态分别观察。

2026-10-19修订：交行侧名创作废完成通知按用户最终更正映射为state_type="3"、use_state="3"；其他直充沿文档示例2/3。核销与作废使用不同的生成方法、任务ID和业务状态，避免互相覆盖。

## 10 模块接口与装配

API新增 `MinisoWriteOffCallbackController`，原始正文按限额读取，固定回执使用Reply DTO，业务输入由application的MinisoWriteOffNotification承载；API仅依赖application。Controller收集原始正文及保留多值的请求头，调用接收用例，不访问Mapper、供应商HTTP或商户token。不要将API模块反向依赖infrastructure。

application已实现以下接口，接收、验签及恢复通过端口分离：

```java
interface MinisoWriteOffAuthenticator {
    MinisoWriteOffNotification authenticate(String rawBody, Map<String, List<String>> headers);
}
interface MinisoWriteOffStore {
    void accept(MinisoWriteOffNotification notice); // 方法返回时T1已提交
    MinisoWriteOffBatchResult processBatch(int shardTotal, int shardIndex, int limit, String workerId);
}
// MinisoWriteOffNotification只含couponCode和LocalDateTime useTime；禁止默认toString输出完整券码。
// ReceiveMinisoWriteOffService先authenticate，再调用accept；不持有Spring/MyBatis或HTTP依赖。
```

infrastructure负责 `MinisoWriteOffAuthenticatorAdapter`、`MinisoWriteOffEventMapper`、`MybatisMinisoWriteOffStore`、永久 `MybatisMinisoWriteOffProtection`、统一订单锁工具、事务执行及专属自动配置；MinisoReceiptStore在SqlSessionFactory之后补充兜底装配，避免Mapper扫描时序导致真实端口缺失。核销签名器留在application并通过商户凭证端口取密钥；主单/扩展/凭据读取、eventId和状态常量使用明确类型，不散落魔法字符串。job模块提供runOnce门面，bootstrap负责XXL handler和dev/test人工入口装配。

入口阻塞工作提交到独立有界执行器，默认4线程、队列256、拒绝策略立即失败；MySQL不运行在Netty事件循环。正文在入队前按字节限额检查。排队/数据库超时不得返回成功；资源释放由Spring生命周期管理。处理Job不占用入口线程池，商户HTTP继续使用既有通知执行通道。

## 11 配置与调度

配置属于 `trade.engine.supplier.miniso.callback`，不复用商户配置：

```yaml
trade:
  engine:
    supplier:
      miniso:
        callback:
          enabled: false
          processing-enabled: false
          key2: ${MINISO_CALLBACK_KEY2:}
          batch-size: 100
          retry-backoff-seconds: 60
          max-retry-count: 100
          max-body-bytes: 4096
          receive-threads: 4
          receive-queue-capacity: 256
          transaction-timeout-seconds: 5
```

enabled控制新入站，processing-enabled控制已提交事件处理。两者默认false，不破坏当前local/mock启动。启用入站必须同时启用处理、具有非空独立key2和真实MySQL/事务/事件/通知/密钥存储依赖；缺任一项启动失败，不静默缺Bean或回退noop。processing-enabled单独开启不要求key2，允许关闭入口或移除已轮换入站密钥后继续处理已认证事件。

回调接收和处理都不受supplier.http-enabled、miniso.enabled或停接新单开关连带关闭。停接新单时仍保留已存在订单的核销与通知恢复。停入口时只改enabled=false，保留processing-enabled=true及商户通知Worker；停处理会积压，不能解释为没有事件。作废/退款保护端口随真实MyBatis及事务依赖独立装配，不受processing-enabled控制；关闭处理仍查持久证据，缺0021或数据库故障时阻止自动退款。

XXL建议5秒执行一次minisoWriteOffProcessJob，默认单分片limit=100；实际周期由环境发布配置记录。dev/test可用受现有环境与权限限制的人工runOnce入口，不新增生产无鉴权控制入口。不新增第二套timer。仅处理Bean存在不代表XXL已经调度，放行时必须核对handler、调度启用及至少一次扫描结果。

## 12 可观测性与人工恢复

入口计数区分accepted（首次持久接收）、duplicate、invalid、signature_failed、conflict、storage_failed、rejected；处理批次返回scanned、processed、retried、manual、deferred和failed，不能把异常统一算成功。监测INIT/RETRY积压、最早received_at滞留时间、MANUAL_REQUIRED数量及VOUCHER_STATE/REDEEMED通知待发送积压。启动runner只读核验0021关键列并初始化缓存；Job轮末刷新，新接收作增量提示。缓存指标不用于资金判断，Prometheus scrape不访问数据库。集群停扫告警按环境聚合最后扫描时间，规则仅保存在仓库监控模板，未发布。

建议告警：持续有到期事件且超过60秒无成功Job扫描，或任一事件进入MANUAL_REQUIRED。阈值属于本轮建议默认值，部署时明确记录；没有采样数据不设置自动退款动作。指标标签只用渠道/操作/错误类别，不用券码、手机号、orgCode、orderId或eventId造成高基数。

固定错误类别包括RECEIPT_PENDING、ORDER_PENDING、MERCHANT_KEY_UNAVAILABLE、OWNERSHIP_CONFLICT、USE_TIME_CONFLICT、USE_TIME_OUT_OF_RANGE、REDEEM_VOID_CONFLICT、NOTIFY_TASK_CONFLICT、DB_UNAVAILABLE、RETRY_EXHAUSTED。last_error只保存这些安全类别及有限非敏感子类别，不能保存异常原文或SQL参数。

接收、Job、数据库异常和新增通知异常分支使用现有LogStormGuard，按稳定错误类别采样；预期等待使用采样warn，内部异常使用采样error并附安全堆栈及eventId/已确认orderId、orgCode/累计次数。异常立即抛给统一边界时不重复日志。数据库原始异常可能包含完整券码，持久化边界转换为无敏感值的分类异常，禁止把原cause传到日志或last_error。

只读核查顺序：按eventId查看接收状态和version；确认可信orderId/orgCode及DB_PERSISTED；核对凭据coupon_status/use_time；按确定性taskId查看通知payload/keyId和发送状态。敏感券码仅在受控数据库权限下读取，不打印到命令输出、聊天或常规日志。核对完成后才按第7.3节执行有审计的CAS重置。

## 13 新旧系统分流与发布

本轮不修改旧卡管、供应商通知URL、线上Nacos/网关/凭证，不发真实券或作废请求。两份主设计先同步，再实现和测试；文档修订不等于部署完成。

若供应商只支持租户级全局回调URL，不能直接把旧新混合流量全部切到新引擎：新服务只能自动处理具有recharge_order_ext及可信凭据的新订单。部署需要供应商独立测试租户/通知入口，或先明确一个按券码归属可靠分流的独立方案；本轮不实现转发旧卡管或未知券的代理路由。

入口对已验签但尚无新服务凭据的事件可靠接收并退避，最终仍无法归属则人工；这种成功回执只表示新引擎接收，不表示旧系统收到。因此混合流量未解决分流前不能切全局URL。不得因未知券码返回成功就宣称新旧系统都完成处理。

发布顺序为0021迁移、默认关闭发布、配置独立key2/商户密钥保护与XXL、确认入口HTTPS及分流、启用processing-enabled、验证恢复扫描、启用入站、受控测试。回滚先停入口，保留恢复处理或回到具有同表兼容能力的版本；不能回滚到完全不认识持久事件的旧版本并删除记录。

## 14 测试与验收合同

本轮已执行本地自动化验证，包含H2/MyBatis事务、WebFlux内存路由、固定协议向量与模拟器回归；没有调用共享中间件或真实名创业务接口。先编写失败测试再实现，验收行为如下：

1. 协议：固定签名及eventId向量、不同正文键顺序/空白、UTF-8和转义兼容、篡改、错误key2、缺失/重复头、nonce/time/sign格式、严格时间、重复JSON键、类型/未知字段、正文限额和Content-Type；历史合法重投不因短时间窗拒绝。
2. 接收：同事件重复/并发只一行、同券异时冲突、提交失败不应答成功、超时后实际提交再重投仍幂等；禁用404、执行池拒绝503、不打印敏感原文。
3. 处理：先回调后凭据、无凭据重启恢复、多节点同事件、同分片扫描、锁顺序及锁竞争、主单/扩展/商户/渠道/批次/手机号归属、处理中退避、失败/退款人工、used补时间、expired有效历史核销、void冲突。
4. 原子性：事实/任务/事件任一步失败全回滚；T3独立提交；旧失败version不能覆盖其它Worker成功；T3数据库故障留下可扫描状态；次数上限及人工CAS恢复。
5. 作废：核销已接收但因密钥异常未处理时禁止退款；作废HTTP前后均复核；CANCEL_REQUESTED与核销不形成永久等待；终态失败的释放补偿分支不能绕过；本地先退款后迟到核销只转人工，不二次扣款。
6. 商户：state_type=3/use_state=2（2026-10-19最终确认）、真实use_time、invalid_time空值参与签名、随机nonce、当前keyId和完整32字节HMAC-SM3；轮换后旧任务不重签、撤销不发送、URL快照不变、空notify_url无需密钥且无任务、code=200响应及重试上限。
7. 装配与兼容：API不反向依赖infrastructure、不阻塞Netty、入口与处理开关独立、缺依赖启动失败、主动发券关闭后恢复仍可运行、现有非MINISO路由及mock场景不受影响。

更新仓库内模拟器或可执行协议示例，用固定测试key2生成入站报文，再用测试商户密钥校验出站HMAC-SM3；不依赖已删除的外部测试前端。运行JDK21针对性模块测试、Maven全模块回归及独立模拟器回归。

执行本地HTTP/H2或真实环境联合测试时，追加 `docs/集成测试.md`，写明日期、scope、profile、Nacos namespace/group/DataId（未连接则明确）、中间件、数据范围、命令、证据路径、结论和遗留问题。固定向量、fake HTTP、H2通过只能证明本地合同；真实名创验收仍需独立key2、HTTPS入口、实际供应商样本、重试约定和端到端通知证据。

## 15 交付边界与后续实施

当前完成详细设计、代码、0021迁移文件、独立恢复Job、可执行协议示例及本地回归；对应入站TODO已移除，开关保持默认关闭。新增本地测试证据写入docs/集成测试.md，不等同外部环境验收。完整V1.2查单响应字段补齐、独立writeOffNotifyUrl、旧库存历史同步、供应商全局回调分流、真实环境部署和自动对账修复不在本次代码实现范围内。

本轮按0021/凭据模型、T1入站、T2/T3与作废防护、V1.2通知/次数上限、API/XXL装配、示例与回归的顺序完成。真实环境执行迁移、分流、启用和端到端验收仍需独立发布流程；如协议、地址或状态语义改变，先修订两份主设计再改代码。未提交、推送或部署。

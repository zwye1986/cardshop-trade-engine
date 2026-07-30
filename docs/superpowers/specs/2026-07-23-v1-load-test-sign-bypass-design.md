# V1 压测签名结果旁路设计

## 目标与边界

为 JMeter 压测简化下单脚本：调用方无需正确计算 SM3 `sign`，也不提供独立算签 HTTP 接口。服务端在压测旁路模式下**仍执行** SM3 摘要计算（保留验签 CPU 成本），但**不比对**客户端 `sign`，一律视为验签通过。

本期范围仅覆盖：

- `POST /v1/cardapi/order/createRechargeOrder` 的签名结果旁路

明确不做：

- 不算签辅助接口
- 不旁路 `getToken`、timestamp、token 鉴权、SM4 解密、业务校验
- 不旁路查单 `/getRechargeOrder`、作废 `/invalid`
- 不跳过 SM3 计算本身（必须调用现有 `V1Sm3SignatureVerifier.verifyCreate`）

## 触发契约

| 项 | 约定 |
| --- | --- |
| Header 名 | `X-Load-Test`（大小写不敏感，按 HTTP 惯例） |
| Header 值 | 与配置口令**完全相等**的共享密钥（不是字面量 `1`） |
| 环境 | 不分 `dev`/`test`/`prod` profile；是否生效只取决于口令是否配置且 Header 是否匹配 |
| 口令未配置 | `trade.engine.v1.load-test-token` 为空或空白时，旁路**永不生效** |

匹配失败（缺 Header、空值、口令不一致）时，走完整验签比对；`sign` 错误返回既有 `501` 口径。

## 配置

| 配置键 | 环境变量建议 | 默认 | 说明 |
| --- | --- | --- | --- |
| `trade.engine.v1.load-test-token` | `TRADE_ENGINE_V1_LOAD_TEST_TOKEN` | `""` | 压测旁路口令；敏感配置，禁止写入日志、禁止提交仓库 |

口令只通过 Nacos / 部署密钥注入。日常联调与生产默认保持空串，旁路关闭。

## 处理流程

在 `V1ProtocolPrecheckService.precheckCreate`（或等价创建订单预检路径）中：

1. 照常解码请求体（明文或 SM4）与 timestamp 校验。
2. 调用 `V1Sm3SignatureVerifier.verifyCreate(...)`，**始终执行** SM3。
3. 若 `signatureResult.valid() == true`：正常通过。
4. 若 `valid == false`：
   - 若请求携带的 `X-Load-Test` 与已配置非空口令常量时间比较相等：视为通过，进入后续受理。
   - 否则：返回 `V1ProtocolFailureReason.SIGN_FAILED`（既有 `501`）。

实现要求：

- Header 从 WebFlux Controller / 请求上下文显式传入 precheck，不使用 ThreadLocal。
- 口令比较使用常量时间比较（如 `MessageDigest.isEqual` 对 UTF-8 字节），避免短路比较泄露。
- `V1Sm3SignatureVerifier` 保持纯校验职责，不感知压测 Header。
- `sign` 字段仍须满足 DTO `@NotBlank`；JMeter 可填任意非空占位串。

## 可观测性

- 旁路命中时写 rate-limited `warn` 日志，至少包含 `appId`、`orgCode`（或等价脱敏商户标识）；**禁止**打印口令、`aesKey`、`Authorization`、完整 `sign`。
- 可选低基数计数器：`trade.engine.v1.load_test.sign_bypass.total`（无高基标签）。

## JMeter 使用约定

```http
POST {base_url}/v1/cardapi/order/createRechargeOrder
Authorization: <token>
X-Load-Test: <与 Nacos 配置一致的口令>
Content-Type: application/json
```

下单 body 按 V1.1 字段构造（`product_id`、`out_order_sn`、`account`、`num`、`notify_url`、`nonce_str`、`timestamp`、`sign`）。`sign` 可为占位值；每笔仍需独立 `out_order_sn` / `nonce_str` / `timestamp`。压测建议 `notify_url` 为空字符串。

`base_url` 示例：`http://apitest.kalaiwang.com`。

## 安全说明

- 持有口令即可在任意已部署环境绕过签名结果比对；口令泄露等同于降低该环境的请求完整性保护。
- 缓解：默认空口令关闭旁路；口令仅密钥系统保存；压测窗口外清空配置；日志与指标不回显口令。
- Token 鉴权、商户身份、业务幂等与余额规则不受本旁路影响。

## 测试要求

- 口令为空：任意 `X-Load-Test` 均不旁路；错误 `sign` → `501`。
- 口令已配置且 Header 匹配：错误 `sign` 仍通过预检；正确 `sign` 也通过。
- 口令已配置但 Header 错误/缺失：错误 `sign` → `501`。
- 旁路路径必须真实调用 verifier（单测可用 spy/计数断言 SM3 被执行）。
- 查单/作废路径行为不变。

## 文档同步（实现阶段）

实现前同步更新：

- `docs/交行直充业务系统详细设计.md`「3. API/V1 接入层」：补充压测 Header 旁路语义与配置项
- `docs/交行直充业务系统高可用方案设计.md`：如有 V1 安全/压测相关章节，补入口令旁路风险与运维约束

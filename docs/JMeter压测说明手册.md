# JMeter 压测说明手册（V1 下单入口）

> 面向测试同学。依据最新方案：`docs/superpowers/specs/2026-07-23-v1-load-test-sign-bypass-design.md`  
> 以及详细设计「3. API/V1 接入层」压测签名旁路约定。  
> 更新日期：2026-07-23

---

## 1. 本期压测范围

| 项 | 说明 |
| --- | --- |
| 目标 | 衡量 **下单入口** 吞吐与延迟（受理链路），不测履约终态、查单、回调 |
| base_url | `http://apitest.kalaiwang.com` |
| 接口 | 仅 `getToken` + `createRechargeOrder` |
| 签名 | **无需正确计算 SM3**；通过压测 Header 旁路「签名结果比对」 |
| 不做 | 查单、作废、算签 HTTP 接口、生产无口令压测 |

---

## 2. 压测前准备（研发/运维配合）

测试开始前，请确认下列项已就绪（口令与密钥由运维/研发线下提供，**不要**写入脚本仓库或群聊明文长期留存）。

| # | 项 | 说明 |
| --- | --- | --- |
| 1 | 压测旁路口令已配置 | Nacos：`trade.engine.v1.load-test-token: 'Jbjhhzstsl~001'`（或环境变量 `TRADE_ENGINE_V1_LOAD_TEST_TOKEN`） |
| 2 | 应用已加载配置 | 改 Nacos 后确认实例已刷新/重启，旁路才生效 |
| 3 | 专用测试商户 | `app_id` / `app_secret` 为测试商户，非生产 |
| 4 | 测试商品 | `product_id` 为**父产品号**（如 MINISO 常用 `80100022`），不是子商品号 |
| 5 | 供应商指向 mock | 避免压测打真实供应商 |
| 6 | SM4 开关 | 见下文 §2.1 |
| 7 | 余额充足 | 压测会真实预扣测试商户余额，注意额度与事后清理 |

### 2.1 SM4 请求体（重要）

压测旁路**只跳过签名结果比对**，**不跳过** SM4 解密。

当前引擎默认 `trade.engine.v1.request-sm4-enabled=true`。若压测窗口仍为 `true`，JMeter 必须发送 SM4 密文（`Content-Type: text/plain`），脚本复杂度高。

**推荐压测窗口做法（与研发确认）：**

- 临时将 `trade.engine.v1.request-sm4-enabled` 设为 `false`
- JMeter 使用明文 JSON：`Content-Type: application/json`
- **压测结束后必须恢复**原 SM4 配置

未关闭 SM4 时，不要指望「只加 `X-Load-Test`」就能用明文 JSON 下单。

### 2.2 压测结束后必做

1. 将 `trade.engine.v1.load-test-token` **清空**（或删掉配置项）并确认生效  
2. 若曾关闭 SM4，恢复 `request-sm4-enabled`  
3. 按约定清理压测订单 / Redis pending / 测试数据（由研发或运维执行）

---

## 3. 接口清单

| 步骤 | Method | URL | 角色 |
| --- | --- | --- | --- |
| Setup | `POST` | `{base_url}/v1/cardapi/auth/getToken` | 取 token（线程组级缓存，不计入下单吞吐） |
| 主压 | `POST` | `{base_url}/v1/cardapi/order/createRechargeOrder` | 唯一加压接口 |

完整示例：

- Token：`http://apitest.kalaiwang.com/v1/cardapi/auth/getToken`
- 下单：`http://apitest.kalaiwang.com/v1/cardapi/order/createRechargeOrder`

> test188 的 nginx 仅把 `/v1/cardapi/**` 转到新交易引擎；请不要用错路径。

---

## 4. 获取 Token

### 请求

```http
POST http://apitest.kalaiwang.com/v1/cardapi/auth/getToken
Content-Type: application/json

{
  "app_id": "<测试商户 app_id>",
  "app_secret": "<测试商户 app_secret>"
}
```

### 成功响应（示例）

```json
{
  "code": 200,
  "msg": "操作成功",
  "data": {
    "token": "<后续 Authorization 使用>"
  }
}
```

### JMeter 建议

1. 单独 Setup 线程组或 Once Only Controller 调用一次  
2. JSON Extractor 取 `$.data.token` → 变量 `authToken`  
3. Token 失败则整轮压测停止，不要继续打单  
4. Token 获取耗时**不要**计入下单 QPS/延迟统计  

---

## 5. 创建直充订单（主压接口）

### 5.1 请求头（必填）

| Header | 值 | 说明 |
| --- | --- | --- |
| `Authorization` | `${authToken}` | getToken 返回的 token，**不要**加 `Bearer ` 前缀（与现网一致） |
| `X-Load-Test` | `<与 Nacos 完全一致的口令>` | 压测签名结果旁路；缺省/错误则仍严格验签 |
| `Content-Type` | `application/json` | 仅在压测窗口已关闭 SM4 时；若仍开 SM4 则为 `text/plain; charset=UTF-8` |

### 5.2 请求体字段

按 **`docs/交行直充接口技术文档-V1.1.docx` §2.2** 构造（与旧卡管一致）。  
**下单请求没有 `order_price`、`product_type`**；金额由服务端按商品售价计算。

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `product_id` | 是 | 父产品号，如 `80100022` |
| `out_order_sn` | 是 | 商户订单号，**每笔唯一** |
| `account` | 是 | 充值账号 |
| `num` | 是 | 购买数量，固定 `1` |
| `notify_url` | 建议空 | 压测填 `""`，避免产生回调任务噪声 |
| `nonce_str` | 是 | 建议 32 位随机串，**每笔独立** |
| `timestamp` | 是 | 毫秒或秒级时间戳，**每笔独立**，偏差需在允许窗口内 |
| `sign` | 是 | **可为任意非空占位**（如 `LOADTEST`），只要字段非空 |

### 5.3 明文 JSON 示例（SM4 已关闭）

```http
POST http://apitest.kalaiwang.com/v1/cardapi/order/createRechargeOrder
Authorization: <token>
X-Load-Test: Jbjhhzstsl~001
Content-Type: application/json

{
  "product_id": "80100022",
  "out_order_sn": "PERF-A39-20260723120000-000001",
  "account": "18186222336",
  "num": 1,
  "notify_url": "",
  "nonce_str": "JANwG4jwiHGMaNc4EtPdbmkKxBhY7daR",
  "timestamp": "1783224958000",
  "sign": "f3374080a58340d9209b0434ae8df8b274b42dfd513dd4e600fec0fe32ec4e6d"
}
```

### 5.4 旁路语义（给测试的口径）

| 条件 | 行为 |
| --- | --- |
| Nacos 口令为空 | 旁路关闭；错误 `sign` → 业务码 `501` |
| Header 缺失 / 与口令不一致 | 旁路不生效；错误 `sign` → `501` |
| Header 与口令完全一致 | 服务端**仍计算** SM3，但**不比对**客户端 `sign`，验签视为通过 |
| 旁路不免除 | Token 鉴权、timestamp、SM4（若开启）、参数校验、余额/幂等/商品规则 |

---

## 6. 成功判定

### HTTP

- HTTP 状态码：`200`（业务失败时也可能 HTTP 200，需看 body `code`）

### 业务码（下单入口压测）

| code | 含义 | 是否计入成功 |
| --- | --- | --- |
| `200` | 操作成功 / 已受理 | 是 |
| `510` | 异步受理（处理中类口径） | 是（与现有 A39 load 模式一致） |
| `501` | 验签失败 | 否（通常表示旁路未生效） |
| 其它 | 参数错误、余额不足、重复单号等 | 否（按场景分析） |

建议在 JMeter 用 JSON Assertion / JSR223 断言：

```text
code == "200" || code == 200 || code == "510" || code == 510
```

（以实际响应 `code` 类型为准，兼容数字/字符串。）

---

## 7. JMeter 脚本要点

### 7.1 线程组建议

| 项 | 建议 |
| --- | --- |
| 模型 | 固定并发（如 10/50/100），或阶梯升压 |
| Token | 每线程组一次；过期再取 |
| 订单号 | `${__time(yyyyMMddHHmmss)}-${__threadNum}-${__counter(FALSE,)}` 或 UUID，保证跨轮不撞幂等 |
| `nonce_str` | 每请求随机 32 字符 |
| `timestamp` | 每请求当前毫秒时间 |
| `sign` | 固定占位即可，如 `LOADTEST` |
| `notify_url` | 空字符串 |
| 超时 | 单请求建议 ≥ 10s |
| 日志 | 压测过程关闭逐笔 Body 打印，避免拖慢客户端 |

### 7.2 变量清单（示例）

| 变量 | 来源 |
| --- | --- |
| `baseUrl` | `http://apitest.kalaiwang.com` |
| `appId` / `appSecret` | A35cecjXS7lZ1W30NMN / A35ZkrekPgvyfuhsijp |
| `loadTestToken` | 与 Nacos `load-test-token` 一致 |
| `productId` | 80100022 |
| `account` | 测试充值账号,不要重复 |
| `authToken` | getToken 提取 |
| `outOrderSn` / `nonceStr` / `timestamp` | 每请求生成 |

### 7.3 Header 配置示例

```text
Authorization: ${authToken}
X-Load-Test: ${loadTestToken}
Content-Type: application/json
```





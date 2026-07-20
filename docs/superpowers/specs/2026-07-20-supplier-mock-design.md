# 供应商下单/查单 Mock 设计

## 目标与边界

在现有 `cardshop-trade-engine-simulator` 中新增可独立启动的供应商 HTTP mock，供 dev/test/pro143 等环境联调 Outbox 派单与异步查单。Mock 协议对齐 `HttpRechargeSupplierGatewayAdapter` 的 JSON 请求/响应约定，不实现真实 MINISO/三方 SDK 鉴权。

核心行为：下单立即返回 `PROCESSING`；按可配置随机区间决定「终态就绪时间」；查单在就绪前继续 `PROCESSING`，就绪后按成功率随机返回 `SUCCESS` 或 `FAILED`。

同机可与 A39 `scenario`/`load` 双进程并存；**同进程不合并**两种模式。首期不做作废（invalid）、不做持久化、不做供应商侧签名校验、不做 HTTP 响应额外 sleep（若后续需要再加 `--http-delay-ms-*`）。

## 运行模式

扩展模拟器运行模式：

- `scenario`：现有单笔商户联调（不变）
- `load`：现有下单入口压测（不变）
- `supplier-mock`：新增；启动常驻 HTTP 服务，阻塞直到进程退出

`supplier-mock` 不要求 `app-secret`/`aes-key` 等商户凭证。参数解析与 A39 凭证校验隔离，避免 mock 启动被商户密钥校验挡住。

启动仍走现有打包产物与 `bin/run-a39-simulator.sh`，例如：

```bash
./bin/run-a39-simulator.sh \
  --mode=supplier-mock \
  --bind-host=0.0.0.0 \
  --port=18082 \
  --final-result-delay-ms-min=1200 \
  --final-result-delay-ms-max=9800 \
  --success-rate=0.95
```

## 协议与路径

响应体字段与 Gateway 解析一致：

```json
{
  "status": "PROCESSING|SUCCESS|FAILED",
  "code": "0|202|E_MOCK_FAIL|...",
  "message": "...",
  "supplier_order_id": "..."
}
```

同一 mock 进程同时注册两套路径，逻辑共用：

| 用途 | 通用路径 | MINISO 兼容路径 |
| --- | --- | --- |
| 下单 | `POST /supplier/dispatch` | `POST /tripartite/coupon/batchGrandCoupon` |
| 查单 | `POST /supplier/query` | `POST /tripartite/coupon/queryGrand` |

请求体按 Gateway 下发字段读取，至少识别：`supplier_request_id`、`order_id`、`out_order_sn`、`org_code`、`account`、`supplier_channel`（查单）。订单身份主键优先 `order_id`；缺失时回退 `supplier_request_id`。

HTTP 状态码：业务响应一律 `200` + JSON body（与当前 Gateway「2xx 再解析 status」一致）。未知路径返回 `404`。

## 状态机

进程内 `ConcurrentHashMap` 保存订单记录，字段至少包含：订单身份、`supplier_order_id`、`readyAt`（epoch millis）、最终 `status`（SUCCESS/FAILED）、创建时间。

1. **下单**
   - 若订单已存在：幂等返回当前记录的 `PROCESSING` 或已就绪终态（不重置 `readyAt`/终态）。
   - 若订单不存在：抽样 `readyAt = now + U[min, max]`；按 `success-rate` 决定终态；立即返回 `PROCESSING` + 新生成的 `supplier_order_id`。
2. **查单**
   - 订单不存在：`status=FAILED`，`code` 使用明确失败码（如 `E_MOCK_NOT_FOUND`），`message` 说明未找到。
   - `now < readyAt`：返回 `PROCESSING`。
   - `now >= readyAt`：返回下单时已确定的终态。

`success-rate` 取值 `[0.0, 1.0]`；`1.0` 表示全部成功，`0.0` 表示全部失败。每次**新订单下单**时独立掷骰，查单不再重掷。

`final-result-delay-ms-min/max` 要求 `0 <= min <= max`。默认建议：`min=1200`，`max=9800`，`success-rate=1.0`，`bind-host=0.0.0.0`，`port=18082`。

## 组件划分

放在 `cardshop-trade-engine-simulator` 内独立包，例如 `...simulator.supplier`：

| 组件 | 职责 |
| --- | --- |
| `SupplierMockOptions` | 解析 CLI/环境变量；校验 delay 区间与 success-rate；不依赖 A39 凭证 |
| `SupplierMockOrderStore` | 内存订单表；下单登记与查单判定 |
| `SupplierMockServer` | JDK `HttpServer`；注册四条路径；读写 JSON |
| 入口分流 | `A39BusinessSimulatorApplication`（或等价入口）识别 `mode=supplier-mock` 后走 mock 分支；`A39SimulatorMode` 增加 `SUPPLIER_MOCK` |

入口解析顺序：先识别 `--mode`/`A39_MODE`；若为 `supplier-mock`，只用 `SupplierMockOptions` 解析，避免触发 A39 必填密钥校验。

## 配置契约

| 参数 | 环境变量建议 | 默认 | 说明 |
| --- | --- | --- | --- |
| `--mode=supplier-mock` | `A39_MODE` | `scenario` | 运行模式 |
| `--bind-host` | `SUPPLIER_MOCK_BIND_HOST` | `0.0.0.0` | 监听地址 |
| `--port` | `SUPPLIER_MOCK_PORT` | `18082` | 监听端口 |
| `--final-result-delay-ms-min` | `SUPPLIER_MOCK_FINAL_RESULT_DELAY_MS_MIN` | `1200` | 终态就绪最小延迟 |
| `--final-result-delay-ms-max` | `SUPPLIER_MOCK_FINAL_RESULT_DELAY_MS_MAX` | `9800` | 终态就绪最大延迟 |
| `--success-rate` | `SUPPLIER_MOCK_SUCCESS_RATE` | `1.0` | 新订单终态成功概率 |

命令行优先于环境变量。`--help` 在 `supplier-mock` 相关说明中列出上述参数。

## 与交易引擎联调

Nacos `cardshop-trade-engine-supplier.yaml` 将渠道 `dispatch-url`/`query-url` 指向本机 mock，例如：

```yaml
trade.engine.supplier.channels.MINISO.dispatch-url: http://127.0.0.1:18082/tripartite/coupon/batchGrandCoupon
trade.engine.supplier.channels.MINISO.query-url: http://127.0.0.1:18082/tripartite/coupon/queryGrand
```

或使用通用路径 `/supplier/dispatch` 与 `/supplier/query`。`timeout-ms` 必须大于 mock 的 HTTP 处理时间；本设计下单/查单默认立即回包，终态等待由交易引擎 `SUPPLIER_QUERY` outbox 退避完成。

## 测试

模块单测至少覆盖：

1. 下单返回 `PROCESSING` 并登记订单。
2. 查单在 `readyAt` 前返回 `PROCESSING`，之后返回预定终态。
3. `success-rate=0` / `1` 边界。
4. 同一 `order_id` 重复下单不重置 `readyAt`/终态。
5. 通用路径与 MINISO 路径行为一致。
6. 查单未知订单返回明确失败。

不要求集成启动真实交易引擎；Gateway 协议兼容性以字段与路径约定为准。

## 文档与发布

- 更新 `README.md` 中 simulator 章节：补充 `supplier-mock` 启动示例与 Nacos 指向说明。
- 不改动主服务生产发布包；仍由 `scripts/package-a39-simulator.sh` 产出独立 zip。
- 明确该模式仅限 dev/test/联调，禁止指向生产真实供应商流量。

## 非目标

- 作废（invalid）mock
- 订单状态磁盘持久化 / 多实例共享状态
- 真实三方签名、租户头、加密体
- 同进程同时运行 A39 + supplier-mock
- 将 mock 并入 bootstrap 主服务

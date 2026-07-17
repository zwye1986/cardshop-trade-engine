# A39 业务请求模拟器设计

## 背景

本工具用于按 `docs/交行直充接口技术文档-V1.1.docx` 模拟机构真实直充业务请求。模拟器作为商户侧客户端运行，先获取 token，再按 `aesKey` 生成 SM3 `sign`，请求 `/v1/cardapi/order/createRechargeOrder`，按响应决定是否查询 `/v1/cardapi/order/getRechargeOrder`，并启动一个 HTTP 回调接收端点。

## 方案

新增独立 Maven 模块 `cardshop-trade-engine-simulator`。该模块不接入 Spring Boot/Nacos/Redis/RabbitMQ，避免测试工具启动时触发交易引擎运行时依赖。它只使用 JDK 21 `HttpClient`、JDK 内置轻量 HTTP server、Jackson 和 BouncyCastle。该模块不加入根 Maven reactor，也不进入主服务发布 zip；通过 `scripts/package-a39-simulator.sh` 单独打包为 `dist/a39-business-simulator-<version>.zip`。

命令行参数提供 `base-url`、`app-id`、`app-secret`、`aes-key`、`product-id`、`account`、`notify-url`、查询次数和间隔等配置。`base-url` 默认 `http://apitest.kalaiwang.com`，并按请求前缀处理，因此也支持 `http://apitest.kalaiwang.com/testSuite/trade-api/dev` 这类带路径的代理前缀；`app-id` 默认 `A35cecjXS7lZ1W30NMN`，`product-id` 默认 `1011`，`account` 默认 `18186222336`，仍支持通过参数或环境变量覆盖；`app-secret/aes-key` 属于凭证，不写入代码仓库，运行脚本会加载部署机本地 `config/a39-simulator.env`。

模拟器传入的 `product-id` 遵循新服务的旧卡管语义，是 `mc_depart_product.parent_id` 对应的父产品号；服务端选择的实际派单子商品号来自授权行和 `mc_product.product_id`。因此模拟器请求号、订单 `product_id` 与 Redis 商品配置 key 不是同一个字段：Redis key 使用 `selected_product_id`，例如父号 `80100022` 选择子号 `80100023` 时使用 `product:80100023:config`。

## 协议细节

签名规则与旧系统 `CardApiUtil.sortMapByAscii`、当前服务端 `V1Sm3SignatureVerifier` 保持一致：移除 `sign` 字段，加入 `aesKey`，按字段名 ASCII 升序拼接 `key=value`，用逗号连接，再计算 SM3 hex。下单签名字段为 `account/aesKey/nonce_str/notify_url/num/out_order_sn/product_id/timestamp`；查单签名字段为 `aesKey/nonce_str/out_order_sn/timestamp`。

当前交易引擎 DTO 需要 `order_price` 和 `product_type`，但旧 V1.1 文档与旧系统验签不把它们纳入签名。模拟器会在请求体中带上这两个字段，默认 `product_type=1`，但保持签名字段与旧协议一致。

回调接收器接受 `/callback/order` 等回调路径，保留原始 JSON 字段名验签，因此 snake_case 和 camelCase 回调都能按发送方原字段校验。验签成功返回 `{"code":200,"msg":"接收成功"}`，失败返回 `{"code":500,"msg":"接收失败，请重试"}`。

## 查询决策

下单响应 `code=200` 或 `code=510` 时进入查询；其他业务失败默认不查。查询轮询到订单终态 `6/11/12` 停止；处理中状态 `0/2/13` 或查询失败按配置间隔重试，直到次数耗尽。

## 日志

模拟器在标准输出打印 `[A39][HTTP][request]`、`[A39][HTTP][response]` 和 `[A39][ORDER][status-change]`。请求日志包含脱敏 curl：`app_secret` 和 `Authorization` 不输出原值；下单/查单请求体保留业务字段和 `sign`，便于联调复现。响应日志输出 HTTP status 和响应体原文。订单状态日志只在查单返回的 `order_status` 相比上一次发生变化时输出。

## 测试

模块测试覆盖：

- SM3 签名 canonical source 与已知摘要。
- 下单/查单请求体生成及签名字段范围。
- 下单响应到查询决策的映射。
- 脱敏 curl、响应日志和订单状态变化日志。
- 回调验签成功和失败响应。

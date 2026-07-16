# V1 商品选品快照缓存与 30 分钟刷新设计

## 背景

旧卡管仍是商户商品授权、售价和商品属性的运营事实源，但旧卡管与 `cardshop-trade-engine` 不共享 Redis。当前 V1 下单每次从 MySQL 读取 `mc_depart_product` 和 `mc_product`，随后只对 Redis 实际子商品配置执行 `put-if-absent`。该实现有两个问题：秒杀/抢购流量会持续读取共享 MySQL；旧卡管正常改价后，已存在的 Redis 商品配置不会刷新，新单会因为价格或版本不一致被拒绝。

业务确认允许商品改价后最长 30 分钟内继续按旧价成交。已受理订单必须继续使用订单内冻结价格，后续改价只影响刷新后的新订单。

## 目标

1. 正常下单只读取 Redis，不按订单访问 MySQL。
2. 商品售价、上下架或类型变更在最长 30 分钟内对新订单生效。
3. 缓存刷新时同一 `orgCode + parentProductId` 只有一个请求访问 MySQL，避免缓存击穿。
4. 父商品选品快照和实际子商品 Lua 配置必须原子更新，不能出现价格或子商品部分切换。
5. Redis 或 MySQL 刷新异常不能导致无限期使用旧价格，也不能绕过 Lua 门禁。
6. 旧卡管商品变更主动同步链路只记录后续待办，本期不作为交付前置。

## 非目标

1. 本期不改造旧卡管商品保存接口。
2. 本期不接入 CDC、Debezium 或新的商品变更 MQ。
3. 本期不实现全库商品配置批量重建任务。
4. 本期不改变已受理订单的价格快照、扣款、退款或供应商派单语义。

## 方案选择

采用“Redis 完整选品快照 + 逻辑软硬过期 + 父商品级单飞刷新”。不使用纯硬 TTL，避免缓存同时到期时大量请求等待；不依赖固定周期全量扫描，避免商品规模增长后持续压迫共享 MySQL。

默认时间参数：

| 参数 | 默认值 | 语义 |
| --- | --- | --- |
| `refresh-after` | `25m` | 进入软过期，可继续按旧价成交并尝试单飞刷新。 |
| `max-stale-age` | `30m` | 硬过期，刷新成功前不得继续按旧价受理。 |
| `physical-ttl` | `60m` | Redis 物理保留时间，用于刷新失败诊断和短暂恢复。 |
| `refresh-lock-ttl` | `5s` | 父商品刷新锁初始 TTL；持有者在 MySQL 查询期间按 token 周期续租。 |
| `refresh-wait-timeout` | `1s` | 硬过期并发请求等待刷新结果的最长时间。 |
| `negative-cache-ttl` | `1m` | 无权限、商品不存在等无可信快照结果的短缓存时间。 |

## 组件边界

### 缓存优先选品器

对现有 `V1ProductCandidateSelector` 增加 Redis 缓存优先装饰器。装饰器负责读取父商品选品快照、判断年龄、获取刷新锁、调用 MySQL 选择器以及等待并发刷新；现有 `MybatisV1ProductCandidateSelector` 保留为可信源读取器，只在缓存缺失或刷新窗口执行。

### 商品快照缓存端口

新增 application 端口表达以下能力：读取选品快照、获取/续租/释放刷新锁、等待刷新结果、原子写入完整选品快照和实际子商品配置、写入短 TTL 负缓存。Infrastructure 使用 `ReactiveStringRedisTemplate` 和 Lua 实现，不把 Redis 类型泄漏到 application。

### 子商品配置修复

现有 `V1ProductConfigPreheatPort` 从 `preheatIfMissing` 升级为 `refreshIfMissingOrStale`。当父商品选品快照有效但实际子商品配置缺失或字段漂移时，仅使用该快照修复 Redis，不访问 MySQL。正常刷新时由快照缓存端口一次 Lua 同时更新两个 key。

## Redis Key

所有 key 使用同一 `{orgCode}` hash tag：

```text
recharge:{orgCode}:product-selection:{parentProductId}
recharge:{orgCode}:product-selection:{parentProductId}:refresh-lock
recharge:{orgCode}:product:{selectedProductId}:config
```

父商品选品快照 Hash 至少包含：

```text
product_id
selected_product_id
supplier_channel
product_name
coupon_type
product_type
product_status
sale_price
face_value
base_price
product_version
selection_fingerprint
validation_status
fail_code
fail_reason
loaded_at
refresh_after
expire_at
cache_kind
```

`cache_kind` 为 `SNAPSHOT` 或 `NEGATIVE`。实际子商品配置保持 `status/version/price/product_type` 字段，供 `accept_recharge_order.lua` 最终裁决。

## 请求流程

### 新鲜快照

快照年龄小于 `refresh-after` 时直接返回 Redis 快照。下单链路不访问 MySQL，并在接单前使用 `refreshIfMissingOrStale` 修复可能被意外删除或漂移的子商品配置。

### 软过期快照

快照年龄达到 `refresh-after` 但小于 `max-stale-age` 时允许按旧价成交。当前请求尝试获取父商品刷新锁：获取成功者同步查询 MySQL、原子刷新缓存并使用新快照；未获取锁者直接使用旧快照，不等待、不查询 MySQL。刷新失败后必须重新取当前时间，只有仍早于旧快照 `expire_at` 才能回退旧价。

### 硬过期快照

快照年龄达到 `max-stale-age` 时不能继续按旧价成交。刷新锁持有者同步查询 MySQL；其他请求在 `refresh-wait-timeout` 内轮询新快照。刷新成功后使用新快照；等待超时、MySQL 不可用或 Redis 原子刷新失败时返回系统繁忙，不形成订单、不预扣余额。

### 缓存缺失

按硬过期处理。只有刷新锁持有者访问 MySQL，其他请求等待刷新结果。缓存首次加载完成后同时写入父商品选品快照和实际子商品配置。

### 负缓存

MySQL 确认商户无该父商品权限、父商品下没有启用子商品或商品源不存在时写入短 TTL 负缓存。负缓存命中时返回对应业务失败，不访问 MySQL；TTL 到期后允许重新加载。负缓存写入异常不改变 MySQL 已确认的业务失败，后续接单只能写失败订单，不预扣余额、不派单。

## 原子刷新

刷新成功后执行一个 Redis Lua，原子完成：

1. 写父商品完整选品快照。
2. 写实际子商品 `status/version/price/product_type`。
3. 设置两个 key 的物理 TTL。
4. 写入 `loaded_at/refresh_after/expire_at`。

刷新锁持有者在 MySQL 查询期间按不超过锁 TTL 三分之一的周期执行 token 校验续租 Lua；续租失败或最终 token fencing 失败时不得提交结果。该机制保证正常 Redis 可用条件下，即使一次 MySQL 查询超过初始 5 秒 TTL，同一父商品仍只有一个回源者。原子 Lua 保证父子 key 不出现部分更新。若选中的子商品发生切换，新订单只会在父商品快照和新子商品配置同时可见后使用新子商品；旧子商品配置可由物理 TTL 自然淘汰。

## 一致性语义

1. Redis 选品快照是最多陈旧 30 分钟的交易缓存，MySQL 仍是商品事实源。
2. 改价后软过期窗口内允许按旧价成交，这是已确认的业务规则。
3. 达到硬过期后必须使用刷新后的新价，不能继续使用旧快照。
4. 每个已受理订单冻结 `selectedProductId`、价格、商品名称、类型和供应商渠道，后续改价不修改历史订单。
5. 父快照保存完整 `V1ProductSelection` 的稳定 SHA-256 `selection_fingerprint`。比较修复和 `accept_recharge_order.lua` 都必须比较该指纹；accept Lua 还原子比较父子商品号、价格、版本、类型、`product_snapshot_available/cache_kind`，并确认 `expire_at > now`。有真实子商品快照时再比较实际子商品配置。底价、渠道、名称、候选校验结果等任一字段切换都必须 fail closed。
6. `mc_depart_product.price` 修改不保证 `mc_product.version` 递增，因此刷新不能只检查版本号；每次 MySQL 刷新必须重写并逐字段同步售价、状态、类型和商品版本。

## 异常处理

| 场景 | 行为 |
| --- | --- |
| 软过期刷新 MySQL 失败 | 当前锁持有请求可继续使用未超过 30 分钟的旧快照；记录刷新失败指标和告警上下文。 |
| 硬过期刷新 MySQL 失败 | 返回系统繁忙，不使用超过 30 分钟的旧价。 |
| 刷新锁持有者长时间查询 | 按 token 周期续租；持有者消失后锁按 TTL 自动释放，并发请求在等待超时后返回系统繁忙。 |
| Redis 原子刷新失败 | 不执行 accept Lua；记录 error 日志和刷新失败指标。 |
| 子商品配置缺失但父商品快照有效 | 使用 `refreshIfMissingOrStale` 从选品快照修复，不访问 MySQL。 |
| 负缓存写入失败 | 返回原始业务失败；不得因缓存异常把无权限商品当作可售商品。 |

高频失败日志必须采用采样、限频或低频聚合，避免 Redis/MySQL 故障期间形成日志风暴。单笔硬过期刷新失败可以记录带 `orgCode/productId` 的 error 日志，不记录账号、密钥或请求明文。

## 可观测性

新增低基数指标：

```text
trade_engine_product_cache_hit_total{result=fresh|soft_stale|hard_stale|miss|negative}
trade_engine_product_cache_refresh_total{result=success|mysql_failed|redis_failed|lock_busy|wait_timeout}
trade_engine_product_cache_refresh_duration_seconds
trade_engine_product_cache_age_seconds
trade_engine_product_cache_mysql_load_total
```

指标标签禁止包含商品号、商户订单号或账号；具体 `orgCode/productId` 只进入受控日志。

## 配置

新增 `trade.engine.v1.product-cache.*` 配置组，并允许通过环境变量/Nacos 覆盖。启动时必须校验：`refresh-after < max-stale-age < physical-ttl`，锁 TTL 和等待超时必须为正值。配置非法时应用启动失败，不能静默回落到每单查询 MySQL。Redis 端口缺失时仅 `trade.engine.environment=local` 可直读 MySQL；dev/test/prod 保留缓存包装器并 fail closed。

## 测试

单元和集成测试至少覆盖：

1. 新鲜快照连续下单不调用 MySQL。
2. 软过期并发请求只有一个调用 MySQL，其他请求继续按旧价成功。
3. 刷新成功后新订单使用新价格，已有订单价格不变。
4. 硬过期并发请求等待同一个刷新结果，不形成 MySQL 击穿。
5. 硬过期刷新失败返回系统繁忙，不写订单、不预扣余额。
6. 子商品从 `80100023` 切换到 sibling 时父子 key 原子切换。
7. 子商品配置被删除后可从有效父商品快照修复，不访问 MySQL。
8. 无权限和商品不存在负缓存生效，TTL 到期后可重新加载。
9. 配置边界、Redis lock TTL、续租、等待超时和日志限频。
10. 真实 Redis 并发测试证明源查询超过初始锁 TTL 时高并发同 key 仍只产生一次 MySQL 加载。
11. 比较修复完成后父快照切换，旧请求在 accept Lua 中返回 `CONFIG_CHANGED`。
12. 负缓存命中和负缓存写失败都保留原业务失败，不要求不存在的子商品配置。

## 上线与回滚

部署前先按现有 MySQL 数据预热 A39 等灰度商户的父商品选品快照和实际子商品配置。灰度期间监控 MySQL 加载次数、缓存年龄、刷新失败和系统繁忙响应。回滚旧版本前不得删除现有实际子商品配置 key；新增父商品选品快照 key 可保留并由 TTL 自动淘汰。

## 后续待办

旧卡管商品变更主动同步到新交易 Redis 不纳入本期实现。后续方案按优先级评估：

1. 旧卡管商品保存事务 outbox 或变更事件。
2. 基于 MySQL binlog/CDC 的增量同步。
3. 以可靠更新时间或版本号为水位的定时增量扫描。

主动同步用于缩短改价生效时间；请求级单飞刷新仍长期保留，作为漏事件、同步延迟和 Redis 数据丢失的兜底。

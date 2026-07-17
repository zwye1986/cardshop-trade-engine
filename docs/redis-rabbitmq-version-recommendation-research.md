# Redis / RabbitMQ 版本推荐调研

> 调研基准日：2026-07-15。  
> 调研范围：只使用 Redis 与 RabbitMQ 官方资料，包括 Redis 官方文档、Redis 官方 GitHub 仓库、RabbitMQ 官方文档与 RabbitMQ 官方 GitHub Release Notes。  
> 适用场景：Java 21 / Spring Boot 后端；Redis 用作高可用缓存、幂等、待补偿数据；RabbitMQ 用作削峰异步消息。

## 结论摘要

| 组件 | 生产推荐 | 保守备选 | 说明 |
| --- | --- | --- | --- |
| Redis | Redis Open Source `8.2.7` 系列最新 patch | Redis Community Edition `7.4.9` 系列最新 patch | Redis 官方版本管理页显示 `8.2` 为 GA 且 EOL 到 `2030-09-01`，`7.4` 为 GA 且 EOL 到 `2029-12-01`；`8.8` 虽是最新 GA，但 EOL 仍为 TBD，且本项目当前主要依赖 Hash/String/ZSET/TTL/Lua/Sentinel 等基础能力，不需要以最新功能为优先。来源：[Redis Open Source version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)、[Redis 8.2 release notes](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.2-release-notes/)、[Redis 7.4 release notes](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisce-7.4-release-notes/)。 |
| RabbitMQ | RabbitMQ `4.3.2` + Erlang/OTP `27.x` 最新 patch | RabbitMQ `4.2.6` + Erlang/OTP `27.x` 最新 patch，仅限已有商业支持或有 2026-07-31 前升级计划 | RabbitMQ 官方 Release Information 显示 `4.3.2` 于 `2026-06-15` 发布，社区支持到 `2026-11-30`；`4.2.6` 于 `2026-05-20` 发布，社区支持只到 `2026-07-31`，但商业支持到 `2030-06-30`。RabbitMQ 官方 Erlang 矩阵显示 `4.3.x/4.2.x` 最小 Erlang/OTP `27.0`、最大完全支持 `27.x`，Erlang `28.x` 仅部分支持且新集群限定，`29` 不支持，`26` 已 EOL。来源：[RabbitMQ Release Information](https://www.rabbitmq.com/release-information)、[RabbitMQ Erlang Version Requirements](https://www.rabbitmq.com/docs/which-erlang)。 |

## Redis 官方事实

Redis 官方版本管理页说明 Redis Open Source 使用 `MAJOR.MINOR.PATCH` 版本号：major 可能包含破坏性变化，minor 增加功能和改进，patch 用于 bugfix 与安全更新；同页还建议使用最新可用版本以获得新功能、性能改进和安全更新。来源：[Redis Open Source version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)。

| Redis 版本线 | 官方发布事实 | 官方生命周期 | 对本项目的判断 |
| --- | --- | --- | --- |
| `8.8` | Redis 官方 Release Notes 标记 `8.8.0` 为 `2026-05` 的 Redis Open Source GA，包含 Array、`INCREX`、`XNACK` 等新能力。来源：[Redis 8.8 release notes](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.8-release-notes/)。 | Redis 官方版本管理页列为 GA，EOL 为 `TBD`。来源：[Redis version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)。 | 可作为“追最新”候选，但 EOL 未定且新功能不是本项目必需；不建议作为本项目第一版生产基线。 |
| `8.6` | Redis 官方 Release Notes 标记 `8.6.0` 为 `2026-02` GA，`8.6.4` 为 `2026-06` patch，升级紧急度为 `HIGH`。来源：[Redis 8.6 release notes](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.6-release-notes/)。 | Redis 官方版本管理页列为 GA，EOL 为 `TBD`。来源：[Redis version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)。 | 比 `8.8` 稍早，但 EOL 同样未定；不如 `8.2` 适合作为长期基线。 |
| `8.4` | Redis 官方 Release Notes 标记 `8.4.0` 为 `2025-11` GA，`8.4.4` 为 `2026-06` patch，升级紧急度为 `HIGH`。来源：[Redis 8.4 release notes](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.4-release-notes/)。 | Redis 官方版本管理页列为 GA，EOL 为 `TBD`。来源：[Redis version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)。 | 位于 `8.2` 与 `8.6` 之间，但生命周期不如 `8.2` 明确。 |
| `8.2` | Redis 官方 Release Notes 标记 `8.2` 为 `2025-08` 的 Redis Open Source GA；`8.2.7` 为 `2026-06` patch，升级紧急度为 `HIGH`，修复 AArch64 启动、`SCAN` 回归、Streams 等问题。来源：[Redis 8.2 release notes](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.2-release-notes/)。 | Redis 官方版本管理页列为 GA，EOL 为 `2030-09-01`。来源：[Redis version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)。 | 推荐生产基线。生命周期最长且明确，patch 已覆盖多轮安全与高优先级 bugfix；对本项目基础 Redis 命令、Lua 原子脚本、ZSET pending 索引和 Sentinel 高可用场景足够。 |
| `8.0` | Redis 官方 Release Notes 标记 `8.0.0` 为 `2025-05` GA，并说明 Redis Community Edition 更名为 Redis Open Source。来源：[Redis 8.0 release notes](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.0-release-notes/)。 | Redis 官方版本管理页列为 GA，EOL 为 `2026-12-01`。来源：[Redis version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)。 | 不建议新建生产基线；距离 EOL 只有约 4.5 个月。 |
| `7.4` | Redis 官方 Release Notes 标记 `7.4` 为 `2024-07` Redis Community Edition GA；`7.4.9` 为 `2026-05` security patch，包含 Lua、`RESTORE` 等安全修复。来源：[Redis 7.4 release notes](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisce-7.4-release-notes/)。 | Redis 官方版本管理页列为 GA，EOL 为 `2029-12-01`。来源：[Redis version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)。 | 推荐保守备选。适合暂不切 Redis 8 的团队；必须使用 `7.4.x` 最新 patch，不应停留在早期 `7.4.x`。 |
| `7.2` | Redis 官方 GitHub `7.2.0` Release 标记其为 Redis `7.2` 首个稳定版本。来源：[redis/redis 7.2.0 release](https://github.com/redis/redis/releases/tag/7.2.0)。 | Redis 官方版本管理页列为 GA，EOL 为 `2029-12-01`。来源：[Redis version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)。 | 已有环境若运行 `7.2.x` 可作为短期兼容基线，但新生产部署优先 `8.2.x` 或 `7.4.x`，避免新建即落后两个以上 minor。 |
| `6.2` | Redis 官方版本管理页仍列出 `6.2`。来源：[Redis version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)。 | Redis 官方版本管理页列为 GA，EOL 为 `2027-04-01`。来源：[Redis version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)。 | 不建议新建生产基线；只适合遗留系统迁移窗口内短期维持。 |

## RabbitMQ 官方事实

RabbitMQ 官方 Release Information 页面用于判断当前 release 是否处在社区支持或扩展商业支持内，并要求跨 release 升级时阅读目标版本文档与 release notes。来源：[RabbitMQ Release Information](https://www.rabbitmq.com/release-information)。

| RabbitMQ 版本线 | 官方发布与维护状态 | Erlang/OTP 要求 | 对本项目的判断 |
| --- | --- | --- | --- |
| `4.3` | RabbitMQ 官方 Release Information 显示最新 patch 为 `4.3.2`，发布日期 `2026-06-15`，社区支持到 `2026-11-30`，商业支持到 `2028-04-30`。来源：[RabbitMQ Release Information](https://www.rabbitmq.com/release-information)。 | RabbitMQ 官方 Erlang 矩阵显示 `4.3.2/4.3.1/4.3.0` 最小 Erlang/OTP `27.0`、最大完全支持 `27.x`；`28.x` 从 `4.2.0` 起仅部分支持且只适合新集群，`29` 不支持，`26` 已 EOL。来源：[RabbitMQ Erlang Version Requirements](https://www.rabbitmq.com/docs/which-erlang)。 | 推荐生产基线。它是基准日仍有社区维护窗口的最新稳定线，适合削峰异步消息、publisher confirm、consumer 重试、DLX/TTL、quorum queue 等常规生产能力。 |
| `4.2` | RabbitMQ 官方 Release Information 显示最新 patch 为 `4.2.6`，发布日期 `2026-05-20`，社区支持到 `2026-07-31`，商业支持到 `2030-06-30`。来源：[RabbitMQ Release Information](https://www.rabbitmq.com/release-information)。 | RabbitMQ 官方 Erlang 矩阵显示 `4.2.0` 至 `4.2.6` 最小 Erlang/OTP `27.0`、最大完全支持 `27.x`；`28.x` 仅部分支持且新集群限定。来源：[RabbitMQ Erlang Version Requirements](https://www.rabbitmq.com/docs/which-erlang)。 | 保守备选仅在已有商业支持或能在 `2026-07-31` 前完成升级计划时成立；社区版新生产部署不应长期停留在 `4.2.x`。 |
| `4.1` | RabbitMQ 官方 Release Information 显示最新 patch `4.1.8` 于 `2026-01-22` 发布，社区支持已于 `2026-01-31` 结束，商业支持到 `2027-04-30`。来源：[RabbitMQ Release Information](https://www.rabbitmq.com/release-information)。 | RabbitMQ 当前兼容矩阵的受支持系列重点已放在 `4.3/4.2`，且 `4.1` 社区支持在基准日前已结束。来源：[RabbitMQ Release Information](https://www.rabbitmq.com/release-information)、[RabbitMQ Erlang Version Requirements](https://www.rabbitmq.com/docs/which-erlang)。 | 不建议新建生产基线；除非被商业支持合同和现有集群约束，否则应升级到 `4.3.x`。 |
| `4.0` | RabbitMQ 官方 Release Information 显示最新 `4.0.9` 于 `2025-04-14` 发布，社区支持已于 `2025-04-30` 结束，商业支持到 `2026-09-30`。来源：[RabbitMQ Release Information](https://www.rabbitmq.com/release-information)。 | RabbitMQ Erlang 矩阵说明从 `4.0.4` 起 `4.0.x` 才兼容 Erlang `27`，早期 `4.0.1` 至 `4.0.3` 使用 Erlang `26.2.x`。来源：[RabbitMQ Erlang Version Requirements](https://www.rabbitmq.com/docs/which-erlang)。 | 不建议新建生产基线；社区维护早已结束。 |
| `3.13` | RabbitMQ 官方 Release Information 显示最新 `3.13.7` 于 `2024-08-26` 发布，社区支持已于 `2024-09-30` 结束，商业支持到 `2029-12-31`。来源：[RabbitMQ Release Information](https://www.rabbitmq.com/release-information)。 | RabbitMQ Erlang 矩阵显示 `3.13.x` 兼容 Erlang `26.0` 至 `26.2.x`；同页也说明 Erlang `26` 已 EOL，不再被 RabbitMQ `4.3` 支持。来源：[RabbitMQ Erlang Version Requirements](https://www.rabbitmq.com/docs/which-erlang)。 | 只适合遗留商业支持场景；不适合作为 Java 21 / Spring Boot 新后端的基准。 |

## 推荐组合

### 生产推荐组合

- Redis：`8.2.7` 或 `8.2.x` 最新 patch。Redis 官方将 `8.2` 标为 GA 且 EOL 到 `2030-09-01`，`8.2.7` 是 `2026-06` 高优先级修复版本。来源：[Redis version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)、[Redis 8.2 release notes](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.2-release-notes/)。
- RabbitMQ：`4.3.2` 或 `4.3.x` 最新 patch，搭配 Erlang/OTP `27.x` 最新 patch。RabbitMQ 官方将 `4.3.2` 作为当前 `4.3` patch，社区支持到 `2026-11-30`；RabbitMQ 官方 Erlang 矩阵要求 `4.3.x` 使用 Erlang/OTP `27.0` 至 `27.x`，且 `28.x` 仅部分支持。来源：[RabbitMQ Release Information](https://www.rabbitmq.com/release-information)、[RabbitMQ Erlang Version Requirements](https://www.rabbitmq.com/docs/which-erlang)。

这个组合的理由是：Redis 侧选择生命周期明确、patch 充分的 `8.2.x`，覆盖缓存、幂等、Lua 原子写入、pending ZSET 和 Sentinel 高可用需求；RabbitMQ 侧选择社区支持窗口仍打开的 `4.3.x`，避免在基准日后半个月就进入社区 EOL 的 `4.2.x`。

### 保守备选组合

- Redis：`7.4.9` 或 `7.4.x` 最新 patch。Redis 官方将 `7.4` 标为 GA 且 EOL 到 `2029-12-01`，`7.4.9` 是 `2026-05` security patch。来源：[Redis version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)、[Redis 7.4 release notes](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisce-7.4-release-notes/)。
- RabbitMQ：`4.2.6` 或 `4.2.x` 最新 patch，搭配 Erlang/OTP `27.x` 最新 patch，但只建议用于已有商业支持或明确计划在 `2026-07-31` 社区支持结束前升级到 `4.3.x` 的环境。来源：[RabbitMQ Release Information](https://www.rabbitmq.com/release-information)、[RabbitMQ Erlang Version Requirements](https://www.rabbitmq.com/docs/which-erlang)。

这个组合适合“先稳住现有运维栈，再分阶段升级”的路线；若没有 RabbitMQ 商业支持，不建议把 `4.2.x` 当作 2026 年下半年的长期生产版本。

## 落地注意事项

1. Redis 不建议新部署 `8.0.x`：Redis 官方给出的 `8.0` EOL 是 `2026-12-01`，生命周期明显短于 `8.2` 与 `7.4`。来源：[Redis version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)。
2. Redis `8.8` 可作为后续跟进对象，但不建议直接作为第一版生产基线：Redis 官方列出 `8.8` 为 GA，EOL 仍为 `TBD`；本项目当前关键路径不依赖 `8.8` 新增命令。来源：[Redis version management](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/)、[Redis 8.8 release notes](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.8-release-notes/)。
3. RabbitMQ 不建议新部署 `4.1.x`、`4.0.x`、`3.13.x` 社区版：RabbitMQ 官方 Release Information 显示这些版本线在 2026-07-15 前均已结束社区支持。来源：[RabbitMQ Release Information](https://www.rabbitmq.com/release-information)。
4. RabbitMQ `4.3.x/4.2.x` 应固定 Erlang/OTP `27.x` 最新 patch；RabbitMQ 官方说明 `28.x` 仅部分支持，`29` 不支持，`26` 已 EOL。来源：[RabbitMQ Erlang Version Requirements](https://www.rabbitmq.com/docs/which-erlang)。
5. 版本升级前应在 dev/test 环境验证 Redis Lua 脚本、Sentinel failover、RabbitMQ publisher confirm、mandatory return、consumer ack/retry/DLX、队列声明参数和监控指标，因为 RabbitMQ 官方也要求跨 release 升级时阅读目标版本文档与 release notes。来源：[RabbitMQ Release Information](https://www.rabbitmq.com/release-information)。

## 官方来源清单

- Redis Open Source version management：https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/
- Redis Open Source 8.8 release notes：https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.8-release-notes/
- Redis Open Source 8.6 release notes：https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.6-release-notes/
- Redis Open Source 8.4 release notes：https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.4-release-notes/
- Redis Open Source 8.2 release notes：https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.2-release-notes/
- Redis Open Source 8.0 release notes：https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.0-release-notes/
- Redis Community Edition 7.4 release notes：https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisce-7.4-release-notes/
- Redis `7.2.0` official GitHub release：https://github.com/redis/redis/releases/tag/7.2.0
- RabbitMQ Release Information：https://www.rabbitmq.com/release-information
- RabbitMQ Erlang Version Requirements：https://www.rabbitmq.com/docs/which-erlang

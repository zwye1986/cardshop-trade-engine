-- mark_db_persisted.lua
-- docs/交行直充业务系统详细设计.md「8.3 异步落库 Consumer」
-- Consumer 确认 MySQL 事务提交后，原子推进 Redis persisted_version；只有 Redis 当前版本已追平本次落库版本时，
-- 才标记 DB_PERSISTED 并清理 pending/org_index 补投入口。

-- KEYS
-- 1 orderKey                recharge:{orgCode}:order:{outOrderSn}
-- 2 idemKey                 recharge:{orgCode}:idem:{outOrderSn}
-- 3 pendingKey              recharge:{orgCode}:persist:pending
-- 4 eventIndexKey           recharge:{orgCode}:order:event_index:{orderId}
-- 5 eventKey                recharge:{orgCode}:order:event:{orderId}:3
-- 6 orgIndexBucketKey       recharge:persist:pending:org_index:{bucket}

local orgCode = ARGV[1]
local orderId = ARGV[2]
local persistedVersion = tonumber(ARGV[3] or '0') or 0
local now = ARGV[4]
local terminalTtlSeconds = tonumber(ARGV[5] or '0')

local function result(code, failCode, failReason)
  return {
    'code', code,
    'order_id', orderId,
    'fail_code', failCode or '',
    'fail_reason', failReason or ''
  }
end

-- docs/交行直充业务系统详细设计.md「8.3 异步落库 Consumer」
-- org_index 是 D5B 补投任务发现“哪些商户仍有 pending 订单”的入口。只要本脚本删除
-- pending zset 成员，就必须立即刷新 org_index；即使订单 hash 已被 TTL/人工清理，也不能留下
-- 空商户索引，否则后续扫描会反复命中没有成员的 bucket。
local function refreshPendingOrgIndex()
  local nextPending = redis.call('ZRANGE', KEYS[3], 0, 0, 'WITHSCORES')
  if #nextPending == 0 then
    redis.call('ZREM', KEYS[6], orgCode)
  else
    redis.call('ZADD', KEYS[6], nextPending[2], orgCode)
  end
end

if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('ZREM', KEYS[3], orderId)
  refreshPendingOrgIndex()
  return result('MARK_FAILED', 'ORDER_NOT_FOUND', '订单快照不存在')
end

local currentOrderId = redis.call('HGET', KEYS[1], 'order_id') or ''
if currentOrderId ~= orderId then
  return result('MARK_FAILED', 'ORDER_ID_MISMATCH', '订单号不匹配')
end

if persistedVersion <= 0 then
  return result('MARK_FAILED', 'INVALID_PERSISTED_VERSION', '落库版本非法')
end

local persistStatus = redis.call('HGET', KEYS[1], 'persist_status') or ''
if persistStatus ~= 'DB_PERSISTED'
    and persistStatus ~= 'PENDING_MQ'
    and persistStatus ~= 'PERSIST_QUEUED'
    and persistStatus ~= 'PERSISTING'
    and persistStatus ~= 'PERSIST_FAILED' then
  return result('MARK_FAILED', 'STATE_NOT_MARKABLE', '订单状态不允许标记落库完成')
end

local currentOrderVersion = tonumber(redis.call('HGET', KEYS[1], 'order_version') or '0') or 0
local currentPersistedVersion = tonumber(redis.call('HGET', KEYS[1], 'persisted_version') or '0') or 0
local nextPersistedVersion = persistedVersion
if currentPersistedVersion > nextPersistedVersion then
  nextPersistedVersion = currentPersistedVersion
end

redis.call('HSET', KEYS[1],
  'persisted_version', tostring(nextPersistedVersion),
  'updated_at', now
)

redis.call('HSET', KEYS[2],
  'persisted_version', tostring(nextPersistedVersion),
  'updated_at', now
)

-- docs/交行直充业务系统高可用方案设计.md「14.2 Redis 与 MySQL 版本博弈」
-- 如果 Redis 当前 order_version 已经高于本次 MySQL 落库版本，说明还有更新事实尚未补刷。
-- 此时只推进 persisted_version，不删除 pending/org_index，避免补偿任务失去继续刷新的入口。
if currentOrderVersion > nextPersistedVersion then
  return result('MARKED', '', '')
end

redis.call('HSET', KEYS[1],
  'persist_status', 'DB_PERSISTED',
  'db_persisted_at', now,
  'updated_at', now
)

redis.call('HSET', KEYS[2],
  'persist_status', 'DB_PERSISTED',
  'db_persisted_at', now,
  'updated_at', now
)

if persistStatus ~= 'DB_PERSISTED' then
  redis.call('HSET', KEYS[5],
    'order_id', orderId,
    'version', '3',
    'persist_status', 'DB_PERSISTED',
    'created_at', now
  )

  redis.call('ZADD', KEYS[4], 3, '3')
end

redis.call('ZREM', KEYS[3], orderId)
refreshPendingOrgIndex()

if terminalTtlSeconds > 0 then
  redis.call('EXPIRE', KEYS[1], terminalTtlSeconds)
  redis.call('EXPIRE', KEYS[2], terminalTtlSeconds)
end

return result('MARKED', '', '')

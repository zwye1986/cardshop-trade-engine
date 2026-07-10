-- mark_pending_republished.lua
-- docs/交行直充业务系统详细设计.md「10.1 XXL-Job Handler 契约」
-- Redis pending 补投发布成功后推进状态。该脚本必须原子判断当前 Redis 订单状态：
-- 1. 当前仍是可补投状态时，推进为 PERSIST_QUEUED 并延后 pending/org_index score。
-- 2. 当前已经被 Consumer 标记为 DB_PERSISTED 时，只修复残留 pending/org_index，不能回退终态。

-- KEYS
-- 1 orderKey             recharge:{orgCode}:order:{outOrderSn}
-- 2 idemKey              recharge:{orgCode}:idem:{outOrderSn}
-- 3 pendingKey           recharge:{orgCode}:persist:pending
-- 4 orgIndexBucketKey    recharge:persist:pending:org_index:{bucket}

-- ARGV
-- 1 orgCode
-- 2 orderId
-- 3 fallbackPersistedVersion
-- 4 persistMessageId
-- 5 publishCount
-- 6 workerId
-- 7 nextRetryMillis
-- 8 nowMillis

local orgCode = ARGV[1]
local orderId = ARGV[2]
local fallbackPersistedVersion = tonumber(ARGV[3] or '1') or 1
local persistMessageId = ARGV[4] or ''
local publishCount = tonumber(ARGV[5] or '1') or 1
local workerId = ARGV[6] or ''
local nextRetryMillis = ARGV[7]
local nowMillis = ARGV[8]

local function result(code, failCode, failReason)
  return {
    'code', code,
    'order_id', orderId,
    'fail_code', failCode or '',
    'fail_reason', failReason or ''
  }
end

-- docs/交行直充业务系统详细设计.md「10.1 XXL-Job Handler 契约」
-- 只要脚本清掉 pending 成员，就必须同步刷新 org_index，避免空商户索引长期留在补投扫描入口。
local function refreshPendingOrgIndex()
  local nextPending = redis.call('ZRANGE', KEYS[3], 0, 0, 'WITHSCORES')
  if #nextPending == 0 then
    redis.call('ZREM', KEYS[4], orgCode)
  else
    redis.call('ZADD', KEYS[4], nextPending[2], orgCode)
  end
end

if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('ZREM', KEYS[3], orderId)
  refreshPendingOrgIndex()
  return result('SKIPPED', 'ORDER_NOT_FOUND', '订单快照不存在')
end

local currentOrderId = redis.call('HGET', KEYS[1], 'order_id') or ''
if currentOrderId ~= orderId then
  return result('SKIPPED', 'ORDER_ID_MISMATCH', '订单号不匹配')
end

local persistStatus = redis.call('HGET', KEYS[1], 'persist_status') or ''
if persistStatus == 'DB_PERSISTED' then
  local persistedVersion = tonumber(redis.call('HGET', KEYS[1], 'persisted_version') or '0') or 0
  if persistedVersion < 1 then
    persistedVersion = fallbackPersistedVersion
  end
  if persistedVersion < 1 then
    persistedVersion = 1
  end

  redis.call('HSET', KEYS[1],
    'persist_status', 'DB_PERSISTED',
    'persisted_version', tostring(persistedVersion),
    'last_republish_worker', workerId,
    'updated_at', nowMillis
  )
  redis.call('HSET', KEYS[2],
    'persist_status', 'DB_PERSISTED',
    'updated_at', nowMillis
  )
  redis.call('ZREM', KEYS[3], orderId)
  refreshPendingOrgIndex()
  return result('REPAIRED_ALREADY_PERSISTED', '', '')
end

-- 如果 Consumer 后续实现 PERSISTING processing window，补投成功回写不能把它拉回 PERSIST_QUEUED。
if persistStatus == 'PERSISTING' then
  return result('SKIPPED', 'ORDER_PERSISTING', '订单正在持久化')
end

if persistStatus ~= 'PENDING_MQ'
    and persistStatus ~= 'PERSIST_QUEUED'
    and persistStatus ~= 'PERSIST_FAILED' then
  return result('SKIPPED', 'STATE_NOT_MARKABLE', '订单状态不允许标记持久化队列')
end

redis.call('HSET', KEYS[1],
  'persist_status', 'PERSIST_QUEUED',
  'persist_msg_id', persistMessageId,
  'mq_publish_count', tostring(math.max(1, publishCount)),
  'last_republish_worker', workerId,
  'next_retry_time', nextRetryMillis,
  'updated_at', nowMillis
)

redis.call('HSET', KEYS[2],
  'persist_status', 'PERSIST_QUEUED',
  'persist_msg_id', persistMessageId,
  'updated_at', nowMillis
)

redis.call('ZADD', KEYS[3], nextRetryMillis, orderId)
redis.call('ZADD', KEYS[4], nextRetryMillis, orgCode)

return result('MARKED', '', '')

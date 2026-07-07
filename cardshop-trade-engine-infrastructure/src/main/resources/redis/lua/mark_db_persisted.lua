-- mark_db_persisted.lua
-- docs/交行直充业务系统详细设计.md「20.13.8 MySQL 异步落库」
-- Consumer 确认 MySQL 事务提交后，原子标记 Redis 订单已落库，并清理 pending/org_index 补投入口。

-- KEYS
-- 1 orderKey                recharge:{orgCode}:order:{outOrderSn}
-- 2 idemKey                 recharge:{orgCode}:idem:{outOrderSn}
-- 3 pendingKey              recharge:{orgCode}:persist:pending
-- 4 eventIndexKey           recharge:{orgCode}:order:event_index:{orderId}
-- 5 eventKey                recharge:{orgCode}:order:event:{orderId}:3
-- 6 orgIndexBucketKey       recharge:persist:pending:org_index:{bucket}

local orgCode = ARGV[1]
local orderId = ARGV[2]
local now = ARGV[3]
local terminalTtlSeconds = tonumber(ARGV[4] or '0')

local function result(code, failCode, failReason)
  return {
    'code', code,
    'order_id', orderId,
    'fail_code', failCode or '',
    'fail_reason', failReason or ''
  }
end

-- docs/交行直充业务系统详细设计.md「20.13.8 MySQL 异步落库」
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

local persistStatus = redis.call('HGET', KEYS[1], 'persist_status') or ''
if persistStatus == 'DB_PERSISTED' then
  redis.call('ZREM', KEYS[3], orderId)
else
  if persistStatus ~= 'PENDING_MQ'
      and persistStatus ~= 'PERSIST_QUEUED'
      and persistStatus ~= 'PERSISTING'
      and persistStatus ~= 'PERSIST_FAILED' then
    return result('MARK_FAILED', 'STATE_NOT_MARKABLE', '订单状态不允许标记落库完成')
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

  redis.call('HSET', KEYS[5],
    'order_id', orderId,
    'version', '3',
    'persist_status', 'DB_PERSISTED',
    'created_at', now
  )

  redis.call('ZADD', KEYS[4], 3, '3')
  redis.call('ZREM', KEYS[3], orderId)
end

refreshPendingOrgIndex()

if terminalTtlSeconds > 0 then
  redis.call('EXPIRE', KEYS[1], terminalTtlSeconds)
  redis.call('EXPIRE', KEYS[2], terminalTtlSeconds)
end

return result('MARKED', '', '')

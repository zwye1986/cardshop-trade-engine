-- mark_persist_failed.lua
-- docs/交行直充业务系统详细设计.md「20.13.8 MySQL 异步落库」
-- Consumer 落库失败时刷新 Redis 失败原因和下次补偿时间；不能删除 pending，否则订单会失去恢复入口。

-- KEYS
-- 1 orderKey                recharge:{orgCode}:order:{outOrderSn}
-- 2 idemKey                 recharge:{orgCode}:idem:{outOrderSn}
-- 3 pendingKey              recharge:{orgCode}:persist:pending
-- 4 eventIndexKey           recharge:{orgCode}:order:event_index:{orderId}
-- 5 eventKey                recharge:{orgCode}:order:event:{orderId}:3
-- 6 orgIndexBucketKey       recharge:persist:pending:org_index:{bucket}

local orgCode = ARGV[1]
local orderId = ARGV[2]
local nextRetryTime = ARGV[3]
local now = ARGV[4]
local failCode = ARGV[5]
local failReason = ARGV[6]
local retryCount = ARGV[7]

local function result(code, resultFailCode, resultFailReason)
  return {
    'code', code,
    'order_id', orderId,
    'fail_code', resultFailCode or '',
    'fail_reason', resultFailReason or ''
  }
end

if redis.call('EXISTS', KEYS[1]) == 0 then
  return result('MARK_FAILED', 'ORDER_NOT_FOUND', '订单快照不存在')
end

local currentOrderId = redis.call('HGET', KEYS[1], 'order_id') or ''
if currentOrderId ~= orderId then
  return result('MARK_FAILED', 'ORDER_ID_MISMATCH', '订单号不匹配')
end

local persistStatus = redis.call('HGET', KEYS[1], 'persist_status') or ''
if persistStatus == 'DB_PERSISTED' then
  return result('MARK_FAILED', 'ALREADY_DB_PERSISTED', '订单已落库，禁止标记失败')
end

if persistStatus ~= 'PENDING_MQ'
    and persistStatus ~= 'PERSIST_QUEUED'
    and persistStatus ~= 'PERSISTING'
    and persistStatus ~= 'PERSIST_FAILED' then
  return result('MARK_FAILED', 'STATE_NOT_MARKABLE', '订单状态不允许标记落库失败')
end

redis.call('HSET', KEYS[1],
  'persist_status', 'PERSIST_FAILED',
  'last_error', failReason,
  'last_error_code', failCode,
  'persist_retry_count', retryCount,
  'next_retry_time', nextRetryTime,
  'updated_at', now
)

redis.call('HSET', KEYS[2],
  'persist_status', 'PERSIST_FAILED',
  'last_error', failReason,
  'last_error_code', failCode,
  'updated_at', now
)

redis.call('HSET', KEYS[5],
  'order_id', orderId,
  'version', '3',
  'persist_status', 'PERSIST_FAILED',
  'fail_code', failCode,
  'fail_reason', failReason,
  'created_at', now
)

redis.call('ZADD', KEYS[4], 3, '3')
redis.call('ZADD', KEYS[3], nextRetryTime, orderId)
redis.call('ZADD', KEYS[6], nextRetryTime, orgCode)

return result('MARKED', '', '')

-- cancel_unconfirmed_accept.lua
-- docs/交行直充业务系统详细设计.md「4. Application 用例编排层」「6. Redis 受理与缓存层」
-- WAIT 未达到副本确认要求时，原子释放 Redis 预扣、清理 pending/org_index，并允许调用方后续重试同一 out_order_sn。

-- KEYS
-- 1 balanceKey              recharge:{orgCode}:balance
-- 2 idemKey                 recharge:{orgCode}:idem:{outOrderSn}
-- 3 orderKey                recharge:{orgCode}:order:{outOrderSn}
-- 4 reservationKey          recharge:{orgCode}:reservation:{orderId}
-- 5 pendingKey              recharge:{orgCode}:persist:pending
-- 6 eventIndexKey           recharge:{orgCode}:order:event_index:{orderId}
-- 7 eventKey                recharge:{orgCode}:order:event:{orderId}:2
-- 8 orgIndexBucketKey       recharge:persist:pending:org_index:{bucket}

local orgCode = ARGV[1]
local orderId = ARGV[2]
local outOrderSn = ARGV[3]
local amount = tonumber(ARGV[4])
local now = ARGV[5]
local reason = ARGV[6]

local function result(code, failCode, failReason)
  return {
    'code', code,
    'order_id', orderId,
    'fail_code', failCode or '',
    'fail_reason', failReason or ''
  }
end

if redis.call('EXISTS', KEYS[3]) == 0 then
  redis.call('DEL', KEYS[2])
  redis.call('ZREM', KEYS[5], orderId)
  return result('CANCELLED', '', '')
end

local currentOrderId = redis.call('HGET', KEYS[3], 'order_id') or ''
if currentOrderId ~= orderId then
  return result('CANCEL_FAILED', 'ORDER_ID_MISMATCH', '订单号不匹配，禁止回滚')
end

local persistStatus = redis.call('HGET', KEYS[3], 'persist_status') or ''
if persistStatus ~= 'PENDING_MQ' then
  return result('CANCEL_FAILED', 'STATE_NOT_CANCELLABLE', '订单状态不允许回滚')
end

local balanceReserved = redis.call('HGET', KEYS[3], 'balance_reserved') or '0'
if balanceReserved == '1' then
  redis.call('HINCRBY', KEYS[1], 'available', amount)
  redis.call('HINCRBY', KEYS[1], 'frozen', -amount)
end

redis.call('HSET', KEYS[3],
  'accept_status', 'ACCEPT_REJECTED',
  'order_status', 'ACCEPT_REJECTED',
  'persist_status', 'ACCEPT_REJECTED',
  'dispatch_allowed', '0',
  'balance_reserved', '0',
  'fail_code', 'WAIT_NOT_CONFIRMED',
  'fail_reason', reason,
  'order_version', '2',
  'updated_at', now
)

redis.call('HSET', KEYS[7],
  'order_id', orderId,
  'version', '2',
  'order_status', 'ACCEPT_REJECTED',
  'validation_status', redis.call('HGET', KEYS[3], 'validation_status') or '',
  'fail_code', 'WAIT_NOT_CONFIRMED',
  'fail_reason', reason,
  'created_at', now
)

redis.call('ZADD', KEYS[6], 2, '2')
redis.call('DEL', KEYS[2])
redis.call('DEL', KEYS[4])
redis.call('ZREM', KEYS[5], orderId)

local nextPending = redis.call('ZRANGE', KEYS[5], 0, 0, 'WITHSCORES')
if #nextPending == 0 then
  redis.call('ZREM', KEYS[8], orgCode)
else
  redis.call('ZADD', KEYS[8], nextPending[2], orgCode)
end

return result('CANCELLED', '', '')

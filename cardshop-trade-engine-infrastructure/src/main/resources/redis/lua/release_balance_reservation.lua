-- release_balance_reservation.lua
-- docs/交行直充业务系统高可用方案设计.md「14.6 订单最终失败/作废退款协议」「16.6 release.lua」
-- 供应商最终失败、作废成功或退款完成后，按 releaseEventId 幂等释放 Redis 预扣。

-- KEYS
-- 1 balanceKey              recharge:{orgCode}:balance
-- 2 orderKey                recharge:{orgCode}:order:{outOrderSn}
-- 3 reservationKey          recharge:{orgCode}:reservation:{orderId}
-- 4 eventIndexKey           recharge:{orgCode}:order:event_index:{orderId}
-- 5 eventKey                recharge:{orgCode}:order:event:{orderId}:4
-- 6 releaseEventKey         recharge:{orgCode}:release:{releaseEventId}

local orgCode = ARGV[1]
local orderId = ARGV[2]
local outOrderSn = ARGV[3]
local amount = tonumber(ARGV[4])
local releaseType = ARGV[5]
local releaseEventId = ARGV[6]
local reason = ARGV[7]
local now = ARGV[8]
local terminalOrderStatus = 'FAILED'
if releaseType == 'REFUND' then
  terminalOrderStatus = 'REFUNDED'
end

local function result(code, failCode, failReason)
  return {
    'code', code,
    'order_id', orderId,
    'fail_code', failCode or '',
    'fail_reason', failReason or ''
  }
end

if releaseEventId == nil or releaseEventId == '' then
  return result('RELEASE_FAILED', 'RELEASE_EVENT_ID_MISSING', '释放事件ID缺失')
end

if amount == nil or amount <= 0 then
  return result('RELEASE_FAILED', 'INVALID_AMOUNT', '释放金额非法')
end

if redis.call('EXISTS', KEYS[6]) == 1 then
  return result('ALREADY_RELEASED', '', '')
end

if redis.call('EXISTS', KEYS[2]) == 0 then
  return result('RELEASE_FAILED', 'ORDER_NOT_FOUND', '订单快照不存在')
end

local currentOrderId = redis.call('HGET', KEYS[2], 'order_id') or ''
if currentOrderId ~= orderId then
  return result('RELEASE_FAILED', 'ORDER_ID_MISMATCH', '订单号不匹配，禁止释放')
end

local reserved = redis.call('HGET', KEYS[2], 'balance_reserved') or '0'
local reservationExists = redis.call('EXISTS', KEYS[3]) == 1
if reserved == '1' and not reservationExists then
  return result('RELEASE_FAILED', 'RESERVATION_NOT_FOUND', '预扣记录不存在，禁止自动释放')
end

local reservationOrderId = ''
local reservationStatus = ''
if reservationExists then
  reservationOrderId = redis.call('HGET', KEYS[3], 'order_id') or ''
  reservationStatus = redis.call('HGET', KEYS[3], 'status') or ''
end
if reservationExists and reservationOrderId ~= orderId then
  return result('RELEASE_FAILED', 'RESERVATION_ORDER_ID_MISMATCH', '预扣记录订单号不匹配')
end

if reserved ~= '1' and reservationExists and reservationStatus ~= 'RELEASED' and reservationStatus ~= 'NONE' then
  return result('RELEASE_FAILED', 'RESERVATION_NOT_RELEASED', '预扣记录未释放，禁止按幂等成功处理')
end

if reserved ~= '1' or reservationStatus == 'RELEASED' then
  redis.call('HSET', KEYS[2],
    'order_status', terminalOrderStatus,
    'dispatch_allowed', '0',
    'balance_reserved', '0',
    'release_type', releaseType,
    'release_event_id', releaseEventId,
    'release_reason', reason,
    'updated_at', now
  )
  redis.call('HSET', KEYS[3],
    'status', 'RELEASED',
    'release_type', releaseType,
    'release_event_id', releaseEventId,
    'release_reason', reason,
    'updated_at', now
  )
  redis.call('SET', KEYS[6], 'ALREADY_RELEASED')
  redis.call('HSET', KEYS[5],
    'order_id', orderId,
    'version', '4',
    'release_event_id', releaseEventId,
    'release_type', releaseType,
    'release_status', 'ALREADY_RELEASED',
    'created_at', now
  )
  redis.call('ZADD', KEYS[4], 4, '4')
  return result('ALREADY_RELEASED', '', '')
end

local reservationAmount = tonumber(redis.call('HGET', KEYS[3], 'amount') or tostring(amount)) or amount
if reservationAmount ~= amount then
  return result('RELEASE_FAILED', 'AMOUNT_MISMATCH', '释放金额与预扣金额不一致')
end

local frozen = tonumber(redis.call('HGET', KEYS[1], 'frozen') or '0')
if frozen < amount then
  return result('RELEASE_FAILED', 'FROZEN_BALANCE_NOT_ENOUGH', '冻结余额不足，禁止自动释放')
end

redis.call('HINCRBY', KEYS[1], 'available', amount)
redis.call('HINCRBY', KEYS[1], 'frozen', -amount)

redis.call('HSET', KEYS[2],
  'order_status', terminalOrderStatus,
  'dispatch_allowed', '0',
  'balance_reserved', '0',
  'release_type', releaseType,
  'release_event_id', releaseEventId,
  'release_reason', reason,
  'updated_at', now
)

redis.call('HSET', KEYS[3],
  'status', 'RELEASED',
  'release_type', releaseType,
  'release_event_id', releaseEventId,
  'release_reason', reason,
  'updated_at', now
)

redis.call('HSET', KEYS[5],
  'order_id', orderId,
  'version', '4',
  'release_event_id', releaseEventId,
  'release_type', releaseType,
  'release_status', 'RELEASED',
  'created_at', now
)

redis.call('SET', KEYS[6], 'RELEASED')
redis.call('ZADD', KEYS[4], 4, '4')

return result('RELEASED', '', '')

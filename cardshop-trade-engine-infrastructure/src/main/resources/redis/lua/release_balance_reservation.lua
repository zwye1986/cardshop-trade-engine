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
local CONFIRM_OPERATION = 'CONFIRM'
local FUNDS_RESERVED = 'RESERVED'
local FUNDS_CONSUMED = 'CONSUMED'
local FUNDS_REFUNDED = 'REFUNDED'
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

local fundsStatus = ''
if reservationExists then
  fundsStatus = redis.call('HGET', KEYS[3], 'funds_status') or ''
end

-- 详细设计9.2成功预扣收尾：成功只消费冻结额，绝不回补available或制造退款事件。
if releaseType == CONFIRM_OPERATION then
  if reserved ~= '1' or not reservationExists
      or redis.call('HGET', KEYS[2], 'org_code') ~= orgCode
      or redis.call('HGET', KEYS[2], 'out_order_sn') ~= outOrderSn
      or redis.call('HGET', KEYS[3], 'out_order_sn') ~= outOrderSn
      or redis.call('HGET', KEYS[2], 'persist_status') ~= 'DB_PERSISTED' then
    return result('CONFIRM_FAILED', 'RESERVATION_CONFIRM_MISMATCH', '消费确认缺少可信预扣或持久化事实')
  end
  local reservedAmount = tonumber(redis.call('HGET', KEYS[3], 'amount') or '')
  if reservedAmount ~= amount then
    return result('CONFIRM_FAILED', 'AMOUNT_MISMATCH', '消费确认金额与预扣金额不一致')
  end
  if reservationStatus == 'RELEASED' or reservationStatus == 'NONE' then
    return result('CONFIRM_FAILED', 'RESERVATION_NOT_CONFIRMABLE', '已释放或无预扣记录不能消费确认')
  end
  if fundsStatus == FUNDS_CONSUMED then
    return result('ALREADY_CONFIRMED', '', '')
  end
  if fundsStatus == '' then
    -- 旧记录可能已被历史缓存重建清过冻结额，禁止借用其他订单冻结额补猜。
    return result('CONFIRM_FAILED', 'RESERVATION_FUNDS_STATUS_MISSING', '旧预扣缺少资金标记，须人工核查')
  end
  if fundsStatus ~= FUNDS_RESERVED then
    return result('CONFIRM_FAILED', 'RESERVATION_NOT_CONFIRMABLE', '预扣状态不允许消费确认')
  end
  local frozen = tonumber(redis.call('HGET', KEYS[1], 'frozen') or '0') or 0
  if frozen < amount then
    return result('CONFIRM_FAILED', 'FROZEN_BALANCE_NOT_ENOUGH', '冻结余额不足，禁止消费确认')
  end
  redis.call('HINCRBY', KEYS[1], 'frozen', -amount)
  redis.call('HSET', KEYS[3], 'status', 'DB_PERSISTED', 'funds_status', FUNDS_CONSUMED,
      'confirm_event_id', releaseEventId, 'confirmed_at', now, 'updated_at', now)
  redis.call('HSET', KEYS[2], 'funds_status', FUNDS_CONSUMED)
  return result('CONFIRMED', '', '')
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

if fundsStatus ~= '' and fundsStatus ~= FUNDS_RESERVED and fundsStatus ~= FUNDS_CONSUMED then
  return result('RELEASE_FAILED', 'RESERVATION_FUNDS_STATUS_INVALID', '资金标记与未释放记录矛盾，禁止重复回补')
end
if fundsStatus == FUNDS_CONSUMED
    and (redis.call('HEXISTS', KEYS[1], 'available') == 0
      or redis.call('HEXISTS', KEYS[1], 'frozen') == 0) then
  return result('RELEASE_FAILED', 'BALANCE_BASELINE_MISSING', '余额基线缺失，禁止凭退款金额创建余额')
end

local frozen = tonumber(redis.call('HGET', KEYS[1], 'frozen') or '0')
if fundsStatus ~= FUNDS_CONSUMED and frozen < amount then
  return result('RELEASE_FAILED', 'FROZEN_BALANCE_NOT_ENOUGH', '冻结余额不足，禁止自动释放')
end

redis.call('HINCRBY', KEYS[1], 'available', amount)
-- 已消费订单作废退款不再扣冻结额，避免侵占其他订单；旧记录仍走原冻结校验。
if fundsStatus ~= FUNDS_CONSUMED then
  redis.call('HINCRBY', KEYS[1], 'frozen', -amount)
end

redis.call('HSET', KEYS[2],
  'order_status', terminalOrderStatus,
  'dispatch_allowed', '0',
  'balance_reserved', '0',
  'funds_status', FUNDS_REFUNDED,
  'release_type', releaseType,
  'release_event_id', releaseEventId,
  'release_reason', reason,
  'updated_at', now
)

redis.call('HSET', KEYS[3],
  'status', 'RELEASED',
  'funds_status', FUNDS_REFUNDED,
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

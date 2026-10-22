-- release_balance_reservation.lua
-- docs/交行直充业务系统详细设计.md「2026-10-21 订单快照失效后的独立资金协议」
-- 资金以独立reservation及同订单操作凭证为准；业务快照失效不阻断消费确认/退款。
-- KEYS: balance, order(可缺失), reservation, eventIndex, event4, releaseEvent
-- ARGV: orgCode, orderId, outOrderSn, amount, operation, operationId, reason, now,
--       expectedFundsStatus(可信MySQL业务资格), alreadyReleasedOnly(已有Delta时禁止再次加钱),
--       allowLegacyCancelledRefund(已扣款且已作废的旧单兼容授权，默认关闭)
local orgCode, orderId, outOrderSn = ARGV[1], ARGV[2], ARGV[3]
local operation, operationId, reason, now = ARGV[5], ARGV[6], ARGV[7], ARGV[8]
local expectedFundsStatus = ARGV[9] or ''
local alreadyReleasedOnly = ARGV[10] == '1'
local allowLegacyCancelledRefund = ARGV[11] == '1'
local CONFIRM = 'CONFIRM'
local RESERVED, CONSUMED, REFUNDED = 'RESERVED', 'CONSUMED', 'REFUNDED'
local LEGACY_CANCELLED = 'LEGACY_CANCELLED'
-- Lua number为double，拒绝超过精确整数范围的资金值，避免精度损失或后置HINCRBY失败。
local MAX_SAFE_INTEGER = 9007199254740991
local function result(code, failCode, failReason)
  return {'code', code, 'order_id', orderId, 'fail_code', failCode or '', 'fail_reason', failReason or ''}
end
local function failed(code, message)
  return result(operation == CONFIRM and 'CONFIRM_FAILED' or 'RELEASE_FAILED', code, message)
end
local function integer(value)
  if value == false or value == nil or not string.match(value, '^%d+$')
      or (#value > 1 and string.sub(value, 1, 1) == '0') then return nil end
  local number = tonumber(value)
  if number == nil or number > MAX_SAFE_INTEGER then return nil end
  return number
end
local function keyType(key) return redis.call('TYPE', key).ok end
local function optionalType(key, expected)
  local actual = keyType(key)
  return actual == 'none' or actual == expected
end
if operationId == nil or operationId == '' then return failed('RELEASE_EVENT_ID_MISSING', '资金操作ID缺失') end
local amount = integer(ARGV[4])
if amount == nil or amount <= 0 then return failed('INVALID_AMOUNT', '资金操作金额非法') end
if operation ~= CONFIRM and operation ~= 'RELEASE' and operation ~= 'REFUND' then
  return failed('INVALID_OPERATION', '资金操作类型非法')
end
-- Redis Lua出错不会回滚已经执行的写入，必须在任何余额变化前检查所有待写键类型与数字。
if not optionalType(KEYS[2], 'hash') or not optionalType(KEYS[4], 'zset')
    or not optionalType(KEYS[5], 'hash') or not optionalType(KEYS[6], 'string') then
  return failed('FUNDS_KEY_TYPE_INVALID', '资金关联键类型冲突')
end
if keyType(KEYS[3]) == 'none' then return failed('RESERVATION_NOT_FOUND', '预扣记录不存在，禁止自动操作') end
if keyType(KEYS[3]) ~= 'hash' then return failed('FUNDS_KEY_TYPE_INVALID', '预扣键类型冲突') end
if keyType(KEYS[1]) ~= 'hash' then return failed('BALANCE_BASELINE_MISSING', '余额基线缺失，禁止创建余额') end
local availableText = redis.call('HGET', KEYS[1], 'available')
local frozenText = redis.call('HGET', KEYS[1], 'frozen')
if not availableText or not frozenText then return failed('BALANCE_BASELINE_MISSING', '余额基线字段缺失') end
local available, frozen = integer(availableText), integer(frozenText)
if available == nil or frozen == nil then return failed('BALANCE_BASELINE_INVALID', '余额基线字段非法') end
local snapshotExists = redis.call('EXISTS', KEYS[2]) == 1
if snapshotExists and (redis.call('HGET', KEYS[2], 'order_id') ~= orderId
    or redis.call('HGET', KEYS[2], 'org_code') ~= orgCode
    or redis.call('HGET', KEYS[2], 'out_order_sn') ~= outOrderSn) then
  return failed('ORDER_ID_MISMATCH', '业务快照归属冲突')
end
local reservationOrg = redis.call('HGET', KEYS[3], 'org_code')
-- 旧版缺org_code仅在商户隔离key及MySQL身份已经核对的内部入口兼容，不补写历史字段。
if (reservationOrg and reservationOrg ~= orgCode)
    or redis.call('HGET', KEYS[3], 'order_id') ~= orderId
    or redis.call('HGET', KEYS[3], 'out_order_sn') ~= outOrderSn then
  return failed('RESERVATION_ORDER_ID_MISMATCH', '预扣记录归属冲突')
end
if integer(redis.call('HGET', KEYS[3], 'amount')) ~= amount then
  return failed('AMOUNT_MISMATCH', '资金操作金额与预扣金额不一致')
end
local status = redis.call('HGET', KEYS[3], 'status') or ''
local funds = redis.call('HGET', KEYS[3], 'funds_status') or ''
local legacyCancelledRefund = false
if funds == '' then
  -- 详细设计「历史已作废订单退款最小兼容」：只有MySQL已核实扣款成功及作废事实的入口可授权。
  -- 不将聚合frozen当成本单资金证据，也不补造CONSUMED或消费确认凭证；其他入口仍失败关闭。
  if not allowLegacyCancelledRefund or operation ~= 'RELEASE' or expectedFundsStatus ~= CONSUMED then
    return failed('RESERVATION_FUNDS_STATUS_MISSING', '旧预扣缺少资金标记，须人工核查')
  end
  if status ~= 'PENDING_MQ' and status ~= 'PERSIST_QUEUED'
      and status ~= 'PERSISTING' and status ~= 'DB_PERSISTED' then
    return failed('RESERVATION_NOT_RELEASABLE', '旧预扣持久化状态不允许兼容退款')
  end
  if (redis.call('HGET', KEYS[3], 'release_event_id') or '') ~= ''
      or (redis.call('HGET', KEYS[3], 'release_type') or '') ~= ''
      or (redis.call('HGET', KEYS[3], 'confirm_event_id') or '') ~= '' then
    return failed('RELEASE_EVIDENCE_MISSING', '旧预扣存在消费或退款凭证冲突，禁止兼容回补')
  end
  legacyCancelledRefund = true
end
local marker = redis.call('GET', KEYS[6])
local markerValue = cjson.encode({orgCode, orderId, outOrderSn, ARGV[4], operationId})
if operation == CONFIRM then
  if status ~= 'DB_PERSISTED' or (funds ~= RESERVED and funds ~= CONSUMED) then
    return failed('RESERVATION_NOT_CONFIRMABLE', '缺少可信落库预扣或资金状态不允许确认')
  end
  if funds == CONSUMED then
    if redis.call('HGET', KEYS[3], 'confirm_event_id') ~= operationId then
      return failed('CONFIRM_EVIDENCE_MISSING', '消费确认操作凭证缺失或冲突')
    end
    if reservationOrg == orgCode then redis.call('PERSIST', KEYS[3]) end
    return result('ALREADY_CONFIRMED')
  end
  if frozen < amount then return failed('FROZEN_BALANCE_NOT_ENOUGH', '冻结余额不足，禁止消费确认') end
  redis.call('HINCRBY', KEYS[1], 'frozen', -amount)
  if reservationOrg == orgCode then redis.call('PERSIST', KEYS[3]) end
  redis.call('HSET', KEYS[3], 'funds_status', CONSUMED, 'confirm_event_id', operationId,
      'confirmed_at', now, 'updated_at', now)
  if snapshotExists then redis.call('HSET', KEYS[2], 'funds_status', CONSUMED) end
  return result('CONFIRMED')
end
if funds == REFUNDED and status == 'RELEASED' then
  local previousId = redis.call('HGET', KEYS[3], 'release_event_id') or ''
  local previousType = redis.call('HGET', KEYS[3], 'release_type') or ''
  if previousId == '' or (previousType ~= 'RELEASE' and previousType ~= 'REFUND') then
    return failed('RELEASE_EVIDENCE_MISSING', '已退款记录缺少可信释放凭证')
  end
  -- 保留旧ID凭证，不因任务ID或新稳定资金ID变化而再次退款。动态键仍属于相同商户hash tag。
  local previousKey = string.sub(KEYS[6], 1, #KEYS[6] - #operationId) .. previousId
  if not optionalType(previousKey, 'string') then return failed('RELEASE_EVIDENCE_MISSING', '原退款凭证类型冲突') end
  local previousMarker = redis.call('GET', previousKey)
  local previousValue = cjson.encode({orgCode, orderId, outOrderSn, ARGV[4], previousId})
  if previousMarker ~= previousValue and previousMarker ~= 'RELEASED' and previousMarker ~= 'ALREADY_RELEASED' then
    return failed('RELEASE_EVIDENCE_MISSING', '原退款凭证缺失或冲突')
  end
  if marker and marker ~= markerValue and not (previousId == operationId and (marker == 'RELEASED' or marker == 'ALREADY_RELEASED')) then
    return failed('RELEASE_EVIDENCE_MISSING', '退款操作凭证冲突')
  end
  return result('ALREADY_RELEASED')
end
if marker or alreadyReleasedOnly then return failed('RELEASE_EVIDENCE_MISSING', '已有退款事实与资金状态冲突，禁止再次回补') end
if not legacyCancelledRefund and funds ~= RESERVED and funds ~= CONSUMED then
  return failed('RESERVATION_FUNDS_STATUS_INVALID', '资金标记与未释放记录矛盾')
end
if funds == CONSUMED and redis.call('HGET', KEYS[3], 'confirm_event_id') ~= 'balance-confirm-' .. orderId then
  return failed('CONFIRM_EVIDENCE_MISSING', '消费标记缺少本订单确认凭证，禁止自动退款')
end
if not legacyCancelledRefund and status ~= 'DB_PERSISTED' then
  return failed('RESERVATION_NOT_RELEASABLE', '预扣尚无可信落库事实')
end
if not legacyCancelledRefund and expectedFundsStatus ~= '' and funds ~= expectedFundsStatus then
  return failed('RESERVATION_FUNDS_STATUS_INVALID', '资金状态与MySQL业务终态不匹配')
end
if funds == RESERVED and frozen < amount then return failed('FROZEN_BALANCE_NOT_ENOUGH', '冻结余额不足，禁止自动释放') end
if available > MAX_SAFE_INTEGER - amount then return failed('BALANCE_BASELINE_INVALID', '回补后余额超过精确整数范围') end
redis.call('HINCRBY', KEYS[1], 'available', amount)
if funds == RESERVED then redis.call('HINCRBY', KEYS[1], 'frozen', -amount) end
if reservationOrg == orgCode then redis.call('PERSIST', KEYS[3]) end
redis.call('HSET', KEYS[3], 'status', 'RELEASED', 'funds_status', REFUNDED, 'release_type', operation,
    'release_event_id', operationId, 'release_reason', reason,
    'refunded_from', legacyCancelledRefund and LEGACY_CANCELLED or funds,
    'released_at', now, 'updated_at', now)
redis.call('SET', KEYS[6], markerValue)
if snapshotExists then
  local terminalStatus = operation == 'REFUND' and 'REFUNDED' or 'FAILED'
  redis.call('HSET', KEYS[2], 'order_status', terminalStatus, 'dispatch_allowed', '0',
      'balance_reserved', '0', 'funds_status', REFUNDED, 'release_type', operation,
      'release_event_id', operationId, 'release_reason', reason, 'updated_at', now)
  redis.call('HSET', KEYS[5], 'order_id', orderId, 'version', '4', 'release_event_id', operationId,
      'release_type', operation, 'release_status', 'RELEASED', 'created_at', now)
  redis.call('ZADD', KEYS[4], 4, '4')
end
return result('RELEASED')

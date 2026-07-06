-- accept_recharge_order.lua
-- docs/交行直充业务系统详细设计.md「20.6 Redis Lua 入参与 Key 构造」「20.13.6 Redis Lua 下单结果」
-- 本脚本只处理 Redis 原子受理；WAIT、cancel 和 MQ confirm 由 Java 编排层在同一接单链路继续推进。

-- KEYS
-- 1 balanceKey              recharge:{orgCode}:balance
-- 2 idemKey                 recharge:{orgCode}:idem:{outOrderSn}
-- 3 orderKey                recharge:{orgCode}:order:{outOrderSn}
-- 4 reservationKey          recharge:{orgCode}:reservation:{orderId}
-- 5 pendingKey              recharge:{orgCode}:persist:pending
-- 6 eventIndexKey           recharge:{orgCode}:order:event_index:{orderId}
-- 7 eventKey                recharge:{orgCode}:order:event:{orderId}:1
-- 8 nonceKey                recharge:{orgCode}:nonce:{nonceStr}
-- 9 merchantConfigKey       recharge:{orgCode}:merchant:config
-- 10 productConfigKey       recharge:{orgCode}:product:{productId}:config
-- 11 repairingLockKey       recharge:{orgCode}:balance:repairing_lock
-- 12 orgIndexBucketKey      recharge:persist:pending:org_index:{bucket}

local orgCode = ARGV[1]
local appId = ARGV[2]
local outOrderSn = ARGV[3]
local orderId = ARGV[4]
local productId = ARGV[5]
local selectedProductId = ARGV[6]
local account = ARGV[7]
local notifyUrl = ARGV[8]
local nonceStr = ARGV[9]
local num = tonumber(ARGV[10])
local amount = tonumber(ARGV[11])
local productName = ARGV[12]
local productValue = ARGV[13]
local productType = ARGV[14]
local couponType = ARGV[15]
local supplierChannel = ARGV[16]
local requestHash = ARGV[17]
local requestHashVersion = ARGV[18]
local requestHashAlg = ARGV[19]
local candidateValidationStatus = ARGV[20]
local candidateFailCode = ARGV[21]
local candidateFailReason = ARGV[22]
local merchantConfigVersion = ARGV[23]
local productConfigVersion = ARGV[24]
local now = ARGV[25]
local nextRetryTime = ARGV[26]
local pendingTtlMs = tonumber(ARGV[27])
local nonceTtlMs = tonumber(ARGV[28])

local function bool_text(value)
  if value then
    return '1'
  end
  return '0'
end

local function result(code, validationStatus, failCode, failReason, dispatchAllowed, balanceReserved, version)
  return {
    'code', code,
    'order_id', orderId,
    'validation_status', validationStatus or '',
    'fail_code', failCode or '',
    'fail_reason', failReason or '',
    'dispatch_allowed', bool_text(dispatchAllowed),
    'balance_reserved', bool_text(balanceReserved),
    'order_version', tostring(version or 0)
  }
end

if redis.call('EXISTS', KEYS[2]) == 1 then
  local oldOrderId = redis.call('HGET', KEYS[2], 'order_id') or ''
  orderId = oldOrderId
  return result('DUPLICATE', '', 'DUPLICATE_OUT_ORDER_SN', '订单号重复', false, false, 0)
end

if redis.call('EXISTS', KEYS[11]) == 1 then
  return result('BALANCE_REPAIRING', '', 'BALANCE_REPAIRING', '商户余额修复中，请稍后重试', false, false, 0)
end

if redis.call('EXISTS', KEYS[8]) == 1 then
  return result('NONCE_REPLAY', '', 'NONCE_REPLAY', '请求随机串重复', false, false, 0)
end

local finalStatus = 'PASSED'
local failCode = ''
local failReason = ''

local function fail_once(code, reason)
  if finalStatus == 'PASSED' then
    finalStatus = 'FAILED'
    failCode = code or ''
    failReason = reason or ''
  end
end

if candidateValidationStatus == 'FAILED' then
  fail_once(candidateFailCode, candidateFailReason)
end

local merchantStatus = redis.call('HGET', KEYS[9], 'status')
local redisMerchantVersion = redis.call('HGET', KEYS[9], 'version')
if merchantStatus == false or redisMerchantVersion ~= merchantConfigVersion then
  return result('CONFIG_CHANGED', '', 'MERCHANT_CONFIG_STALE', '商户配置缺失或版本不一致', false, false, 0)
end
if merchantStatus ~= 'ENABLED' then
  fail_once('MERCHANT_DISABLED', '商户已停用')
end

local productStatus = redis.call('HGET', KEYS[10], 'status')
local redisProductVersion = redis.call('HGET', KEYS[10], 'version')
local redisProductPrice = redis.call('HGET', KEYS[10], 'price')
local redisProductType = redis.call('HGET', KEYS[10], 'product_type')
if productStatus == false or redisProductVersion ~= productConfigVersion then
  return result('CONFIG_CHANGED', '', 'PRODUCT_CONFIG_STALE', '商品配置缺失或版本不一致', false, false, 0)
end
if productStatus ~= 'ENABLED' then
  fail_once('PRODUCT_NO_PERMISSION', '商品无权限或已停用')
end
if tostring(redisProductType or '') ~= tostring(productType) or tostring(productType) ~= '1' then
  fail_once('NOT_RECHARGE_PRODUCT', '非直充商品')
end
if tostring(redisProductPrice or '') ~= tostring(amount) then
  fail_once('PRICE_INVALID', '商品价格已变化')
end
if num ~= 1 then
  fail_once('INVALID_NUM', '首期直充只支持购买数量为1')
end

local available = tonumber(redis.call('HGET', KEYS[1], 'available') or '0')
if finalStatus == 'PASSED' and available < amount then
  fail_once('BALANCE_NOT_ENOUGH', '商户余额不足')
end

local nonceSet = redis.call('SET', KEYS[8], nonceStr, 'NX', 'PX', nonceTtlMs)
if not nonceSet then
  return result('NONCE_REPLAY', '', 'NONCE_REPLAY', '请求随机串重复', false, false, 0)
end

local dispatchAllowed = finalStatus == 'PASSED'
local balanceReserved = finalStatus == 'PASSED'
local orderStatus = 'VALIDATION_FAILED'
if finalStatus == 'PASSED' then
  orderStatus = 'ACCEPTED'
  redis.call('HINCRBY', KEYS[1], 'available', -amount)
  redis.call('HINCRBY', KEYS[1], 'frozen', amount)
end

redis.call('HSET', KEYS[3],
  'order_id', orderId,
  'org_code', orgCode,
  'app_id', appId,
  'out_order_sn', outOrderSn,
  'product_id', productId,
  'selected_product_id', selectedProductId,
  'account', account,
  'notify_url', notifyUrl,
  'num', tostring(num),
  'order_price', tostring(amount),
  'product_name', productName,
  'product_value', productValue,
  'product_type', productType,
  'coupon_type', couponType,
  'supplier_channel', supplierChannel,
  'request_hash', requestHash,
  'request_hash_version', requestHashVersion,
  'request_hash_alg', requestHashAlg,
  'order_status', orderStatus,
  'validation_status', finalStatus,
  'fail_code', failCode,
  'fail_reason', failReason,
  'dispatch_allowed', bool_text(dispatchAllowed),
  'balance_reserved', bool_text(balanceReserved),
  'persist_status', 'PENDING_MQ',
  'order_version', '1',
  'persisted_version', '0',
  'created_at', now,
  'updated_at', now
)

redis.call('HSET', KEYS[4],
  'order_id', orderId,
  'out_order_sn', outOrderSn,
  'request_hash', requestHash,
  'amount', tostring(amount),
  'status', dispatchAllowed and 'PENDING_MQ' or 'NONE',
  'created_at', now,
  'updated_at', now
)

redis.call('HSET', KEYS[2],
  'request_hash', requestHash,
  'order_id', orderId,
  'persist_status', 'PENDING_MQ',
  'created_at', now
)

redis.call('HSET', KEYS[7],
  'order_id', orderId,
  'version', '1',
  'order_status', orderStatus,
  'validation_status', finalStatus,
  'fail_code', failCode,
  'fail_reason', failReason,
  'created_at', now
)

redis.call('ZADD', KEYS[6], 1, '1')
redis.call('ZADD', KEYS[5], nextRetryTime, orderId)
redis.call('ZADD', KEYS[12], nextRetryTime, orgCode)

redis.call('PEXPIRE', KEYS[2], pendingTtlMs)
redis.call('PEXPIRE', KEYS[3], pendingTtlMs)
redis.call('PEXPIRE', KEYS[4], pendingTtlMs)
redis.call('PEXPIRE', KEYS[6], pendingTtlMs)
redis.call('PEXPIRE', KEYS[7], pendingTtlMs)
redis.call('PEXPIRE', KEYS[8], nonceTtlMs)

if finalStatus == 'PASSED' then
  return result('ACCEPTED', finalStatus, '', '', true, true, 1)
end
return result('ACCEPTED_FAILED', finalStatus, failCode, failReason, false, false, 1)

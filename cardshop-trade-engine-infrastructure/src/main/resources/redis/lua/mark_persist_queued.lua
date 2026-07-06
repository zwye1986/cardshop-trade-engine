-- mark_persist_queued.lua
-- docs/交行直充业务系统详细设计.md「20.5 下单服务实现细节」
-- RabbitMQ publisher confirm ack 后推进 Redis 持久化状态。该脚本不清理 pending，pending 只能在 MySQL 已落库后清理。

-- KEYS
-- 1 orderKey                recharge:{orgCode}:order:{outOrderSn}
-- 2 idemKey                 recharge:{orgCode}:idem:{outOrderSn}
-- 3 eventIndexKey           recharge:{orgCode}:order:event_index:{orderId}
-- 4 eventKey                recharge:{orgCode}:order:event:{orderId}:2

local orderId = ARGV[1]
local persistMessageId = ARGV[2]
local publishCount = ARGV[3]
local now = ARGV[4]

local function result(code, failCode, failReason)
  return {
    'code', code,
    'order_id', orderId,
    'fail_code', failCode or '',
    'fail_reason', failReason or ''
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
if persistStatus ~= 'PENDING_MQ' and persistStatus ~= 'PERSIST_QUEUED' then
  return result('MARK_FAILED', 'STATE_NOT_MARKABLE', '订单状态不允许标记持久化队列')
end

redis.call('HSET', KEYS[1],
  'persist_status', 'PERSIST_QUEUED',
  'persist_msg_id', persistMessageId,
  'mq_publish_count', publishCount,
  'updated_at', now
)

redis.call('HSET', KEYS[2],
  'persist_status', 'PERSIST_QUEUED',
  'persist_msg_id', persistMessageId,
  'updated_at', now
)

redis.call('HSET', KEYS[4],
  'order_id', orderId,
  'version', '2',
  'persist_status', 'PERSIST_QUEUED',
  'persist_msg_id', persistMessageId,
  'created_at', now
)

redis.call('ZADD', KEYS[3], 2, '2')

return result('MARKED', '', '')

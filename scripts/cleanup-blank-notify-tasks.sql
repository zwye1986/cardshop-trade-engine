-- 清理历史空 notify_url 回调任务（dev/test 运维脚本）
-- 参见 docs/交行直充业务系统详细设计.md「9.3 回调设计」：
-- 仅清理DIRECT_JSON残留空URL任务；BOCOM_OPENAPI必须保留待恢复，不能伪成功。
-- 避免 XXL-Job 继续扫描并制造无效错误日志。不物理删除，便于审计。

-- 1) 预览：按状态统计空 URL 非终态任务
SELECT status, COUNT(*) AS cnt
FROM recharge_notify_task
WHERE delivery_channel = 'DIRECT_JSON'
  AND (notify_url IS NULL OR TRIM(notify_url) = '')
  AND status IN ('INIT', 'FAILED', 'PROCESSING', 'MANUAL_REQUIRED')
GROUP BY status
ORDER BY status;

-- 2) 预览：样本 task_id（最多 50 条）
SELECT task_id, order_id, status, retry_count, next_retry_time, LEFT(IFNULL(last_error, ''), 80) AS last_error
FROM recharge_notify_task
WHERE delivery_channel = 'DIRECT_JSON'
  AND (notify_url IS NULL OR TRIM(notify_url) = '')
  AND status IN ('INIT', 'FAILED', 'PROCESSING', 'MANUAL_REQUIRED')
ORDER BY next_retry_time ASC, id ASC
LIMIT 50;

-- 3) 清理：标记 SUCCESS，释放租约，清空 last_error
UPDATE recharge_notify_task
SET status = 'SUCCESS',
    locked_by = NULL,
    locked_until = NULL,
    last_error = '',
    updated_at = NOW()
WHERE delivery_channel = 'DIRECT_JSON'
  AND (notify_url IS NULL OR TRIM(notify_url) = '')
  AND status IN ('INIT', 'FAILED', 'PROCESSING', 'MANUAL_REQUIRED');

-- 4) 复核：空 URL 非终态应归零
SELECT status, COUNT(*) AS cnt
FROM recharge_notify_task
WHERE delivery_channel = 'DIRECT_JSON'
  AND (notify_url IS NULL OR TRIM(notify_url) = '')
  AND status IN ('INIT', 'FAILED', 'PROCESSING', 'MANUAL_REQUIRED')
GROUP BY status
ORDER BY status;

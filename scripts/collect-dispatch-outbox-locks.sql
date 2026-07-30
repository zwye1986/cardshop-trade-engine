-- 采集 recharge_dispatch_outbox 的 InnoDB 锁等待与持锁信息，定位 persist 批事务与
-- timer worker 在哪个二级索引页上争用。压测期间持续执行（建议配合 scripts/watch-dispatch-outbox-locks.sh 每 5-10 秒采一轮）。
--
-- 关联：
--   docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」「8.3 异步落库 Consumer」
--   docs/集成测试.md PERF-DIAG-A39-PRO143-20260723（timer ON/OFF 对照、outbox phase 争用）
--
-- 前置：
--   1) 压测进行中（入口 ≥150 QPS、persist-shard-concurrency 已设、timer-enabled 保持目标态）。
--   2) performance_schema.data_locks / data_lock_waits 可读。阿里云 RDS 默认通常开启；
--      若 data_locks 为空，执行文末「启用 instruments」段后重试。
--   3) 用 mysql client 以 --table -vv 运行单轮；持续采集用 watch-dispatch-outbox-locks.sh。
--
-- 预期结论（基于现有 DDL/SQL，见 docs/集成测试.md 根因分析）：
--   - 争用索引：idx_dispatch_outbox_due(status, next_retry_time, id) 或
--     idx_dispatch_outbox_lease(status, locked_until, id)，而非 uk_dispatch_outbox_event_id。
--     原因：claim UPDATE 修改 status/locked_until = 删旧索引条目 + 插入新条目，
--     与 persist INSERT 新行(status=INIT, next_retry_time=now+1s) 在同一 B+ 树区间相撞。
--   - persist 侧 waiting_lock_mode 多为 'X,GAP,INSERT_INTENTION' 或 'S,REC_NOT_GAP'。
--   - 若确认 idx_dispatch_outbox_due 是争用页，则方案 C（dispatch_outbox 移出 persist 事务）
--     收益来源为：persist 事务不再 INSERT 该索引，自然不再与 timer 索引维护抢页。

-- ======================================================================
-- 第 1 段：实时锁等待 — 谁在等谁、等哪个索引、什么锁模式
-- ======================================================================
SELECT
  w.REQUESTING_ENGINE_TRANSACTION_ID  AS waiting_trx,
  b.BLOCKING_ENGINE_TRANSACTION_ID    AS blocking_trx,
  w.OBJECT_NAME                       AS table_name,
  w.INDEX_NAME                        AS waiting_on_index,
  w.LOCK_MODE                         AS waiting_lock_mode,
  w.LOCK_TYPE                         AS lock_type,
  b.LOCK_MODE                         AS blocking_lock_mode,
  FROM_UNIXTIME(w.REQUESTING_TRX_STARTED / 1000000) AS waiting_since,
  w.REQUESTING_TRX_WAIT_STARTED_NS / 1000000000     AS wait_seconds
FROM performance_schema.data_lock_waits w
JOIN performance_schema.data_locks b
  ON b.ENGINE_LOCK_ID = w.BLOCKING_ENGINE_LOCK_ID
WHERE w.OBJECT_NAME = 'recharge_dispatch_outbox';

-- ======================================================================
-- 第 2 段：阻塞方实际持有的全部锁（看 timer claim 锁了几个索引）
-- 重点：同一 ENGINE_TRANSACTION_ID 是否同时在 idx_dispatch_outbox_due
--       和 idx_dispatch_outbox_lease 上持 X 锁（索引维护）。
-- ======================================================================
SELECT
  ENGINE_TRANSACTION_ID AS trx,
  INDEX_NAME,
  LOCK_MODE,
  LOCK_TYPE,
  LOCK_STATUS,
  COUNT(*) AS rows_locked
FROM performance_schema.data_locks
WHERE OBJECT_NAME = 'recharge_dispatch_outbox'
  AND LOCK_STATUS = 'GRANTED'
GROUP BY ENGINE_TRANSACTION_ID, INDEX_NAME, LOCK_MODE, LOCK_TYPE, LOCK_STATUS
ORDER BY ENGINE_TRANSACTION_ID, INDEX_NAME;

-- ======================================================================
-- 第 3 段：被阻塞的具体 SQL — 区分 persist INSERT vs timer claim
-- trx_query / current_sql 含 "insert into recharge_dispatch_outbox" = persist 批事务被卡；
-- 含 "update recharge_dispatch_outbox ... set status='PROCESSING'" = timer claim 在等。
-- ======================================================================
SELECT
  trx.trx_id,
  trx.trx_state,
  trx.trx_started,
  trx.trx_wait_started,
  trx.trx_rows_locked,
  trx.trx_weight,
  SUBSTRING(trx.trx_query, 1, 200) AS trx_query,
  SUBSTRING(es.SQL_TEXT, 1, 200)   AS current_sql
FROM information_schema.innodb_trx trx
LEFT JOIN performance_schema.threads t
  ON t.PROCESSLIST_ID = trx.trx_mysql_thread_id
LEFT JOIN performance_schema.events_statements_current es
  ON es.THREAD_ID = t.THREAD_ID
WHERE trx.trx_state = 'LOCK WAIT'
ORDER BY trx.trx_wait_started DESC
LIMIT 50;

-- ======================================================================
-- 第 4 段：争用记录的 key 值 — 判断 waiting 与 blocking 是否物理相邻页
-- 对 idx_dispatch_outbox_due，LOCK_DATA 形如 ('INIT','2026-07-24 10:00:01',12345)。
-- 若 waiting 与 blocking 的 LOCK_DATA 时间戳相邻、id 相邻，说明落同一 B+ 树页或相邻页。
-- ======================================================================
SELECT
  dl.ENGINE_TRANSACTION_ID AS trx,
  dl.INDEX_NAME,
  dl.LOCK_MODE,
  dl.LOCK_TYPE,
  dl.LOCK_STATUS,
  dl.LOCK_DATA
FROM performance_schema.data_locks dl
WHERE dl.OBJECT_NAME = 'recharge_dispatch_outbox'
  AND dl.INDEX_NAME IN
      ('idx_dispatch_outbox_due', 'idx_dispatch_outbox_lease', 'uk_dispatch_outbox_event_id')
ORDER BY dl.INDEX_NAME, dl.LOCK_STATUS DESC, dl.LOCK_DATA
LIMIT 100;

-- ======================================================================
-- 第 5 段：dispatch_outbox 长事务 — 排查是否有 worker 把事务横跨在 HTTP 上
-- 预期：timer worker 每次 orderPort.* 调用是独立短事务自动提交，trx_age 应 < 1s。
-- 若出现 trx_age 数秒且 status=PROCESSING 的 outbox 行，需进一步看该 trx 是否持锁
-- 跨越供应商 HTTP（用第 2 段的 ENGINE_TRANSACTION_ID 对齐）。
-- ======================================================================
SELECT
  trx.trx_id,
  trx.trx_state,
  TIMESTAMPDIFF(SECOND, trx.trx_started, NOW()) AS trx_age_seconds,
  trx.trx_rows_locked,
  trx.trx_rows_modified,
  SUBSTRING(trx.trx_query, 1, 150) AS trx_query
FROM information_schema.innodb_trx trx
WHERE trx.trx_tables_in_use > 0
   OR trx.trx_tables_locked > 0
ORDER BY trx.trx_started ASC
LIMIT 50;

-- ======================================================================
-- 第 6 段：outbox 事件状态分布 — 确认 INIT/PROCESSING 行数，辅助判断 timer 喂入速率
-- ======================================================================
SELECT
  status,
  COUNT(*) AS cnt,
  MIN(next_retry_time) AS earliest_due,
  MAX(next_retry_time) AS latest_due,
  MIN(created_at)      AS oldest_created
FROM recharge_dispatch_outbox
GROUP BY status
ORDER BY status;

-- ======================================================================
-- 第 7 段：INNODB STATUS — LATEST DETECTED DEADLOCK 段直接打印两个事务
-- 各自持锁和等锁的索引名 + record hex，是最直接的"具体索引页"证据。
-- 用 \G 竖排输出；客户端不支持 \G 时去掉换行符整段跑。
-- ======================================================================
SHOW ENGINE INNODB STATUS;

-- ======================================================================
-- 附：启用 performance_schema 锁 instruments（data_locks 为空时先执行本段再重跑上面）
-- 阿里云 RDS 若已默认开启可跳过；部分实例需在控制台参数组开启 performance_schema。
-- ======================================================================
-- UPDATE performance_schema.setup_instruments
-- SET enabled = 'YES', timed = 'YES'
-- WHERE name LIKE 'wait/lock/rowdb/innodb%'
--    OR name LIKE 'transaction/innodb%';
--
-- UPDATE performance_schema.setup_consumers
-- SET enabled = 'YES'
-- WHERE name IN ('events_waits_current', 'events_waits_history_long',
--                'events_statements_history_long');
--
-- SELECT 'instruments enabled; re-run sections 1-7 after a few seconds' AS note;

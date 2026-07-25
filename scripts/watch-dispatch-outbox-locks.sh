#!/usr/bin/env bash
# 压测期间持续采集 recharge_dispatch_outbox 的 InnoDB 锁等待信息，定位 persist 事务与
# timer worker 在哪个二级索引页上争用。每轮执行 scripts/collect-dispatch-outbox-locks.sql
# 的第 1-7 段，并在输出间打印时间戳分隔，便于事后对比"争用随入口 QPS 演进"。
#
# 关联：
#   docs/交行直充业务系统详细设计.md「9.1 派单 Outbox」「8.3 异步落库 Consumer」
#   docs/集成测试.md PERF-DIAG-A39-PRO143-20260723
#
# 用法（pro143 示例）:
#   MYSQL_HOST=rm-xxxx.mysql.rds.aliyuncs.com \
#   MYSQL_PORT=3306 \
#   MYSQL_USER=readonly_user \
#   MYSQL_PASSWORD={{MYSQL_PASSWORD}} \
#   MYSQL_DB=cardshop_test \
#   INTERVAL=8 \
#   ROUNDS=30 \
#   bash scripts/watch-dispatch-outbox-locks.sh | tee outbox-locks-$(date +%Y%m%d%H%M%S).log
#
# 说明:
#   - 需要一个对 performance_schema.data_locks / data_lock_waits / information_schema.innodb_trx
#     有读权限的账号；生产建议用只读账号，避免误写。
#   - 第 7 段 SHOW ENGINE INNODB STATUS 需要 PROCESS 权限；无权限时该段报错不影响前 6 段。
#   - INTERVAL 默认 8 秒；ROUNDS 默认 0（无限，Ctrl+C 停止）。
#   - 密码经 MYSQL_PWD 环境变量传给 mysql 客户端，不使用 -p 内联，避免密码出现在进程列表。
#   - 输出用 mysql --table -vv，表头清晰；重定向到文件后可按分隔符切轮对比。
#   - 锁等待是瞬时状态：单轮空结果不代表无争用，需连续多轮捕捉稳态。若连续 3 轮
#     data_lock_waits 为空但 persist ready 仍上涨，说明争用可能在 idx_dispatch_outbox_due
#     的插入意向锁（data_locks 里表现为 LOCK_MODE 含 GAP/INSERT_INTENTION，需看第 4 段），
#     或已不在锁等待层而在共享 RDS 邻库 IO 层（见 docs/集成测试.md 3.4 节）。

set -euo pipefail

INTERVAL="${INTERVAL:-8}"
ROUNDS="${ROUNDS:-0}"
SQL_FILE="$(dirname "$0")/collect-dispatch-outbox-locks.sql"

: "${MYSQL_HOST:?MYSQL_HOST required}"
: "${MYSQL_PORT:?MYSQL_PORT required}"
: "${MYSQL_USER:?MYSQL_USER required}"
: "${MYSQL_DB:?MYSQL_DB required}"
: "${MYSQL_PASSWORD:?MYSQL_PASSWORD required (use env var, do not inline)}"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "missing $SQL_FILE" >&2
  exit 1
fi

round=0
while true; do
  round=$((round + 1))
  ts=$(date '+%Y-%m-%d %H:%M:%S')
  echo ""
  echo "==================== round=${round} ts=${ts} interval=${INTERVAL}s ===================="
  MYSQL_PWD="$MYSQL_PASSWORD" mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" \
        -D "$MYSQL_DB" \
        --table -vv < "$SQL_FILE" 2>&1 || true

  if [[ "$ROUNDS" -gt 0 && "$round" -ge "$ROUNDS" ]]; then
    echo ""
    echo "reached ROUNDS=${ROUNDS}, stop."
    break
  fi
  sleep "$INTERVAL"
done

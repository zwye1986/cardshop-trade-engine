#!/usr/bin/env python3
"""Cleanup A39 load-test data on pro: MQ + MySQL + Redis runtime. Keep Redis config keys.

Balance restore uses only SUCCESS balance-delta rows (already applied to mc_ext_depart).
INIT/unflushed deltas must not be added back, or balance would be over-restored.
"""
from __future__ import annotations

from pathlib import Path
import json, urllib.parse, urllib.request, re, pymysql, time, base64
from redis.sentinel import Sentinel

APP = Path("/data/.cardshop/apps/cardshop-trade-engine-0.1.0-SNAPSHOT")
ORG = "A39"
BATCH = 2000
STAMP = time.strftime("%Y%m%d%H%M%S")
LOG = Path(f"/tmp/te-cleanup-a39-{STAMP}.log")
RESULT = Path(f"/tmp/te-cleanup-a39-{STAMP}.result.json")


def log(msg: str) -> None:
    line = f"{time.strftime('%F %T')} {msg}"
    print(line, flush=True)
    with LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for line in (APP / "config/.prod.env").read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k] = v.strip().strip("\"'")
    return env


def main() -> None:
    LOG.write_text("")
    env = load_env()
    log("=== CLEANUP START ===")
    log(f"log={LOG} result={RESULT}")

    base = "http://" + env["NACOS_SERVER_ADDR"] + "/nacos"
    login = urllib.parse.urlencode(
        {"username": env["NACOS_USERNAME"], "password": env["NACOS_PASSWORD"]}
    ).encode()
    with urllib.request.urlopen(base + "/v1/auth/login", data=login, timeout=10) as r:
        token = json.loads(r.read().decode()).get("accessToken", "")
    qs = urllib.parse.urlencode(
        {
            "dataId": "cardshop-trade-engine-prod.yaml",
            "group": env["NACOS_GROUP"],
            "tenant": env["NACOS_NAMESPACE"],
            "accessToken": token,
        }
    )
    with urllib.request.urlopen(base + "/v1/cs/configs?" + qs, timeout=15) as r:
        body = re.sub(
            r"\$\{([A-Z0-9_]+)\}",
            lambda m: env.get(m.group(1), m.group(0)),
            r.read().decode(),
        )

    m = re.search(r"url:\s*jdbc:mysql://([^/\s]+)/([^\?\s]+)", body)
    ds = re.search(r"datasource:[\s\S]*?username:\s*(\S+)[\s\S]*?password:\s*(\S+)", body)
    host, db = m.group(1), m.group(2)
    port = 3306
    if ":" in host:
        host, port_s = host.split(":", 1)
        port = int(port_s)
    rb = re.search(
        r"data:\n\s+redis:\n(.*?)(?:\n    [a-z]|\n  [a-z]|\nrabbitmq:)", body, re.S
    ).group(1)
    rpw = re.search(r"password:\s*(\S+)", rb).group(1)
    rdb = int(re.search(r"database:\s*(\d+)", rb).group(1))
    master = re.search(r"master:\s*(\S+)", rb).group(1)
    nodes = re.findall(r"-\s*(\d+\.\d+\.\d+\.\d+:\d+)", rb)
    prefix = re.search(r"key-prefix:\s*(\S+)", body).group(1)
    org_tag = "{A39}"
    log(f"prefix={prefix} mysql={host}:{port}/{db} redis_db={rdb}")

    # --- 1) RabbitMQ purge via management API ---
    log("=== 1) RabbitMQ purge ===")
    mq_host = env["SPRING_RABBITMQ_HOST"]
    mq_user = env["SPRING_RABBITMQ_USERNAME"]
    mq_pwd = env["SPRING_RABBITMQ_PASSWORD"]
    vhost = env.get("SPRING_RABBITMQ_VIRTUAL_HOST", "/")
    auth = base64.b64encode(f"{mq_user}:{mq_pwd}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}", "content-type": "application/json"}
    vq = urllib.parse.quote(vhost, safe="")
    for name in (
        "recharge.persist.mysql.queue",
        "recharge.persist.mysql.retry.queue",
        "recharge.persist.mysql.dlq",
    ):
        url = (
            f"http://{mq_host}:15673/api/queues/{vq}/"
            f"{urllib.parse.quote(name, safe='')}/contents"
        )
        req = urllib.request.Request(url, data=b"{}", headers=headers, method="DELETE")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                log(f"purged {name} status={resp.status}")
        except Exception as e:
            log(f"purge {name} ERR {e}")

    # --- 2) MySQL cleanup + balance restore ---
    log("=== 2) MySQL cleanup A39 / PERF ===")
    conn = pymysql.connect(
        host=host,
        port=port,
        user=ds.group(1),
        password=ds.group(2),
        database=db,
        charset="utf8mb4",
        connect_timeout=10,
        read_timeout=600,
        write_timeout=600,
        autocommit=True,
    )
    cur = conn.cursor()

    def count(sql: str, args=None):
        cur.execute(sql, args or ())
        return cur.fetchone()[0]

    before = {
        "ext": count("SELECT COUNT(*) FROM recharge_order_ext WHERE org_code=%s", (ORG,)),
        "mc_order": count("SELECT COUNT(*) FROM mc_order WHERE org_code=%s", (ORG,)),
        "depart": count("SELECT COUNT(*) FROM mc_depart_record WHERE org_code=%s", (ORG,)),
        "depart_sum": float(
            count(
                "SELECT COALESCE(SUM(`value`),0) FROM mc_depart_record WHERE org_code=%s",
                (ORG,),
            )
        ),
        "dispatch": count(
            "SELECT COUNT(*) FROM recharge_dispatch_outbox WHERE org_code=%s", (ORG,)
        ),
        "delta": count(
            "SELECT COUNT(*) FROM recharge_balance_delta_outbox WHERE org_code=%s", (ORG,)
        ),
        "delta_sum": float(
            count(
                "SELECT COALESCE(SUM(delta_amount),0) FROM recharge_balance_delta_outbox WHERE org_code=%s",
                (ORG,),
            )
        ),
        "delta_success_sum": float(
            count(
                "SELECT COALESCE(SUM(delta_amount),0) FROM recharge_balance_delta_outbox "
                "WHERE org_code=%s AND status='SUCCESS'",
                (ORG,),
            )
        ),
        "notify": count("SELECT COUNT(*) FROM recharge_notify_task WHERE org_code=%s", (ORG,)),
        "anomaly": count(
            "SELECT COUNT(*) FROM recharge_accept_anomaly WHERE org_code=%s", (ORG,)
        ),
        "legacy_outbox": count(
            "SELECT COUNT(*) FROM recharge_outbox WHERE org_code=%s", (ORG,)
        ),
        "flow": count(
            "SELECT COUNT(*) FROM mc_order_supplier_flow "
            "WHERE out_order_no LIKE 'PERF-%%' OR out_order_no LIKE 'A39MOCK%%' "
            "OR out_order_no LIKE 'A3920%%'"
        ),
        "mysql_balance": int(
            count("SELECT balance FROM mc_ext_depart WHERE org_code=%s", (ORG,))
        ),
    }
    log(f"BEFORE {before}")

    deleted: dict[str, int] = {}

    def del_loop(label: str, sql: str, args=None) -> int:
        total = 0
        while True:
            cur.execute(sql, args or ())
            n = cur.rowcount
            total += n
            if n:
                log(f"  {label}: +{n} (cum={total})")
            if n < BATCH:
                break
        deleted[label] = total
        return total

    del_loop(
        "notify",
        f"DELETE FROM recharge_notify_task WHERE org_code=%s LIMIT {BATCH}",
        (ORG,),
    )
    del_loop(
        "anomaly",
        f"DELETE FROM recharge_accept_anomaly WHERE org_code=%s LIMIT {BATCH}",
        (ORG,),
    )
    del_loop(
        "legacy_outbox",
        f"DELETE FROM recharge_outbox WHERE org_code=%s LIMIT {BATCH}",
        (ORG,),
    )
    del_loop(
        "dispatch",
        f"DELETE FROM recharge_dispatch_outbox WHERE org_code=%s LIMIT {BATCH}",
        (ORG,),
    )
    del_loop(
        "delta",
        f"DELETE FROM recharge_balance_delta_outbox WHERE org_code=%s LIMIT {BATCH}",
        (ORG,),
    )
    del_loop(
        "flow",
        f"""DELETE FROM mc_order_supplier_flow
            WHERE out_order_no LIKE 'PERF-%%'
               OR out_order_no LIKE 'A39MOCK%%'
               OR out_order_no LIKE 'A3920%%'
            LIMIT {BATCH}""",
    )
    del_loop(
        "depart",
        f"DELETE FROM mc_depart_record WHERE org_code=%s LIMIT {BATCH}",
        (ORG,),
    )
    del_loop(
        "ext",
        f"DELETE FROM recharge_order_ext WHERE org_code=%s LIMIT {BATCH}",
        (ORG,),
    )
    del_loop(
        "mc_order",
        f"DELETE FROM mc_order WHERE org_code=%s LIMIT {BATCH}",
        (ORG,),
    )

    # Only SUCCESS deltas were applied to mc_ext_depart; undo those.
    restore = -before["delta_success_sum"]
    new_bal = int(before["mysql_balance"] + restore)
    cur.execute(
        "UPDATE mc_ext_depart SET balance=%s, edit_time=NOW() WHERE org_code=%s",
        (new_bal, ORG),
    )
    log(
        f"MYSQL balance restore +{restore} (from SUCCESS deltas only): "
        f"{before['mysql_balance']} -> {new_bal} (rows={cur.rowcount})"
    )

    after = {
        "ext": count("SELECT COUNT(*) FROM recharge_order_ext WHERE org_code=%s", (ORG,)),
        "mc_order": count("SELECT COUNT(*) FROM mc_order WHERE org_code=%s", (ORG,)),
        "depart": count("SELECT COUNT(*) FROM mc_depart_record WHERE org_code=%s", (ORG,)),
        "dispatch": count(
            "SELECT COUNT(*) FROM recharge_dispatch_outbox WHERE org_code=%s", (ORG,)
        ),
        "delta": count(
            "SELECT COUNT(*) FROM recharge_balance_delta_outbox WHERE org_code=%s", (ORG,)
        ),
        "notify": count("SELECT COUNT(*) FROM recharge_notify_task WHERE org_code=%s", (ORG,)),
        "flow": count(
            "SELECT COUNT(*) FROM mc_order_supplier_flow "
            "WHERE out_order_no LIKE 'PERF-%%' OR out_order_no LIKE 'A39MOCK%%' "
            "OR out_order_no LIKE 'A3920%%'"
        ),
        "mysql_balance": int(
            count("SELECT balance FROM mc_ext_depart WHERE org_code=%s", (ORG,))
        ),
    }
    log(f"AFTER {after}")
    conn.close()

    # --- 3) Redis cleanup keep config ---
    log("=== 3) Redis cleanup (keep config/balance/product) ===")
    sentinels = [(h.split(":")[0], int(h.split(":")[1])) for h in nodes]
    sentinel = Sentinel(sentinels, socket_timeout=5, password=rpw)
    r = sentinel.master_for(
        master, socket_timeout=10, password=rpw, db=rdb, decode_responses=True
    )
    assert r.ping()

    KEEP_EXACT = {
        f"{prefix}:recharge:{org_tag}:merchant:config",
        f"{prefix}:recharge:{org_tag}:balance",
    }
    KEEP_PREFIXES = (
        f"{prefix}:recharge:{org_tag}:product:",
        f"{prefix}:recharge:{org_tag}:product-selection:",
    )

    def should_keep(key: str) -> bool:
        if key in KEEP_EXACT:
            return True
        return any(key.startswith(p) for p in KEEP_PREFIXES)

    kept_before = []
    cursor = 0
    while True:
        cursor, keys = r.scan(
            cursor=cursor, match=f"{prefix}:recharge:{org_tag}:*", count=2000
        )
        for k in keys:
            if should_keep(k):
                kept_before.append(k)
                log(f" KEEP {k}")
        if cursor == 0:
            break

    deleted_redis = 0
    cursor = 0
    while True:
        cursor, keys = r.scan(
            cursor=cursor, match=f"{prefix}:recharge:{org_tag}:*", count=2000
        )
        batch = [k for k in keys if not should_keep(k)]
        if batch:
            for i in range(0, len(batch), 500):
                chunk = batch[i : i + 500]
                r.unlink(*chunk)
                deleted_redis += len(chunk)
            if deleted_redis % 10000 < 500 or cursor == 0:
                log(f"progress deleted~{deleted_redis}")
        if cursor == 0:
            break

    cursor = 0
    while True:
        cursor, keys = r.scan(
            cursor=cursor, match="recharge:A39:deliver:frequency:*", count=2000
        )
        if keys:
            for i in range(0, len(keys), 500):
                chunk = keys[i : i + 500]
                r.unlink(*chunk)
                deleted_redis += len(chunk)
        if cursor == 0:
            break

    idx_deleted = 0
    cursor = 0
    while True:
        cursor, keys = r.scan(
            cursor=cursor,
            match=f"{prefix}:recharge:persist:pending:org_index:*",
            count=500,
        )
        for k in keys:
            removed = r.zrem(k, ORG)
            if removed:
                idx_deleted += int(removed)
        if cursor == 0:
            break
    log(f"REDIS org_index ZREM A39 count={idx_deleted}")

    bal_key = f"{prefix}:recharge:{org_tag}:balance"
    old = r.hgetall(bal_key) or {}
    r.hset(
        bal_key,
        mapping={
            "available": str(new_bal),
            "frozen": "0",
            "version": str(int(old.get("version") or 0) + 1),
        },
    )
    log(f"REDIS balance reset {old} -> {r.hgetall(bal_key)}")

    remain = []
    cursor = 0
    while True:
        cursor, keys = r.scan(
            cursor=cursor, match=f"{prefix}:recharge:{org_tag}:*", count=2000
        )
        remain.extend(keys)
        if cursor == 0:
            break
    log(f"remaining A39 keys={len(remain)}")
    for k in sorted(remain):
        log(f" REMAIN {k}")

    log("=== 4) Final verify ===")
    for name in (
        "recharge.persist.mysql.queue",
        "recharge.persist.mysql.retry.queue",
        "recharge.persist.mysql.dlq",
    ):
        url = f"http://{mq_host}:15673/api/queues/{vq}/{urllib.parse.quote(name, safe='')}"
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                info = json.loads(resp.read().decode())
                log(f"queue {name}: {info.get('messages', 0)}")
        except Exception as e:
            log(f"queue {name} ERR {e}")

    merchant_key = f"{prefix}:recharge:{org_tag}:merchant:config"
    result = {
        "before": before,
        "after": after,
        "deleted": deleted,
        "restore": restore,
        "new_bal": new_bal,
        "redis_deleted": deleted_redis,
        "redis_remain": remain,
        "kept_before": kept_before,
        "merchant_config_exists": bool(r.exists(merchant_key)),
        "redis_balance": r.hgetall(bal_key),
    }
    RESULT.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    log(f"merchant config exists: {result['merchant_config_exists']}")
    log(f"redis balance: {result['redis_balance']}")
    log("=== CLEANUP END ===")


if __name__ == "__main__":
    main()

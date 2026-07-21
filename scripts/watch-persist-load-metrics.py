#!/usr/bin/env python3
"""压测期间实时观察异步落库相关指标。

用法（pro143 示例）:
  python3 scripts/watch-persist-load-metrics.py \\
    --env-file /data/.cardshop/apps/cardshop-trade-engine-0.1.0-SNAPSHOT/config/.prod.env \\
    --prefix PERF-A39-C200-T5-5M-20260720095301 \\
    --interval 10 \\
    --prom-url http://127.0.0.1:10080/actuator/prometheus \\
    --app-log /data/.cardshop/apps/cardshop-trade-engine-0.1.0-SNAPSHOT/logs/cardshop-trade-engine.log

说明:
  - 窗口内成功落库数：MySQL recharge_order_ext 本前缀 DB_PERSISTED 计数（需 --prefix）
  - batch size / mysql duration / listener / Hikari：Prometheus 差分与窗口均值
  - mysql duration / listener 的 p50/p95/p99：Timer 无 histogram 时，用相邻采样 Δsum/Δcount 估算
  - Rabbit ready/unacked/retry/DLQ：Management API
  - 08S01 / Read timed out：应用日志窗口增量计数
  - slow log / lock wait：尽力查询；账号无权限时标明 N/A
"""
from __future__ import annotations

import argparse
import base64
import json
import math
import re
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path


QUEUES = (
    "recharge.persist.mysql.queue",
    "recharge.persist.mysql.retry.queue",
    "recharge.persist.mysql.dlq",
)

LOG_PATTERNS = (
    "08S01",
    "Read timed out",
    "Communications link failure",
    "StatementIsClosed",
    "MYSQL_PERSIST_FAILED",
    "Lock wait",
)
# 详细设计「8.3」phase 白名单：拆表后为 dispatch / balance 两段，旧 recharge_outbox 仅作历史对比。
MYSQL_PHASES = (
    "mc_order",
    "recharge_order_ext",
    "mc_depart_record",
    "recharge_dispatch_outbox",
    "recharge_balance_delta_outbox",
    "commit",
)


def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k] = v.strip().strip("\"'")
    return env


def parse_prom(text: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^(\S+)\s+([0-9.eE+-]+)$", line)
        if m:
            out[m.group(1)] = float(m.group(2))
    return out


def find_metric(metrics: dict[str, float], *parts: str) -> float | None:
    for k, v in metrics.items():
        if all(p in k for p in parts):
            return v
    return None


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


@dataclass
class PromWindow:
    mysql_means: list[float] = field(default_factory=list)
    phase_means: dict[str, list[float]] = field(
        default_factory=lambda: {phase: [] for phase in MYSQL_PHASES}
    )
    listener_means: list[float] = field(default_factory=list)
    size_means: list[float] = field(default_factory=list)
    prev: dict[str, float] | None = None

    def observe(self, cur: dict[str, float]) -> None:
        if self.prev is not None:
            self._add_mean(
                self.mysql_means,
                self.prev,
                cur,
                "trade_engine_persist_batch_mysql_duration_seconds_count",
                "trade_engine_persist_batch_mysql_duration_seconds_sum",
            )
            for phase in MYSQL_PHASES:
                self._add_mean(
                    self.phase_means[phase],
                    self.prev,
                    cur,
                    "trade_engine_persist_batch_mysql_phase_duration_seconds_count",
                    "trade_engine_persist_batch_mysql_phase_duration_seconds_sum",
                    extra_contains=f'phase="{phase}"',
                )
            self._add_mean(
                self.listener_means,
                self.prev,
                cur,
                "spring_rabbitmq_listener_seconds_count",
                "spring_rabbitmq_listener_seconds_sum",
                extra_contains="recharge.persist",
            )
            pc = find_metric(self.prev, "trade_engine_persist_batch_size_count") or 0.0
            ps = find_metric(self.prev, "trade_engine_persist_batch_size_sum") or 0.0
            cc = find_metric(cur, "trade_engine_persist_batch_size_count") or 0.0
            cs = find_metric(cur, "trade_engine_persist_batch_size_sum") or 0.0
            if cc - pc > 0:
                self.size_means.append((cs - ps) / (cc - pc))
        self.prev = cur

    @staticmethod
    def _add_mean(
        bucket: list[float],
        prev: dict[str, float],
        cur: dict[str, float],
        count_key: str,
        sum_key: str,
        extra_contains: str = "",
    ) -> None:
        pc = sc = None
        ps = ss = None
        for k, v in prev.items():
            if count_key in k and (not extra_contains or extra_contains in k):
                pc = v
            if sum_key in k and (not extra_contains or extra_contains in k):
                ps = v
        for k, v in cur.items():
            if count_key in k and (not extra_contains or extra_contains in k):
                sc = v
            if sum_key in k and (not extra_contains or extra_contains in k):
                ss = v
        if pc is None or sc is None or ps is None or ss is None:
            return
        dc = sc - pc
        if dc > 0:
            bucket.append((ss - ps) / dc)


def fetch_prom(url: str) -> dict[str, float]:
    with urllib.request.urlopen(url, timeout=8) as resp:
        return parse_prom(resp.read().decode())


def rabbit_queues(env: dict[str, str]) -> dict[str, dict[str, int]]:
    host = env["SPRING_RABBITMQ_HOST"]
    user = env["SPRING_RABBITMQ_USERNAME"]
    pwd = env["SPRING_RABBITMQ_PASSWORD"]
    vhost = env.get("SPRING_RABBITMQ_VIRTUAL_HOST", "/")
    auth = base64.b64encode(f"{user}:{pwd}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}"}
    vq = urllib.parse.quote(vhost, safe="")
    last_err = None
    for port in (15673, 15672, 15674):
        url = f"http://{host}:{port}/api/queues/{vq}"
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=5) as resp:
                qs = json.loads(resp.read().decode())
            out = {}
            for q in qs:
                name = q.get("name", "")
                if name in QUEUES:
                    out[name] = {
                        "ready": int(q.get("messages_ready") or 0),
                        "unacked": int(q.get("messages_unacknowledged") or 0),
                        "consumers": int(q.get("consumers") or 0),
                    }
            return out
        except Exception as e:  # noqa: BLE001
            last_err = e
    raise RuntimeError(f"rabbit mgmt unavailable: {last_err}")


def nacos_yaml(env: dict[str, str]) -> str:
    addr = env["NACOS_SERVER_ADDR"].split(",")[0]
    tok = json.loads(
        urllib.request.urlopen(
            urllib.request.Request(
                f"http://{addr}/nacos/v1/auth/login",
                urllib.parse.urlencode(
                    {
                        "username": env.get("NACOS_USERNAME", "nacos"),
                        "password": env.get("NACOS_PASSWORD", ""),
                    }
                ).encode(),
            ),
            timeout=10,
        ).read()
    )["accessToken"]
    q = urllib.parse.urlencode(
        {
            "dataId": env.get("NACOS_DATA_ID", "cardshop-trade-engine-prod.yaml"),
            "group": env["NACOS_GROUP"],
            "tenant": env["NACOS_NAMESPACE"],
            "accessToken": tok,
        }
    )
    return urllib.request.urlopen(f"http://{addr}/nacos/v1/cs/configs?{q}", timeout=10).read().decode()


def mysql_conn(env: dict[str, str]):
    import pymysql

    body = nacos_yaml(env)
    url = re.search(r"url:\s*(jdbc:mysql://[^\s]+)", body).group(1)
    user = re.search(r"username:\s*(\S+)", body).group(1).strip("\"'")
    pwd = re.search(r"password:\s*(\S+)", body).group(1).strip("\"'")
    m = re.match(r"jdbc:mysql://([^/:]+)(?::(\d+))?/([^?]+)", url)
    return pymysql.connect(
        host=m.group(1),
        port=int(m.group(2) or 3306),
        user=user,
        password=pwd,
        database=m.group(3),
        connect_timeout=8,
        read_timeout=20,
    )


def mysql_persisted(env: dict[str, str], prefix: str) -> int | str:
    if not prefix:
        return "N/A(no --prefix)"
    try:
        conn = mysql_conn(env)
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM recharge_order_ext "
            "WHERE org_code=%s AND out_order_sn LIKE %s AND persist_status=%s",
            ("A39", f"{prefix}%", "DB_PERSISTED"),
        )
        n = int(cur.fetchone()[0])
        conn.close()
        return n
    except Exception as e:  # noqa: BLE001
        return f"ERR({type(e).__name__})"


def mysql_diag(env: dict[str, str]) -> dict[str, str]:
    out = {
        "slow_query_log": "N/A",
        "long_query_time": "N/A",
        "innodb_trx_running": "N/A",
        "innodb_trx_lock_wait": "N/A",
        "lock_waits": "N/A",
    }
    try:
        conn = mysql_conn(env)
        cur = conn.cursor()
        cur.execute("SHOW VARIABLES LIKE 'slow_query_log'")
        row = cur.fetchone()
        if row:
            out["slow_query_log"] = str(row[1])
        cur.execute("SHOW VARIABLES LIKE 'long_query_time'")
        row = cur.fetchone()
        if row:
            out["long_query_time"] = str(row[1])
        cur.execute(
            "SELECT trx_state, COUNT(*) FROM information_schema.INNODB_TRX GROUP BY trx_state"
        )
        states = {r[0]: int(r[1]) for r in cur.fetchall()}
        out["innodb_trx_running"] = str(states.get("RUNNING", 0))
        out["innodb_trx_lock_wait"] = str(states.get("LOCK WAIT", 0))
        try:
            cur.execute("SELECT COUNT(*) FROM sys.innodb_lock_waits")
            out["lock_waits"] = str(int(cur.fetchone()[0]))
        except Exception as e:  # noqa: BLE001
            out["lock_waits"] = f"N/A({type(e).__name__})"
        conn.close()
    except Exception as e:  # noqa: BLE001
        out["error"] = f"{type(e).__name__}:{e}"
    return out


def count_log_patterns(path: Path, offset: int) -> tuple[int, dict[str, int]]:
    data = path.read_bytes()
    chunk = data[offset:].decode("utf-8", errors="replace")
    counts = {p: chunk.count(p) for p in LOG_PATTERNS}
    return len(data), counts


def fmt_pct(vals: list[float]) -> str:
    if not vals:
        return "n/a"
    s = sorted(vals)
    return (
        f"p50={percentile(s,0.50):.3f}s "
        f"p95={percentile(s,0.95):.3f}s "
        f"p99={percentile(s,0.99):.3f}s "
        f"avg={statistics.fmean(s):.3f}s "
        f"n={len(s)}"
    )


def fmt_phase_pct(phase_means: dict[str, list[float]]) -> str:
    parts = []
    for phase in MYSQL_PHASES:
        vals = phase_means.get(phase, [])
        if not vals:
            continue
        parts.append(f"{phase}: {fmt_pct(vals[-30:])}")
    return " | ".join(parts) if parts else "n/a"


def render(
    ts: str,
    persisted,
    failed_delta: float,
    success_delta: float,
    queues: dict[str, dict[str, int]],
    size_avg: float | None,
    size_max: float | None,
    mysql_pct: str,
    phase_pct: str,
    mysql_max: float | None,
    listener_pct: str,
    listener_max: float | None,
    hikari_active,
    hikari_pending,
    log_counts: dict[str, int],
    diag: dict[str, str],
) -> str:
    main = queues.get("recharge.persist.mysql.queue", {})
    retry = queues.get("recharge.persist.mysql.retry.queue", {})
    dlq = queues.get("recharge.persist.mysql.dlq", {})
    lines = [
        f"==== {ts} ====",
        f"窗口内成功落库数(DB_PERSISTED): {persisted}",
        f"批失败累计(delta from start): failed={failed_delta:.0f}  success={success_delta:.0f}",
        f"Rabbit main ready/unacked/consumers: "
        f"{main.get('ready','?')}/{main.get('unacked','?')}/{main.get('consumers','?')}",
        f"Rabbit retry ready: {retry.get('ready','?')}   DLQ ready: {dlq.get('ready','?')}",
        f"persist_batch_size: avg={size_avg if size_avg is not None else 'n/a'} "
        f"max={size_max if size_max is not None else 'n/a'}",
        f"persist_batch_mysql_duration (window估): {mysql_pct}  max_gauge={mysql_max}",
        f"persist_batch_mysql_phase_duration (window估): {phase_pct}",
        f"spring_rabbitmq_listener (window估): {listener_pct}  max_gauge={listener_max}",
        f"Hikari active/pending: {hikari_active}/{hikari_pending}",
        "日志增量: "
        + " ".join(f"{k}={v}" for k, v in log_counts.items()),
        "MySQL: "
        + f"slow_query_log={diag.get('slow_query_log')} "
        + f"long_query_time={diag.get('long_query_time')} "
        + f"trx_running={diag.get('innodb_trx_running')} "
        + f"trx_lock_wait={diag.get('innodb_trx_lock_wait')} "
        + f"lock_waits={diag.get('lock_waits')}",
    ]
    if "error" in diag:
        lines.append(f"MySQL diag error: {diag['error']}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Watch persist load metrics in realtime")
    parser.add_argument(
        "--env-file",
        default="/data/.cardshop/apps/cardshop-trade-engine-0.1.0-SNAPSHOT/config/.prod.env",
    )
    parser.add_argument("--prom-url", default="http://127.0.0.1:10080/actuator/prometheus")
    parser.add_argument(
        "--app-log",
        default="/data/.cardshop/apps/cardshop-trade-engine-0.1.0-SNAPSHOT/logs/cardshop-trade-engine.log",
    )
    parser.add_argument("--prefix", default="", help="out_order_sn / out_order prefix for DB_PERSISTED")
    parser.add_argument("--interval", type=int, default=10)
    parser.add_argument("--once", action="store_true", help="print one sample and exit")
    args = parser.parse_args()

    env_path = Path(args.env_file)
    if not env_path.exists():
        print(f"env file not found: {env_path}", file=sys.stderr)
        return 2
    env = load_env(env_path)
    log_path = Path(args.app_log)
    log_offset = log_path.stat().st_size if log_path.exists() else 0

    baseline = fetch_prom(args.prom_url)
    window = PromWindow()
    window.observe(baseline)

    print(
        "watching persist metrics; Ctrl+C to stop\n"
        f"prefix={args.prefix or '(none)'} interval={args.interval}s\n"
        "note: p50/p95/p99 are estimated from sample-interval means "
        "(Timer has no histogram buckets)\n",
        flush=True,
    )

    while True:
        try:
            prom = fetch_prom(args.prom_url)
            window.observe(prom)
            queues = rabbit_queues(env)
            persisted = mysql_persisted(env, args.prefix)
            diag = mysql_diag(env)
            log_offset, log_counts = count_log_patterns(log_path, log_offset)

            fail0 = find_metric(baseline, "trade_engine_persist_batch_orders_total", 'result="failed"') or 0
            ok0 = find_metric(baseline, "trade_engine_persist_batch_orders_total", 'result="success"') or 0
            fail1 = find_metric(prom, "trade_engine_persist_batch_orders_total", 'result="failed"') or 0
            ok1 = find_metric(prom, "trade_engine_persist_batch_orders_total", 'result="success"') or 0

            size_c0 = find_metric(baseline, "trade_engine_persist_batch_size_count") or 0
            size_s0 = find_metric(baseline, "trade_engine_persist_batch_size_sum") or 0
            size_c1 = find_metric(prom, "trade_engine_persist_batch_size_count") or 0
            size_s1 = find_metric(prom, "trade_engine_persist_batch_size_sum") or 0
            size_avg = ((size_s1 - size_s0) / (size_c1 - size_c0)) if size_c1 > size_c0 else None
            size_max = find_metric(prom, "trade_engine_persist_batch_size_max")

            mysql_max = find_metric(prom, "trade_engine_persist_batch_mysql_duration_seconds_max")
            listener_max = find_metric(
                prom, "spring_rabbitmq_listener_seconds_max", "recharge.persist"
            )
            hikari_active = find_metric(prom, "hikaricp_connections_active")
            hikari_pending = find_metric(prom, "hikaricp_connections_pending")

            text = render(
                time.strftime("%Y-%m-%d %H:%M:%S"),
                persisted,
                fail1 - fail0,
                ok1 - ok0,
                queues,
                round(size_avg, 3) if size_avg is not None else None,
                size_max,
                fmt_pct(window.mysql_means[-30:]),  # recent window estimates
                fmt_phase_pct(window.phase_means),
                mysql_max,
                fmt_pct(window.listener_means[-30:]),
                listener_max,
                hikari_active,
                hikari_pending,
                log_counts,
                diag,
            )
            print(text, flush=True)
            if args.once:
                return 0
            time.sleep(max(1, args.interval))
        except KeyboardInterrupt:
            print("stopped", flush=True)
            return 0
        except Exception as e:  # noqa: BLE001
            print(f"[warn] sample failed: {type(e).__name__}: {e}", flush=True)
            if args.once:
                return 1
            time.sleep(max(1, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())

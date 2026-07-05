#!/usr/bin/env python3
"""Watchdog — يفحص صحة الـ Worker كل 10 دقائق."""

import os
import sys
import logging
import requests
from datetime import datetime, timezone, timedelta

try:
    import redis as redis_lib
except ImportError:
    redis_lib = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("watchdog")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY", "")
REDIS_URL = os.getenv("REDIS_URL", "")
WATCHDOG_NAME = os.getenv("WATCHDOG_NAME", "watchdog-1")
ALERT_WEBHOOK = os.getenv("ALERT_WEBHOOK", "")

# ⚠️ apikey فقط — ممنوع Authorization: Bearer مع sb_secret_
HEADERS = {
    "apikey": SUPABASE_SECRET_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

SHARED_LOCK_KEY = "monitoring:active_worker"


def parse_timestamp(ts: str | None) -> datetime | None:
    if not ts:
        return None
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def get_state() -> dict | None:
    try:
        url = f"{SUPABASE_URL}/rest/v1/system_state?id=eq.1&select=*"
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data:
                return data[0]
    except requests.RequestException as exc:
        logger.error("Failed to read system_state: %s", exc)
    return None


def get_worker_heartbeat(worker_name: str) -> dict | None:
    try:
        url = f"{SUPABASE_URL}/rest/v1/worker_heartbeats?worker_name=eq.{worker_name}&select=*"
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data:
                return data[0]
    except requests.RequestException as exc:
        logger.error("Failed to read worker_heartbeats: %s", exc)
    return None


def get_redis_lock_owner() -> str | None:
    if not REDIS_URL or redis_lib is None:
        return None
    try:
        client = redis_lib.from_url(REDIS_URL, decode_responses=True)
        return client.get(SHARED_LOCK_KEY)
    except Exception as exc:
        logger.error("Redis error: %s", exc)
        return None


def upsert_watchdog_heartbeat(status: str):
    try:
        url = f"{SUPABASE_URL}/rest/v1/watchdog_heartbeats?on_conflict=watchdog_name"
        body = {
            "watchdog_name": WATCHDOG_NAME,
            "status": status,
            "last_beat": datetime.now(timezone.utc).isoformat(),
        }
        h = dict(HEADERS)
        h["Prefer"] = "resolution=merge-duplicates,return=minimal"
        requests.post(url, json=body, headers=h, timeout=10)
    except requests.RequestException as exc:
        logger.error("Failed to write watchdog heartbeat: %s", exc)


def send_alert(message: str):
    if not ALERT_WEBHOOK:
        logger.warning("No ALERT_WEBHOOK configured. Alert: %s", message)
        return
    try:
        requests.post(ALERT_WEBHOOK, json={"content": message}, timeout=10)
    except requests.RequestException as exc:
        logger.error("Failed to send alert: %s", exc)


def main():
    problems = []

    # 1) اقرأ system_state
    state = get_state()
    if not state:
        problems.append("Cannot read system_state from Supabase.")
        upsert_watchdog_heartbeat("ERROR")
        send_alert("🔴 Watchdog: Cannot read system_state from Supabase.")
        sys.exit(1)

    active_worker = state.get("active_worker", "none")
    status = state.get("status", "UNKNOWN")
    worker_start_str = state.get("worker_start_time")

    logger.info("State: status=%s, active_worker=%s", status, active_worker)

    if status == "RUNNING" and active_worker != "none":
        # 2) تحقق من worker_start_time ليس أقدم من 10 دقائق
        start_dt = parse_timestamp(worker_start_str)
        if start_dt:
            age_minutes = (datetime.now(timezone.utc) - start_dt).total_seconds() / 60
            if age_minutes > 10:
                # Worker يعمل منذ أكثر من 10 دقائق — تحقق من النبضة
                hb = get_worker_heartbeat(active_worker)
                if hb:
                    last_beat = parse_timestamp(hb.get("last_beat"))
                    if last_beat:
                        hb_age = (datetime.now(timezone.utc) - last_beat).total_seconds() / 60
                        if hb_age > 2:
                            problems.append(
                                f"Worker '{active_worker}' heartbeat stale ({hb_age:.1f} min). "
                                f"Started {age_minutes:.1f} min ago."
                            )
                else:
                    problems.append(
                        f"Worker '{active_worker}' has no heartbeat. "
                        f"Started {age_minutes:.1f} min ago."
                    )

        # 3) تحقق من Redis Lock
        lock_owner = get_redis_lock_owner()
        if lock_owner is not None and lock_owner != active_worker:
            problems.append(
                f"Redis lock owner ('{lock_owner}') != active_worker ('{active_worker}')"
            )
        elif lock_owner is None and REDIS_URL:
            problems.append(
                f"Worker '{active_worker}' is RUNNING but Redis lock is missing."
            )

    # 4) اكتب نبضة الـ Watchdog
    if problems:
        upsert_watchdog_heartbeat("WARNING")
        alert_msg = "🔴 Watchdog Alert:\n" + "\n".join(f"• {p}" for p in problems)
        send_alert(alert_msg)
        for p in problems:
            logger.error(p)
        sys.exit(1)
    else:
        upsert_watchdog_heartbeat("OK")
        logger.info("Watchdog check passed. All healthy.")
        sys.exit(0)


if __name__ == "__main__":
    main()
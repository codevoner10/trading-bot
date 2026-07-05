#!/usr/bin/env python3
"""Watchdog — checks worker health and alerts on issues."""

import os
import sys
import logging
from datetime import datetime, timezone, timedelta

import requests
import redis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s'
)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY", "")
REDIS_URL = os.environ.get("REDIS_URL", "")
WATCHDOG_NAME = os.environ.get("WATCHDOG_NAME", "watchdog_1")
ALERT_WEBHOOK = os.environ.get("ALERT_WEBHOOK", "")

HEADERS = {
    "apikey": SUPABASE_SECRET_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

SHARED_LOCK_KEY = "monitoring:active_worker"
STALE_HEARTBEAT_MINUTES = 3
IDLE_TOO_LONG_MINUTES = 10


def send_alert(message):
    if not ALERT_WEBHOOK:
        logger.info("No ALERT_WEBHOOK configured. Skipping alert.")
        return
    try:
        requests.post(ALERT_WEBHOOK, json={"content": message}, timeout=10)
        logger.info("Alert sent to webhook.")
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")


def get_system_state():
    try:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/system_state?id=eq.1&select=*",
            headers=HEADERS,
            timeout=30
        )
        if resp.status_code == 200 and resp.json():
            return resp.json()[0]
        logger.error(f"get_system_state failed: {resp.status_code} {resp.text[:200]}")
        return None
    except Exception as e:
        logger.error(f"get_system_state exception: {e}")
        return None


def get_last_worker_heartbeat(worker_name):
    try:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/worker_heartbeats",
            params={
                "worker_name": f"eq.{worker_name}",
                "select": "last_beat",
                "limit": "1"
            },
            headers=HEADERS,
            timeout=30
        )
        if resp.status_code == 200 and resp.json():
            return resp.json()[0].get("last_beat")
        return None
    except Exception as e:
        logger.error(f"get_last_worker_heartbeat error: {e}")
        return None


def get_redis_lock_owner():
    if not REDIS_URL:
        return None
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        return r.get(SHARED_LOCK_KEY)
    except Exception as e:
        logger.error(f"Redis error: {e}")
        return None


def upsert_watchdog_heartbeat(status):
    now = datetime.now(timezone.utc).isoformat()
    data = {
        "watchdog_name": WATCHDOG_NAME,
        "status": status,
        "last_beat": now,
        "updated_at": now
    }
    headers = dict(HEADERS)
    headers["Prefer"] = "resolution=merge-duplicates"
    try:
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/watchdog_heartbeats?on_conflict=watchdog_name",
            headers=headers,
            json=data,
            timeout=30
        )
        return resp.status_code < 400
    except Exception as e:
        logger.error(f"upsert_watchdog_heartbeat error: {e}")
        return False


def parse_iso(ts_str):
    if not ts_str:
        return None
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def main():
    if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
        logger.error("Missing SUPABASE_URL or SUPABASE_SECRET_KEY.")
        sys.exit(1)

    state = get_system_state()
    if not state:
        msg = "CRITICAL: Cannot read system_state from Supabase."
        logger.error(msg)
        send_alert(msg)
        sys.exit(1)

    active_worker = state.get("active_worker", "none")
    status = state.get("status", "IDLE")
    worker_start_time_str = state.get("worker_start_time")
    updated_at_str = state.get("updated_at")

    now = datetime.now(timezone.utc)
    issues = []

    if status == "RUNNING" and active_worker != "none":
        # --- Check 1: Redis lock matches DB ---
        redis_owner = get_redis_lock_owner()
        if redis_owner != active_worker:
            issues.append(
                f"Redis lock mismatch: lock_owner={redis_owner}, "
                f"db_active_worker={active_worker}"
            )

        # --- Check 2: worker_start_time not older than 10 minutes (without heartbeat) ---
        last_hb_str = get_last_worker_heartbeat(active_worker)
        if last_hb_str:
            hb_time = parse_iso(last_hb_str)
            if hb_time:
                elapsed_hb = now - hb_time
                if elapsed_hb > timedelta(minutes=STALE_HEARTBEAT_MINUTES):
                    issues.append(
                        f"Worker {active_worker} heartbeat stale: "
                        f"{elapsed_hb.total_seconds():.0f}s ago"
                    )
        else:
            # No heartbeat at all — check if start_time is old
            if worker_start_time_str:
                start_time = parse_iso(worker_start_time_str)
                if start_time:
                    elapsed_start = now - start_time
                    if elapsed_start > timedelta(minutes=IDLE_TOO_LONG_MINUTES):
                        issues.append(
                            f"Worker {active_worker} started "
                            f"{elapsed_start.total_seconds():.0f}s ago "
                            f"but no heartbeat found"
                        )
            else:
                issues.append(
                    f"Worker {active_worker} is RUNNING but has no heartbeat "
                    f"and no start_time"
                )

        # --- Check 3: worker_start_time exists ---
        if not worker_start_time_str:
            issues.append("Worker status is RUNNING but worker_start_time is NULL")

    else:
        # System is IDLE — check if idle too long
        if updated_at_str:
            updated_at = parse_iso(updated_at_str)
            if updated_at:
                elapsed_idle = now - updated_at
                if elapsed_idle > timedelta(minutes=IDLE_TOO_LONG_MINUTES):
                    issues.append(
                        f"System IDLE for {elapsed_idle.total_seconds():.0f}s — "
                        f"no worker claiming shift"
                    )

    # --- Write watchdog heartbeat ---
    watchdog_status = "OK" if not issues else "ALERT"
    upsert_watchdog_heartbeat(watchdog_status)

    if issues:
        alert_msg = (
            f"⚠️ **Watchdog Alert** ({WATCHDOG_NAME})\n"
            + "\n".join(f"• {i}" for i in issues)
        )
        logger.error(alert_msg)
        send_alert(alert_msg)
        sys.exit(1)

    logger.info("Watchdog: All systems nominal.")
    sys.exit(0)


if __name__ == "__main__":
    main()
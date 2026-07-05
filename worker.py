#!/usr/bin/env python3
"""24/7 Monitoring Worker — يعمل لمدة 5.5 ساعات في GitHub Actions."""

import os
import sys
import time
import signal
import logging
import requests
from datetime import datetime, timezone

try:
    import redis as redis_lib
except ImportError:
    redis_lib = None

from health_checks import HealthChecker

# ─── Logging ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("worker")


# ═══════════════════════════════════════════════════
# SupabaseClient — يستخدم requests فقط (لا supabase-py)
# ═══════════════════════════════════════════════════
class SupabaseClient:
    MAX_RETRIES = 3
    CIRCUIT_THRESHOLD = 3

    def __init__(self, base_url: str, secret_key: str):
        self.base_url = base_url.rstrip("/")
        self.secret_key = secret_key
        self.session = requests.Session()
        # ⚠️ مفتاح sb_secret_ يُرسل في apikey فقط — ممنوع Authorization: Bearer
        self.headers = {
            "apikey": secret_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self.consecutive_failures = 0
        self.circuit_open = False

    def _request(self, method, path, json_body=None, params=None, extra_headers=None):
        if self.circuit_open:
            logger.warning("Circuit breaker open — skipping request.")
            return None

        headers = dict(self.headers)
        if extra_headers:
            headers.update(extra_headers)

        url = f"{self.base_url}/rest/v1/{path}"

        for attempt in range(self.MAX_RETRIES):
            try:
                resp = self.session.request(
                    method, url, json=json_body, params=params,
                    headers=headers, timeout=15,
                )
                if resp.status_code < 400:
                    self.consecutive_failures = 0
                    if resp.text:
                        try:
                            return resp.json()
                        except ValueError:
                            return None
                    return None
                else:
                    logger.warning(
                        "API %s %s → %d: %s",
                        method, path, resp.status_code, resp.text[:200],
                    )
            except requests.RequestException as exc:
                logger.warning("Request error: %s", exc)

            if attempt < self.MAX_RETRIES - 1:
                sleep_time = 2 ** (attempt + 1)  # 2s, 4s
                time.sleep(sleep_time)

        self.consecutive_failures += 1
        if self.consecutive_failures >= self.CIRCUIT_THRESHOLD:
            self.circuit_open = True
            logger.error("Circuit breaker OPENED after %d consecutive failures.",
                         self.consecutive_failures)
        return None

    def get_state(self):
        result = self._request("GET", "system_state?id=eq.1&select=*")
        if result and len(result) > 0:
            return result[0]
        return None

    def update_state(self, data: dict):
        self._request(
            "PATCH", "system_state?id=eq.1",
            json_body=data,
            extra_headers={"Prefer": "return=minimal"},
        )

    def upsert_heartbeat(self, table: str, name: str, status: str):
        conflict_col = "watchdog_name" if "watchdog" in table else "worker_name"
        body = {
            conflict_col: name,
            "status": status,
            "last_beat": datetime.now(timezone.utc).isoformat(),
        }
        self._request(
            "POST", f"{table}?on_conflict={conflict_col}",
            json_body=body,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def log_event(self, event_type: str, severity: str, details: dict | None = None):
        body = {
            "event_type": event_type,
            "severity": severity,
            "details": details or {},
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._request("POST", "event_log", json_body=body)


# ═══════════════════════════════════════════════════
# RedisClient — قفل موزع بمفتاح مشترك
# ═══════════════════════════════════════════════════
class RedisClient:
    SHARED_LOCK_KEY = "monitoring:active_worker"

    def __init__(self, redis_url: str | None):
        self.enabled = bool(redis_url) and redis_lib is not None
        if self.enabled:
            # rediss:// يتضمن SSL تلقائياً — لا تضف ssl=True
            self.client = redis_lib.from_url(redis_url, decode_responses=True)
        else:
            self.client = None

    def acquire_lock(self, worker_id: str, ttl: int = 300) -> bool:
        if not self.enabled:
            return True
        result = self.client.set(self.SHARED_LOCK_KEY, worker_id, nx=True, ex=ttl)
        return bool(result)

    def renew_lock(self, worker_id: str, ttl: int = 300) -> bool:
        if not self.enabled:
            return True
        current = self.client.get(self.SHARED_LOCK_KEY)
        if current == worker_id:
            self.client.expire(self.SHARED_LOCK_KEY, ttl)
            return True
        return False

    def release_lock(self, worker_id: str) -> bool:
        if not self.enabled:
            return True
        current = self.client.get(self.SHARED_LOCK_KEY)
        if current == worker_id:
            self.client.delete(self.SHARED_LOCK_KEY)
            return True
        return False

    def get_lock_owner(self) -> str | None:
        if not self.enabled:
            return None
        return self.client.get(self.SHARED_LOCK_KEY)


# ═══════════════════════════════════════════════════
# MonitoringWorker
# ═══════════════════════════════════════════════════
class MonitoringWorker:

    def __init__(self):
        self.worker_id = os.getenv("WORKER_ID", "Worker_A")
        self.heartbeat_interval = int(os.getenv("HEARTBEAT_INTERVAL", "60"))
        self.claim_timeout = int(os.getenv("CLAIM_TIMEOUT", "300"))
        self.max_runtime_minutes = int(os.getenv("MAX_RUNTIME_MINUTES", "330"))

        supabase_url = os.getenv("SUPABASE_URL", "")
        supabase_key = os.getenv("SUPABASE_SECRET_KEY", "")
        redis_url = os.getenv("REDIS_URL", "")

        self.supabase = SupabaseClient(supabase_url, supabase_key)
        self.redis = RedisClient(redis_url)
        self.health_checker = HealthChecker(self.supabase)

        self.running = True
        self.start_time: datetime | None = None

    # ─── Signal Handlers ───
    def shutdown(self, signum=None, frame=None):
        logger.info("Signal %s received — shutting down...", signum)
        self.running = False

    # ─── Claim Shift ───
    def claim_shift(self) -> bool:
        # 1) احصل على Redis Lock أولاً
        if not self.redis.acquire_lock(self.worker_id, ttl=self.claim_timeout):
            logger.info("[%s] Redis Lock held by another worker. Exiting.", self.worker_id)
            return False

        # 2) اكتب في Supabase (best-effort)
        now_iso = datetime.now(timezone.utc).isoformat()
        self.supabase.update_state({
            "active_worker": self.worker_id,
            "worker_start_time": now_iso,
            "status": "RUNNING",
            "updated_at": now_iso,
        })
        self.supabase.log_event("WORKER_START", "info", {"worker_id": self.worker_id})
        logger.info("[%s] Shift claimed successfully.", self.worker_id)
        return True

    # ─── Send Heartbeat ───
    def send_heartbeat(self):
        self.supabase.upsert_heartbeat("worker_heartbeats", self.worker_id, "ALIVE")
        self.redis.renew_lock(self.worker_id, ttl=self.claim_timeout)
        logger.info("[%s] Heartbeat sent.", self.worker_id)

    # ─── Run Health Checks ───
    def run_health_checks(self):
        results = self.health_checker.check_all()
        for r in results:
            if not r.get("ok"):
                self.supabase.log_event(
                    "HEALTH_CHECK_FAIL", "warning",
                    {"check": r.get("check"), "details": r.get("details")},
                )
                logger.warning("Health check failed: %s — %s", r.get("check"), r.get("details"))
            else:
                logger.info("Health check OK: %s", r.get("check"))

    # ─── Release Shift ───
    def release_shift(self):
        now_iso = datetime.now(timezone.utc).isoformat()
        self.supabase.update_state({
            "active_worker": "none",
            "worker_start_time": None,
            "status": "IDLE",
            "updated_at": now_iso,
        })
        self.redis.release_lock(self.worker_id)
        self.supabase.log_event("WORKER_STOP", "info", {"worker_id": self.worker_id})
        logger.info("[%s] Shift released.", self.worker_id)

    # ─── Should Stop? ───
    def _should_stop(self) -> bool:
        if not self.start_time:
            return False
        elapsed = datetime.now(timezone.utc) - self.start_time
        return elapsed.total_seconds() >= self.max_runtime_minutes * 60

    # ─── Main Run Loop ───
    def run(self):
        self.start_time = datetime.now(timezone.utc)

        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)

        if not self.claim_shift():
            sys.exit(0)

        last_heartbeat = 0.0
        last_health_check = 0.0

        try:
            while self.running and not self._should_stop():
                now = time.time()

                if self.supabase.circuit_open:
                    logger.error("Circuit breaker open — Supabase unreachable. Stopping.")
                    break

                if now - last_heartbeat >= self.heartbeat_interval:
                    self.send_heartbeat()
                    last_heartbeat = now

                if now - last_health_check >= 30:
                    self.run_health_checks()
                    last_health_check = now

                time.sleep(5)

            if self._should_stop():
                logger.info("[%s] Max runtime (%d min) reached.", self.worker_id, self.max_runtime_minutes)
        finally:
            self.release_shift()


# ═══════════════════════════════════════════════════
# Entry Point
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    worker = MonitoringWorker()
    worker.run()
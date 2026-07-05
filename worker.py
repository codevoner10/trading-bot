#!/usr/bin/env python3
"""24/7 Monitoring Worker — main worker process."""

import os
import sys
import time
import signal
import logging
from datetime import datetime, timezone

import requests
import redis

from health_checks import HealthChecker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s'
)
logger = logging.getLogger(__name__)


class SupabaseClient:
    """Supabase REST API client using requests only."""

    def __init__(self, url, secret_key):
        self.url = url.rstrip("/")
        self.secret_key = secret_key
        self.session = requests.Session()
        self.headers = {
            "apikey": secret_key,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        self.consecutive_failures = 0
        self.circuit_open = False

    def _request(self, method, endpoint, json=None, extra_headers=None):
        if self.circuit_open:
            logger.warning("Circuit breaker is open. Skipping request.")
            return None

        url = f"{self.url}/rest/v1/{endpoint}"
        headers = dict(self.headers)
        if extra_headers:
            headers.update(extra_headers)

        for attempt in range(3):
            try:
                resp = self.session.request(
                    method, url, headers=headers, json=json, timeout=30
                )
                if resp.status_code < 400:
                    self.consecutive_failures = 0
                    if resp.status_code == 204 or not resp.text:
                        return True
                    try:
                        return resp.json()
                    except ValueError:
                        return True
                else:
                    logger.error(f"HTTP {resp.status_code}: {resp.text[:300]}")
            except requests.RequestException as e:
                logger.error(f"Request exception: {e}")

            self.consecutive_failures += 1
            if self.consecutive_failures >= 3:
                self.circuit_open = True
                logger.critical("Circuit breaker opened after 3 consecutive failures.")
                return None

            if attempt < 2:
                backoff = 2 ** (attempt + 1)
                logger.warning(f"Retrying in {backoff}s (attempt {attempt + 1}/3)")
                time.sleep(backoff)

        return None

    def get_state(self):
        result = self._request("GET", "system_state?id=eq.1&select=*")
        if result and isinstance(result, list) and len(result) > 0:
            return result[0]
        return None

    def update_state(self, data):
        data["updated_at"] = datetime.now(timezone.utc).isoformat()
        return self._request("PATCH", "system_state?id=eq.1", json=data)

    def upsert_heartbeat(self, table, worker_name, status):
        now = datetime.now(timezone.utc).isoformat()
        if table == "worker_heartbeats":
            data = {
                "worker_name": worker_name,
                "status": status,
                "last_beat": now,
                "updated_at": now
            }
            conflict_col = "worker_name"
        else:
            data = {
                "watchdog_name": worker_name,
                "status": status,
                "last_beat": now,
                "updated_at": now
            }
            conflict_col = "watchdog_name"

        extra = {"Prefer": "resolution=merge-duplicates"}
        return self._request(
            "POST",
            f"{table}?on_conflict={conflict_col}",
            json=data,
            extra_headers=extra
        )

    def log_event(self, event_type, severity, details=None):
        data = {
            "event_type": event_type,
            "severity": severity,
            "details": details or {},
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        return self._request("POST", "event_log", json=data)


class RedisClient:
    """Redis client for distributed locking via Upstash."""

    SHARED_LOCK_KEY = "monitoring:active_worker"

    def __init__(self, redis_url):
        self.redis_client = None
        if redis_url:
            try:
                self.redis_client = redis.from_url(redis_url, decode_responses=True)
                self.redis_client.ping()
                logger.info("Connected to Redis.")
            except Exception as e:
                logger.error(f"Redis connection failed: {e}")
                self.redis_client = None
        else:
            logger.info("No REDIS_URL set — running without lock (graceful degradation).")

    def acquire_lock(self, worker_id, ttl=300):
        if not self.redis_client:
            return True
        try:
            result = self.redis_client.set(
                self.SHARED_LOCK_KEY, worker_id, nx=True, ex=ttl
            )
            return bool(result)
        except Exception as e:
            logger.error(f"Redis acquire_lock error: {e}")
            return False

    def renew_lock(self, worker_id, ttl=300):
        if not self.redis_client:
            return True
        try:
            current = self.redis_client.get(self.SHARED_LOCK_KEY)
            if current == worker_id:
                self.redis_client.expire(self.SHARED_LOCK_KEY, ttl)
                return True
            logger.warning(f"Cannot renew lock: owned by {current}, not {worker_id}")
            return False
        except Exception as e:
            logger.error(f"Redis renew_lock error: {e}")
            return True

    def release_lock(self, worker_id):
        if not self.redis_client:
            return True
        try:
            current = self.redis_client.get(self.SHARED_LOCK_KEY)
            if current == worker_id:
                self.redis_client.delete(self.SHARED_LOCK_KEY)
                return True
            logger.warning(f"Cannot release lock: owned by {current}, not {worker_id}")
            return False
        except Exception as e:
            logger.error(f"Redis release_lock error: {e}")
            return True

    def get_lock_owner(self):
        if not self.redis_client:
            return None
        try:
            return self.redis_client.get(self.SHARED_LOCK_KEY)
        except Exception as e:
            logger.error(f"Redis get_lock_owner error: {e}")
            return None


class MonitoringWorker:
    """Main monitoring worker process."""

    def __init__(self):
        self.supabase_url = os.environ.get("SUPABASE_URL", "")
        self.supabase_key = os.environ.get("SUPABASE_SECRET_KEY", "")
        self.redis_url = os.environ.get("REDIS_URL", "")
        self.worker_id = os.environ.get(
            "WORKER_ID", f"Worker_{int(time.time())}"
        )
        self.heartbeat_interval = int(os.environ.get("HEARTBEAT_INTERVAL", "60"))
        self.claim_timeout = int(os.environ.get("CLAIM_TIMEOUT", "300"))
        self.max_runtime_minutes = int(os.environ.get("MAX_RUNTIME_MINUTES", "330"))

        if not self.supabase_url or not self.supabase_key:
            logger.error("Missing SUPABASE_URL or SUPABASE_SECRET_KEY.")
            sys.exit(1)

        self.db = SupabaseClient(self.supabase_url, self.supabase_key)
        self.redis = RedisClient(self.redis_url)
        self.checker = HealthChecker(self.db)

        self.running = True
        self.start_time = None

        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)

    def shutdown(self, signum=None, frame=None):
        logger.info(f"Received signal {signum}. Initiating shutdown...")
        self.running = False

    def _should_stop(self):
        if self.start_time is None:
            return False
        elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds() / 60
        if elapsed >= self.max_runtime_minutes:
            logger.info(f"Max runtime reached ({self.max_runtime_minutes} min).")
            return True
        return False

    def claim_shift(self):
        if not self.redis.acquire_lock(self.worker_id, ttl=self.claim_timeout):
            logger.info("Another worker holds the lock. Exiting gracefully.")
            sys.exit(0)

        now = datetime.now(timezone.utc)
        result = self.db.update_state({
            "active_worker": self.worker_id,
            "worker_start_time": now.isoformat(),
            "status": "RUNNING"
        })

        if result is None:
            logger.critical("Failed to update DB after acquiring lock. Releasing lock.")
            self.redis.release_lock(self.worker_id)
            sys.exit(1)

        self.start_time = now
        logger.info(f"Shift claimed by {self.worker_id}.")
        self.db.log_event("worker_start", "info", {"worker_id": self.worker_id})

    def send_heartbeat(self):
        self.db.upsert_heartbeat("worker_heartbeats", self.worker_id, "RUNNING")
        self.redis.renew_lock(self.worker_id, ttl=self.claim_timeout)
        logger.debug("Heartbeat sent.")

    def run_health_checks(self):
        results = self.checker.check_all()
        for r in results:
            if r.get("status") != "ok":
                logger.warning(f"Health check FAILED: {r}")
                self.db.log_event("health_check_fail", "warning", r)
            else:
                logger.debug(f"Health check OK: {r['check']}")

    def release_shift(self):
        try:
            self.db.update_state({
                "active_worker": "none",
                "worker_start_time": None,
                "status": "IDLE"
            })
            self.db.log_event("worker_stop", "info", {"worker_id": self.worker_id})
            logger.info("Shift released in DB.")
        except Exception as e:
            logger.error(f"Error releasing shift in DB: {e}")
        finally:
            self.redis.release_lock(self.worker_id)
            logger.info(f"Redis lock released by {self.worker_id}.")

    def run(self):
        self.claim_shift()
        try:
            last_heartbeat = 0
            last_health_check = 0
            while self.running and not self._should_stop():
                now_ts = time.time()
                if now_ts - last_heartbeat >= self.heartbeat_interval:
                    self.send_heartbeat()
                    last_heartbeat = now_ts
                if now_ts - last_health_check >= 30:
                    self.run_health_checks()
                    last_health_check = now_ts
                time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            self.db.log_event("worker_error", "error", {"error": str(e)})
        finally:
            self.release_shift()


if __name__ == "__main__":
    worker = MonitoringWorker()
    worker.run()
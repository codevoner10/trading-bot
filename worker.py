#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
24/7 Monitoring Bot Worker
Compatible with Supabase New API Keys (sb_secret_...)
"""

import os
import sys
import time
import json
import signal
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import requests
import redis

# ─── Logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("worker")

# ─── Configuration ───────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY", "")  # sb_secret_...
REDIS_URL = os.environ.get("REDIS_URL", "")
WORKER_ID = os.environ.get("WORKER_ID", f"worker_{os.getpid()}")
CLAIM_TIMEOUT_SECONDS = int(os.environ.get("CLAIM_TIMEOUT_SECONDS", "60"))

# Validate
if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
    logger.error("❌ Missing SUPABASE_URL or SUPABASE_SECRET_KEY")
    sys.exit(1)

# ─── Supabase REST Client ────────────────────────────────────────────
class SupabaseClient:
    """
    PostgREST client using the NEW Supabase Secret Key format.
    CRITICAL: sb_secret_... keys are NOT JWTs. They must ONLY be sent
    in the 'apikey' header. Do NOT put them in 'Authorization: Bearer'.
    """
    
    def __init__(self, base_url: str, secret_key: str):
        self.base_url = base_url
        self.secret_key = secret_key
        self.rest_url = f"{base_url}/rest/v1"
        
        # ✅ CORRECT HEADERS for new sb_secret_... keys
        self.headers = {
            "apikey": self.secret_key,           # ← المفتاح السري هنا فقط
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Prefer": "return=representation",
        }
        # ❌ NO Authorization: Bearer header for sb_secret_ keys!
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def _handle_response(self, response: requests.Response, operation: str) -> Any:
        """Handle HTTP responses with detailed logging."""
        if response.status_code >= 400:
            logger.error(
                f"❌ {operation} failed | HTTP {response.status_code} | "
                f"URL: {response.url} | Body: {response.text[:500]}"
            )
            response.raise_for_status()
        return response.json() if response.text else {}
    
    def get(self, table: str, params: Optional[Dict] = None) -> Any:
        """SELECT (Read) - Works with RLS if policies allow."""
        url = f"{self.rest_url}/{table}"
        response = self.session.get(url, params=params or {})
        return self._handle_response(response, f"GET {table}")
    
    def post(self, table: str, data: Dict) -> Any:
        """INSERT (Create) - Requires RLS INSERT policy or secret key bypass."""
        url = f"{self.rest_url}/{table}"
        headers = {**self.headers, "Prefer": "return=representation"}
        response = self.session.post(url, headers=headers, json=data)
        return self._handle_response(response, f"POST {table}")
    
    def patch(self, table: str, column: str, value: Any, data: Dict) -> Any:
        """UPDATE - Requires RLS UPDATE policy or secret key bypass."""
        url = f"{self.rest_url}/{table}?{column}=eq.{value}"
        response = self.session.patch(url, json=data)
        return self._handle_response(response, f"PATCH {table}")
    
    def upsert(self, table: str, data: Dict, on_conflict: str) -> Any:
        """UPSERT - Insert or update on conflict."""
        url = f"{self.rest_url}/{table}"
        headers = {
            **self.headers,
            "Prefer": "return=representation,resolution=merge-duplicates",
        }
        params = {"on_conflict": on_conflict}
        response = self.session.post(url, headers=headers, params=params, json=data)
        return self._handle_response(response, f"UPSERT {table}")


# ─── Redis Client (Upstash) ────────────────────────────────────────
class RedisClient:
    def __init__(self, redis_url: str):
        if not redis_url:
            self.client = None
            logger.warning("⚠️ No REDIS_URL provided. Running without Redis.")
            return
        
        try:
            self.client = redis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            self.client.ping()
            logger.info("✅ Redis connected")
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            self.client = None
    
    def set_lock(self, key: str, value: str, ttl: int) -> bool:
        if not self.client:
            return True  # Graceful degradation
        return self.client.set(key, value, nx=True, ex=ttl) is not None
    
    def refresh_lock(self, key: str, ttl: int) -> bool:
        if not self.client:
            return True
        return self.client.expire(key, ttl)
    
    def release_lock(self, key: str) -> bool:
        if not self.client:
            return True
        return self.client.delete(key) > 0


# ─── Worker Logic ────────────────────────────────────────────────────
class MonitoringWorker:
    def __init__(self):
        self.db = SupabaseClient(SUPABASE_URL, SUPABASE_SECRET_KEY)
        self.redis = RedisClient(REDIS_URL)
        self.running = True
        self.lock_key = "monitoring:active_worker"
        self.heartbeat_interval = 10  # seconds
        
        # Handle graceful shutdown
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)
    
    def _shutdown(self, signum, frame):
        logger.info("🛑 Shutdown signal received. Releasing claim...")
        self.running = False
    
    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()
    
    def claim_shift(self) -> bool:
        """
        محاولة الحصول على قفل العمل (Claim Shift).
        إذا نجحت، يتم كتابة الحالة في system_state.
        """
        try:
            # 1. محاولة الحصول على قفل Redis (إذا متاح)
            lock_acquired = self.redis.set_lock(
                self.lock_key, WORKER_ID, CLAIM_TIMEOUT_SECONDS
            )
            
            if not lock_acquired:
                logger.info("🔒 Another worker has the shift. Standing by...")
                return False
            
            # 2. كتابة الحالة في Supabase system_state
            # نستخدم PATCH لتحديث السجل id=1
            update_data = {
                "active_worker": WORKER_ID,
                "worker_start_time": self._now(),
                "status": "active",
                "updated_at": self._now(),
            }
            
            self.db.patch("system_state", "id", 1, update_data)
            logger.info(f"✅ Shift claimed by {WORKER_ID}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to claim shift: {e}")
            return False
    
    def send_heartbeat(self):
        """إرسال نبضة حياة للـ worker."""
        try:
            data = {
                "worker_id": WORKER_ID,
                "timestamp": self._now(),
                "status": "healthy",
            }
            # Upsert لتجنب تكرار السجلات
            self.db.upsert(
                "worker_heartbeats",
                data,
                on_conflict="worker_id"
            )
            self.redis.refresh_lock(self.lock_key, CLAIM_TIMEOUT_SECONDS)
        except Exception as e:
            logger.error(f"❌ Heartbeat failed: {e}")
    
    def log_event(self, event_type: str, message: str, details: Optional[Dict] = None):
        """تسجيل حدث في جدول event_log."""
        try:
            data = {
                "event_type": event_type,
                "message": message,
                "details": json.dumps(details) if details else None,
                "worker_id": WORKER_ID,
                "created_at": self._now(),
            }
            self.db.post("event_log", data)
        except Exception as e:
            logger.error(f"❌ Failed to log event: {e}")
    
    def run_health_checks(self):
        """تشغيل فحوصات المراقبة (أضف منطقك هنا)."""
        # مثال: فحص حالة النظام
        try:
            state = self.db.get("system_state", {"id": "eq.1"})
            logger.info(f"📊 System state: {state}")
            self.log_event("health_check", "System check completed", {"state": state})
        except Exception as e:
            self.log_event("error", f"Health check failed: {str(e)}")
            raise
    
    def release_shift(self):
        """تحرير الـ Shift عند الإغلاق."""
        try:
            self.db.patch("system_state", "id", 1, {
                "active_worker": None,
                "status": "idle",
                "updated_at": self._now(),
            })
            self.redis.release_lock(self.lock_key)
            logger.info("🔓 Shift released")
        except Exception as e:
            logger.error(f"❌ Failed to release shift: {e}")
    
    def run(self):
        """الحلقة الرئيسية للـ Worker."""
        logger.info(f"🚀 Worker {WORKER_ID} starting...")
        
        if not self.claim_shift():
            logger.info("⏳ Could not claim shift. Exiting.")
            sys.exit(0)
        
        self.log_event("worker_start", f"Worker {WORKER_ID} started")
        
        last_heartbeat = 0
        
        try:
            while self.running:
                # تشغيل الفحوصات
                self.run_health_checks()
                
                # إرسال نبضة حياة كل X ثواني
                now = time.time()
                if now - last_heartbeat >= self.heartbeat_interval:
                    self.send_heartbeat()
                    last_heartbeat = now
                
                time.sleep(5)
                
        except Exception as e:
            logger.error(f"💥 Worker crashed: {e}")
            self.log_event("worker_crash", str(e))
            raise
        finally:
            self.release_shift()
            logger.info("👋 Worker stopped")


# ─── Entry Point ─────────────────────────────────────────────────────
if __name__ == "__main__":
    worker = MonitoringWorker()
    worker.run()
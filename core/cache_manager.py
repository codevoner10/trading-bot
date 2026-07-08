import os
from typing import Optional
import redis

class CacheManager:
    """إدارة الذاكرة المؤقتة مع آلية تراجع للذاكرة المحلية"""
    
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL")
        self.redis_client = None
        self.local_cache = {}
        
        if self.redis_url:
            try:
                self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
                self.redis_client.ping()
            except Exception as e:
                print(f"[Cache Warning] Redis failed, using local memory: {e}")
                self.redis_client = None

    def get_last_fetch_time(self, worker_id: str) -> Optional[float]:
        key = f"last_fetch_{worker_id}"
        try:
            if self.redis_client:
                val = self.redis_client.get(key)
                return float(val) if val else None
            return self.local_cache.get(key)
        except Exception:
            return self.local_cache.get(key)

    def set_last_fetch_time(self, worker_id: str, timestamp: float) -> None:
        key = f"last_fetch_{worker_id}"
        try:
            if self.redis_client:
                self.redis_client.set(key, timestamp)
            else:
                self.local_cache[key] = timestamp
        except Exception:
            self.local_cache[key] = timestamp
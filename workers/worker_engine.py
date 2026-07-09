import os
import sys
import time
import asyncio
from datetime import datetime, timezone
from core.database_manager import DatabaseManager
from core.cache_manager import CacheManager
from core.notifier import TelegramNotifier
from workers.api_client import MarketDataClient

class WorkerEngine:
    """المحرك الرئيسي للعامل"""
    
    def __init__(self):
        self.worker_id = os.getenv("WORKER_ID", "Unknown_Worker")
        self.db = DatabaseManager()
        self.cache = CacheManager()
        self.notifier = TelegramNotifier()
        self.api = MarketDataClient()
        self.start_time = time.time()
        self.last_heartbeat_time = 0
        self.is_rate_limited = False

    async def initialize(self) -> bool:
        state = self.db.read_system_state()
        if not state:
            await self.notifier.send_db_error(self.worker_id)
            return False
            
        # --- حماية التعارض الزمني (Dual-Active Protection) ---
        if state.get('active_worker') == self.worker_id:
            last_hb_str = state.get('last_worker_heartbeat')
            if last_hb_str:
                try:
                    last_hb_dt = datetime.fromisoformat(last_hb_str)
                    if last_hb_dt.tzinfo is None:
                        last_hb_dt = last_hb_dt.replace(tzinfo=timezone.utc)
                    diff_seconds = (datetime.now(timezone.utc) - last_hb_dt).total_seconds()
                    
                    # إذا كانت آخر نبضة منذ أقل من دقيقتين، فالنسخة الأخرى تعمل
                    if diff_seconds < 120:
                        print(f"[{self.worker_id}] Detected active instance (heartbeat {diff_seconds:.0f}s ago). Exiting silently.")
                        sys.exit(0)
                except Exception:
                    pass
        # --------------------------------------------------------

        if not self.db.claim_active_role('worker', self.worker_id):
            return False
            
        self.db.log_event("START", "Worker", self.worker_id, "Worker started shift successfully.")
        active_watchdog = state.get('active_watchdog', 'Unknown')
        await self.notifier.send_worker_start(self.worker_id, "EUR/USD", active_watchdog)
        return True

    async def run_main_loop(self):
        while True:
            current_time = time.time()
            
            # Hard Stop (5.5 ساعات = 19800 ثانية)
            if (current_time - self.start_time) >= 19800:
                duration = current_time - self.start_time
                self.db.log_event("HARD_STOP", "Worker", self.worker_id, "Exceeded 5.5 hours limit.")
                await self.notifier.send_worker_hard_stop(self.worker_id, duration)
                break
                
            # Handover Check
            state = self.db.read_system_state()
            if state and state.get('active_worker') != self.worker_id:
                duration = current_time - self.start_time
                next_worker = state.get('active_worker')
                self.db.log_event("HANDOVER", "Worker", self.worker_id, f"Detected new worker: {next_worker}")
                await self.notifier.send_handover(self.worker_id, next_worker, duration)
                break
                
            # Heartbeat (كل 60 ثانية)
            if (current_time - self.last_heartbeat_time) >= 60:
                if self.db.update_heartbeat('worker', self.worker_id):
                    self.last_heartbeat_time = current_time
                    
            # Data Collection (كل 15 دقيقة = 900 ثانية)
            if not self.is_rate_limited:
                last_fetch = self.cache.get_last_fetch_time(self.worker_id)
                if not last_fetch or (current_time - last_fetch) >= 900:
                    await self.collect_market_data(current_time)
                    
            await asyncio.sleep(30)

    async def collect_market_data(self, current_time: float):
        success, candle_data, is_limited = await self.api.fetch_market_data("EUR/USD", "15min")
        
        if is_limited:
            self.is_rate_limited = True
            self.db.log_event("API_LIMIT", "Worker", self.worker_id, "API rate limit (429) reached.")
            await self.notifier.send_api_limit(self.worker_id)
            return
            
        if success and candle_data:
            self.cache.set_last_fetch_time(self.worker_id, current_time)
            await self.notifier.send_market_data(self.worker_id, "EUR/USD", candle_data)
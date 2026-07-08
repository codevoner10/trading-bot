import os
import time
import asyncio
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
            await self.notifier.send_message(f"🚨 <b>{self.worker_id}</b> فشل الاتصال بقاعدة البيانات. إيقاف آمن.")
            return False
        if not self.db.claim_active_role('worker', self.worker_id):
            await self.notifier.send_message(f"🚨 <b>{self.worker_id}</b> فشل الاستحواذ.")
            return False
        self.db.log_event("START", "Worker", self.worker_id, "Worker started shift successfully.")
        await self.notifier.send_message(f"🟢 <b>{self.worker_id}</b> بدأ وردية جديدة.")
        return True

    async def run_main_loop(self):
        while True:
            current_time = time.time()
            
            # Hard Stop (5.5 ساعات)
            if (current_time - self.start_time) >= 19800:
                self.db.log_event("HARD_STOP", "Worker", self.worker_id, "Exceeded 5.5 hours limit.")
                await self.notifier.send_message(f"⚠️ <b>{self.worker_id}</b> بلغ حد الإغلاق الإجباري.")
                break
                
            # Handover Check
            state = self.db.read_system_state()
            if state and state.get('active_worker') != self.worker_id:
                self.db.log_event("HANDOVER", "Worker", self.worker_id, f"Detected new worker: {state.get('active_worker')}")
                await self.notifier.send_message(f"✅ <b>{self.worker_id}</b> سلم الوردية بنجاح.")
                break
                
            # Heartbeat (كل 60 ثانية)
            if (current_time - self.last_heartbeat_time) >= 60:
                if self.db.update_heartbeat('worker', self.worker_id):
                    self.last_heartbeat_time = current_time
                    
            # Data Collection (كل 15 دقيقة)
            if not self.is_rate_limited:
                last_fetch = self.cache.get_last_fetch_time(self.worker_id)
                if not last_fetch or (current_time - last_fetch) >= 900:
                    await self.collect_market_data(current_time)
                    
            await asyncio.sleep(30)

    async def collect_market_data(self, current_time: float):
        success, price, is_limited = await self.api.fetch_price("EUR/USD")
        if is_limited:
            self.is_rate_limited = True
            self.db.log_event("API_LIMIT", "Worker", self.worker_id, "API rate limit (429) reached.")
            await self.notifier.send_message(f"🚫 <b>{self.worker_id}</b> استنفد حد الطلبات (429).")
            return
        if success and price:
            self.cache.set_last_fetch_time(self.worker_id, current_time)
            await self.notifier.send_message(f"📊 <b>{self.worker_id}</b>\nEUR/USD Price: <b>{price}</b>")
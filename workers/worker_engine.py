import os
import sys
import time
import asyncio
from datetime import datetime, timezone
from core.database_manager import DatabaseManager
from core.cache_manager import CacheManager
from core.notifier import TelegramNotifier
from core.github_client import GitHubActionsClient
from workers.api_client import MarketDataClient

class WorkerEngine:
    def __init__(self):
        self.worker_id = os.getenv("WORKER_ID", "Unknown_Worker")
        self.trigger_source = os.getenv("TRIGGER_SOURCE", "GitHub_Cron")
        self.db = DatabaseManager()
        self.cache = CacheManager()
        self.notifier = TelegramNotifier()
        self.api = MarketDataClient()
        self.github = GitHubActionsClient()
        self.start_time = time.time()
        self.last_heartbeat_time = 0
        self.is_rate_limited = False
        self.has_dispatched_next = False # لمنع التكرار الاستباقي

    async def initialize(self) -> bool:
        state = self.db.read_system_state()
        if not state:
            await self.notifier.send_db_error(self.worker_id)
            return False
            
        # حماية التعارض الزمني
        if state.get('active_worker') == self.worker_id:
            last_hb_str = state.get('last_worker_heartbeat')
            if last_hb_str:
                try:
                    last_hb_dt = datetime.fromisoformat(last_hb_str)
                    if last_hb_dt.tzinfo is None: last_hb_dt = last_hb_dt.replace(tzinfo=timezone.utc)
                    if (datetime.now(timezone.utc) - last_hb_dt).total_seconds() < 120:
                        print(f"[{self.worker_id}] Detected active instance. Exiting silently.")
                        sys.exit(0)
                except: pass

        # حساب الفجوة الزمنية (Gap Detection)
        prev_worker = state.get('active_worker', 'none')
        gap_seconds = 0.0
        if prev_worker != 'none' and prev_worker != self.worker_id:
            last_hb_str = state.get('last_worker_heartbeat')
            if last_hb_str:
                try:
                    last_hb_dt = datetime.fromisoformat(last_hb_str)
                    if last_hb_dt.tzinfo is None: last_hb_dt = last_hb_dt.replace(tzinfo=timezone.utc)
                    gap_seconds = (datetime.now(timezone.utc) - last_hb_dt).total_seconds()
                except: pass

        if not self.db.claim_active_role('worker', self.worker_id):
            return False
            
        self.db.log_event("START", "Worker", self.worker_id, f"Trigger: {self.trigger_source}")
        active_watchdog = state.get('active_watchdog', 'Unknown')
        await self.notifier.send_worker_start(self.worker_id, "EUR/USD", active_watchdog, self.trigger_source, prev_worker, gap_seconds)
        return True

    async def run_main_loop(self):
        while True:
            current_time = time.time()
            
            # 1. التسليم الاستباقي (Proactive Dispatch) عند 3.75 ساعات (13500 ثانية)
            if not self.has_dispatched_next and (current_time - self.start_time) >= 13500:
                if self.worker_id.startswith("Worker_"):
                    letter = self.worker_id[-1]
                    next_letter = 'A' if letter == 'F' else chr(ord(letter) + 1)
                    workflow_file = f"worker_{next_letter.lower()}.yml"
                    await self.github.dispatch_workflow(workflow_file, "Proactive_Handover")
                    self.has_dispatched_next = True

            # 2. الإغلاق الإجباري (Hard Stop) عند 5.5 ساعات (19800 ثانية)
            if (current_time - self.start_time) >= 19800:
                duration = current_time - self.start_time
                self.db.log_event("HARD_STOP", "Worker", self.worker_id, "Exceeded 5.5 hours.")
                await self.notifier.send_worker_hard_stop(self.worker_id, duration)
                break
                
            # 3. التسليم العادي (Handover Check)
            state = self.db.read_system_state()
            if state and state.get('active_worker') != self.worker_id:
                duration = current_time - self.start_time
                next_worker = state.get('active_worker')
                self.db.log_event("HANDOVER", "Worker", self.worker_id, f"Detected new worker: {next_worker}")
                await self.notifier.send_worker_shift_summary(self.worker_id, next_worker, self.start_time, duration)
                break
                
            # 4. نبضة الحياة (Heartbeat)
            if (current_time - self.last_heartbeat_time) >= 60:
                if self.db.update_heartbeat('worker', self.worker_id):
                    self.last_heartbeat_time = current_time
                    
            # 5. جمع البيانات (Data Collection)
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
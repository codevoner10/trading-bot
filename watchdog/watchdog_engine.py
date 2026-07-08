import os
import time
import asyncio
from datetime import datetime, timezone
from typing import Dict
from core.database_manager import DatabaseManager
from core.notifier import TelegramNotifier
from watchdog.github_client import GitHubActionsClient

class WatchdogEngine:
    """المحرك الرئيسي لكلب الحراسة"""
    
    def __init__(self):
        self.watchdog_id = os.getenv("WATCHDOG_ID", "Unknown_Watchdog")
        self.db = DatabaseManager()
        self.notifier = TelegramNotifier()
        self.github = GitHubActionsClient()
        self.start_time = time.time()
        self.last_heartbeat_time = 0

    async def initialize(self) -> bool:
        state = self.db.read_system_state()
        if not state:
            await self.notifier.send_message(f"🚨 <b>{self.watchdog_id}</b> فشل الاتصال بقاعدة البيانات.")
            return False
        if not self.db.claim_active_role('watchdog', self.watchdog_id):
            return False
        self.db.log_event("WATCHDOG_START", "Watchdog", self.watchdog_id, "Started monitoring shift.")
        await self.notifier.send_message(f"👀 <b>{self.watchdog_id}</b> بدأ وردية المراقبة.")
        return True

    async def run_main_loop(self):
        while True:
            current_time = time.time()
            
            # Hard Stop (5.5 ساعات)
            if (current_time - self.start_time) >= 19800:
                await self.handle_hard_stop()
                break
                
            state = self.db.read_system_state()
            if state and state.get('active_watchdog') != self.watchdog_id:
                self.db.log_event("WATCHDOG_STOP", "Watchdog", self.watchdog_id, "Handed over.")
                await self.notifier.send_message(f"✅ <b>{self.watchdog_id}</b> سلم وردية المراقبة.")
                break
                
            # Heartbeat (كل 60 ثانية)
            if (current_time - self.last_heartbeat_time) >= 60:
                if self.db.update_heartbeat('watchdog', self.watchdog_id):
                    self.last_heartbeat_time = current_time
                    
            if state:
                await self.check_worker_health(state)
                
            await asyncio.sleep(60)

    async def check_worker_health(self, state: Dict):
        active_worker = state.get('active_worker')
        last_hb_str = state.get('last_worker_heartbeat')
        if not active_worker or active_worker == 'none' or not last_hb_str: return
            
        try:
            last_hb_dt = datetime.fromisoformat(last_hb_str)
            if last_hb_dt.tzinfo is None:
                last_hb_dt = last_hb_dt.replace(tzinfo=timezone.utc)
            diff_seconds = (datetime.now(timezone.utc) - last_hb_dt).total_seconds()
            
            if diff_seconds >= 180:
                await self.handle_worker_failure(active_worker)
        except Exception as e:
            print(f"[Watchdog Error] Parse time failed: {e}")

    async def handle_worker_failure(self, dead_worker_name: str):
        attempts = self.db.read_system_state().get('backup_attempts', 0)
        if attempts >= 3:
            print(f"[Watchdog] Safe Mode. Attempts exhausted ({attempts}).")
            return
            
        new_attempts = self.db.increment_backup_attempts()
        self.db.log_event("FAIL", "Worker", dead_worker_name, "Declared dead.")
        self.db.log_event("BACKUP", "Watchdog", self.watchdog_id, f"Triggering Backup_Z. Attempt {new_attempts}/3")
        await self.notifier.send_message(f"🚨 <b>طوارئ!</b>\nالعامل <b>{dead_worker_name}</b> مات.\nجاري تشغيل Backup_Z... [المحاولة {new_attempts}/3]")
        await self.github.dispatch_workflow('backup_worker.yml')

    async def handle_hard_stop(self):
        self.db.log_event("HARD_STOP", "Watchdog", self.watchdog_id, "Exceeded 5.5 hours.")
        await self.notifier.send_message(f"⚠️ <b>{self.watchdog_id}</b> بلغ حد الإغلاق الإجباري. تشغيل الكلب التالي إجبارياً.")
        next_watchdog = "Beta" if self.watchdog_id == "Alpha" else "Alpha"
        await self.github.dispatch_workflow(f'watchdog_{next_watchdog.lower()}.yml')
        
        for _ in range(10):
            state = self.db.read_system_state()
            if state and state.get('active_watchdog') != self.watchdog_id: break
            await asyncio.sleep(30)
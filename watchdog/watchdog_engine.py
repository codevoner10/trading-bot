import os
import sys
import time
import asyncio
from datetime import datetime, timezone
from core.database_manager import DatabaseManager
from core.notifier import TelegramNotifier
from core.github_client import GitHubActionsClient

class WatchdogEngine:
    def __init__(self):
        self.watchdog_id = os.getenv("WATCHDOG_ID", "Unknown_Watchdog")
        self.trigger_source = os.getenv("TRIGGER_SOURCE", "GitHub_Cron")
        self.db = DatabaseManager()
        self.notifier = TelegramNotifier()
        self.github = GitHubActionsClient()
        self.start_time = time.time()
        self.last_heartbeat_time = 0

    async def initialize(self) -> bool:
        state = self.db.read_system_state()
        if not state:
            await self.notifier.send_db_error(self.watchdog_id)
            return False
            
        # حماية التعارض الزمني
        if state.get('active_watchdog') == self.watchdog_id:
            last_hb_str = state.get('last_watchdog_heartbeat')
            if last_hb_str:
                try:
                    last_hb_dt = datetime.fromisoformat(last_hb_str)
                    if last_hb_dt.tzinfo is None: last_hb_dt = last_hb_dt.replace(tzinfo=timezone.utc)
                    if (datetime.now(timezone.utc) - last_hb_dt).total_seconds() < 120:
                        print(f"[{self.watchdog_id}] Detected active instance. Exiting silently.")
                        sys.exit(0)
                except: pass

        # حساب الفجوة الزمنية للمراقبة
        prev_watchdog = state.get('active_watchdog', 'none')
        gap_seconds = 0.0
        if prev_watchdog != 'none' and prev_watchdog != self.watchdog_id:
            last_hb_str = state.get('last_watchdog_heartbeat')
            if last_hb_str:
                try:
                    last_hb_dt = datetime.fromisoformat(last_hb_str)
                    if last_hb_dt.tzinfo is None: last_hb_dt = last_hb_dt.replace(tzinfo=timezone.utc)
                    gap_seconds = (datetime.now(timezone.utc) - last_hb_dt).total_seconds()
                except: pass

        if not self.db.claim_active_role('watchdog', self.watchdog_id):
            return False
            
        self.db.log_event("WATCHDOG_START", "Watchdog", self.watchdog_id, f"Trigger: {self.trigger_source}")
        await self.notifier.send_watchdog_start(self.watchdog_id, self.trigger_source, prev_watchdog, gap_seconds)
        return True

    async def run_main_loop(self):
        while True:
            current_time = time.time()
            
            # الإغلاق الإجباري للكلب (5.5 ساعات)
            if (current_time - self.start_time) >= 19800:
                await self.handle_hard_stop()
                break
                
            # التسليم الطبيعي للكلب
            state = self.db.read_system_state()
            if state and state.get('active_watchdog') != self.watchdog_id:
                duration = current_time - self.start_time
                next_watchdog = state.get('active_watchdog')
                self.db.log_event("WATCHDOG_STOP", "Watchdog", self.watchdog_id, "Handed over.")
                await self.notifier.send_watchdog_shift_summary(self.watchdog_id, next_watchdog, self.start_time, duration)
                break
                
            # نبضة حياة الكلب
            if (current_time - self.last_heartbeat_time) >= 60:
                if self.db.update_heartbeat('watchdog', self.watchdog_id):
                    self.last_heartbeat_time = current_time
                    
            # فحص صحة العمال (Health & Shift Delay Check)
            if state:
                await self.check_worker_health(state)
                
            await asyncio.sleep(60)

    async def check_worker_health(self, state: dict):
        active_worker = state.get('active_worker')
        last_hb_str = state.get('last_worker_heartbeat')
        worker_start_str = state.get('worker_start_time')
        
        if not active_worker or active_worker == 'none' or not last_hb_str: return
            
        try:
            last_hb_dt = datetime.fromisoformat(last_hb_str)
            if last_hb_dt.tzinfo is None: last_hb_dt = last_hb_dt.replace(tzinfo=timezone.utc)
            
            # 1. فحص موت العامل (No Heartbeat for 3 minutes)
            diff_hb_seconds = (datetime.now(timezone.utc) - last_hb_dt).total_seconds()
            
            if diff_hb_seconds >= 180:
                duration_before_death = 0.0
                if worker_start_str:
                    start_dt = datetime.fromisoformat(worker_start_str)
                    if start_dt.tzinfo is None: start_dt = start_dt.replace(tzinfo=timezone.utc)
                    duration_before_death = (last_hb_dt - start_dt).total_seconds()
                    
                await self.handle_worker_failure(active_worker, last_hb_str, diff_hb_seconds / 60, duration_before_death, "Dead (No Heartbeat)")
                return

            # 2. فحص تأخر الوردية (Shift Delay - أكثر من 4 ساعات و 15 دقيقة = 16500 ثانية)
            if worker_start_str:
                start_dt = datetime.fromisoformat(worker_start_str)
                if start_dt.tzinfo is None: start_dt = start_dt.replace(tzinfo=timezone.utc)
                diff_shift_seconds = (datetime.now(timezone.utc) - start_dt).total_seconds()
                
                if diff_shift_seconds >= 16500:
                    # العامل حي (يرسل نبضات) لكن تأخر في تسليم الوردية!
                    await self.handle_worker_failure(active_worker, last_hb_str, 0, diff_shift_seconds, "Shift Delay (Overworked)")
                    
        except Exception as e:
            print(f"[Watchdog Error] Parse time failed: {e}")

    async def handle_worker_failure(self, dead_worker: str, last_hb: str, elapsed_min: float, duration_before_death: float, reason: str):
        attempts = self.db.read_system_state().get('backup_attempts', 0)
        
        if attempts >= 3:
            await self.notifier.send_safe_mode()
            return
            
        new_attempts = self.db.increment_backup_attempts()
        self.db.log_event("FAIL", "Worker", dead_worker, f"Reason: {reason}")
        self.db.log_event("BACKUP", "Watchdog", self.watchdog_id, f"Triggering Backup_Z. Attempt {new_attempts}/3")
        
        # إرسال إنذار مخصص لسبب الفشل
        await self.notifier.send_worker_fail(dead_worker, last_hb, elapsed_min, duration_before_death, reason)
        await self.notifier.send_emergency_dispatch(self.watchdog_id, new_attempts)
        
        await self.github.dispatch_workflow('backup_worker.yml', "Watchdog_Emergency")

    async def handle_hard_stop(self):
        self.db.log_event("HARD_STOP", "Watchdog", self.watchdog_id, "Exceeded 5.5 hours.")
        next_watchdog = "Beta" if self.watchdog_id == "Alpha" else "Alpha"
        await self.github.dispatch_workflow(f'watchdog_{next_watchdog.lower()}.yml', "Hard_Stop_Dispatch")
        for _ in range(10):
            state = self.db.read_system_state()
            if state and state.get('active_watchdog') != self.watchdog_id: break
            await asyncio.sleep(30)
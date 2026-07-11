import os
import sys
import time
import asyncio
import importlib
from datetime import datetime, timezone
from core.database_manager import DatabaseManager
from core.cache_manager import CacheManager
from core.notifier import TelegramNotifier
from core.github_client import GitHubActionsClient

class WorkerEngine:
    def __init__(self):
        self.worker_id = os.getenv("WORKER_ID", "Unknown_Worker")
        self.trigger_source = os.getenv("TRIGGER_SOURCE", "GitHub_Cron")
        self.active_task = os.getenv("ACTIVE_TASK", "eur_usd_fetcher") # اسم السائق الافتراضي
        self.db = DatabaseManager()
        self.cache = CacheManager()
        self.notifier = TelegramNotifier()
        self.github = GitHubActionsClient()
        self.start_time = time.time()
        self.last_heartbeat_time = 0
        self.last_task_run_time = 0
        self.has_dispatched_next = False

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

        # حساب الفجوة الزمنية
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
            
        self.db.log_event("START", "Worker", self.worker_id, f"Trigger: {self.trigger_source}, Task: {self.active_task}")
        await self.notifier.send_worker_start(self.worker_id, self.trigger_source, prev_worker, gap_seconds)
        return True

    async def run_main_loop(self):
        while True:
            current_time = time.time()
            
            # 1. التسليم الاستباقي (3.75 ساعات)
            if not self.has_dispatched_next and (current_time - self.start_time) >= 13500:
                if self.worker_id.startswith("Worker_"):
                    letter = self.worker_id[-1]
                    next_letter = 'A' if letter == 'F' else chr(ord(letter) + 1)
                    workflow_file = f"worker_{next_letter.lower()}.yml"
                    await self.github.dispatch_workflow(workflow_file, "Proactive_Handover")
                    self.has_dispatched_next = True

            # 2. الإغلاق الإجباري (5.5 ساعات)
            if (current_time - self.start_time) >= 19800:
                duration = current_time - self.start_time
                self.db.log_event("HARD_STOP", "Worker", self.worker_id, "Exceeded 5.5 hours.")
                await self.notifier.send_worker_hard_stop(self.worker_id, duration)
                break
                
            # 3. التسليم العادي
            state = self.db.read_system_state()
            if state and state.get('active_worker') != self.worker_id:
                duration = current_time - self.start_time
                next_worker = state.get('active_worker')
                self.db.log_event("HANDOVER", "Worker", self.worker_id, f"Detected new worker: {next_worker}")
                await self.notifier.send_worker_shift_summary(self.worker_id, next_worker, self.start_time, duration)
                break
                
            # 4. نبضة الحياة
            if (current_time - self.last_heartbeat_time) >= 60:
                if self.db.update_heartbeat('worker', self.worker_id):
                    self.last_heartbeat_time = current_time
                    
            # 5. تشغيل السائق الديناميكي (Dynamic Driver Execution) - كل 15 دقيقة
            if (current_time - self.last_task_run_time) >= 900:
                await self.execute_dynamic_task()
                self.last_task_run_time = current_time
                    
            await asyncio.sleep(30)

    async def execute_dynamic_task(self):
        """تحميل وتشغيل السائق في بيئة معزولة لمنع انهيار المركبة"""
        try:
            # استيراد ديناميكي للسائق من مجلد tasks
            module = importlib.import_module(f"tasks.{self.active_task}")
            # تنفيذ دالة السائق (يجب أن تكون async وتقبل worker_id)
            await module.execute_task(self.worker_id)
        except Exception as e:
            # عزل الخطأ: المركبة لا تنهار، ترسل إنذار وتكمل النبضات
            print(f"[{self.worker_id}] Task '{self.active_task}' crashed: {e}")
            await self.notifier.send_task_failure(self.worker_id, self.active_task, str(e))
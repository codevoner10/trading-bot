import os
from datetime import datetime, timezone
from supabase import create_client, Client
from typing import Dict, Optional

class DatabaseManager:
    """مدير قاعدة البيانات المركزي لجميع عمليات القراءة والكتابة"""
    
    def __init__(self):
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
        if not supabase_url or not supabase_key:
            raise ValueError("Supabase credentials are missing in environment variables.")
        self.client: Client = create_client(supabase_url, supabase_key)

    def get_current_utc(self) -> datetime:
        """إرجاع الوقت الحالي بتوقيت UTC الموحد"""
        return datetime.now(timezone.utc)

    def read_system_state(self) -> Optional[Dict]:
        """قراءة الحالة الحالية للنظام"""
        try:
            response = self.client.table('system_state').select('*').eq('id', 1).execute()
            if response.data:
                return response.data[0]
            return None
        except Exception as e:
            print(f"[DB Error] Failed to read system state: {e}")
            return None

    def claim_active_role(self, role: str, name: str) -> bool:
        """الاستحواذ على دور (عامل أو كلب حراسة)"""
        try:
            current_time = self.get_current_utc().isoformat()
            update_data = {
                f'active_{role}': name,
                f'{role}_start_time': current_time,
                f'last_{role}_heartbeat': current_time
            }
            self.client.table('system_state').update(update_data).eq('id', 1).execute()
            
            # تصفير عداد الطوارئ للعمال الأساسيين فقط
            if role == 'worker' and name != 'Backup_Z':
                self.reset_backup_counter()
                
            return True
        except Exception as e:
            print(f"[DB Error] Failed to claim role: {e}")
            return False

    def update_heartbeat(self, role: str, name: str) -> bool:
        """تحديث نبضة الحياة"""
        try:
            current_time = self.get_current_utc().isoformat()
            self.client.table('system_state').update({
                f'last_{role}_heartbeat': current_time
            }).eq('id', 1).execute()
            
            if role == 'worker':
                self.client.table('worker_heartbeats').upsert({
                    'worker_name': name,
                    'last_heartbeat': current_time,
                    'is_active': True
                }).execute()
            return True
        except Exception as e:
            print(f"[DB Error] Failed to update heartbeat: {e}")
            return False

    def increment_backup_attempts(self) -> Optional[int]:
        """زيادة عداد محاولات الطوارئ"""
        try:
            state = self.read_system_state()
            if not state: return None
            current_attempts = state.get('backup_attempts', 0) + 1
            self.client.table('system_state').update({'backup_attempts': current_attempts}).eq('id', 1).execute()
            return current_attempts
        except Exception as e:
            print(f"[DB Error] Failed to increment backup attempts: {e}")
            return None

    def reset_backup_counter(self) -> bool:
        """تصفير عداد الطوارئ"""
        try:
            self.client.table('system_state').update({'backup_attempts': 0}).eq('id', 1).execute()
            return True
        except Exception as e:
            print(f"[DB Error] Failed to reset backup counter: {e}")
            return False

    def log_event(self, event_type: str, component: str, name: str, message: str) -> None:
        """تسجيل حدث في الصندوق الأسود"""
        try:
            self.client.table('event_log').insert({
                'event_type': event_type, 'component': component, 'name': name, 'message': message
            }).execute()
        except Exception as e:
            print(f"[DB Log Error] Failed to log event: {e}")
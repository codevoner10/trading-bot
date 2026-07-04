import os
import time
import json
import requests
import redis
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client

# ===================== CONFIGURATION =====================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
UPSTASH_REDIS_URL = os.getenv("UPSTASH_REDIS_URL")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID_OPS = os.getenv("TELEGRAM_CHAT_ID")
CHAT_ID_MARKET = os.getenv("TELEGRAM_CHAT_ID_MARKET")
GH_PAT = os.getenv("GH_PAT")
GH_REPO = os.getenv("GH_REPO")

# ===================== TIME FUNCTIONS =====================
def get_utc_now():
    return datetime.now(timezone.utc)

def format_utc(dt):
    return dt.isoformat()

def parse_utc(time_str):
    if not time_str:
        return None
    try:
        if time_str.endswith('Z'):
            time_str = time_str[:-1] + '+00:00'
        return datetime.fromisoformat(time_str)
    except:
        return None

# ===================== HYBRID STATE MANAGER =====================
class HybridStateManager:
    def __init__(self, component_name="System"):
        self.component_name = component_name
        self.supabase: Client = None
        self.redis_client = None
        
        self.memory_state = {"active_worker": "none", "worker_start_time": "", "backup_attempts": 0}
        self.memory_cache = {} # For analysis timestamps & watchdog HBs
        
        self.sb_alerted = False
        self.redis_alerted = False
        self.last_sb_alert_time = None
        self.last_redis_alert_time = None
        self.last_sb_reconnect = get_utc_now()
        self.last_redis_reconnect = get_utc_now()
        
        self._connect_supabase()
        self._connect_redis()

    def _connect_supabase(self):
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("[WARN] Supabase URL/KEY missing. Falling back to memory.")
            return
        try:
            self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            self.supabase.table("system_state").select("id").eq("id", 1).execute()
            print("[INFO] Connected to Supabase.")
        except Exception as e:
            print(f"[ERROR] Supabase connection failed: {e}")
            self.supabase = None
            self._alert_sb_down()

    def _connect_redis(self):
        if not UPSTASH_REDIS_URL:
            return
        try:
            self.redis_client = redis.from_url(UPSTASH_REDIS_URL, ssl=True, decode_responses=True)
            self.redis_client.ping()
            print("[INFO] Connected to Upstash Redis.")
        except Exception as e:
            print(f"[ERROR] Redis connection failed: {e}")
            self.redis_client = None
            self._alert_redis_down()

    def _alert_sb_down(self):
        now = get_utc_now()
        if not self.sb_alerted or (self.last_sb_alert_time and now - self.last_sb_alert_time > timedelta(minutes=10)):
            send_telegram(f"⚠️ <b>[SYSTEM]</b> Supabase is down for {self.component_name}. Falling back to memory.", channel="ops")
            self.sb_alerted = True
            self.last_sb_alert_time = now

    def _alert_redis_down(self):
        now = get_utc_now()
        if not self.redis_alerted or (self.last_redis_alert_time and now - self.last_redis_alert_time > timedelta(minutes=10)):
            send_telegram(f"⚠️ <b>[SYSTEM]</b> Redis is down for {self.component_name}. Falling back to memory.", channel="ops")
            self.redis_alerted = True
            self.last_redis_alert_time = now

    def _reconnect_supabase(self):
        now = get_utc_now()
        if (now - self.last_sb_reconnect).total_seconds() < 600:
            return
        self.last_sb_reconnect = now
        if not self.supabase:
            self._connect_supabase()
            if self.supabase:
                send_telegram(f"✅ <b>[RECOVERED]</b> {self.component_name} reconnected to Supabase.", channel="ops")
                self.sb_alerted = False
                if self.memory_state["active_worker"] != "none":
                    self.update_state(self.memory_state)

    def _reconnect_redis(self):
        now = get_utc_now()
        if (now - self.last_redis_reconnect).total_seconds() < 600:
            return
        self.last_redis_reconnect = now
        if not self.redis_client:
            self._connect_redis()
            if self.redis_client:
                send_telegram(f"✅ <b>[RECOVERED]</b> {self.component_name} reconnected to Redis.", channel="ops")
                self.redis_alerted = False
                for k, v in self.memory_cache.items():
                    self.set_cache(k, v, ttl=3600)

    # --- STATE METHODS (Supabase Primary) ---
    def get_state(self):
        if self.supabase:
            try:
                resp = self.supabase.table("system_state").select("*").eq("id", 1).single().execute()
                if resp.data:
                    return {
                        "active_worker": resp.data.get("active_worker", "none"),
                        "worker_start_time": resp.data.get("worker_start_time", ""),
                        "backup_attempts": resp.data.get("backup_attempts", 0)
                    }
            except Exception as e:
                print(f"[ERROR] Supabase read state failed: {e}")
                self.supabase = None
                self._alert_sb_down()
        return self.memory_state

    def update_state(self, updates):
        if self.supabase:
            try:
                self.supabase.table("system_state").update(updates).eq("id", 1).execute()
                return True
            except Exception as e:
                print(f"[ERROR] Supabase update state failed: {e}")
                self.supabase = None
                self._alert_sb_down()
        self.memory_state.update(updates)
        return False

    def get_worker_heartbeat(self, worker_name):
        if self.supabase:
            try:
                resp = self.supabase.table("worker_heartbeats").select("last_heartbeat").eq("worker_name", worker_name).single().execute()
                if resp.data:
                    return resp.data.get("last_heartbeat")
            except Exception as e:
                if "JSON mapped" not in str(e):
                    print(f"[ERROR] Supabase get HB failed: {e}")
                self.supabase = None
                self._alert_sb_down()
        return self.memory_cache.get(f"worker_hb_{worker_name}")

    def update_worker_heartbeat(self, worker_name, time_str):
        if self.supabase:
            try:
                self.supabase.table("worker_heartbeats").upsert({"worker_name": worker_name, "last_heartbeat": time_str}).execute()
                return True
            except Exception as e:
                print(f"[ERROR] Supabase update HB failed: {e}")
                self.supabase = None
                self._alert_sb_down()
        self.memory_cache[f"worker_hb_{worker_name}"] = time_str
        return False

    def log_event(self, event_type, worker_name, message):
        if self.supabase:
            try:
                self.supabase.table("event_log").insert({
                    "event_type": event_type,
                    "worker_name": worker_name,
                    "message": message
                }).execute()
            except Exception as e:
                print(f"[ERROR] Supabase log event failed: {e}")
                self.supabase = None
                self._alert_sb_down()

    def cleanup_old_events(self):
        if self.supabase:
            try:
                cutoff = (get_utc_now() - timedelta(days=7)).isoformat()
                self.supabase.table("event_log").delete().lt("created_at", cutoff).execute()
            except Exception:
                pass

    # --- CACHE METHODS (Redis Primary) ---
    def get_cache(self, key):
        if self.redis_client:
            try:
                return self.redis_client.get(key)
            except Exception as e:
                print(f"[ERROR] Redis get cache failed: {e}")
                self.redis_client = None
                self._alert_redis_down()
        return self.memory_cache.get(key)

    def set_cache(self, key, value, ttl=3600):
        if self.redis_client:
            try:
                self.redis_client.set(key, value, ex=ttl)
                return True
            except Exception as e:
                print(f"[ERROR] Redis set cache failed: {e}")
                self.redis_client = None
                self._alert_redis_down()
        self.memory_cache[key] = value
        return False

    # --- WATCHDOG HEARTBEATS (Redis Primary) ---
    def update_watchdog_heartbeat(self, wd_name, time_str):
        return self.set_cache(f"watchdog:hb:{wd_name}", time_str, ttl=3600)

    def get_watchdog_heartbeat(self, wd_name):
        return self.get_cache(f"watchdog:hb:{wd_name}")

# ===================== TELEGRAM =====================
def send_telegram(message, channel="ops"):
    token = TELEGRAM_TOKEN
    chat_id = CHAT_ID_OPS if channel == "ops" else CHAT_ID_MARKET
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[ERROR] Telegram send failed: {e}")

# ===================== MARKET DATA =====================
def fetch_market_data(worker_name, symbol="EUR/USD"):
    key_map = {
        "Worker_A": os.getenv("TWELVEDATA_KEY_A"),
        "Worker_B": os.getenv("TWELVEDATA_KEY_B"),
        "Worker_C": os.getenv("TWELVEDATA_KEY_C"),
        "Worker_D": os.getenv("TWELVEDATA_KEY_D"),
        "Worker_E": os.getenv("TWELVEDATA_KEY_E"),
        "Worker_F": os.getenv("TWELVEDATA_KEY_F"),
        "Backup_Z": os.getenv("TWELVEDATA_KEY_BACKUP")
    }
    
    primary_key = key_map.get(worker_name)
    fallback_key = os.getenv("TWELVEDATA_KEY_BACKUP")
    
    keys_to_try = [primary_key]
    if worker_name != "Backup_Z" and fallback_key:
        keys_to_try.append(fallback_key)

    for key in keys_to_try:
        if not key:
            continue
        try:
            url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=15min&outputsize=1&apikey={key}"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get("status") == "ok" and "values" in data:
                candle = data["values"][0]
                return {
                    "symbol": symbol,
                    "timeframe": "M15",
                    "open": float(candle["open"]),
                    "high": float(candle["high"]),
                    "low": float(candle["low"]),
                    "close": float(candle["close"]),
                    "datetime": candle["datetime"]
                }
            elif data.get("code") == 429:
                continue 
        except:
            continue
    return None

def format_market_message(data, worker_name):
    change = data["close"] - data["open"]
    change_pct = (change / data["open"]) * 100 if data["open"] != 0 else 0
    arrow = "🟢" if change >= 0 else "🔴"
    
    return (
        f"📊 <b>MARKET DATA</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💱 <b>Symbol:</b> {data['symbol']}\n"
        f"⏱ <b>Timeframe:</b> {data['timeframe']}\n"
        f"💹 <b>Price:</b> {data['close']:.5f}\n"
        f"📈 <b>High:</b> {data['high']:.5f}\n"
        f"📉 <b>Low:</b> {data['low']:.5f}\n"
        f"🔓 <b>Open:</b> {data['open']:.5f}\n"
        f"{arrow} <b>Change:</b> {change:+.5f} ({change_pct:+.2f}%)\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ <b>Time:</b> {data['datetime']}\n"
        f"👤 <b>Worker:</b> {worker_name}"
    )

# ===================== GITHUB ACTIONS TRIGGER =====================
def trigger_github_workflow(workflow_file, inputs=None):
    if not GH_PAT or not GH_REPO:
        print("[ERROR] GitHub PAT or Repo not configured.")
        return False
    
    url = f"https://api.github.com/repos/{GH_REPO}/actions/workflows/{workflow_file}/dispatches"
    headers = {
        "Authorization": f"token {GH_PAT}",
        "Accept": "application/vnd.github.v3+json"
    }
    payload = {"ref": "main", "inputs": inputs or {}}
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 204:
            print(f"[INFO] Triggered workflow: {workflow_file}")
            return True
        else:
            print(f"[ERROR] GitHub trigger failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"[ERROR] GitHub trigger exception: {e}")
        return False
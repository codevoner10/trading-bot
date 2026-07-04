import os
import requests
from datetime import datetime, timezone

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram(msg):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram not configured")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=10)
        return True
    except Exception as e:
        print(f"Telegram error: {e}")
        return False

def test_supabase():
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_KEY", "").strip()
    
    if not url or not key:
        return "❌ Supabase: URL أو KEY فارغ"
    
    # فحص إذا كان المفتاح يحتوي على علامات تنصيص أو مسافات
    if url.startswith('"') or url.startswith("'"):
        return f"⚠️ Supabase: URL يحتوي على علامات تنصيص! ({url[:10]}...)"
    if key.startswith('"') or key.startswith("'"):
        return f"⚠️ Supabase: KEY يحتوي على علامات تنصيص! ({key[:10]}...)"
    
    headers = {"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    try:
        resp = requests.get(f"{url}/rest/v1/system_state?id=eq.1", headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return f"✅ Supabase: OK (active_worker={data[0].get('active_worker','?')})"
        else:
            return f"❌ Supabase: Status {resp.status_code} | {resp.text[:80]}"
    except Exception as e:
        return f"❌ Supabase: {type(e).__name__}: {str(e)[:80]}"

def test_redis():
    redis_url = os.getenv("UPSTASH_REDIS_URL", "").strip()
    if not redis_url:
        return "❌ Redis: URL فارغ"
    try:
        import redis
        client = redis.from_url(redis_url, decode_responses=True)
        if client.ping():
            return "✅ Redis: OK (PING نجح)"
        else:
            return "❌ Redis: PING فشل"
    except Exception as e:
        return f"❌ Redis: {type(e).__name__}: {str(e)[:80]}"

def test_telegram():
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return "❌ Telegram: TOKEN أو CHAT_ID فارغ"
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMe"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200 and resp.json().get("ok"):
            bot_name = resp.json()["result"].get("username", "?")
            return f"✅ Telegram: OK (@{bot_name})"
        else:
            return f"❌ Telegram: Status {resp.status_code}"
    except Exception as e:
        return f"❌ Telegram: {type(e).__name__}: {str(e)[:80]}"

def test_twelve_data():
    key = os.getenv("TWELVEDATA_KEY_A") or os.getenv("TWELVEDATA_KEY_BACKUP")
    if not key:
        return "❌ TwelveData: لا يوجد مفتاح"
    try:
        url = f"https://api.twelvedata.com/time_series?symbol=EUR/USD&interval=15min&outputsize=1&apikey={key}"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data.get("status") == "ok":
            price = data["values"][0].get("close", "?")
            return f"✅ TwelveData: OK (Price={price})"
        else:
            return f"❌ TwelveData: {data.get('message','?')[:80]}"
    except Exception as e:
        return f"❌ TwelveData: {type(e).__name__}: {str(e)[:80]}"

def test_github():
    pat = os.getenv("GH_PAT")
    repo = os.getenv("GH_REPO")
    if not pat or not repo:
        return "❌ GitHub: PAT أو REPO فارغ"
    try:
        headers = {"Authorization": f"token {pat}", "Accept": "application/vnd.github.v3+json"}
        resp = requests.get(f"https://api.github.com/repos/{repo}", headers=headers, timeout=10)
        if resp.status_code == 200:
            return f"✅ GitHub: OK ({repo})"
        else:
            return f"❌ GitHub: Status {resp.status_code}"
    except Exception as e:
        return f"❌ GitHub: {type(e).__name__}: {str(e)[:80]}"

def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    
    results = [
        f"🔍 <b>اختبار API</b> | <i>{now}</i>",
        "",
        test_supabase(),
        test_redis(),
        test_telegram(),
        test_twelve_data(),
        test_github(),
        "",
        "📌 إذا ظهر ❌ فالمشكلة في الـ Secret"
    ]
    
    msg = "\n".join(results)
    print("="*50)
    print(msg)
    print("="*50)
    
    send_telegram(msg)

if __name__ == "__main__":
    main()
import os
import requests

def clean_env(name):
    val = os.getenv(name, "")
    val = val.strip()
    if len(val) >= 2 and ((val[0] == '"' and val[-1] == '"') or (val[0] == "'" and val[-1] == "'")):
        val = val[1:-1]
    return val

URL = clean_env("SUPABASE_URL")
KEY = clean_env("SUPABASE_KEY")

headers = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

print("=" * 50)
print("اختبار الكتابة في Supabase")
print("=" * 50)

# 1. GET (قراءة)
print("\n→ GET system_state:")
resp = requests.get(f"{URL}/rest/v1/system_state?id=eq.1", headers=headers, timeout=10)
print(f"  Status: {resp.status_code}")
print(f"  Body: {resp.text[:200]}")

# 2. PATCH (تحديث)
print("\n→ PATCH system_state (update active_worker):")
from datetime import datetime, timezone
now = datetime.now(timezone.utc).isoformat()
resp = requests.patch(
    f"{URL}/rest/v1/system_state?id=eq.1",
    headers=headers,
    json={"active_worker": "TEST_WRITE", "updated_at": now},
    timeout=10
)
print(f"  Status: {resp.status_code}")
print(f"  Body: {resp.text[:200]}")

# 3. POST (إدراج heartbeat)
print("\n→ POST worker_heartbeats:")
resp = requests.post(
    f"{URL}/rest/v1/worker_heartbeats",
    headers=headers,
    json={"worker_name": "TEST_WRITE", "last_heartbeat": now},
    timeout=10
)
print(f"  Status: {resp.status_code}")
print(f"  Body: {resp.text[:200]}")

# 4. إعادة القيمة
print("\n→ إعادة active_worker إلى none:")
resp = requests.patch(
    f"{URL}/rest/v1/system_state?id=eq.1",
    headers=headers,
    json={"active_worker": "none"},
    timeout=10
)
print(f"  Status: {resp.status_code}")

print("\n" + "=" * 50)
if all(r.status_code in [200, 201, 204] for r in [resp]):
    print("✅ الكتابة تعمل!")
else:
    print("❌ المفتاح لا يملك صلاحية الكتابة!")
    print("   اذهب إلى Supabase → API Keys → تعديل المفتاح → امنحه كل الصلاحيات")
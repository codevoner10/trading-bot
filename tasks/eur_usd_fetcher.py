import os
import httpx
from datetime import datetime, timezone

# السائق يقرأ مفاتيحه الخاصة ومستقلة عن المركبة
TASK_BOT_TOKEN = os.getenv("TASK_BOT_TOKEN")
TASK_CHAT_ID = os.getenv("TASK_CHAT_ID")
API_KEY = os.getenv("API_KEY")

async def _send_task_message(text: str):
    """إرسال رسالة إلى قناة المهام المخصصة"""
    if not TASK_BOT_TOKEN or not TASK_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TASK_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TASK_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with httpx.AsyncClient() as client:
            await client.post(url, json=payload, timeout=10.0)
    except Exception as e:
        print(f"[Task Notifier Error] {e}")

async def fetch_market_data(symbol: str = "EUR/USD", interval: str = "15min") -> dict:
    """جلب بيانات الشموع من Twelve Data"""
    base_url = "https://api.twelvedata.com/time_series"
    params = {"symbol": symbol, "interval": interval, "outputsize": 1, "apikey": API_KEY}
    
    async with httpx.AsyncClient() as client:
        response = await client.get(base_url, params=params, timeout=10.0)
        if response.status_code == 200:
            data = response.json()
            if "values" in data and len(data["values"]) > 0:
                candle = data["values"][0]
                open_p = float(candle.get("open", 0))
                high_p = float(candle.get("high", 0))
                low_p = float(candle.get("low", 0))
                close_p = float(candle.get("close", 0))
                change = close_p - open_p
                percent_change = (change / open_p) * 100 if open_p != 0 else 0
                return {
                    "open": open_p, "high": high_p, "low": low_p, "close": close_p,
                    "change": change, "percent_change": percent_change
                }
    return None

async def execute_task(worker_id: str):
    """نقطة الدخول الموحدة للسائق (The Contract)"""
    # 1. جلب البيانات
    data = await fetch_market_data("EUR/USD", "15min")
    
    if not data:
        await _send_task_message(f"⚠️ [{worker_id}] فشل جلب بيانات الشموع.")
        return

    # 2. تنسيق الرسالة
    change_emoji = "🟢" if data['change'] >= 0 else "🔴"
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    
    msg = (
        f"📊 MARKET DATA\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💱 Symbol: EUR/USD\n"
        f"⏱ Timeframe: M15\n"
        f"💹 Price: {data['close']:.5f}\n"
        f"📈 High: {data['high']:.5f}\n"
        f"📉 Low: {data['low']:.5f}\n"
        f"🔓 Open: {data['open']:.5f}\n"
        f"{change_emoji} Change: {data['change']:.5f} ({data['percent_change']:.2f}%)\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ Time: {current_time}\n"
        f"👤 Worker: {worker_id}"
    )

    # 3. الإرسال لقناة المهام
    await _send_task_message(msg)
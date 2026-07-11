import os
import httpx
from datetime import datetime, timezone

class TelegramNotifier:
    """إرسال الإشعارات والإنذارات الاحترافية إلى تيليجرام"""
    
    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def _format_duration(self, seconds: float) -> str:
        seconds = int(seconds)
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        if hours > 0: return f"{hours} ساعات و {minutes} دقائق"
        return f"{minutes} دقائق"

    def _get_utc_time(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def _get_time_from_ts(self, timestamp: float) -> str:
        return datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    async def _send(self, text: str) -> None:
        if not self.bot_token or not self.chat_id: return
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
        try:
            async with httpx.AsyncClient() as client:
                await client.post(self.base_url, json=payload, timeout=10.0)
        except Exception as e:
            print(f"[Notifier Error] Failed to send message: {e}")

    # --- رسائل العمال ---
    async def send_worker_start(self, worker_id: str, symbol: str, watchdog: str, trigger_source: str, prev_worker: str, gap_seconds: float) -> None:
        if gap_seconds > 180:
            msg = (
                f"⚠️ [GAP DETECTED] {worker_id} بدأ وردية جديدة\n"
                f"━━━━━━━━━━━━━━━\n"
                f"👤 العامل: {worker_id}\n"
                f"⏰ وقت البدء: {self._get_utc_time()}\n"
                f"📥 استلم من: {prev_worker}\n"
                f"🔌 مصدر التشغيل: {trigger_source}\n"
                f"🕳️ مدة انقطاع جمع البيانات: {self._format_duration(gap_seconds)}\n"
                f"🚨 ملاحظة: النظام عانى من فجوة. جاري استئناف العمل..."
            )
        else:
            msg = (
                f"🟢 [START] {worker_id} بدأ وردية جديدة\n"
                f"━━━━━━━━━━━━━━━\n"
                f"👤 العامل: {worker_id}\n"
                f"⏰ وقت البدء: {self._get_utc_time()}\n"
                f"🔄 المدة المجدولة: 4 ساعات\n"
                f"📊 الرمز المستهدف: {symbol}\n"
                f"🛡️ المراقب الحالي: {watchdog}\n"
                f"📥 استلم من: {prev_worker}\n"
                f"🔌 مصدر التشغيل: {trigger_source}\n"
                f"🔄 الحالة: استلام سلس، لا توجد فجوات زمنية."
            )
        await self._send(msg)

    async def send_worker_shift_summary(self, current_worker: str, next_worker: str, start_time_ts: float, duration_seconds: float) -> None:
        msg = (
            f"🏁 [SHIFT SUMMARY] {current_worker} أنهى وردية جمع البيانات\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📤 المسلم: {current_worker}\n"
            f"📥 المستلم: {next_worker}\n"
            f"⏰ وقت البدء: {self._get_time_from_ts(start_time_ts)}\n"
            f"⏰ وقت الانتهاء: {self._get_utc_time()}\n"
            f"⏱️ مدة العمل الفعلية: {self._format_duration(duration_seconds)}\n"
            f"💡 الحالة: تسليم سلس (Proactive Handover)"
        )
        await self._send(msg)

    async def send_worker_hard_stop(self, worker_id: str, duration_seconds: float) -> None:
        msg = (
            f"⚠️ [HARD STOP] {worker_id} تجاوز الحد الأقصى للوقت\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👤 العامل: {worker_id}\n"
            f"⏱️ مدة العمل الفعلية: {self._format_duration(duration_seconds)}\n"
            f"📝 السبب: الخروج اضطرارياً لتجنب قتل GitHub للعملية."
        )
        await self._send(msg)

    async def send_api_limit(self, worker_id: str) -> None:
        msg = (
            f"🚫 [API LIMIT] {worker_id} استنفد حد الطلبات\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👤 العامل: {worker_id}\n"
            f"⚠️ الحالة: توقف جلب البيانات مؤقتاً (429)\n"
            f"🫀 ملاحظة: العامل ما زال حياً ويُرسل النبضات لحماية وردليته."
        )
        await self._send(msg)

    async def send_market_data(self, worker_id: str, symbol: str, data: dict) -> None:
        change_emoji = "🟢" if data['change'] >= 0 else "🔴"
        msg = (
            f"📊 MARKET DATA\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💱 Symbol: {symbol}\n"
            f"⏱ Timeframe: M15\n"
            f"💹 Price: {data['close']:.5f}\n"
            f"📈 High: {data['high']:.5f}\n"
            f"📉 Low: {data['low']:.5f}\n"
            f"🔓 Open: {data['open']:.5f}\n"
            f"{change_emoji} Change: {data['change']:.5f} ({data['percent_change']:.2f}%)\n"
            f"━━━━━━━━━━━━━━━\n"
            f"⏰ Time: {self._get_utc_time()}\n"
            f"👤 Worker: {worker_id}"
        )
        await self._send(msg)

    # --- رسائل كلاب الحراسة ---
    async def send_watchdog_start(self, watchdog_id: str, trigger_source: str, prev_watchdog: str, gap_seconds: float) -> None:
        if gap_seconds > 180:
            msg = (
                f"⚠️ [GAP DETECTED] {watchdog_id} بدأ وردية المراقبة\n"
                f"━━━━━━━━━━━━━━━\n"
                f"👤 المراقب: {watchdog_id}\n"
                f"⏰ وقت البدء: {self._get_utc_time()}\n"
                f"📥 استلم من: {prev_watchdog}\n"
                f"🔌 مصدر التشغيل: {trigger_source}\n"
                f"🕳️ مدة انقطاع المراقبة: {self._format_duration(gap_seconds)}\n"
                f"🚨 ملاحظة: النظام كان بدون مراقب. جاري الفحص الاستباقي..."
            )
        else:
            msg = (
                f"👀 [START] {watchdog_id} بدأ وردية المراقبة\n"
                f"━━━━━━━━━━━━━━━\n"
                f"👤 المراقب: {watchdog_id}\n"
                f"⏰ وقت البدء: {self._get_utc_time()}\n"
                f"📥 استلم من: {prev_watchdog}\n"
                f"🔌 مصدر التشغيل: {trigger_source}\n"
                f"🔄 الحالة: استلام سلس، لا توجد فجوات زمنية."
            )
        await self._send(msg)

    async def send_watchdog_shift_summary(self, current_watchdog: str, next_watchdog: str, start_time_ts: float, duration_seconds: float) -> None:
        msg = (
            f"🏁 [SHIFT SUMMARY] {current_watchdog} أنهى وردية المراقبة\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📤 المسلم: {current_watchdog}\n"
            f"📥 المستلم: {next_watchdog}\n"
            f"⏰ وقت البدء: {self._get_time_from_ts(start_time_ts)}\n"
            f"⏰ وقت الانتهاء: {self._get_utc_time()}\n"
            f"⏱️ مدة العمل الفعلية: {self._format_duration(duration_seconds)}\n"
            f"💡 الحالة: تسليم سلس (Proactive Handover)"
        )
        await self._send(msg)

    async def send_worker_fail(self, dead_worker: str, last_hb: str, elapsed_min: float, duration_before_death: float) -> None:
        msg = (
            f"🔴 [FAIL] {dead_worker} توقف عن العمل!\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💀 العامل المتوقف: {dead_worker}\n"
            f"⏱️ مدة العمل قبل التوقف: {self._format_duration(duration_before_death)}\n"
            f"⏰ آخر نبضة مسجلة: {last_hb}\n"
            f"⏱ الوقت المنقضي دون نبضات: {elapsed_min:.1f} دقيقة\n"
            f"🚨 المراقب المكتشف: {os.getenv('WATCHDOG_ID', 'Unknown')}"
        )
        await self._send(msg)

    async def send_emergency_dispatch(self, watchdog_id: str, attempt: int) -> None:
        msg = (
            f"🚨 [EMERGENCY] {watchdog_id} يستدعي عامل الطوارئ\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🔄 العامل الاحتياطي: Backup_Z\n"
            f"🤖 المراقب المسؤول: {watchdog_id}\n"
            f"🔁 محاولة التشغيل رقم: {attempt}/3"
        )
        await self._send(msg)

    async def send_safe_mode(self) -> None:
        msg = (
            f"🛑 [SAFE MODE] النظام يدخل في وضع الراحة الآمنة\n"
            f"━━━━━━━━━━━━━━━\n"
            f"⚠️ السبب: استنفاد 3 محاولات لتشغيل الطوارئ فاشلة.\n"
            f"🛠 الإجراء: توقف التدخل التلقائي لمنع الإزعاج (Anti-Spam).\n"
            f"👨‍💻 مطلوب: تدخل بشري لفحص النظام."
        )
        await self._send(msg)

    async def send_db_error(self, component_name: str) -> None:
        msg = (
            f"🚨 [DB ERROR] {component_name} فشل في الاتصال بقاعدة البيانات\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🛑 الإجراء: إيقاف تشغيل آمن (Fail-Safe).\n"
            f"💡 السبب: منع تشغيل عاملين متعارضين (Dual-Active) في غياب التنسيق."
        )
        await self._send(msg)
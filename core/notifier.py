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
        """تحويل الثواني إلى صيغة مقروءة (ساعات ودقائق)"""
        seconds = int(seconds)
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        if hours > 0:
            return f"{hours} ساعات و {minutes} دقائق"
        return f"{minutes} دقائق"

    def _get_utc_time(self) -> str:
        """إرجاع الوقت الحالي بصيغة موحدة"""
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    async def _send(self, text: str) -> None:
        """دالة الإرسال الأساسية (Fail-safe)"""
        if not self.bot_token or not self.chat_id: return
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
        try:
            async with httpx.AsyncClient() as client:
                await client.post(self.base_url, json=payload, timeout=10.0)
        except Exception as e:
            print(f"[Notifier Error] Failed to send message: {e}")

    # --- رسائل العمال ---
    async def send_worker_start(self, worker_id: str, symbol: str, watchdog: str) -> None:
        msg = (
            f"🟢 [START] {worker_id} بدأ وردية جديدة\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👤 العامل: {worker_id}\n"
            f"⏰ وقت البدء: {self._get_utc_time()}\n"
            f"🔄 المدة المجدولة: 4 ساعات\n"
            f"📊 الرمز المستهدف: {symbol}\n"
            f"🛡️ المراقب الحالي: {watchdog}"
        )
        await self._send(msg)

    async def send_handover(self, current_worker: str, next_worker: str, duration_seconds: float) -> None:
        msg = (
            f"✅ [HANDOVER] {current_worker} سلم الوردية بنجاح\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📤 المسلم: {current_worker}\n"
            f"📥 المستلم: {next_worker}\n"
            f"⏰ وقت التسليم: {self._get_utc_time()}\n"
            f"⏱️ مدة العمل الفعلية: {self._format_duration(duration_seconds)}\n"
            f"💡 الحالة: تسليم سلس (Zero Downtime)"
        )
        await self._send(msg)

    async def send_worker_hard_stop(self, worker_id: str, duration_seconds: float) -> None:
        msg = (
            f"⚠️ [HARD STOP] {worker_id} تجاوز الحد الأقصى للوقت\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👤 العامل: {worker_id}\n"
            f"⏱️ مدة العمل الفعلية: {self._format_duration(duration_seconds)}\n"
            f"📝 السبب: الخروج اضطرارياً لتجنب قتل GitHub للعملية.\n"
            f"🚨 ملاحظة: العامل التالي تأخر، قد يتدخل الـ Watchdog."
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

    async def send_market_data(self, worker_id: str, symbol: str, price: float) -> None:
        msg = (
            f"📊 MARKET DATA\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💱 الرمز: {symbol}\n"
            f"💹 السعر الحالي: {price}\n"
            f"⏱ الفاصل الزمني: 15 دقيقة\n"
            f"👤 العامل: {worker_id}\n"
            f"⏰ الوقت: {self._get_utc_time()}"
        )
        await self._send(msg)

    # --- رسائل كلاب الحراسة ---
    async def send_watchdog_start(self, watchdog_id: str) -> None:
        msg = (
            f"👀 [WATCHDOG] {watchdog_id} بدأ وردية المراقبة\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👤 المراقب: {watchdog_id}\n"
            f"⏰ وقت البدء: {self._get_utc_time()}\n"
            f"🔄 الفترة: 2.5 ساعات (قابلة للتمدد 5.5 كحد أقصى)"
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

    async def send_watchdog_self_dispatch(self, watchdog_id: str, next_watchdog: str) -> None:
        msg = (
            f"⚙️ [SELF-DISPATCH] {watchdog_id} بلغ حد الإغلاق الإجباري\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👤 المراقب: {watchdog_id}\n"
            f"⏱ الحد الزمني: 5.5 ساعات\n"
            f"🔄 الإجراء: تم إرسال أمر تشغيل إجباري لـ {next_watchdog} عبر API."
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
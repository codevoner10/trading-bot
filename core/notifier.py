import os
import httpx

class TelegramNotifier:
    """إرسال الإشعارات والإنذارات إلى تيليجرام"""
    
    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    async def send_message(self, text: str) -> None:
        if not self.bot_token or not self.chat_id: return
            
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(self.base_url, json=payload, timeout=10.0)
        except Exception as e:
            print(f"[Notifier Error] Failed to send message: {e}")
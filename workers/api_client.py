import os
import httpx
from typing import Tuple, Optional

class MarketDataClient:
    """جلب البيانات من Twelve Data API"""
    
    def __init__(self):
        self.api_key = os.getenv("API_KEY")
        self.base_url = "https://api.twelvedata.com/price"
        if not self.api_key:
            raise ValueError("API_KEY is missing in environment variables.")

    async def fetch_price(self, symbol: str = "EUR/USD") -> Tuple[bool, Optional[float], bool]:
        params = {"symbol": symbol, "apikey": self.api_key}
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(self.base_url, params=params, timeout=10.0)
                if response.status_code == 429: return False, None, True
                if response.status_code == 200:
                    data = response.json()
                    if "price" in data: return True, float(data["price"]), False
                return False, None, False
        except Exception as e:
            print(f"[API Error] Fetch failed: {e}")
            return False, None, False
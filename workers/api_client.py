import os
import httpx
from typing import Tuple, Optional, Dict

class MarketDataClient:
    """جلب بيانات الشموع اليابانية (OHLC) من Twelve Data API"""
    
    def __init__(self):
        self.api_key = os.getenv("API_KEY")
        self.base_url = "https://api.twelvedata.com/time_series"
        if not self.api_key:
            raise ValueError("API_KEY is missing in environment variables.")

    async def fetch_market_data(self, symbol: str = "EUR/USD", interval: str = "15min") -> Tuple[bool, Optional[Dict], bool]:
        """
        جلب بيانات الشمعة (Open, High, Low, Close).
        Returns: (نجاح العملية, بيانات الشمعة, هل تم استنفاد الطلبات 429)
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "outputsize": 1,
            "apikey": self.api_key
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(self.base_url, params=params, timeout=10.0)
                
                # معالجة استنفاد الطلبات (Rate Limit)
                if response.status_code == 429:
                    return False, None, True
                
                if response.status_code == 200:
                    data = response.json()
                    if "values" in data and len(data["values"]) > 0:
                        candle = data["values"][0]
                        
                        open_p = float(candle.get("open", 0))
                        high_p = float(candle.get("high", 0))
                        low_p = float(candle.get("low", 0))
                        close_p = float(candle.get("close", 0))
                        
                        # حساب التغيّر والنسبة المئوية
                        change = close_p - open_p
                        percent_change = (change / open_p) * 100 if open_p != 0 else 0
                        
                        candle_data = {
                            "open": open_p,
                            "high": high_p,
                            "low": low_p,
                            "close": close_p,
                            "change": change,
                            "percent_change": percent_change
                        }
                        return True, candle_data, False
                    else:
                        print(f"[API Error] Unexpected format: {data}")
                        return False, None, False
                
                print(f"[API Error] Status: {response.status_code}")
                return False, None, False
                
        except Exception as e:
            print(f"[API Error] Fetch failed: {e}")
            return False, None, False
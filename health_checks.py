"""منطق الفحوصات الصحية — قابل للتخصيص."""

import logging

logger = logging.getLogger(__name__)


class HealthChecker:
    """فحوصات صحية قابلة للتوسعة."""

    def __init__(self, supabase_client):
        self.supabase = supabase_client

    def check_all(self):
        """تُرجع قائمة نتائج الفحوصات. كل عنصر dict يحتوي على check, ok, details."""
        results = []
        results.append(self._check_supabase_connection())
        return results

    def _check_supabase_connection(self):
        try:
            state = self.supabase.get_state()
            if state:
                return {
                    "check": "supabase_connection",
                    "ok": True,
                    "details": {"status": state.get("status")},
                }
            return {
                "check": "supabase_connection",
                "ok": False,
                "details": "get_state() returned None",
            }
        except Exception as exc:
            return {
                "check": "supabase_connection",
                "ok": False,
                "details": str(exc),
            }
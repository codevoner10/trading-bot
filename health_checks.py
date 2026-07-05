"""Health check logic — customizable. Add your own checks here."""

import logging

logger = logging.getLogger(__name__)


class HealthChecker:
    """Runs health checks and returns results list."""

    def __init__(self, supabase_client):
        self.db = supabase_client

    def check_all(self):
        results = []
        results.append(self._check_db_connection())
        results.append(self._check_system_state())
        return results

    def _check_db_connection(self):
        try:
            state = self.db.get_state()
            if state:
                return {
                    "check": "db_connection",
                    "status": "ok",
                    "details": {"db_status": state.get("status", "unknown")}
                }
            return {
                "check": "db_connection",
                "status": "fail",
                "details": "Cannot read system_state"
            }
        except Exception as e:
            return {
                "check": "db_connection",
                "status": "error",
                "details": str(e)
            }

    def _check_system_state(self):
        try:
            state = self.db.get_state()
            if not state:
                return {
                    "check": "system_state",
                    "status": "fail",
                    "details": "No state returned"
                }
            active = state.get("active_worker", "none")
            status = state.get("status", "UNKNOWN")
            return {
                "check": "system_state",
                "status": "ok",
                "details": {"active_worker": active, "status": status}
            }
        except Exception as e:
            return {
                "check": "system_state",
                "status": "error",
                "details": str(e)
            }
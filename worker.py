import os
import sys
import time
from datetime import timedelta
from utils import HybridStateManager, get_utc_now, format_utc, parse_utc, send_telegram, fetch_market_data, format_market_message

WORKER_NAME = os.getenv("WORKER_NAME", "Worker_A")
SYMBOL = os.getenv("SYMBOL", "EUR/USD")
MAX_RUNTIME = timedelta(hours=5, minutes=30)
HEARTBEAT_INTERVAL = 60
ANALYSIS_INTERVAL = 15 * 60

def main():
    if not WORKER_NAME or WORKER_NAME == "unknown":
        send_telegram("❌ <b>[ERROR]</b> Worker started without WORKER_NAME.", channel="ops")
        sys.exit(1)

    state = HybridStateManager(WORKER_NAME)
    
    if not state.supabase:
        send_telegram(f"❌ <b>[FATAL]</b> {WORKER_NAME} cannot start without Supabase state. Exiting.", channel="ops")
        sys.exit(1)

    start_time = get_utc_now()
    hard_stop_time = start_time + MAX_RUNTIME
    
    # 1. Overlap Safety
    current_state = state.get_state()
    existing_start_time = parse_utc(current_state.get("worker_start_time"))
    if existing_start_time and existing_start_time > start_time:
        send_telegram(f"⚠️ <b>[YIELD]</b> {WORKER_NAME} found newer active worker. Exiting.", channel="ops")
        return

    # 2. Claim Shift
    state.update_state({
        "active_worker": WORKER_NAME,
        "worker_start_time": format_utc(start_time),
        "backup_attempts": 0
    })
    state.update_worker_heartbeat(WORKER_NAME, format_utc(start_time))
    state.log_event("START", WORKER_NAME, f"{WORKER_NAME} started shift at {start_time}")
    state.cleanup_old_events()
    
    send_telegram(
        f"🟢 <b>[START]</b> {WORKER_NAME} بدأ وردية جديدة\n"
        f"⏰ <b>الوقت:</b> {start_time.strftime('%H:%M:%S UTC')}\n"
        f"📊 <b>الرمز:</b> {SYMBOL}",
        channel="ops"
    )
    
    last_heartbeat = start_time
    last_analysis = None
    api_error_count = 0
    api_alert_sent = False
    last_reconnect_check = start_time
    
    # 3. Main Loop
    while True:
        try:
            current_time = get_utc_now()
            
            # Hard Stop Check
            if current_time > hard_stop_time:
                send_telegram(f"⚠️ <b>[HARD STOP]</b> {WORKER_NAME} reached 5.5h limit. Exiting.", channel="ops")
                state.log_event("HARD_STOP", WORKER_NAME, "Reached 5.5h limit")
                break

            # Reconnection Check
            if (current_time - last_reconnect_check).total_seconds() >= 600:
                state._reconnect_supabase()
                state._reconnect_redis()
                last_reconnect_check = current_time
                
            # Handover Check
            current_state = state.get_state()
            if current_state.get("active_worker") != WORKER_NAME:
                new_worker = current_state.get("active_worker", "unknown")
                send_telegram(f"✅ <b>[HANDOVER]</b> {WORKER_NAME} سلم الوردية لـ {new_worker}", channel="ops")
                state.log_event("HANDOVER", WORKER_NAME, f"Handed over to {new_worker}")
                break
                
            # Update Heartbeat (Every 60s)
            if (current_time - last_heartbeat).total_seconds() >= HEARTBEAT_INTERVAL:
                # P1 FIX: Re-verify ownership to prevent race condition stale heartbeat
                if state.get_state().get("active_worker") == WORKER_NAME:
                    state.update_worker_heartbeat(WORKER_NAME, format_utc(current_time))
                    last_heartbeat = current_time
                
            # Market Analysis (Every 15m)
            if last_analysis is None or (current_time - last_analysis).total_seconds() >= ANALYSIS_INTERVAL:
                market_data = fetch_market_data(WORKER_NAME, SYMBOL)
                
                if market_data:
                    msg = format_market_message(market_data, WORKER_NAME)
                    send_telegram(msg, channel="market")
                    state.set_cache("analysis:last_analysis_time", format_utc(current_time), ttl=3600)
                    last_analysis = current_time
                    api_error_count = 0
                    api_alert_sent = False
                else:
                    api_error_count += 1
                    if api_error_count >= 3 and not api_alert_sent:
                        send_telegram(f"⚠️ <b>[API ERROR]</b> {WORKER_NAME}: Keys exhausted. Suppressing alerts.", channel="ops")
                        state.log_event("ERROR", WORKER_NAME, "API keys exhausted")
                        api_alert_sent = True
            
            time.sleep(30)
            
        except Exception as e:
            send_telegram(f"❌ <b>[EXCEPTION]</b> {WORKER_NAME} crashed: {str(e)}. Retrying in 30s.", channel="ops")
            state.log_event("ERROR", WORKER_NAME, f"Exception: {str(e)}")
            time.sleep(30)

if __name__ == "__main__":
    main()
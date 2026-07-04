import os
import sys
import time
from datetime import timedelta
from utils import HybridStateManager, get_utc_now, parse_utc, send_telegram, trigger_github_workflow

WATCHDOG_NAME = os.getenv("WATCHDOG_NAME", "Watchdog_Alpha")
NEXT_WATCHDOG_MAP = {
    "Watchdog_Alpha": "Watchdog_Beta",
    "Watchdog_Beta": "Watchdog_Gamma",
    "Watchdog_Gamma": "Watchdog_Alpha"
}
ALL_WATCHDOGS = ["Watchdog_Alpha", "Watchdog_Beta", "Watchdog_Gamma"]

RUNTIME_DURATION = timedelta(hours=2, minutes=10)
TRIGGER_NEXT_AT = timedelta(hours=1, minutes=45)
HEARTBEAT_TIMEOUT = timedelta(minutes=3)
SLEEP_INTERVAL = 60

def main():
    if not WATCHDOG_NAME or WATCHDOG_NAME not in NEXT_WATCHDOG_MAP:
        send_telegram("❌ <b>[ERROR]</b> Watchdog started with invalid name.", channel="ops")
        sys.exit(1)

    state = HybridStateManager(WATCHDOG_NAME)

    if not state.supabase or not state.redis_client:
        send_telegram("❌ <b>[FATAL]</b> Watchdog cannot start without Supabase & Redis. Exiting.", channel="ops")
        sys.exit(1)

    start_time = get_utc_now()
    exit_time = start_time + RUNTIME_DURATION
    next_triggered = False
    backup_cooldown_until = None
    last_reconnect_check = start_time
    peer_alerted = {wd: False for wd in ALL_WATCHDOGS}
    
    send_telegram(f"🐕 <b>[WATCHDOG START]</b> {WATCHDOG_NAME} is monitoring.", channel="ops")
    
    while True:
        try:
            current_time = get_utc_now()
            
            if (current_time - last_reconnect_check).total_seconds() >= 600:
                state._reconnect_supabase()
                state._reconnect_redis()
                last_reconnect_check = current_time
            
            # 1. Relay Trigger
            if not next_triggered and (current_time - start_time) >= TRIGGER_NEXT_AT:
                next_wd = NEXT_WATCHDOG_MAP.get(WATCHDOG_NAME)
                if next_wd:
                    trigger_github_workflow("watchdog_relay.yml", {"watchdog_name": next_wd})
                    send_telegram(f"🔗 <b>[RELAY]</b> {WATCHDOG_NAME} triggered {next_wd}.", channel="ops")
                next_triggered = True
                
            # 2. Exit Condition
            if current_time >= exit_time:
                send_telegram(f"🛑 <b>[WATCHDOG EXIT]</b> {WATCHDOG_NAME} finished shift.", channel="ops")
                break
                
            # 3. Write Self-Heartbeat
            state.update_watchdog_heartbeat(WATCHDOG_NAME, get_utc_now().isoformat())
            
            # 4. Peer Watchdog Check
            for peer in ALL_WATCHDOGS:
                if peer == WATCHDOG_NAME:
                    continue
                peer_hb_str = state.get_watchdog_heartbeat(peer)
                peer_hb = parse_utc(peer_hb_str)
                
                if peer_hb and (current_time - peer_hb) > HEARTBEAT_TIMEOUT:
                    if not peer_alerted[peer]:
                        send_telegram(f"🚨 <b>[PEER FAIL]</b> {WATCHDOG_NAME} detected dead peer: {peer}!", channel="ops")
                        trigger_github_workflow("watchdog_relay.yml", {"watchdog_name": peer})
                        peer_alerted[peer] = True
                elif peer_hb and (current_time - peer_hb) <= HEARTBEAT_TIMEOUT:
                    peer_alerted[peer] = False

            # 5. Worker Heartbeat Check
            current_state = state.get_state()
            active_worker = current_state.get("active_worker", "none")
            
            if active_worker != "none":
                # P1 FIX: Will transparently fall back to Redis if Supabase is down
                worker_hb_str = state.get_worker_heartbeat(active_worker)
                worker_hb = parse_utc(worker_hb_str)
                
                if worker_hb:
                    time_diff = current_time - worker_hb
                    
                    if time_diff > HEARTBEAT_TIMEOUT:
                        if backup_cooldown_until and current_time < backup_cooldown_until:
                            pass 
                        else:
                            backup_attempts = int(current_state.get("backup_attempts", 0))
                            if backup_attempts < 3:
                                send_telegram(
                                    f"🔴 <b>[FAIL]</b> {active_worker} is dead! (No heartbeat for {time_diff.total_seconds()/60:.1f}m)\n"
                                    f"🚨 <b>[WATCHDOG]</b> {WATCHDOG_NAME} calling Backup. (Attempt {backup_attempts + 1}/3)",
                                    channel="ops"
                                )
                                state.log_event("FAIL", active_worker, f"Heartbeat timeout detected by {WATCHDOG_NAME}")
                                trigger_github_workflow("backup.yml", {"reason": f"{active_worker} timeout"})
                                state.update_state({"backup_attempts": backup_attempts + 1})
                                backup_cooldown_until = current_time + timedelta(minutes=5)
                            else:
                                send_telegram(f"⛔ <b>[WATCHDOG]</b> Max backup attempts reached. Waiting.", channel="ops")
                                backup_cooldown_until = current_time + timedelta(minutes=10)
            
            time.sleep(SLEEP_INTERVAL)
            
        except Exception as e:
            send_telegram(f"❌ <b>[EXCEPTION]</b> {WATCHDOG_NAME} crashed: {str(e)}. Retrying in 60s.", channel="ops")
            time.sleep(60)

if __name__ == "__main__":
    main()
import sys
import os
import asyncio

# حيلة برمجية لجعل بايثون يتعرف على مجلد المشروع الرئيسي
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from watchdog.watchdog_engine import WatchdogEngine

async def main():
    watchdog = WatchdogEngine()
    initialized = await watchdog.initialize()
    if not initialized:
        print(f"[{watchdog.watchdog_id}] Initialization failed. Exiting.")
        sys.exit(1)
        
    try:
        print(f"[{watchdog.watchdog_id}] Entering monitoring loop.")
        await watchdog.run_main_loop()
    except KeyboardInterrupt:
        print(f"[{watchdog.watchdog_id}] Manually interrupted.")
    except Exception as e:
        print(f"[{watchdog.watchdog_id}] Unexpected error: {e}")
    finally:
        print(f"[{watchdog.watchdog_id}] Shutting down gracefully.")

if __name__ == "__main__":
    asyncio.run(main())
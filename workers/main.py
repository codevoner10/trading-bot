import sys
import os
import asyncio

# حيلة برمجية لجعل بايثون يتعرف على مجلد المشروع الرئيسي لاستيراد مكتبات core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers.worker_engine import WorkerEngine

async def main():
    worker = WorkerEngine()
    initialized = await worker.initialize()
    if not initialized:
        print(f"[{worker.worker_id}] Initialization failed. Exiting.")
        sys.exit(1)
        
    try:
        print(f"[{worker.worker_id}] Entering main loop.")
        await worker.run_main_loop()
    except KeyboardInterrupt:
        print(f"[{worker.worker_id}] Manually interrupted.")
    except Exception as e:
        print(f"[{worker.worker_id}] Unexpected error: {e}")
    finally:
        print(f"[{worker.worker_id}] Shutting down gracefully.")

if __name__ == "__main__":
    asyncio.run(main())
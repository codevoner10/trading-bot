import os
import sys

# MUST set env BEFORE importing worker
os.environ["WORKER_NAME"] = "Backup_Z"
os.environ["SYMBOL"] = os.getenv("SYMBOL", "EUR/USD")

from worker import main

if __name__ == "__main__":
    main()
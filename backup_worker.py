import os
from worker import main

if __name__ == "__main__":
    os.environ["WORKER_NAME"] = "Backup_Z"
    main()
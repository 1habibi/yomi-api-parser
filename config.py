import os
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
BASE_URL = f"https://kodikapi.com/list?token={API_TOKEN}&types=anime-serial,anime&with_material_data=true&genres_type=all&lgbt=false"

BATCH_SIZE = 200
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "60"))

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "charset": "utf8mb4",
    "connect_timeout": 30,
    "autocommit": False
}

DB_POOL_MIN_SIZE = 5
DB_POOL_MAX_SIZE = 20

LOG_FILE = "log.txt"
LAST_SYNC_FILE = "last_sync.txt"

YEAR_MIN = 1880
YEAR_MAX = 2100

API_RETRY_COUNT = 3
API_RETRY_BACKOFF_BASE = 2

CONSECUTIVE_OLD_THRESHOLD = 50

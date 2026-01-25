import os
from datetime import datetime
from typing import Optional
from config import LAST_SYNC_FILE
from utils.logging_config import logger


def load_last_sync() -> Optional[datetime]:
    if not os.path.exists(LAST_SYNC_FILE):
        return None

    try:
        with open(LAST_SYNC_FILE, "r") as f:
            ts = f.read().strip()
            return datetime.fromisoformat(ts)
    except Exception as e:
        logger.warning(f"Failed to load last sync time: {e}")
        return None


def save_last_sync(dt: datetime) -> None:
    try:
        with open(LAST_SYNC_FILE, "w") as f:
            f.write(dt.isoformat())
        logger.info(f"Saved last sync time: {dt.isoformat()}")
    except Exception as e:
        logger.error(f"Failed to save last sync time: {e}", exc_info=True)

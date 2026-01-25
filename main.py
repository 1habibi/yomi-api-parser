import asyncio
import signal

from config import SYNC_INTERVAL_SECONDS
from utils.logging_config import logger
from db.connection import pool
from sync import periodic_sync


async def main() -> None:
    stop_event = asyncio.Event()

    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, initiating shutdown...")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await pool.create()
        await periodic_sync(stop_event, SYNC_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, shutting down...")
        stop_event.set()

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise

    finally:
        await pool.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")

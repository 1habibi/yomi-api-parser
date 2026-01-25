import asyncio
import aiohttp
from typing import Optional, Dict, Any
from config import API_RETRY_COUNT, API_RETRY_BACKOFF_BASE
from utils.logging_config import logger


async def fetch_page(
    session: aiohttp.ClientSession,
    url: str,
    retries: int = API_RETRY_COUNT
) -> Optional[Dict[str, Any]]:
    if not url:
        return None

    for attempt in range(1, retries + 1):
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                return await resp.json()

        except aiohttp.ClientError as e:
            logger.error(f"Client error fetching {url}: {e} (attempt {attempt}/{retries})")
            if attempt == retries:
                raise

        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching {url} (attempt {attempt}/{retries})")
            if attempt == retries:
                raise

        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e} (attempt {attempt}/{retries})")
            if attempt == retries:
                raise

        if attempt < retries:
            wait_time = API_RETRY_BACKOFF_BASE ** attempt
            logger.info(f"Retrying in {wait_time} seconds...")
            await asyncio.sleep(wait_time)

    return None


def create_session(timeout_seconds: int = 60) -> aiohttp.ClientSession:
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    return aiohttp.ClientSession(timeout=timeout)

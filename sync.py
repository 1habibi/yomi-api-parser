import asyncio
from typing import Optional
from datetime import datetime

from config import BASE_URL, BATCH_SIZE, CONSECUTIVE_OLD_THRESHOLD
from utils.logging_config import logger
from utils.metrics import SyncMetrics
from utils.parsers import parse_datetime
from utils.sync_state import load_last_sync, save_last_sync
from api.client import fetch_page, create_session
from db.connection import pool
from db.schema import ensure_tables
from db.cache import Cache
from db.operations import upsert_anime, sync_relations, check_new_translation


async def fetch_and_save(stop_event: Optional[asyncio.Event] = None) -> None:
    metrics = SyncMetrics()

    async with pool.acquire() as conn:
        try:
            await ensure_tables(conn)
            cache = Cache()
            await cache.load(conn)

            last_sync = load_last_sync()
            is_first_sync = last_sync is None
            newest_update_overall: Optional[datetime] = None

            if is_first_sync:
                logger.info("First sync - will process all records")
            else:
                logger.info(f"Incremental sync - last sync was {last_sync}")

            async with create_session(timeout_seconds=60) as session:
                page_url = BASE_URL
                page_num = 0
                total_count = 0
                consecutive_old = 0

                while page_url:
                    if stop_event and stop_event.is_set():
                        logger.info("Stop signal received, halting sync")
                        break

                    try:
                        data = await fetch_page(session, page_url)
                    except Exception as e:
                        logger.error(f"Failed to fetch page: {e}", exc_info=True)
                        metrics.mark_error()
                        break

                    if not data:
                        break

                    page_num += 1
                    logger.info(f"=== Processing page {page_num} ===")

                    page_url = data.get("next_page")
                    results = data.get("results", [])

                    for item in results:
                        if stop_event and stop_event.is_set():
                            logger.info("Stop signal received during page processing")
                            break

                        item_updated = parse_datetime(item.get("updated_at"))
                        if not is_first_sync and last_sync and item_updated and item_updated <= last_sync:
                            consecutive_old += 1
                            if consecutive_old >= CONSECUTIVE_OLD_THRESHOLD:
                                logger.info(f"Encountered {consecutive_old} consecutive old records, stopping")
                                page_url = None 
                                break
                            continue
                        else:
                            consecutive_old = 0

                        try:
                            material = item.get("material_data") or {}
                            anime_id, changed, added = await upsert_anime(conn, item, material)

                            new_translation_added = False
                            if not changed and not added and anime_id:
                                new_translation_added = await check_new_translation(conn, anime_id, item)

                            if changed or new_translation_added:
                                await sync_relations(conn, cache, anime_id, item, material)

                                if added:
                                    metrics.mark_added()
                                    logger.debug(f"Added new anime ID {anime_id}: {item.get('title')}")
                                elif new_translation_added:
                                    metrics.mark_updated()
                                    logger.info(f"Added new translation to anime ID {anime_id}")
                                else:
                                    metrics.mark_updated()
                                    logger.debug(f"Updated anime ID {anime_id}: {item.get('title')}")
                            else:
                                metrics.mark_unchanged()

                            total_count += 1

                            if total_count % BATCH_SIZE == 0:
                                await conn.commit()
                                logger.info(
                                    f"Commit after {total_count} records. "
                                    f"Added: {metrics.added_count}, "
                                    f"Updated: {metrics.updated_count}, "
                                    f"Unchanged: {metrics.unchanged_count}"
                                )

                            if item_updated:
                                if newest_update_overall is None or item_updated > newest_update_overall:
                                    newest_update_overall = item_updated

                        except Exception as e:
                            logger.error(f"Error processing anime {item.get('id')}: {e}", exc_info=True)
                            metrics.mark_error()

                    await conn.commit()
                    logger.info("Page sync committed")

            if newest_update_overall:
                save_last_sync(newest_update_overall)
            await conn.commit()
            metrics.log_summary()

        except Exception as e:
            logger.error(f"Sync failed: {e}", exc_info=True)
            raise


async def periodic_sync(stop_event: asyncio.Event, interval_seconds: int) -> None:
    logger.info(f"Starting periodic sync (interval: {interval_seconds} seconds)")

    while not stop_event.is_set():
        logger.info("=== Starting sync cycle ===")

        try:
            await fetch_and_save(stop_event)
        except Exception as e:
            logger.error(f"Sync cycle failed: {e}", exc_info=True)

        if stop_event.is_set():
            break

        logger.info(f"=== Sync cycle completed. Waiting {interval_seconds} seconds ===")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            pass

    logger.info("Periodic sync stopped")

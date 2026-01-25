"""Database operations for anime data."""
import json
import aiomysql
from typing import Dict, Any, Optional, Tuple, Set
from datetime import datetime
from utils.parsers import parse_datetime, parse_date
from utils.logging_config import logger
from models.anime import (
    AnimeDict,
    MaterialDict,
    extract_genres,
    extract_screenshots,
    get_person_mapping,
    get_studios,
    get_blocked_countries,
    normalize_blocked_seasons
)
from db.cache import Cache


async def find_existing_anime(
    conn: aiomysql.Connection,
    item: AnimeDict,
    material: Optional[MaterialDict]
) -> Optional[int]:
    """
    Find existing anime by shikimori_id, imdb_id, or title+year.

    Args:
        conn: Database connection
        item: Anime item data
        material: Material data

    Returns:
        Anime ID if found, None otherwise
    """
    async with conn.cursor() as cur:
        shikimori_id = item.get("shikimori_id") or (material or {}).get("shikimori_id")
        imdb_id = item.get("imdb_id")
        title_orig = item.get("title_orig")
        year = item.get("year")

        conditions = []
        params = []

        if shikimori_id:
            conditions.append("shikimori_id = %s")
            params.append(shikimori_id)

        if imdb_id:
            conditions.append("imdb_id = %s")
            params.append(imdb_id)

        if title_orig and year:
            conditions.append("(title_orig = %s AND year = %s)")
            params.extend([title_orig, year])

        if not conditions:
            return None

        where_clause = " OR ".join(conditions)
        query = f"""
            SELECT id, kodik_id, updated_at FROM anime
            WHERE {where_clause}
            ORDER BY updated_at DESC
            LIMIT 1
        """

        await cur.execute(query, params)
        row = await cur.fetchone()

        if row:
            anime_id = row[0]
            logger.info(f"Found existing anime ID {anime_id} for kodik_id {item.get('id')}")
            return anime_id

        return None


def build_anime_values(item: AnimeDict, material: Optional[MaterialDict]) -> Dict[str, Any]:
    """
    Build anime values dictionary from item and material data.

    Single source of truth for anime field mapping to eliminate duplication.

    Args:
        item: Anime item data
        material: Material data

    Returns:
        Dictionary of anime field values
    """
    mat = material or {}
    return {
        "kodik_type": item.get("type"),
        "link": item.get("link"),
        "title": item.get("title"),
        "title_orig": item.get("title_orig"),
        "other_title": item.get("other_title"),
        "year": item.get("year"),
        "last_season": item.get("last_season"),
        "last_episode": item.get("last_episode"),
        "episodes_count": item.get("episodes_count"),
        "kinopoisk_id": item.get("kinopoisk_id"),
        "imdb_id": item.get("imdb_id"),
        "shikimori_id": item.get("shikimori_id") or mat.get("shikimori_id"),
        "quality": item.get("quality"),
        "camrip": 1 if item.get("camrip") else 0,
        "lgbt": 1 if item.get("lgbt") else 0,
        "created_at": parse_datetime(item.get("created_at")),
        "updated_at": parse_datetime(item.get("updated_at")),
        "description": mat.get("description"),
        "anime_description": mat.get("anime_description"),
        "poster_url": mat.get("poster_url"),
        "anime_poster_url": mat.get("anime_poster_url"),
        "premiere_world": parse_date(mat.get("premiere_world")),
        "aired_at": parse_date(mat.get("aired_at")),
        "released_at": parse_date(mat.get("released_at")),
        "rating_mpaa": mat.get("rating_mpaa"),
        "minimal_age": mat.get("minimal_age"),
        "episodes_total": mat.get("episodes_total"),
        "episodes_aired": mat.get("episodes_aired"),
        "imdb_rating": mat.get("imdb_rating"),
        "imdb_votes": mat.get("imdb_votes"),
        "shikimori_rating": mat.get("shikimori_rating"),
        "shikimori_votes": mat.get("shikimori_votes"),
        "next_episode_at": parse_datetime(mat.get("next_episode_at")),
        "all_status": mat.get("all_status"),
        "anime_kind": mat.get("anime_kind"),
        "duration": mat.get("duration")
    }


async def upsert_anime(
    conn: aiomysql.Connection,
    item: AnimeDict,
    material: Optional[MaterialDict]
) -> Tuple[int, bool, bool]:
    """
    Insert or update anime record.

    Args:
        conn: Database connection
        item: Anime item data
        material: Material data

    Returns:
        Tuple of (anime_id, changed, added)
    """
    async with conn.cursor() as cur:
        kodik_id = item.get("id")
        item_updated = parse_datetime(item.get("updated_at"))

        # Check if anime already exists by matching criteria
        existing_anime_id = await find_existing_anime(conn, item, material)

        # Build values once for both INSERT and UPDATE
        values = build_anime_values(item, material)

        if existing_anime_id:
            # Check if update is needed
            await cur.execute(
                "SELECT updated_at FROM anime WHERE id=%s",
                (existing_anime_id,)
            )
            row = await cur.fetchone()

            if row:
                db_updated = row[0]
                if db_updated and item_updated and item_updated <= db_updated:
                    return existing_anime_id, False, False

            # Update existing record
            set_clause = ", ".join([f"{k}=%s" for k in values.keys()])
            await cur.execute(
                f"UPDATE anime SET {set_clause} WHERE id=%s",
                (*values.values(), existing_anime_id)
            )

            # Update kodik_id separately to ensure it's current
            await cur.execute(
                "UPDATE anime SET kodik_id = %s WHERE id = %s",
                (kodik_id, existing_anime_id)
            )

            return existing_anime_id, True, False

        else:
            # Check if record exists by kodik_id
            await cur.execute(
                "SELECT id, updated_at FROM anime WHERE kodik_id=%s",
                (kodik_id,)
            )
            row = await cur.fetchone()

            if row:
                # Update existing record by kodik_id
                anime_id = row[0]
                values_with_kodik = {"kodik_id": kodik_id, **values}
                set_clause = ", ".join([f"{k}=%s" for k in values_with_kodik.keys() if k != "kodik_id"])
                update_values = [v for k, v in values_with_kodik.items() if k != "kodik_id"]
                await cur.execute(
                    f"UPDATE anime SET {set_clause} WHERE id=%s",
                    (*update_values, anime_id)
                )
                return anime_id, True, False
            else:
                # Insert new record
                values_with_kodik = {"kodik_id": kodik_id, **values}
                cols = ", ".join(values_with_kodik.keys())
                placeholders = ", ".join(["%s"] * len(values_with_kodik))
                await cur.execute(
                    f"INSERT INTO anime ({cols}) VALUES ({placeholders})",
                    tuple(values_with_kodik.values())
                )
                return cur.lastrowid, True, True


async def fetch_existing_genres(conn: aiomysql.Connection, anime_id: int) -> Set[int]:
    """Fetch existing genre IDs for an anime."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT genre_id FROM anime_genres WHERE anime_id=%s",
            (anime_id,)
        )
        rows = await cur.fetchall()
        return {row[0] for row in rows}


async def fetch_existing_screenshots(conn: aiomysql.Connection, anime_id: int) -> Set[str]:
    """Fetch existing screenshot URLs for an anime."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT url FROM anime_screenshots WHERE anime_id=%s",
            (anime_id,)
        )
        rows = await cur.fetchall()
        return {row[0] for row in rows}


async def fetch_existing_persons(conn: aiomysql.Connection, anime_id: int) -> Set[Tuple[int, str]]:
    """Fetch existing (person_id, role) tuples for an anime."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT person_id, role FROM anime_persons WHERE anime_id=%s",
            (anime_id,)
        )
        rows = await cur.fetchall()
        return {(row[0], row[1]) for row in rows}


async def fetch_existing_studios(conn: aiomysql.Connection, anime_id: int) -> Set[int]:
    """Fetch existing studio IDs for an anime."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT studio_id FROM anime_studios WHERE anime_id=%s",
            (anime_id,)
        )
        rows = await cur.fetchall()
        return {row[0] for row in rows}


async def sync_relations(
    conn: aiomysql.Connection,
    cache: Cache,
    anime_id: int,
    item: AnimeDict,
    material: Optional[MaterialDict]
) -> None:
    """
    Sync anime relations (genres, screenshots, persons, studios, etc.).

    Uses smart diffing to only insert/delete what changed instead of DELETE ALL.

    Args:
        conn: Database connection
        cache: Cache instance
        anime_id: Anime ID
        item: Anime item data
        material: Material data
    """
    async with conn.cursor() as cur:
        # Smart sync genres
        genres = extract_genres(material)
        if genres:
            existing_genre_ids = await fetch_existing_genres(conn, anime_id)
            new_genre_ids = set(await cache.get_genre_ids_batch(conn, genres))

            to_delete = existing_genre_ids - new_genre_ids
            to_insert = new_genre_ids - existing_genre_ids

            if to_delete:
                await cur.execute(
                    f"DELETE FROM anime_genres WHERE anime_id=%s AND genre_id IN ({','.join(['%s']*len(to_delete))})",
                    (anime_id, *to_delete)
                )

            if to_insert:
                values = [(anime_id, gid) for gid in to_insert]
                await cur.executemany(
                    "INSERT IGNORE INTO anime_genres (anime_id, genre_id) VALUES (%s, %s)",
                    values
                )
        else:
            # No genres - delete all
            await cur.execute("DELETE FROM anime_genres WHERE anime_id=%s", (anime_id,))

        # Smart sync screenshots
        screenshots = extract_screenshots(item, material)
        if screenshots:
            existing_screenshots = await fetch_existing_screenshots(conn, anime_id)
            new_screenshots = set(screenshots)

            to_delete_urls = existing_screenshots - new_screenshots
            to_insert_urls = new_screenshots - existing_screenshots

            if to_delete_urls:
                placeholders = ','.join(['%s'] * len(to_delete_urls))
                await cur.execute(
                    f"DELETE FROM anime_screenshots WHERE anime_id=%s AND url IN ({placeholders})",
                    (anime_id, *to_delete_urls)
                )

            if to_insert_urls:
                values = [(anime_id, url) for url in to_insert_urls]
                await cur.executemany(
                    "INSERT INTO anime_screenshots (anime_id, url) VALUES (%s, %s)",
                    values
                )
        else:
            # No screenshots - delete all
            await cur.execute("DELETE FROM anime_screenshots WHERE anime_id=%s", (anime_id,))

        # Smart sync persons
        person_mapping = get_person_mapping(material)
        new_persons: Set[Tuple[int, str]] = set()

        for role, people in person_mapping.items():
            if people:
                person_ids = await cache.get_person_ids_batch(conn, people)
                for pid in person_ids:
                    new_persons.add((pid, role))

        if new_persons:
            existing_persons = await fetch_existing_persons(conn, anime_id)

            to_delete = existing_persons - new_persons
            to_insert = new_persons - existing_persons

            if to_delete:
                # Delete removed persons
                for pid, role in to_delete:
                    await cur.execute(
                        "DELETE FROM anime_persons WHERE anime_id=%s AND person_id=%s AND role=%s",
                        (anime_id, pid, role)
                    )

            if to_insert:
                values = [(anime_id, pid, role) for pid, role in to_insert]
                await cur.executemany(
                    "INSERT IGNORE INTO anime_persons (anime_id, person_id, role) VALUES (%s, %s, %s)",
                    values
                )
        else:
            # No persons - delete all
            await cur.execute("DELETE FROM anime_persons WHERE anime_id=%s", (anime_id,))

        # Smart sync studios
        studios = get_studios(material)
        if studios:
            existing_studio_ids = await fetch_existing_studios(conn, anime_id)
            new_studio_ids = set(await cache.get_studio_ids_batch(conn, studios))

            to_delete = existing_studio_ids - new_studio_ids
            to_insert = new_studio_ids - existing_studio_ids

            if to_delete:
                await cur.execute(
                    f"DELETE FROM anime_studios WHERE anime_id=%s AND studio_id IN ({','.join(['%s']*len(to_delete))})",
                    (anime_id, *to_delete)
                )

            if to_insert:
                values = [(anime_id, sid) for sid in to_insert]
                await cur.executemany(
                    "INSERT IGNORE INTO anime_studios (anime_id, studio_id) VALUES (%s, %s)",
                    values
                )
        else:
            # No studios - delete all
            await cur.execute("DELETE FROM anime_studios WHERE anime_id=%s", (anime_id,))

        # Handle translations (not smart sync - uses check before insert)
        tr = item.get("translation")
        if tr:
            await cur.execute(
                "SELECT id FROM anime_translations WHERE anime_id=%s AND external_id=%s",
                (anime_id, tr.get("id"))
            )
            existing = await cur.fetchone()

            if not existing:
                await cur.execute(
                    "INSERT INTO anime_translations (anime_id, external_id, title, trans_type) VALUES (%s,%s,%s,%s)",
                    (anime_id, tr.get("id"), tr.get("title"), tr.get("type"))
                )
                logger.info(f"Added new translation '{tr.get('title')}' for anime ID {anime_id}")

        # Always delete and recreate blocked_countries and blocked_seasons (simpler for these)
        await cur.execute("DELETE FROM blocked_countries WHERE anime_id=%s", (anime_id,))
        await cur.execute("DELETE FROM blocked_seasons WHERE anime_id=%s", (anime_id,))

        # Blocked countries
        countries = get_blocked_countries(item)
        if countries:
            values = [(anime_id, country) for country in countries]
            await cur.executemany(
                "INSERT INTO blocked_countries (anime_id, country) VALUES (%s, %s)",
                values
            )

        # Blocked seasons
        blocked_seasons = normalize_blocked_seasons(item.get("blocked_seasons"))
        if blocked_seasons:
            if blocked_seasons == {"all": "all"}:
                await cur.execute(
                    "INSERT INTO blocked_seasons (anime_id, season, blocked_data) VALUES (%s, %s, %s)",
                    (anime_id, "all", json.dumps("all"))
                )
            else:
                values = [(anime_id, season, json.dumps(data)) for season, data in blocked_seasons.items()]
                await cur.executemany(
                    "INSERT INTO blocked_seasons (anime_id, season, blocked_data) VALUES (%s, %s, %s)",
                    values
                )


async def check_new_translation(
    conn: aiomysql.Connection,
    anime_id: int,
    item: AnimeDict
) -> bool:
    """
    Check if item contains a new translation for the anime.

    Args:
        conn: Database connection
        anime_id: Anime ID
        item: Anime item data

    Returns:
        True if new translation exists, False otherwise
    """
    tr = item.get("translation")
    if not tr:
        return False

    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT COUNT(*) FROM anime_translations WHERE anime_id=%s AND external_id=%s",
            (anime_id, tr.get("id"))
        )
        result = await cur.fetchone()
        return result and result[0] == 0

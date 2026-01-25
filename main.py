import asyncio
import aiohttp
import aiomysql
import json
from datetime import datetime, timezone, date
import logging
from logging.handlers import RotatingFileHandler
import os
from dotenv import load_dotenv
import signal

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

LOG_FILE = "log.txt"
LAST_SYNC_FILE = "last_sync.txt"

def load_last_sync():
    if os.path.exists(LAST_SYNC_FILE):
        with open(LAST_SYNC_FILE, "r") as f:
            ts = f.read().strip()
            try:
                return datetime.fromisoformat(ts)
            except Exception:
                return None
    return None

def save_last_sync(dt):
    with open(LAST_SYNC_FILE, "w") as f:
        f.write(dt.isoformat())

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5, encoding="utf-8")
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def parse_datetime(s):
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None

def parse_date(s):
    if not s:
        return None
    try:
        if len(s) == 4 and s.isdigit():
            year = int(s)
            if 1880 <= year <= 2100:        
                return date(year, 1, 1)
            else:
                logger.warning(f"Некорректный год: {s}")
                return None
        return datetime.fromisoformat(s).date()
    except ValueError as e:
        logger.warning(f"Ошибка парсинга даты '{s}': {e}")
        try:
            from dateutil.parser import parse
            dt = parse(s, fuzzy=True)
            if dt.year < 1880 or dt.year > 2100:
                logger.warning(f"Некорректный год в дате: {s}")
                return None
            return dt.date()
        except (ImportError, ValueError) as e:
            logger.error(f"Не удалось распарсить дату '{s}': {e}")
            return None

CREATE_TABLES = [
"""
CREATE TABLE IF NOT EXISTS anime (
    id INT AUTO_INCREMENT PRIMARY KEY,
    kodik_id VARCHAR(100) NOT NULL UNIQUE,
    kodik_type VARCHAR(50),
    link TEXT,
    title TEXT,
    title_orig TEXT,
    other_title TEXT,
    year INT,
    last_season INT,
    last_episode INT,
    episodes_count INT,
    kinopoisk_id VARCHAR(50),
    imdb_id VARCHAR(50),
    shikimori_id VARCHAR(50),
    quality VARCHAR(100),
    camrip TINYINT(1),
    lgbt TINYINT(1),
    created_at DATETIME,
    updated_at DATETIME,
    description TEXT,
    anime_description TEXT,
    poster_url TEXT,
    anime_poster_url TEXT,
    premiere_world DATE,
    aried_at DATE,
    released_at DATE,
    rating_mpaa VARCHAR(20),
    minimal_age INT,
    episodes_total INT,
    episodes_aired INT,
    imdb_rating FLOAT,
    imdb_votes INT,
    shikimori_rating FLOAT,
    shikimori_votes FLOAT,
    next_episode_at DATETIME,
    all_status VARCHAR(50),
    anime_kind VARCHAR(50),
    duration INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
"""
CREATE TABLE IF NOT EXISTS anime_translations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    anime_id INT,
    external_id INT,
    title TEXT,
    trans_type VARCHAR(50),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
"""
CREATE TABLE IF NOT EXISTS genres (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(200) UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
"""
CREATE TABLE IF NOT EXISTS anime_genres (
    anime_id INT,
    genre_id INT,
    PRIMARY KEY (anime_id, genre_id),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
"""
CREATE TABLE IF NOT EXISTS anime_screenshots (
    id INT AUTO_INCREMENT PRIMARY KEY,
    anime_id INT,
    url TEXT,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
"""
CREATE TABLE IF NOT EXISTS persons (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
"""
CREATE TABLE IF NOT EXISTS anime_persons (
    anime_id INT,
    person_id INT,
    role VARCHAR(50),
    PRIMARY KEY (anime_id, person_id, role),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
    FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
"""
CREATE TABLE IF NOT EXISTS studios (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
"""
CREATE TABLE IF NOT EXISTS anime_studios (
    anime_id INT,
    studio_id INT,
    PRIMARY KEY (anime_id, studio_id),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
    FOREIGN KEY (studio_id) REFERENCES studios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
"""
CREATE TABLE IF NOT EXISTS blocked_countries (
    id INT AUTO_INCREMENT PRIMARY KEY,
    anime_id INT,
    country VARCHAR(100),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",
"""
CREATE TABLE IF NOT EXISTS blocked_seasons (
    id INT AUTO_INCREMENT PRIMARY KEY,
    anime_id INT,
    season VARCHAR(50),
    blocked_data JSON,
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
]

async def ensure_tables(conn):
    async with conn.cursor() as cur:
        for sql in CREATE_TABLES:
            await cur.execute(sql)
    await conn.commit()

class Cache:
    def __init__(self):
        self.genres = {}
        self.persons = {}
        self.studios = {}

    async def load(self, conn):
        async with conn.cursor() as cur:
            await cur.execute("SELECT id, name FROM genres")
            rows = await cur.fetchall()
            self.genres = {name: id for id, name in rows}

            await cur.execute("SELECT id, name FROM persons")
            rows = await cur.fetchall()
            self.persons = {name: id for id, name in rows}

            await cur.execute("SELECT id, name FROM studios")
            rows = await cur.fetchall()
            self.studios = {name: id for id, name in rows}

    async def get_or_create(self, conn, table, cache, name):
        if not name:
            return None
        if name in cache:
            return cache[name]
        async with conn.cursor() as cur:
            await cur.execute(f"INSERT IGNORE INTO {table} (name) VALUES (%s)", (name,))
            await cur.execute(f"SELECT id FROM {table} WHERE name=%s", (name,))
            row = await cur.fetchone()
            if row:
                cache[name] = row[0]
                return row[0]
        return None

async def find_existing_anime(conn, item, material):
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
            anime_id, kodik_id, updated_at = row
            logger.info(f"Найдено существующее аниме ID {anime_id} для kodik_id {kodik_id}")
            return anime_id

        return None

async def upsert_anime(conn, item, material):
    async with conn.cursor() as cur:
        kodik_id = item.get("id")
        item_updated = parse_datetime(item.get("updated_at"))

        existing_anime_id = await find_existing_anime(conn, item, material)

        if existing_anime_id:
            await cur.execute("SELECT updated_at FROM anime WHERE id=%s", (existing_anime_id,))
            row = await cur.fetchone()

            if row:
                db_updated = row[0]
                if db_updated and item_updated and item_updated <= db_updated:
                    return existing_anime_id, False, False

            values = {
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
                "shikimori_id": item.get("shikimori_id"),
                "quality": item.get("quality"),
                "camrip": 1 if item.get("camrip") else 0,
                "lgbt": 1 if item.get("lgbt") else 0,
                "created_at": parse_datetime(item.get("created_at")),
                "updated_at": item_updated,
                "description": (material or {}).get("description"),
                "anime_description": (material or {}).get("anime_description"),
                "poster_url": (material or {}).get("poster_url"),
                "anime_poster_url": (material or {}).get("anime_poster_url"),
                "premiere_world": parse_date((material or {}).get("premiere_world")),
                "aried_at": parse_date((material or {}).get("aried_at")),
                "released_at": parse_date((material or {}).get("released_at")),
                "rating_mpaa": (material or {}).get("rating_mpaa"),
                "minimal_age": (material or {}).get("minimal_age"),
                "episodes_total": (material or {}).get("episodes_total"),
                "episodes_aired": (material or {}).get("episodes_aired"),
                "imdb_rating": (material or {}).get("imdb_rating"),
                "imdb_votes": (material or {}).get("imdb_votes"),
                "shikimori_rating": (material or {}).get("shikimori_rating"),
                "shikimori_votes": (material or {}).get("shikimori_votes"),
                "next_episode_at": parse_datetime((material or {}).get("next_episode_at")),
                "all_status": (material or {}).get("all_status"),
                "anime_kind": (material or {}).get("anime_kind"),
                "duration": (material or {}).get("duration")
            }

            set_clause = ", ".join([f"{k}=%s" for k in values.keys()])
            await cur.execute(f"UPDATE anime SET {set_clause} WHERE id=%s", (*values.values(), existing_anime_id))

            await cur.execute("UPDATE anime SET kodik_id = %s WHERE id = %s", (kodik_id, existing_anime_id))

            return existing_anime_id, True, False

        else:
            await cur.execute("SELECT id, updated_at FROM anime WHERE kodik_id=%s", (kodik_id,))
            row = await cur.fetchone()

            values = {
                "kodik_id": kodik_id,
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
                "shikimori_id": item.get("shikimori_id"),
                "quality": item.get("quality"),
                "camrip": 1 if item.get("camrip") else 0,
                "lgbt": 1 if item.get("lgbt") else 0,
                "created_at": parse_datetime(item.get("created_at")),
                "updated_at": item_updated,
                "description": (material or {}).get("description"),
                "anime_description": (material or {}).get("anime_description"),
                "poster_url": (material or {}).get("poster_url"),
                "anime_poster_url": (material or {}).get("anime_poster_url"),
                "premiere_world": parse_date((material or {}).get("premiere_world")),
                "aried_at": parse_date((material or {}).get("aried_at")),
                "released_at": parse_date((material or {}).get("released_at")),
                "rating_mpaa": (material or {}).get("rating_mpaa"),
                "minimal_age": (material or {}).get("minimal_age"),
                "episodes_total": (material or {}).get("episodes_total"),
                "episodes_aired": (material or {}).get("episodes_aired"),
                "imdb_rating": (material or {}).get("imdb_rating"),
                "imdb_votes": (material or {}).get("imdb_votes"),
                "shikimori_rating": (material or {}).get("shikimori_rating"),
                "shikimori_votes": (material or {}).get("shikimori_votes"),
                "next_episode_at": parse_datetime((material or {}).get("next_episode_at")),
                "all_status": (material or {}).get("all_status"),
                "anime_kind": (material or {}).get("anime_kind"),
                "duration": (material or {}).get("duration")
            }

            if row:
                anime_id = row[0]
                set_clause = ", ".join([f"{k}=%s" for k in values.keys() if k != "kodik_id"])
                await cur.execute(f"UPDATE anime SET {set_clause} WHERE id=%s", (*[v for k, v in values.items() if k != "kodik_id"], anime_id))
                return anime_id, True, False
            else:
                cols = ", ".join(values.keys())
                placeholders = ", ".join(["%s"] * len(values))
                await cur.execute(
                    f"INSERT INTO anime ({cols}) VALUES ({placeholders})",
                    tuple(values.values())
                )
                return cur.lastrowid, True, True

async def sync_relations(conn, cache, anime_id, item, material):
    async with conn.cursor() as cur:
        await cur.execute("DELETE FROM anime_genres WHERE anime_id=%s", (anime_id,))
        await cur.execute("DELETE FROM anime_screenshots WHERE anime_id=%s", (anime_id,))
        await cur.execute("DELETE FROM anime_persons WHERE anime_id=%s", (anime_id,))
        await cur.execute("DELETE FROM anime_studios WHERE anime_id=%s", (anime_id,))
        await cur.execute("DELETE FROM blocked_countries WHERE anime_id=%s", (anime_id,))
        await cur.execute("DELETE FROM blocked_seasons WHERE anime_id=%s", (anime_id,))

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
                logger.info(f"Добавлена новая озвучка '{tr.get('title')}' для аниме ID {anime_id}")

        genres = (material or {}).get("anime_genres") or (material or {}).get("genres") or (material or {}).get("all_genres")
        if genres:
            for g in genres:
                gid = await cache.get_or_create(conn, "genres", cache.genres, g)
                if gid:
                    await cur.execute("INSERT IGNORE INTO anime_genres (anime_id, genre_id) VALUES (%s,%s)", (anime_id, gid))

        anime_screenshots = (item.get("screenshots") or []) + ((material or {}).get("screenshots") or [])
        for url in set(anime_screenshots):
            await cur.execute("INSERT INTO anime_screenshots (anime_id, url) VALUES (%s,%s)", (anime_id, url))

        mapping = {
            "actor": (material or {}).get("actors"),
            "director": (material or {}).get("directors"),
            "producer": (material or {}).get("producers"),
            "writer": (material or {}).get("writers"),
            "composer": (material or {}).get("composers"),
        }
        for role, people in mapping.items():
            if people:
                for name in people:
                    pid = await cache.get_or_create(conn, "persons", cache.persons, name)
                    if pid:
                        await cur.execute(
                            "INSERT IGNORE INTO anime_persons (anime_id, person_id, role) VALUES (%s,%s,%s)",
                            (anime_id, pid, role)
                        )

        studios = (material or {}).get("anime_studios")
        if studios:
            for name in studios:
                sid = await cache.get_or_create(conn, "studios", cache.studios, name)
                if sid:
                    await cur.execute(
                        "INSERT IGNORE INTO anime_studios (anime_id, studio_id) VALUES (%s,%s)",
                        (anime_id, sid)
                    )

        countries = item.get("blocked_countries") or []
        for country in countries:
            await cur.execute(
                "INSERT INTO blocked_countries (anime_id, country) VALUES (%s,%s)",
                (anime_id, country)
            )

        seasons = item.get("blocked_seasons") or {}
        if isinstance(seasons, dict):
            for season, data in seasons.items():
                await cur.execute(
                    "INSERT INTO blocked_seasons (anime_id, season, blocked_data) VALUES (%s,%s,%s)",
                    (anime_id, season, json.dumps(data))
                )
        elif isinstance(seasons, str) and seasons == "all":
            await cur.execute(
                "INSERT INTO blocked_seasons (anime_id, season, blocked_data) VALUES (%s,%s,%s)",
                (anime_id, "all", json.dumps("all"))
            )

async def fetch_page(session, url, retries=3):
    if not url:
        return None
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            logger.error(f"Ошибка при запросе {url}: {e} (попытка {attempt}/{retries})")
            if attempt == retries:
                raise
            await asyncio.sleep(2 ** attempt)

async def fetch_and_save(stop_event=None):
    added_count = 0
    updated_count = 0
    unchanged_count = 0

    conn = await aiomysql.connect(**DB_CONFIG)
    try:
        await ensure_tables(conn)
        cache = Cache()
        await cache.load(conn)

        last_sync = load_last_sync()
        is_first_sync = last_sync is None
        newest_update_overall = None

        session_timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=session_timeout) as session:
            page_url = BASE_URL
            page_num = 0
            count = 0

            while page_url:
                if stop_event and stop_event.is_set():
                    logger.info("Остановка fetch_and_save по stop_event.")
                    break

                try:
                    data = await fetch_page(session, page_url)
                except Exception as e:
                    logger.error("Ошибка при получении страницы: %s", e, exc_info=True)
                    break

                if not data:
                    break
                page_num += 1
                logger.info(f"=== Обработка страницы {page_num} ===")

                page_url = data.get("next_page")
                results = data.get("results", [])

                stop_fetching = False
                for item in results:
                    item_updated = parse_datetime(item.get("updated_at"))

                    if not is_first_sync and last_sync and item_updated and item_updated <= last_sync:
                        stop_fetching = True
                        break

                    material = item.get("material_data") or {}
                    anime_id, changed, added = await upsert_anime(conn, item, material)

                    new_translation_added = False
                    if not changed and not added and anime_id:
                        tr = item.get("translation")
                        if tr:
                            async with conn.cursor() as check_cur:
                                await check_cur.execute(
                                    "SELECT COUNT(*) FROM anime_translations WHERE anime_id=%s AND external_id=%s",
                                    (anime_id, tr.get("id"))
                                )
                                translation_count = await check_cur.fetchone()
                                if translation_count and translation_count[0] == 0:
                                    new_translation_added = True

                    if changed or new_translation_added:
                        await sync_relations(conn, cache, anime_id, item, material)
                        if added:
                            added_count += 1
                        elif new_translation_added:
                            updated_count += 1
                            logger.info(f"Добавлена новая озвучка к существующему аниме ID {anime_id}")
                        else:
                            updated_count += 1
                    else:
                        unchanged_count += 1

                    count += 1
                    if count % BATCH_SIZE == 0:
                        await conn.commit()
                        logger.info(f"Коммит после {count} записей. Добавлено: {added_count}, Обновлено: {updated_count}, Без изменений: {unchanged_count}")

                    if item_updated:
                        if newest_update_overall is None or item_updated > newest_update_overall:
                            newest_update_overall = item_updated

                await conn.commit()
                logger.info("Синхронизация страницы/пакета завершена.")

                if stop_fetching:
                    logger.info("Встречена старая запись, останавливаем обработку дальнейших страниц.")
                    break

        if newest_update_overall:
            save_last_sync(newest_update_overall)

        await conn.commit()
        logger.info(f"Финал: всего обработано {count} записей. Добавлено: {added_count}, Обновлено: {updated_count}, Без изменений: {unchanged_count}")
    finally:
        try:
            await conn.close()
        except Exception:
            pass

async def periodic_sync(stop_event: asyncio.Event):
    logger.info(f"Запуск периодической синхронизации (интервал: {SYNC_INTERVAL_SECONDS} секунд).")
    while not stop_event.is_set():
        logger.info("=== Начало синхронизации ===")
        try:
            await fetch_and_save(stop_event)
        except Exception as e:
            logger.error("Синхронизация завершилась с ошибкой: %s", e, exc_info=True)

        if stop_event.is_set():
            break

        logger.info(f"=== Синхронизация завершена. Ожидание {SYNC_INTERVAL_SECONDS} секунд ===")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=SYNC_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass

async def main():
    stop_event = asyncio.Event()

    async def shutdown():
        logger.info("Остановка: stop_event выставлен.")
        stop_event.set()

    try:
        await periodic_sync(stop_event)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt получен, запускаем shutdown...")
        await shutdown()
    finally:
        logger.info("Все ресурсы закрыты, выход.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем (KeyboardInterrupt).")

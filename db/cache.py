import asyncio
import aiomysql
from typing import Dict, Optional
from utils.logging_config import logger


class Cache:
    def __init__(self):
        self.genres: Dict[str, int] = {}
        self.persons: Dict[str, int] = {}
        self.studios: Dict[str, int] = {}
        self._locks: Dict[str, asyncio.Lock] = {
            "genres": asyncio.Lock(),
            "persons": asyncio.Lock(),
            "studios": asyncio.Lock(),
        }

    async def load(self, conn: aiomysql.Connection) -> None:
        logger.info("Loading cache from database...")
        async with conn.cursor() as cur:
            await cur.execute("SELECT id, name FROM genres")
            rows = await cur.fetchall()
            self.genres = {name: id for id, name in rows}
            logger.info(f"Loaded {len(self.genres)} genres")

            await cur.execute("SELECT id, name FROM persons")
            rows = await cur.fetchall()
            self.persons = {name: id for id, name in rows}
            logger.info(f"Loaded {len(self.persons)} persons")

            await cur.execute("SELECT id, name FROM studios")
            rows = await cur.fetchall()
            self.studios = {name: id for id, name in rows}
            logger.info(f"Loaded {len(self.studios)} studios")

        logger.info("Cache loaded successfully")

    async def get_or_create(
        self,
        conn: aiomysql.Connection,
        table: str,
        cache: Dict[str, int],
        name: str
    ) -> Optional[int]:
        if not name:
            return None

        if name in cache:
            return cache[name]

        async with self._locks[table]:  
            if name in cache:
                return cache[name]

            async with conn.cursor() as cur:
                await cur.execute(
                    f"INSERT IGNORE INTO {table} (name) VALUES (%s)",
                    (name,)
                )
                await cur.execute(
                    f"SELECT id FROM {table} WHERE name=%s",
                    (name,)
                )
                row = await cur.fetchone()

                if row:
                    entity_id = row[0]
                    cache[name] = entity_id
                    return entity_id

        return None

    async def get_genre_id(self, conn: aiomysql.Connection, name: str) -> Optional[int]:
        return await self.get_or_create(conn, "genres", self.genres, name)

    async def get_person_id(self, conn: aiomysql.Connection, name: str) -> Optional[int]:
        return await self.get_or_create(conn, "persons", self.persons, name)

    async def get_studio_id(self, conn: aiomysql.Connection, name: str) -> Optional[int]:
        return await self.get_or_create(conn, "studios", self.studios, name)

    async def get_genre_ids_batch(
        self,
        conn: aiomysql.Connection,
        names: list[str]
    ) -> list[int]:
        ids = []
        for name in names:
            if name:
                genre_id = await self.get_genre_id(conn, name)
                if genre_id:
                    ids.append(genre_id)
        return ids

    async def get_person_ids_batch(
        self,
        conn: aiomysql.Connection,
        names: list[str]
    ) -> list[int]:
        ids = []
        for name in names:
            if name:
                person_id = await self.get_person_id(conn, name)
                if person_id:
                    ids.append(person_id)
        return ids

    async def get_studio_ids_batch(
        self,
        conn: aiomysql.Connection,
        names: list[str]
    ) -> list[int]:
        ids = []
        for name in names:
            if name:
                studio_id = await self.get_studio_id(conn, name)
                if studio_id:
                    ids.append(studio_id)
        return ids

    def clear(self) -> None:
        self.genres.clear()
        self.persons.clear()
        self.studios.clear()
        logger.info("Cache cleared")

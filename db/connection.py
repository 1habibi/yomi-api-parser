import aiomysql
from typing import Optional
from contextlib import asynccontextmanager
from config import DB_CONFIG, DB_POOL_MIN_SIZE, DB_POOL_MAX_SIZE
from utils.logging_config import logger


class ConnectionPool:
    def __init__(self):
        self._pool: Optional[aiomysql.Pool] = None

    async def create(self) -> None:
        if self._pool is None:
            logger.info(f"Creating connection pool (min={DB_POOL_MIN_SIZE}, max={DB_POOL_MAX_SIZE})")
            self._pool = await aiomysql.create_pool(
                minsize=DB_POOL_MIN_SIZE,
                maxsize=DB_POOL_MAX_SIZE,
                **DB_CONFIG
            )
            logger.info("Connection pool created successfully")

    async def close(self) -> None:
        if self._pool:
            logger.info("Closing connection pool")
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            logger.info("Connection pool closed")

    @asynccontextmanager
    async def acquire(self):
        if self._pool is None:
            await self.create()

        async with self._pool.acquire() as conn:
            yield conn

    @property
    def size(self) -> int:
        if self._pool:
            return self._pool.size
        return 0

    @property
    def freesize(self) -> int:
        if self._pool:
            return self._pool.freesize
        return 0

pool = ConnectionPool()

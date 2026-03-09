import asyncpg
from typing import Optional
from config.credentials import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
)


class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.pool.Pool] = None

    async def connect(self):
        """
        Create connection pool
        """
        self.pool = await asyncpg.create_pool(
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            host=DB_HOST,
            port=int(DB_PORT),
            min_size=2,
            max_size=10,
        )

        print("✅ Connected to PostgreSQL")

    async def close(self):
        """
        Close pool
        """
        if self.pool:
            await self.pool.close()
            print("❌ Database connection closed")

    async def execute(self, query: str, *args):
        async with self.pool.acquire() as connection:
            return await connection.execute(query, *args)

    async def fetch(self, query: str, *args):
        async with self.pool.acquire() as connection:
            return await connection.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(query, *args)


# Singleton instance
db = Database()

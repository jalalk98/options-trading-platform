import asyncio
from backend.db.db import db


async def test_connection():
    await db.connect()

    result = await db.fetch("SELECT 1;")
    print("Test query result:", result)

    await db.close()


if __name__ == "__main__":
    asyncio.run(test_connection())

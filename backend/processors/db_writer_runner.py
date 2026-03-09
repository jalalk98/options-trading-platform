import asyncio
from backend.services.db_writer import db_writer

asyncio.run(db_writer())
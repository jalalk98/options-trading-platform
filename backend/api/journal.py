# backend/api/journal.py  (laptop/DuckDB branch)
# Read-only GET /api/journal — reads from trade_journal/latest.parquet via DuckDB.
# POST /sync, PATCH, DELETE are no-ops (data is managed on the server).

from typing import Optional
from fastapi import APIRouter
from backend.services import duckdb_adapter as _duckdb

router = APIRouter()


@router.get("/journal")
async def get_journal(
    date:   Optional[str] = None,
    symbol: Optional[str] = None,
):
    from datetime import datetime, timezone, timedelta
    date_str = date or datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d")
    return await _duckdb.query_trade_journal(date_str, symbol)


@router.post("/journal/sync")
async def sync_journal():
    return {"synced": 0, "message": "Sync not available in laptop mode — run on server."}


@router.patch("/journal/{entry_id}")
async def update_journal(entry_id: int):
    return {"message": "Edit not available in laptop mode."}


@router.delete("/journal/{entry_id}")
async def delete_journal(entry_id: int):
    return {"message": "Delete not available in laptop mode."}

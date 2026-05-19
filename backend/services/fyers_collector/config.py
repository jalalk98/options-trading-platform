"""
Fyers collector configuration.
Reads from .env.fyers at the project root (git-ignored).
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Project root / .env.fyers
_ENV_FILE = Path(__file__).resolve().parents[3] / ".env.fyers"
load_dotenv(_ENV_FILE, override=False)

FYERS_APP_ID       = os.getenv("FYERS_APP_ID", "")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN", "")

FYERS_REDIS_STREAM = os.getenv("FYERS_REDIS_STREAM", "fyers_ticks_stream")
FYERS_REDIS_GROUP  = os.getenv("FYERS_REDIS_GROUP",  "fyers_writer_group")
FYERS_DB_TABLE     = os.getenv("FYERS_DB_TABLE",     "fyers_ticks")
FYERS_NUM_STRIKES  = int(os.getenv("FYERS_NUM_STRIKES", "8"))

# Streamer consumer group (separate from writer group — both read the same stream)
FYERS_STREAMER_GROUP    = "fyers_streamer_group"
FYERS_STREAMER_CONSUMER = "fyers_streamer_1"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

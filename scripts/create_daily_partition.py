#!/usr/bin/env python3
"""
Create gap_ticks daily partitions.

Strategy:
  - Ensures 7 days of partitions exist going forward
  - Idempotent — safe to run any time
  - Sends Telegram alert on success, failure, or when new partitions created
  - If cron misses a day, next run catches up

Cron entry (runs daily at 6 PM):
  0 18 * * * cd ~/projects/options-trading-platform && \\
    ./venv/bin/python3 scripts/create_daily_partition.py \\
    >> ~/partition_cron.log 2>&1
"""
import asyncio
import asyncpg
import sys
from datetime import date, timedelta, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.credentials import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)

DAYS_AHEAD = 7


def read_secrets():
    secrets = {}
    path = Path.home() / '.kite_secrets'
    for line in path.read_text().splitlines():
        if '=' in line and not line.startswith('#'):
            k, _, v = line.partition('=')
            secrets[k.strip()] = v.strip()
    return secrets


def send_telegram(text):
    """Send Telegram message — silent failure if creds missing or net down."""
    try:
        import requests
        secrets = read_secrets()
        token = secrets.get('TELEGRAM_BOT_TOKEN')
        chat_id = secrets.get('TELEGRAM_CHAT_ID')
        if not token or not chat_id:
            print(f"[telegram] No creds — skipping alert")
            return
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": text},
            timeout=10,
        )
    except Exception as e:
        print(f"[telegram] Alert failed: {e}")


async def ensure_partition(conn, target_date: date):
    partition_name = f"gap_ticks_{target_date.strftime('%Y_%m_%d')}"
    next_date = target_date + timedelta(days=1)

    exists = await conn.fetchval(f"""
        SELECT EXISTS (
            SELECT 1 FROM pg_class
            WHERE relname = '{partition_name}'
        )
    """)

    if exists:
        return False

    await conn.execute(f"""
        CREATE TABLE {partition_name}
        PARTITION OF gap_ticks
        FOR VALUES FROM ('{target_date}') TO ('{next_date}')
    """)

    await conn.execute(f"""
        CREATE UNIQUE INDEX {partition_name}_stream_id_idx
        ON {partition_name} (stream_id)
        WHERE stream_id IS NOT NULL
    """)

    return True


async def main():
    now_str = datetime.now().isoformat(timespec='seconds')
    print(f"[{now_str}] Partition cron starting...")

    try:
        conn = await asyncpg.connect(
            host=DB_HOST, port=int(DB_PORT),
            user=DB_USER, password=DB_PASSWORD,
            database=DB_NAME,
        )
    except Exception as e:
        err_msg = f"❌ Partition cron FAILED to connect to DB: {e}"
        print(err_msg)
        send_telegram(err_msg)
        sys.exit(1)

    try:
        today = date.today()
        created_dates = []

        for offset in range(DAYS_AHEAD):
            target = today + timedelta(days=offset)
            try:
                if await ensure_partition(conn, target):
                    created_dates.append(target.isoformat())
            except Exception as e:
                err_msg = (
                    f"❌ Partition cron FAILED\n"
                    f"Could not create partition for {target}\n"
                    f"Error: {e}"
                )
                print(err_msg)
                send_telegram(err_msg)
                sys.exit(1)

        # Count total partitions for the alert
        total = await conn.fetchval("""
            SELECT COUNT(*)
            FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            WHERE parent.relname = 'gap_ticks'
        """)

        if created_dates:
            msg = (
                f"✅ Partition cron — {len(created_dates)} new partition(s) created\n"
                f"Dates: {', '.join(created_dates)}\n"
                f"Total partitions now: {total}\n"
                f"Buffer: {DAYS_AHEAD} days ahead"
            )
            print(msg)
            send_telegram(msg)
        else:
            # Silent success — don't spam Telegram every day
            msg = (
                f"✓ Partition cron — all {DAYS_AHEAD} days already have partitions "
                f"(total: {total})"
            )
            print(msg)
            # No telegram for silent success

    except Exception as e:
        err_msg = f"❌ Partition cron FAILED: {e}"
        print(err_msg)
        send_telegram(err_msg)
        sys.exit(1)

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())

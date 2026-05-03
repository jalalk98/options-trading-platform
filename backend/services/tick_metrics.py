"""
Tick rate metrics for monitoring KiteConnect feed health.

Tracks ticks-per-minute per index group. Logs every 60s to a
file that scripts/check_tick_rate.sh can read for alerting.

Why: On Apr 21, 22, 24, KiteConnect's WebSocket stayed connected
but the tick stream was severely throttled (Zerodha-side issue
during NSE expiry weeks). No on_close fired, so no alert. We
need rate-based detection.
"""
import threading
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

LOG_FILE = Path.home() / 'tick_rate.log'
FLUSH_INTERVAL_SEC = 60  # log every minute


# ── Symbol classification ─────────────────────────────────
def _classify(symbol: str) -> str:
    """Map a Kite tradingsymbol to an index group."""
    if not symbol:
        return 'OTHER'
    s = symbol.upper()
    if s in ('NIFTY 50', 'NIFTY'):
        return 'NIFTY_INDEX'
    if s in ('NIFTY BANK', 'BANKNIFTY'):
        return 'BANKNIFTY_INDEX'
    if s in ('SENSEX', 'BSE SENSEX'):
        return 'SENSEX_INDEX'
    if s in ('NIFTY FIN SERVICE', 'FINNIFTY'):
        return 'FINNIFTY_INDEX'
    if s in ('NIFTY MIDCAP SELECT', 'NIFTY MID SELECT', 'MIDCPNIFTY'):
        return 'MIDCPNIFTY_INDEX'
    if s.startswith('BANKNIFTY'):
        return 'BANKNIFTY'
    if s.startswith('MIDCPNIFTY'):
        return 'MIDCPNIFTY'
    if s.startswith('FINNIFTY'):
        return 'FINNIFTY'
    if s.startswith('SENSEX'):
        return 'SENSEX'
    if s.startswith('NIFTY'):
        return 'NIFTY'
    return 'OTHER'


# ── Counter state ─────────────────────────────────────────
_lock = threading.Lock()
_counts: dict = defaultdict(int)
_total = 0
_started = False


def record(tick_count: int = 1, symbol: str = None) -> None:
    """Record N ticks (default 1). If symbol provided, classify it."""
    global _total
    with _lock:
        _total += tick_count
        if symbol:
            _counts[_classify(symbol)] += tick_count


def record_batch(ticks: list) -> None:
    """Record a batch of raw KiteConnect tick dicts."""
    if not ticks:
        return
    global _total
    with _lock:
        for t in ticks:
            sym = t.get('tradingsymbol') or t.get('symbol') or ''
            _counts[_classify(sym)] += 1
            _total += 1


def _flush_loop() -> None:
    """Background thread: every 60s, write counts to log and reset."""
    global _total
    while True:
        time.sleep(FLUSH_INTERVAL_SEC)
        with _lock:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            parts = [f"total={_total}"]
            for grp in sorted(_counts.keys()):
                parts.append(f"{grp}={_counts[grp]}")
            line = f"{now} | " + " | ".join(parts)
            try:
                with open(LOG_FILE, 'a') as f:
                    f.write(line + '\n')
            except Exception as e:
                print(f"[tick_metrics] log write failed: {e}")
            _counts.clear()
            _total = 0


def start() -> None:
    """Start the background flush thread (idempotent)."""
    global _started
    with _lock:
        if _started:
            return
        _started = True
    t = threading.Thread(target=_flush_loop, daemon=True, name='tick_metrics')
    t.start()
    print(f"[tick_metrics] started, logging to {LOG_FILE} every {FLUSH_INTERVAL_SEC}s")

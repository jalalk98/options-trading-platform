#!/usr/bin/env python3
"""
Place a 1-qty SBIN regular limit-buy at 5% below LTP (within circuit limits, won't fill),
cancel it immediately, and print a JSON result.

Used by pre_session_check.sh to verify Kite API order connectivity.
Exit codes: 0 = placed + cancelled OK, 1 = any failure.
"""
import json
import sys
import os
import math

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN
from kiteconnect import KiteConnect
from kiteconnect.exceptions import KiteException

result = {
    "placed": False,
    "order_id": None,
    "cancelled": False,
    "ltp": None,
    "test_price": None,
    "error": None,
}

if not KITE_API_KEY or not KITE_ACCESS_TOKEN:
    result["error"] = "KITE_API_KEY or KITE_ACCESS_TOKEN not set in env"
    print(json.dumps(result))
    sys.exit(1)

try:
    kite = KiteConnect(api_key=KITE_API_KEY)
    kite.set_access_token(KITE_ACCESS_TOKEN)

    ltp_data = kite.ltp(["NSE:SBIN"])
    ltp = ltp_data["NSE:SBIN"]["last_price"]
    # Place 5% below LTP — within circuit limits but won't fill
    test_price = math.floor(ltp * 0.95 * 20) / 20  # round down to nearest 0.05 tick
    result["ltp"] = ltp
    result["test_price"] = test_price

    order_id = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=kite.EXCHANGE_NSE,
        tradingsymbol="SBIN",
        transaction_type=kite.TRANSACTION_TYPE_BUY,
        quantity=1,
        product=kite.PRODUCT_CNC,
        order_type=kite.ORDER_TYPE_LIMIT,
        price=test_price,
    )
    result["placed"] = True
    result["order_id"] = order_id

except KiteException as e:
    result["error"] = f"ltp_or_place_order failed: {e}"
    print(json.dumps(result))
    sys.exit(1)
except Exception as e:
    result["error"] = f"place_order unexpected error: {e}"
    print(json.dumps(result))
    sys.exit(1)

try:
    kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=order_id)
    result["cancelled"] = True
except Exception as e:
    result["error"] = f"cancel_order failed (order {order_id} may still be open!): {e}"
    print(json.dumps(result))
    sys.exit(1)

print(json.dumps(result))
sys.exit(0)

#!/usr/bin/env python3
"""
Test Kite order API connectivity using a real Nifty option contract.
  1. Fetch Nifty 50 LTP → compute ATM strike (nearest 50)
  2. Find the nearest-expiry NIFTY CE for that strike in NFO instruments
  3. Place 1-lot NRML limit buy at ₹0.05 (far below market — will never fill)
  4. Cancel immediately

Exit codes: 0 = placed + cancelled OK, 1 = any failure.
"""
import json
import sys
import os
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN
from kiteconnect import KiteConnect
from kiteconnect.exceptions import KiteException

result = {
    "placed": False,
    "order_id": None,
    "cancelled": False,
    "nifty_ltp": None,
    "atm_strike": None,
    "symbol": None,
    "expiry": None,
    "lot_size": None,
    "option_ltp": None,
    "test_price": None,
    "error": None,
}

if not KITE_API_KEY or not KITE_ACCESS_TOKEN:
    result["error"] = "KITE_API_KEY or KITE_ACCESS_TOKEN not set"
    print(json.dumps(result))
    sys.exit(1)

try:
    kite = KiteConnect(api_key=KITE_API_KEY)
    kite.set_access_token(KITE_ACCESS_TOKEN)

    # Step 1: Nifty 50 LTP → ATM strike
    ltp_data = kite.ltp(["NSE:NIFTY 50"])
    nifty_ltp = ltp_data["NSE:NIFTY 50"]["last_price"]
    atm_strike = round(nifty_ltp / 50) * 50
    result["nifty_ltp"] = nifty_ltp
    result["atm_strike"] = atm_strike

    # Step 2: Nearest-expiry NIFTY CE at ATM strike
    instruments = kite.instruments("NFO")
    today = date.today()
    candidates = [
        i for i in instruments
        if i["name"] == "NIFTY"
        and i["instrument_type"] == "CE"
        and i["expiry"] >= today
        and i["strike"] == atm_strike
    ]
    if not candidates:
        result["error"] = f"No NIFTY CE found for strike {atm_strike} with expiry >= {today}"
        print(json.dumps(result))
        sys.exit(1)

    candidates.sort(key=lambda x: x["expiry"])
    instrument = candidates[0]
    symbol   = instrument["tradingsymbol"]
    lot_size = instrument["lot_size"]
    result["symbol"]   = symbol
    result["expiry"]   = str(instrument["expiry"])
    result["lot_size"] = lot_size

    # Step 3: Fetch option LTP → place 50% below (within circuit limits, won't fill)
    opt_ltp_data = kite.ltp([f"NFO:{symbol}"])
    option_ltp = opt_ltp_data[f"NFO:{symbol}"]["last_price"]
    if not option_ltp:
        result["error"] = f"Option LTP unavailable for {symbol} — cannot compute safe test price"
        print(json.dumps(result))
        sys.exit(1)
    # Round down to nearest 0.05 tick
    test_price = max(0.05, int(option_ltp * 0.8 / 0.05) * 0.05)
    result["option_ltp"] = option_ltp
    result["test_price"] = test_price

    order_id = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=kite.EXCHANGE_NFO,
        tradingsymbol=symbol,
        transaction_type=kite.TRANSACTION_TYPE_BUY,
        quantity=lot_size,
        product=kite.PRODUCT_NRML,
        order_type=kite.ORDER_TYPE_LIMIT,
        price=test_price,
    )
    result["placed"]   = True
    result["order_id"] = order_id

except KiteException as e:
    result["error"] = f"API error: {e}"
    print(json.dumps(result))
    sys.exit(1)
except Exception as e:
    result["error"] = f"Unexpected error: {e}"
    print(json.dumps(result))
    sys.exit(1)

# Step 4: Cancel immediately
try:
    kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=order_id)
    result["cancelled"] = True
except Exception as e:
    result["error"] = f"cancel_order failed (order {order_id} may still be open!): {e}"
    print(json.dumps(result))
    sys.exit(1)

print(json.dumps(result))
sys.exit(0)

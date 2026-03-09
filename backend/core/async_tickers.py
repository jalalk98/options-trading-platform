# async_ticker_ranjanak.py
import asyncio
import json
import logging
import ssl
import struct
import threading
from datetime import datetime
import six
import websockets



class MainTicker:
    EXCHANGE_MAP = {
        "nse": 1,
        "nfo": 2,
        "cds": 3,
        "bse": 4,
        "bfo": 5,
        "bsecds": 6,
        "mcx": 7,
        "mcxsx": 8,
        "indices": 9
    }

    CONNECT_TIMEOUT = 30
    RECONNECT_MAX_DELAY = 60
    RECONNECT_MAX_TRIES = 50
    ROOT_URI = "wss://ws.kite.trade"

    MODE_FULL = "full"
    MODE_QUOTE = "quote"
    MODE_LTP = "ltp"

    _message_code = 11
    _message_subscribe = "subscribe"
    _message_unsubscribe = "unsubscribe"
    _message_setmode = "mode"

    _minimum_reconnect_max_delay = 5
    _maximum_reconnect_max_tries = 300

    def __init__(self, api_key, access_token, loop,  kite1, token_to_OrderId_Mod_Dic, kws,
                 debug=False, root=None, reconnect=True, reconnect_max_tries=RECONNECT_MAX_TRIES, 
                 reconnect_max_delay=RECONNECT_MAX_DELAY, connect_timeout=CONNECT_TIMEOUT, on_reconnect_callback=None):
        
        self.api_key = api_key
        self.access_token = access_token
        self.ws_url = f"wss://ws.kite.trade?api_key={api_key}&access_token={access_token}"

        self.debug = debug
        self.ws = None

        self.on_ticks = None
        self.on_connect = None
        self.on_message = None
        self.on_close = None
        self.on_error = None
        self.on_reconnect = None
        self.on_noreconnect = None
        self.websocket_thread = None
        self.on_reconnect_callback = on_reconnect_callback

        self.on_order_update = None

        self.subscribed_tokens = {}

        self.reconnect = reconnect
        self.reconnect_max_tries = reconnect_max_tries
        self.reconnect_max_delay = reconnect_max_delay
        self.reconnect_attempts = 0

        self.loop = loop  # Use the event loop passed from the main application
        self.token_to_OrderId_Mod_Dic = token_to_OrderId_Mod_Dic
        self.kite1 = kite1
        self.kws = self  # Set self-reference

        self.reconnect_lock = asyncio.Lock()  # Add a lock for controlling reconnects
        self.reconnect_in_progress = False  # To track if reconnect is active
        self.first_connect = True  # Track if it's the first connection

    def _user_agent(self):
        return "KiteTicker-Python/3.0"

    async def _connect(self):
        logging.info("Connecting to WebSocket...")
        try:
            self.ws = await websockets.connect(self.ws_url, extra_headers={"User-Agent": self._user_agent()})
            self.ws.ws_instance = self  # Set the MainTicker instance on the WebSocket object
            self.reconnect_attempts = 0  # Reset reconnect attempts on successful connection
            logging.info("Connected to WebSocket.")

            # Call _on_connect callback
            await self._on_connect()

            # Start _listen in a background task to prevent blocking
            asyncio.create_task(self._listen())

        except websockets.InvalidStatusCode as e:
            if e.status_code == 403:
                logging.error("Connection rejected with HTTP 403 Forbidden")
                await self._close(code=1003, reason="HTTP 403 Forbidden")
                return
            logging.error(f"Invalid status code: {e.status_code}")
            if self.reconnect:
                await self._reconnect()
        except Exception as e:
            logging.error(f"Error during WebSocket connection: {str(e)}")
            if self.reconnect:
                await self._reconnect()

    
    async def _reconnect(self):
        async with self.reconnect_lock:
            if self.reconnect_in_progress:
                logging.info("Reconnect already in progress, skipping duplicate attempt.")
                return
            self.reconnect_in_progress = True

            try:
                self.reconnect_attempts += 1

                if self.reconnect_attempts > self.reconnect_max_tries:
                    logging.error("Maximum reconnect attempts reached. Not attempting further.")
                    await self._on_noreconnect()
                    return

                delay = min(self.reconnect_max_delay, self.reconnect_attempts)
                logging.info(f"Reconnecting in {delay} seconds...")
                await asyncio.sleep(delay)

                logging.info("Starting reconnection process.")
                await self._connect()

                # Resubscribe to tokens after reconnection
                if self.subscribed_tokens:
                    logging.info("Resubscribing tokens after reconnection.")
                    await self.resubscribe()

                # Trigger _on_reconnect safely
                if self._on_reconnect:
                    try:
                        logging.info("Triggering _on_reconnect.")
                        await self._on_reconnect(self.reconnect_attempts)
                    except Exception as e:
                        logging.error(f"Error in _on_reconnect: {str(e)}")

                # Trigger custom on_reconnect_callback if defined
                if callable(self.on_reconnect_callback):
                    logging.info("Triggering custom on_reconnect_callback.")
                    await self.on_reconnect_callback()
                else:
                    logging.warning("on_reconnect_callback is not callable or not set.")

            except Exception as e:
                logging.error(f"Reconnection failed: {str(e)}")
                await self._reconnect()

            finally:
                self.reconnect_in_progress = False


    def connect(self, threaded=False, proxy=None):
        if threaded:
            self.websocket_thread = threading.Thread(target=self._connect_thread)
            self.websocket_thread.daemon = True
            self.websocket_thread.start()
        else:
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._connect())

    def _connect_thread(self):
        asyncio.run_coroutine_threadsafe(self._connect(), self.loop)


    async def _listen(self):
        logging.info("Listening for messages...")
        try:
            async for message in self.ws:
                is_binary = isinstance(message, bytes)
                await self._on_message(message, is_binary)
        except websockets.ConnectionClosed as e:
            logging.error(f"Connection closed: {e.code} - {e.reason}")
            
            # If an unexpected close, attempt to cancel orders first
            if e.code == 1006:  # Code 1006 indicates abnormal closure
                logging.info("Triggering from listen")
                await self.handle_unexpected_close()
            
            # Call the on_close callback if defined
            if self.on_close:
                await self.on_close(self.ws, e.code, e.reason)

            # Attempt to reconnect if allowed
            if self.reconnect:
                await self._reconnect()
        except Exception as e:
            logging.error(f"Error: {e}")
            await self._on_error(self.ws, type(e).__name__, str(e))
            if self.reconnect:
                await self._reconnect()

    async def _on_connect(self):
        if self.on_connect:
            await self.on_connect(self.ws)

    async def _on_message(self, message, is_binary):
        if self.on_message:
            await self.on_message(self.ws, message, is_binary)

        if self.on_ticks and is_binary and len(message) > 4:
            ticks = self._parse_binary(message)
            await self.on_ticks(self.ws, ticks)

        if not is_binary:
            await self._parse_text_message(message)

    async def _on_error(self, ws, code, reason):
        logging.error(f"Connection error: {code} - {reason}")
        if self.on_error:
            await self.on_error(self.ws, code, reason)

    def _on_reconnect(self, attempts_count):
        if self.on_reconnect:
            return self.on_reconnect(self, attempts_count)

    def _on_noreconnect(self):
        if self.on_noreconnect:
            return self.on_noreconnect(self)

    async def _parse_text_message(self, payload):
        if not six.PY2 and isinstance(payload, bytes):
            payload = payload.decode("utf-8")

        try:
            data = json.loads(payload)
        except ValueError:
            logging.error(f"Invalid JSON received: {payload}")
            return "Invalid JSON received"

        if self.on_order_update and data.get("type") == "order" and data.get("data"):
            await self.on_order_update(self, data["data"])
        elif data.get("type") == "error":
            await self._on_error(self.ws, 0, data.get("data"))
        else:
            logging.debug(f"Unhandled message type: {data}")
            return f"Unhandled message type: {data}"
    
    async def subscribe(self, token_list):
        """Subscribes to a list of tokens with retry logic."""
        try:
            message = json.dumps({"a": self._message_subscribe, "v": token_list})
            await asyncio.wait_for(self.ws.send(message), timeout=2)
            logging.info(f"Subscribing to tokens: {token_list}")

            for token in token_list:
                self.subscribed_tokens[token] = self.MODE_QUOTE
            return True

        except (asyncio.TimeoutError, Exception) as e:
            logging.error(f"First subscribe attempt failed: {e}")
            try:
                await asyncio.wait_for(self.ws.send(message), timeout=2)
                logging.info(f"Retrying to subscribe to tokens: {token_list}")

                for token in token_list:
                    self.subscribed_tokens[token] = self.MODE_QUOTE
                return True

            except (asyncio.TimeoutError, Exception) as retry_exception:
                logging.error(f"Retry subscribe failed: {retry_exception}")
                await self.handle_unexpected_close()
                await self._close(reason="Error during subscribe and after retry.")
                raise


    async def unsubscribe(self, token_list):
        try:
            # Create unsubscribe message
            message = json.dumps({"a": self._message_unsubscribe, "v": token_list})
            
            # First attempt to send the unsubscribe message with a 2-second timeout
            await asyncio.wait_for(self.ws.send(message), timeout=2)
            logging.info(f"Unsubscribing from tokens: {token_list}")

            # Remove tokens from subscribed list
            for token in token_list:
                self.subscribed_tokens.pop(token, None)

            return True

        except (asyncio.TimeoutError, Exception) as e:
            logging.error(f"First unsubscribe attempt failed: {e}")
            
            # Retry unsubscribing once more with a timeout
            try:
                await asyncio.wait_for(self.ws.send(message), timeout=2)
                logging.info(f"Retrying to unsubscribe from tokens: {token_list}")

                # Remove tokens from subscribed list after retry
                for token in token_list:
                    self.subscribed_tokens.pop(token, None)

                return True
            
            except (asyncio.TimeoutError, Exception) as retry_exception:
                logging.error(f"Retry unsubscribe failed: {retry_exception}")

                # Ensure all open orders are canceled before closing the connection
                await self.handle_unexpected_close()
                
                # Close the connection after ensuring orders are canceled
                await self._close(reason="Error during unsubscribe and after retry.")

                # Raise the exception to indicate failure
                raise

    async def set_mode(self, mode, token_list):
        try:
            message = json.dumps({"a": self._message_setmode, "v": [mode, token_list]})
            await self.ws.send(message)
            logging.debug(f"Setting mode {mode} for tokens: {token_list}")

            for token in token_list:
                self.subscribed_tokens[token] = mode

            return True
        except Exception as e:
            await self._close(reason=f"Error while setting mode: {str(e)}")
            raise

    async def resubscribe(self):
        """Handles resubscription to the tokens currently in subscribed_tokens."""
        if self.subscribed_tokens:
            tokens = list(self.subscribed_tokens.keys())
            logging.info(f"Resubscribing to tokens: {tokens}")
            await self.subscribe(tokens)


    async def _close(self, code=None, reason=None):
        logging.debug(f"Closing WebSocket connection with code: {code}, reason: {reason}")
        if code is None:
            code = 1000  # Normal closure
        if reason is None:
            reason = "Normal closure"

        # Call handle_unexpected_close if closing due to an abnormal code (1006)
        if code == 1006:
            await self.handle_unexpected_close()

        # Close the WebSocket if open
        if self.ws:
            await self.ws.close(code, reason)
            self.ws = None
        
        logging.error(f"Connection closed due to: {reason}")

    async def close(self, code=None, reason=None):
        self.stop_retry()
        await self._close(code, reason)

    def stop_retry(self):
        self.reconnect = False

    async def handle_unexpected_close(self):
        pass
        """
        Handle unexpected WebSocket closure by canceling all open orders
        before attempting a reconnection or final closure.
        """

    def _parse_binary(self, bin):
        packets = self._split_packets(bin)
        data = []

        for packet in packets:
            instrument_token = self._unpack_int(packet, 0, 4)
            segment = instrument_token & 0xff

            if segment == self.EXCHANGE_MAP["cds"]:
                divisor = 10000000.0
            elif segment == self.EXCHANGE_MAP["bsecds"]:
                divisor = 10000.0
            else:
                divisor = 100.0

            tradable = False if segment == self.EXCHANGE_MAP["indices"] else True

            if len(packet) == 8:
                data.append({
                    "tradable": tradable,
                    "mode": self.MODE_LTP,
                    "instrument_token": instrument_token,
                    "last_price": self._unpack_int(packet, 4, 8) / divisor
                })
            elif len(packet) == 28 or len(packet) == 32:
                mode = self.MODE_QUOTE if len(packet) == 28 else self.MODE_FULL

                d = {
                    "tradable": tradable,
                    "mode": mode,
                    "instrument_token": instrument_token,
                    "last_price": self._unpack_int(packet, 4, 8) / divisor,
                    "ohlc": {
                        "high": self._unpack_int(packet, 8, 12) / divisor,
                        "low": self._unpack_int(packet, 12, 16) / divisor,
                        "open": self._unpack_int(packet, 16, 20) / divisor,
                        "close": self._unpack_int(packet, 20, 24) / divisor
                    }
                }

                d["change"] = 0
                if d["ohlc"]["close"] != 0:
                    d["change"] = (d["last_price"] - d["ohlc"]["close"]) * 100 / d["ohlc"]["close"]

                if len(packet) == 32:
                    try:
                        timestamp = datetime.fromtimestamp(self._unpack_int(packet, 28, 32))
                    except Exception:
                        timestamp = None

                    d["exchange_timestamp"] = timestamp

                data.append(d)
            elif len(packet) == 44 or len(packet) == 184:
                mode = self.MODE_QUOTE if len(packet) == 44 else self.MODE_FULL

                d = {
                    "tradable": tradable,
                    "mode": mode,
                    "instrument_token": instrument_token,
                    "last_price": self._unpack_int(packet, 4, 8) / divisor,
                    "last_traded_quantity": self._unpack_int(packet, 8, 12),
                    "average_traded_price": self._unpack_int(packet, 12, 16) / divisor,
                    "volume_traded": self._unpack_int(packet, 16, 20),
                    "total_buy_quantity": self._unpack_int(packet, 20, 24),
                    "total_sell_quantity": self._unpack_int(packet, 24, 28),
                    "ohlc": {
                        "open": self._unpack_int(packet, 28, 32) / divisor,
                        "high": self._unpack_int(packet, 32, 36) / divisor,
                        "low": self._unpack_int(packet, 36, 40) / divisor,
                        "close": self._unpack_int(packet, 40, 44) / divisor
                    }
                }

                d["change"] = 0
                if d["ohlc"]["close"] != 0:
                    d["change"] = (d["last_price"] - d["ohlc"]["close"]) * 100 / d["ohlc"]["close"]

                if len(packet) == 184:
                    try:
                        last_trade_time = datetime.fromtimestamp(self._unpack_int(packet, 44, 48))
                    except Exception:
                        last_trade_time = None

                    try:
                        timestamp = datetime.fromtimestamp(self._unpack_int(packet, 60, 64))
                    except Exception:
                        timestamp = None

                    d["last_trade_time"] = last_trade_time
                    d["oi"] = self._unpack_int(packet, 48, 52)
                    d["oi_day_high"] = self._unpack_int(packet, 52, 56)
                    d["oi_day_low"] = self._unpack_int(packet, 56, 60)
                    d["exchange_timestamp"] = timestamp

                    depth = {
                        "buy": [],
                        "sell": []
                    }

                    for i, p in enumerate(range(64, len(packet), 12)):
                        depth["sell" if i >= 5 else "buy"].append({
                            "quantity": self._unpack_int(packet, p, p + 4),
                            "price": self._unpack_int(packet, p + 4, p + 8) / divisor,
                            "orders": self._unpack_int(packet, p + 8, p + 10, byte_format="H")
                        })

                    d["depth"] = depth

                data.append(d)

        return data

    def _unpack_int(self, bin, start, end, byte_format="I"):
        return struct.unpack(">" + byte_format, bin[start:end])[0]

    def _split_packets(self, bin):
        if len(bin) < 2:
            return []

        number_of_packets = self._unpack_int(bin, 0, 2, byte_format="H")
        packets = []

        j = 2
        for i in range(number_of_packets):
            packet_length = self._unpack_int(bin, j, j + 2, byte_format="H")
            packets.append(bin[j + 2: j + 2 + packet_length])
            j = j + 2 + packet_length

        return packets

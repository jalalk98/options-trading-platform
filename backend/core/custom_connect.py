# -*- coding: utf-8 -*-
"""
    custom_connect.py

    API wrapper for Kite Connect REST APIs.

    :copyright: (c) 2021 by Zerodha Technology.
    :license: see LICENSE for details.
"""
from six import StringIO, PY2
from urllib.parse import urljoin
import logging
import datetime
import httpx
from datetime import datetime, timedelta
import pytz
import time
import asyncio
from config.logging_config import logger  # Import the configured logger
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN


api_key = KITE_API_KEY
access_token = KITE_ACCESS_TOKEN

# from __version__ import __version__, __title__
import kiteconnect.exceptions as ex

log = logging.getLogger(__name__)


class KiteConnect_custom(object):
    """
    The Kite Connect API wrapper class.

    In production, you may initialise a single instance of this class per `api_key`.
    """

    # Default root API endpoint. It's possible to
    # override this by passing the `root` parameter during initialisation.
    _default_root_uri = "https://api.kite.trade"
    _default_login_uri = "https://kite.zerodha.com/connect/login"
    _default_timeout = 7  # In seconds

    # Kite connect header version
    kite_header_version = "3"

    # Constants
    # Products
    PRODUCT_MIS = "MIS"
    PRODUCT_CNC = "CNC"
    PRODUCT_NRML = "NRML"
    PRODUCT_CO = "CO"

    # Order types
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_LIMIT = "LIMIT"
    ORDER_TYPE_SLM = "SL-M"
    ORDER_TYPE_SL = "SL"

    # Varities
    VARIETY_REGULAR = "regular"
    VARIETY_CO = "co"
    VARIETY_AMO = "amo"
    VARIETY_ICEBERG = "iceberg"
    VARIETY_AUCTION = "auction"

    # Transaction type
    TRANSACTION_TYPE_BUY = "BUY"
    TRANSACTION_TYPE_SELL = "SELL"

    # Validity
    VALIDITY_DAY = "DAY"
    VALIDITY_IOC = "IOC"
    VALIDITY_TTL = "TTL"

    # Position Type
    POSITION_TYPE_DAY = "day"
    POSITION_TYPE_OVERNIGHT = "overnight"

    # Exchanges
    EXCHANGE_NSE = "NSE"
    EXCHANGE_BSE = "BSE"
    EXCHANGE_NFO = "NFO"
    EXCHANGE_CDS = "CDS"
    EXCHANGE_BFO = "BFO"
    EXCHANGE_MCX = "MCX"
    EXCHANGE_BCD = "BCD"

    # Margins segments
    MARGIN_EQUITY = "equity"
    MARGIN_COMMODITY = "commodity"

    # Status constants
    STATUS_COMPLETE = "COMPLETE"
    STATUS_REJECTED = "REJECTED"
    STATUS_CANCELLED = "CANCELLED"

    # GTT order type
    GTT_TYPE_OCO = "two-leg"
    GTT_TYPE_SINGLE = "single"

    # GTT order status
    GTT_STATUS_ACTIVE = "active"
    GTT_STATUS_TRIGGERED = "triggered"
    GTT_STATUS_DISABLED = "disabled"
    GTT_STATUS_EXPIRED = "expired"
    GTT_STATUS_CANCELLED = "cancelled"
    GTT_STATUS_REJECTED = "rejected"
    GTT_STATUS_DELETED = "deleted"

    # URIs to various calls
    _routes = {
        "api.token": "/session/token",
        "api.token.invalidate": "/session/token",
        "api.token.renew": "/session/refresh_token",
        "user.profile": "/user/profile",
        "user.margins": "/user/margins",
        "user.margins.segment": "/user/margins/{segment}",

        "orders": "/orders",
        "trades": "/trades",

        "order.info": "/orders/{order_id}",
        "order.place": "/orders/{variety}",
        "order.modify": "/orders/{variety}/{order_id}",
        "order.cancel": "/orders/{variety}/{order_id}",
        "order.trades": "/orders/{order_id}/trades",

        "portfolio.positions": "/portfolio/positions",
        "portfolio.holdings": "/portfolio/holdings",
        "portfolio.holdings.auction": "/portfolio/holdings/auctions",
        "portfolio.positions.convert": "/portfolio/positions",

        # MF api endpoints
        "mf.orders": "/mf/orders",
        "mf.order.info": "/mf/orders/{order_id}",
        "mf.order.place": "/mf/orders",
        "mf.order.cancel": "/mf/orders/{order_id}",

        "mf.sips": "/mf/sips",
        "mf.sip.info": "/mf/sips/{sip_id}",
        "mf.sip.place": "/mf/sips",
        "mf.sip.modify": "/mf/sips/{sip_id}",
        "mf.sip.cancel": "/mf/sips/{sip_id}",

        "mf.holdings": "/mf/holdings",
        "mf.instruments": "/mf/instruments",

        "market.instruments.all": "/instruments",
        "market.instruments": "/instruments/{exchange}",
        "market.margins": "/margins/{segment}",
        "market.historical": "/instruments/historical/{instrument_token}/{interval}",
        "market.trigger_range": "/instruments/trigger_range/{transaction_type}",

        "market.quote": "/quote",
        "market.quote.ohlc": "/quote/ohlc",
        "market.quote.ltp": "/quote/ltp",

        # GTT endpoints
        "gtt": "/gtt/triggers",
        "gtt.place": "/gtt/triggers",
        "gtt.info": "/gtt/triggers/{trigger_id}",
        "gtt.modify": "/gtt/triggers/{trigger_id}",
        "gtt.delete": "/gtt/triggers/{trigger_id}",

        # Margin computation endpoints
        "order.margins": "/margins/orders",
        "order.margins.basket": "/margins/basket",
        "order.contract_note": "/charges/orders",
    }

    def __init__(self,
                api_key,
                access_token=None,
                root=None,
                debug=False,
                timeout=None,
                proxies=None,
                pool=None,
                disable_ssl=False):
        
        self.debug = debug
        self.api_key = api_key
        self.session_expiry_hook = None
        self.disable_ssl = disable_ssl
        self.access_token = access_token
        self.proxies = proxies if proxies else {}

        self.root = root or self._default_root_uri
        self.timeout = timeout or self._default_timeout

        self.reqsession = httpx.AsyncClient(verify=False)
        if pool:
            limits = httpx.Limits(
                max_keepalive_connections=pool.get('max_keepalive_connections', None),
                max_connections=pool.get('max_connections', None)
            )
            self.reqsession = httpx.AsyncClient(limits=limits)
    
    def set_access_token(self, access_token):
        """Set the `access_token` received after a successful authentication."""
        self.access_token = access_token

    # def _user_agent(self):
    #     return (__title__ + "-python/").capitalize() + __version__

    async def place_order_custom(self,
                variety,
                exchange,
                tradingsymbol,
                transaction_type,
                quantity,
                product,
                order_type,
                price=None,
                validity=None,
                validity_ttl=None,
                disclosed_quantity=None,
                trigger_price=None,
                iceberg_legs=None,
                iceberg_quantity=None,
                auction_number=None,
                tag=None):
        """Place an order."""
        params = locals()
        del (params["self"])

        for k in list(params.keys()):
            if params[k] is None:
                del (params[k])

        return (await self._post_custom("order.place",
                        url_args={"variety": variety},
                        params=params))["order_id"]
    
    async def _post_custom(self, route, url_args=None, params=None, is_json=False, query_params=None):
        """Alias for sending a POST request."""
        return await self._request_custom(route, "POST", url_args=url_args, params=params, is_json=is_json, query_params=query_params)
    

    async def _request_custom(self, route, method, url_args=None, params=None, is_json=False, query_params=None):
        """Make an HTTP request."""
        # Form a restful URL
        if url_args:
            uri = self._routes[route].format(**url_args)
        else:
            uri = self._routes[route]

        url = urljoin(self.root, uri)

        # Custom headers
        headers = {
            "X-Kite-Version": self.kite_header_version,
            "User-Agent": self._user_agent()
        }

        if self.api_key and self.access_token:
            # set authorization header
            auth_header = self.api_key + ":" + self.access_token
            headers["Authorization"] = "token {}".format(auth_header)

        if self.debug:
            log.debug("Request: {method} {url} {params} {headers}".format(method=method, url=url, params=params, headers=headers))

        # prepare url query params
        if method in ["GET", "DELETE"]:
            query_params = params

        try:
            r = await self.reqsession.request(method,
                                    url,
                                    json=params if (method in ["POST", "PUT"] and is_json) else None,
                                    data=params if (method in ["POST", "PUT"] and not is_json) else None,
                                    params=query_params,
                                    headers=headers,
                                    timeout=self.timeout)
        # Any requests lib related exceptions are raised here - https://requests.readthedocs.io/en/latest/api/#exceptions
        except Exception as e:
            raise e

        if self.debug:
            log.debug("Response: {code} {content}".format(code=r.status_code, content=r.content))

        # Validate the content type.
        if "json" in r.headers["content-type"]:
            try:
                data = r.json()
            except ValueError:
                raise ex.DataException("Couldn't parse the JSON response received from the server: {content}".format(
                    content=r.content))

            # api error
            if data.get("status") == "error" or data.get("error_type"):
                # Call session hook if its registered and TokenException is raised
                if self.session_expiry_hook and r.status_code == 403 and data["error_type"] == "TokenException":
                    self.session_expiry_hook()

                # native Kite errors
                exp = getattr(ex, data.get("error_type"), ex.GeneralException)
                raise exp(data["message"], code=r.status_code)

            return data["data"]
        elif "csv" in r.headers["content-type"]:
            return r.content
        else:
            raise ex.DataException("Unknown Content-Type ({content_type}) with response: ({content})".format(
                content_type=r.headers["content-type"],
                content=r.content))
    
    async def tkinter_hard_code_regular_buy_order(
        self,
        exchange,
        trade_symbol,
        qty,
        price,
        trig_price,
        api_key,
        access_token
    ):
        """
        Place a regular buy order without retries, logging, and handling exceptions.
        """
        data = {
            "variety": "regular",
            "exchange": exchange,
            "tradingsymbol": trade_symbol,
            "transaction_type": "BUY",
            "quantity": qty,
            "product": "NRML",
            "order_type": "SL",
            "validity": "DAY",
            "trigger_price": trig_price,
            "price": price,
        }
        headers = {
            "X-Kite-Version": "3",
            "User-Agent": "Kiteconnect-python/5.0.1",
            "Authorization": f"token {api_key}:{access_token}",
        }

        async with httpx.AsyncClient(verify=False) as client:
            try:
                # Make the HTTP POST request
                response = await client.post(
                    "https://api.kite.trade/orders/regular",
                    data=data,
                    headers=headers,
                    timeout=7
                )

                # Log and handle HTTP status codes
                response.raise_for_status()

                # Parse JSON response
                if "json" in response.headers.get("content-type", ""):
                    try:
                        response_data = response.json()
                    except ValueError as e:
                        logger.error(f"Failed to parse JSON response: {e}")
                        return {"status": "error", "message": "Invalid JSON response"}

                    # Check for errors in the API response
                    if response_data.get("status") == "error":
                        logger.error(f"API Error: {response_data.get('message')}")
                        if response_data.get("error_type") == "TokenException":
                            if self.session_expiry_hook:
                                self.session_expiry_hook()  # Trigger session expiry hook if applicable
                            return {"status": "error", "message": "Session expired"}
                        return response_data

                    # Log success response
                    logger.info(f"Buy order placed successfully: {response_data}")
                    return response_data  # Return the successful response

                else:
                    logger.error("Invalid response content type, expected JSON.")
                    return {"status": "error", "message": "Invalid response content type"}

            except httpx.ReadTimeout:
                logger.error("Read timeout occurred.")
                return {"status": "error", "message": "Read timeout"}
            except httpx.ConnectTimeout:
                logger.error("Connection timeout occurred.")
                return {"status": "error", "message": "Connection timeout"}
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP status error occurred: {e}")
                logger.error(f"Response content: {e.response.text}")  # Log the error response content
                if e.response.status_code == 400:
                    logger.error("400 Bad Request encountered.")
                    return {"status": "error", "message": f"400 Bad Request - {e.response.text}"}
            except httpx.RequestError as e:
                logger.error(f"Request error: {e}")
                return {"status": "error", "message": "Request error"}
            except Exception as e:
                logger.exception(f"Unexpected error: {e}")
                return {"status": "error", "message": "Unexpected error"}

        # Fallback error response if the request was not successful
        return {"status": "error", "message": "Failed to place buy order"}


    async def hard_code_regular_buy_order(self,exchange, trade_symbol, qty, price, trig_price, api_key, access_token, token_to_OrderId_Mod_Dic=None, token=None, max_retries=2):
        data = {'variety': 'regular', 
                'exchange': f'{exchange}', 
                'tradingsymbol': f'{trade_symbol}', 
                'transaction_type': 'BUY', 
                'quantity': f'{qty}', 
                'product': 'NRML',
                'order_type': 'SL', 
                'validity': 'DAY',
                'trigger_price':f'{trig_price}',
                'price':f'{price}'
                }
        headers = {
            'X-Kite-Version': '3', 
            'User-Agent': 'Kiteconnect-python/5.0.1', 
            'Authorization': f'token {api_key}:{access_token}'
                }
        
        attempt = 0
        while attempt < max_retries:
            async with httpx.AsyncClient(verify=False) as client:  # Use a context manager for the session
                try:
                    r = await client.request('POST',
                                                        'https://api.kite.trade/orders/regular',
                                                        json=None,
                                                        data=data,
                                                        params=None,
                                                        headers=headers,
                                                        timeout=7)

                    r.raise_for_status()  # Raise an exception for HTTP errors   
                    # If no exception was raised, break out of the retry loop
                    break
                
                except httpx.ReadTimeout:
                    logger.exception(f"Attempt {attempt + 1}/{max_retries} - Read timeout occurred.")
                    return await self.query_orders_for_status(token, token_to_OrderId_Mod_Dic, api_key, access_token)
                except httpx.ConnectTimeout:
                    logger.exception(f"Attempt {attempt + 1}/{max_retries} - Connection timeout occurred.")
                except httpx.HTTPStatusError as e:
                    logger.exception(f"Attempt {attempt + 1}/{max_retries} - HTTP status error: {str(e)}")
                    # Log the response content for additional context
                    logger.error(f"HTTP status error response content: {e.response.text}")
                    # Handle 400 Bad Request without retrying
                    if e.response.status_code == 400:
                        logger.error("400 Bad Request encountered. Not retrying.")
                        
                        return {"status": "error", "message": "400 Bad Request"}
                except httpx.RequestError as e:
                    logger.exception(f"Attempt {attempt + 1}/{max_retries} - Request error while posting GTT order: {str(e)}")
                except Exception as e:
                    logger.exception(f"Attempt {attempt + 1}/{max_retries} - Request error while posting normal order.")
                
                attempt += 1
                # If this was the last attempt, raise the exception
                if attempt < max_retries:
                    logger.info(f"Retrying Normal Buy order... (Attempt {attempt + 1})")
                    await asyncio.sleep(0.5)
                else:
                    logger.error("Maximum retry attempts exceeded for hard_code_regular_buy_order")
                    return {"status": "error", "message": "Maximum retry attempts exceeded"}

        # Validate the content type.
        if "json" in r.headers["content-type"]:
            try:
                data = r.json()
            except ValueError:
                raise ex.DataException("Couldn't parse the JSON response received from the server: {content}".format(
                    content=r.content))

            # api error
            if data.get("status") == "error" or data.get("error_type"):
                # Call session hook if its registered and TokenException is raised
                if self.session_expiry_hook and r.status_code == 403 and data["error_type"] == "TokenException":
                    self.session_expiry_hook()

                # native Kite errors
                exp = getattr(ex, data.get("error_type"), ex.GeneralException)
                raise exp(data["message"], code=r.status_code)

            logger.info(f"Normal Buy Order Status : {data}")
            return data

        return {"status": "error"}

    async def hardcode_orders(self,api_key, access_token):
        headers = {
            'X-Kite-Version': '3',
            'User-Agent': 'Kiteconnect-python/5.0.1',
            'Authorization': f'token {api_key}:{access_token}'
        }
        try:
            async with httpx.AsyncClient(verify=False) as client:
                r = await client.get('https://api.kite.trade/orders', headers=headers, timeout=7)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            logger.exception(f"Error while querying orders: {str(e)}")
            return {"status": "error", "message": "Error while querying orders"}
        
    async def hardcode_gtt_orders(self,api_key, access_token):
        headers = {
            'X-Kite-Version': '3',
            'User-Agent': 'Kiteconnect-python/5.0.1',
            'Authorization': f'token {api_key}:{access_token}'
        }
        try:
            r = await self.reqsession.get('https://api.kite.trade/gtt/triggers', headers=headers, timeout=7)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.exception(f"Error while querying gtt orders: {str(e)}")
            return {"status": "error", "message": "Error while querying gtt orders"}

    async def get_orders_by_status_and_type(self, status, transaction_type, api_key, access_token, tradingsymbol=None):
        """
        Fetch orders filtered by status, transaction type, and optionally by tradingsymbol.
        """
        orders = await self.hardcode_orders(api_key, access_token)
        if orders['status'] == 'success':
            filtered_orders = [
                order for order in orders['data']
                if order['status'] == status and order['transaction_type'] == transaction_type
                and (tradingsymbol is None or order['tradingsymbol'] == tradingsymbol)
            ]
            return filtered_orders
        else:
            logger.error("Failed to fetch orders")
            return []

    
    async def get_gtt_orders_by_status(self, status, api_key, access_token):
        orders = await self.hardcode_gtt_orders(api_key, access_token)
        if orders.get('status') == 'success':
            filtered_orders = [
                order for order in orders['data']
                if order['status'] == status
            ]
            return filtered_orders
        else:
            logger.error("Failed to fetch GTT orders")
            return []

    async def cancel_orders_by_status_and_type(self, status, transaction_type, api_key, access_token, max_retries=2, token_to_OrderId_Mod_Dic=None):
        orders_to_cancel = await self.get_orders_by_status_and_type(status, transaction_type, api_key, access_token)
        for order in orders_to_cancel:
            order_id = order['order_id']
            # Cancel order if the dictionary is None or the order_id is not in the dictionary
            if token_to_OrderId_Mod_Dic is None or order_id not in [details['order_id'] for details in token_to_OrderId_Mod_Dic.values()]:
                logger.info(f"Cancelling found: {order_id} *****************Order id cancelled which is not found in our tracking dictionary************************************")
                result = await self.hard_code_regular_cancel_order(order_id, api_key=api_key, access_token=access_token)
                if result.get("status") == "success":
                    logger.info(f"Successfully cancelled order ID: {order_id}")
                else:
                    logger.error(f"Failed to cancel order ID: {order_id}")

    async def cancel_gtt_orders_by_status(self, status, api_key, access_token, token_to_OrderId_Mod_Dic=None):
        orders_to_cancel = await self.get_gtt_orders_by_status(status, api_key, access_token)
        for order in orders_to_cancel:
            order_id = order['id']
            # Cancel GTT order if the dictionary is None or the order_id is not in the dictionary
            if token_to_OrderId_Mod_Dic is None or order_id not in [details['gtt_order_id'] for details in token_to_OrderId_Mod_Dic.values()]:
                result = await self.hard_code_gtt_cancel_order(order_id, api_key=api_key, access_token=access_token)
                if result.get("status") == "success":
                    logger.info(f"Successfully cancelled GTT order ID: {order_id}")
                else:
                    logger.error(f"Failed to cancel GTT order ID: {order_id}")

    async def query_orders_for_status(self, tok, token_to_OrderId_Mod_Dic, api_key, access_token):
        headers = {
            'X-Kite-Version': '3', 
            'User-Agent': 'Kiteconnect-python/5.0.1', 
            'Authorization': f'token {api_key}:{access_token}'
        }
        try:
            r = await self.reqsession.get('https://api.kite.trade/orders', headers=headers, timeout=7)
            r.raise_for_status()
            orders = r.json()
            for order in orders['data']:
                if order["status"] == "TRIGGER PENDING" and order["instrument_token"] == tok:
                    order_id = order["order_id"]
                    if order_id not in token_to_OrderId_Mod_Dic.values():
                        token_to_OrderId_Mod_Dic[tok] = {'order_id': order_id, 'modification_required': True}
                        logger.info(f"Order recorded for {tok} with Order ID: {order_id}")
                        return {"status": "success", "data": {"order_id": order_id}}
            return {"status": "error", "message": "No TRIGGER PENDING orders found for the given token"}
        except Exception as e:
            logger.exception(f"Error while querying orders: {str(e)}")
            return {"status": "error", "message": "Error while querying orders"}


    async def hard_code_regular_cancel_order(self, order_id, access_token, api_key, max_retries=2):
        headers = {
            'X-Kite-Version': '3',
            'User-Agent': 'Kiteconnect-python/5.0.1',
            'Authorization': f'token {api_key}:{access_token}'
        }
        
        url = f'https://api.kite.trade/orders/regular/{order_id}'

        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(verify=False) as client:
                    r = await client.request(
                        'DELETE', 
                        url, 
                        headers=headers, 
                        json=None, 
                        data=None,
                        timeout=7
                    )
                    rep = r.json()
                    logger.info(f"Cancel Normal Order Status: {rep}")

                    if rep.get("status") == "success":
                        return rep  # Return the response if successful
                    else:
                        message = rep.get("message", "")
                        if "Order cannot be cancelled as it is being processed. Try later." in message:
                            logger.warning(f"Known issue encountered: {message}")
                            if attempt < max_retries - 1:
                                logger.info(f"Retrying Normal cancel order due to known issue... (Attempt {attempt + 2})")
                                continue  # Retry the request
                            else:
                                return rep  # Return the response if max retries reached
                        else:
                            logger.error(f"Cancel Normal order failed with response: {rep}")
                            if attempt < max_retries - 1:
                                logger.info(f"Retrying Normal cancel order... (Attempt {attempt + 2})")
                                continue  # Retry the request
                            else:
                                return rep  # Return the response if max retries reached

            except Exception as e:
                logger.error(f"Exception during normal cancel order attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying normal cancel order... (Attempt {attempt + 2})")
                else:
                    raise  # Raise the exception if max retries reached

    async def hard_code_regular_modify_order(self, order_id, price, trig_price, access_token, api_key, max_retries=3):
        data = {'variety': 'regular', 
                'order_id': f'{order_id}',
                'trigger_price':f'{trig_price}',
                'price':f'{price}'
                }
        headers = {'X-Kite-Version': '3',
                'User-Agent': 'Kiteconnect-python/5.0.1',
                'Authorization': f'token {api_key}:{access_token}'}
        
        url = f'https://api.kite.trade/orders/regular/{order_id}'

        attempt = 0
        while attempt < max_retries:
            try:
                r = await self.reqsession.request('PUT', 
                                                url, 
                                                headers=headers, 
                                                json=None,
                                                data=data,
                                                timeout=7)
                r.raise_for_status()  # Raise an exception for HTTP errors
                rep = r.json()
                logger.info(f"Regular Order Modify Status:{rep}")

                if rep.get("status") == "success":
                    return rep  # Return the response if successful
                else:
                    logger.error(f"Regular order Modify failed with response: {rep}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying Regular Modify order... (Attempt {attempt + 2})")
                    return rep
            except httpx.ReadTimeout:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - Read timeout occurred.")
            except httpx.ConnectTimeout:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - Connection timeout occurred.")
            except httpx.HTTPStatusError as e:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - HTTP status error: {str(e)}")
                logger.error(f"HTTP status error response content: {e.response.text}")
                try:
                    rep = e.response.json()
                    message = rep.get("message", "")
                    if "Order cannot be modified as it is being processed. Try later." in message:
                        logger.warning(f"Ignoring known issue: {message}")
                        return {"status": "error", "message": "ignore processed"}
                    elif "Maximum allowed order modifications exceeded." in message:
                        logger.warning(f"Ignoring known issue: {message}")
                        return {"status": "error", "message": "ignore maximum"}
                    elif "Too many requests" in message:
                        logger.warning(f"Handling too many request issue: {message}")
                        return {"status": "error", "message": "Too many requests"}
                except ValueError:
                    logger.exception("Failed to parse JSON response from HTTP status error.")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying Normal Modify order... (Attempt {attempt + 1})")
                        await asyncio.sleep(1)
                    else:
                        return {"status": "error", "message": "HTTP status error without parsable message"}
            except httpx.RequestError as e:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - Request error while modifying order: {str(e)}")
            except Exception as e:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - Request error while modifying order: {str(e)}")
            attempt += 1
            if attempt < max_retries:
                logger.info(f"Retrying Normal Modify order... (Attempt {attempt + 1})")
                await asyncio.sleep(1)
            else:
                logger.error("Maximum retry attempts exceeded for hard_code_regular_modify_order")
                return {"status": "error", "message": "Maximum retry attempts exceeded"}

        return {"status": "error", "message": "Maximum retry attempts exceeded", "data": None, "error_type": "RetryExceeded"}

    async def tkinter_hard_code_regular_modify_order(self, order_id, price, trig_price, access_token, api_key):
        """
        Modify an existing regular order. Performs only one attempt.
        """
        data = {
            'variety': 'regular',
            'order_id': f'{order_id}',
            'trigger_price': f'{trig_price}',
            'price': f'{price}'
        }
        headers = {
            'X-Kite-Version': '3',
            'User-Agent': 'Kiteconnect-python/5.0.1',
            'Authorization': f'token {api_key}:{access_token}'
        }
        url = f'https://api.kite.trade/orders/regular/{order_id}'

        try:
            # Make the PUT request
            r = await self.reqsession.request(
                'PUT',
                url,
                headers=headers,
                json=None,
                data=data,
                timeout=7
            )

            # Raise HTTP errors explicitly
            r.raise_for_status()

            # Parse the JSON response
            rep = r.json()
            logger.info(f"Regular Order Modify Status: {rep}")

            if rep.get("status") == "success":
                return rep  # Return the response if successful
            else:
                logger.error(f"Order modification failed with response: {rep}")
                return {"status": "error", "message": "Modification failed", "data": rep}

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP status error occurred: {str(e)}")
            logger.error(f"Response content: {e.response.text}")
            try:
                # Parse error message from the response
                error_data = e.response.json()
                error_message = error_data.get("message", "Unknown error")
                logger.error(f"Error message from server: {error_message}")

                if "Trigger price for stoploss" in error_message:
                    logger.warning(f"Specific issue identified: {error_message}")
                    return {
                        "status": "error",
                        "message": "Invalid stoploss parameters",
                        "data": error_data
                    }
                elif "Too many requests" in error_message:
                    logger.warning(f"Handling rate limiting: {error_message}")
                    return {
                        "status": "error",
                        "message": "Rate limit exceeded",
                        "data": error_data
                    }
                else:
                    return {
                        "status": "error",
                        "message": "HTTP 400 Bad Request",
                        "data": error_data
                    }

            except ValueError:
                logger.error("Failed to parse JSON response for HTTP status error.")
                return {
                    "status": "error",
                    "message": "HTTP status error without parsable response",
                    "data": None
                }

        except httpx.RequestError as e:
            logger.error(f"Request error occurred: {str(e)}")
            return {
                "status": "error",
                "message": "Request error while modifying order",
                "data": None
            }

        except Exception as e:
            logger.error(f"Unexpected error occurred: {str(e)}")
            return {
                "status": "error",
                "message": "Unexpected error",
                "data": None
            }


    async def hard_code_modify_limit_type(self, order_id, price, trig_price, access_token, api_key, type, max_retries=2):
        data = {'variety': 'regular', 
                'order_id': f'{order_id}',
                'trigger_price':f'{trig_price}',
                'price':f'{price}',
                'order_type':f'{type}'
                }
        headers = {'X-Kite-Version': '3',
                'User-Agent': 'Kiteconnect-python/5.0.1',
                'Authorization': f'token {api_key}:{access_token}'}
        
        url = f'https://api.kite.trade/orders/regular/{order_id}'

        attempt = 0
        while attempt < max_retries:
            try:
                r = await self.reqsession.request('PUT', 
                                                url, 
                                                headers=headers, 
                                                json=None,
                                                data=data,
                                                timeout=7)
                r.raise_for_status()  # Raise an exception for HTTP errors
                rep = r.json()
                logger.info(f"Regular Order Modify to LIMIT Status:{rep}")

                if rep.get("status") == "success":
                    return rep  # Return the response if successful
                else:
                    logger.error(f"Regular order Modify to LIMIT failed with response: {rep}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying Regular Modify order to LIMIT ... (Attempt {attempt + 2})")
                    return rep
            except httpx.ReadTimeout:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - Read timeout occurred.")
            except httpx.ConnectTimeout:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - Connection timeout occurred.")
            except httpx.HTTPStatusError as e:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - HTTP status error: {str(e)}")
                logger.error(f"HTTP status error response content: {e.response.text}")
                try:
                    rep = e.response.json()
                    message = rep.get("message", "")
                    if "Order cannot be modified as it is being processed. Try later." in message:
                        logger.warning(f"Ignoring known issue: {message}")
                        return {"status": "error", "message": "ignore processed"}
                    elif "Maximum allowed order modifications exceeded." in message:
                        logger.warning(f"Ignoring known issue: {message}")
                        return {"status": "error", "message": "ignore maximum"}
                except ValueError:
                    logger.exception("Failed to parse JSON response from HTTP status error.")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying Normal Modify order... (Attempt {attempt + 1})")
                        await asyncio.sleep(1)
                    else:
                        return {"status": "error", "message": "HTTP status error without parsable message"}
            except httpx.RequestError as e:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - Request error while modifying order to LIMIT: {str(e)}")
            except Exception as e:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - Request error while modifying order to LIMIT: {str(e)}")
            attempt += 1
            if attempt < max_retries:
                logger.info(f"Retrying Normal Modify order... (Attempt {attempt + 1})")
                await asyncio.sleep(1)
            else:
                logger.error("Maximum retry attempts exceeded for hard_code_modify_sl_to_limit")
                return {"status": "error", "message": "Maximum retry attempts exceeded"}

        return {"status": "error", "message": "Maximum retry attempts exceeded", "data": None, "error_type": "RetryExceeded"}


    async def hard_code_regular_sell_order(self,exchange, trade_symbol, qty, stop_loss_price, trig_price, api_key, access_token):
        # global stop_loss_price
        # stop_loss_price = ltp - (ltp*0.1)# stop loss at -10% with round to nearest decimal 0.10
        # stop_loss_price = round(stop_loss_price * 10) / 10.0 
        # trigger_price = stop_loss_price*0.02
        # trigger_price = round(stop_loss_price + trigger_price)* 10 / 10.0
        # logger.info(f" LTP: {ltp}, Stop Loss Price: {stop_loss_price}, Trigger Price: {trigger_price}")
        # print("ENTERED SELL FUNCTION")
        data = {'variety': 'regular', 
                'exchange': f'{exchange}', 
                'tradingsymbol': f'{trade_symbol}', 
                'transaction_type': 'SELL', 
                'quantity': f'{qty}', 
                'product': 'NRML',
                'order_type': 'SL', 
                'validity': 'DAY',
                'trigger_price':f'{trig_price}',
                'price':f'{stop_loss_price}'
                }
        headers = {
            'X-Kite-Version': '3', 
            'User-Agent': 'Kiteconnect-python/5.0.1', 
            'Authorization': f'token {api_key}:{access_token}'
                }
        # print("Sending request to Zerodha")
        try:
            r = await self.reqsession.request('POST',
                                                'https://api.kite.trade/orders/regular',
                                                json=None,
                                                data=data,
                                                params=None,
                                                headers=headers,
                                                timeout=7)
            print("Response received", r.status_code, r.text)
            r.raise_for_status()
            # Any requests lib related exceptions are raised here - https://requests.readthedocs.io/en/latest/api/#exceptions
        except Exception as e:
            raise e
        
        # Validate the content type.
        if "json" in r.headers["content-type"]:
            try:
                data = r.json()
            except ValueError:
                raise ex.DataException("Couldn't parse the JSON response received from the server: {content}".format(
                    content=r.content))

            # api error
            if data.get("status") == "error" or data.get("error_type"):
                # Call session hook if its registered and TokenException is raised
                if self.session_expiry_hook and r.status_code == 403 and data["error_type"] == "TokenException":
                    self.session_expiry_hook()

                # native Kite errors
                exp = getattr(ex, data.get("error_type"), ex.GeneralException)
                raise exp(data["message"], code=r.status_code)

            return data
            # return data["data"]    

    async def hard_code_regular_target_order(self,exchange, trade_symbol, qty, target_price, api_key, access_token):
            
            # target_price = round((ltp*10)* 10) / 10.0 # 10 times target
            # trig_price = target_price-(target_price*0.05)
            # trig_price = round(trig_price * 10) / 10.0

            data = {'variety': 'regular', 
                    'exchange': f'{exchange}', 
                    'tradingsymbol': f'{trade_symbol}', 
                    'transaction_type': 'SELL', 
                    'quantity': f'{qty}', 
                    'product': 'NRML',
                    'order_type': 'LIMIT', 
                    'validity': 'DAY',
                    'trigger_price':f'{target_price}',
                    'price':f'{target_price}'
                    }
            headers = {
                'X-Kite-Version': '3', 
                'User-Agent': 'Kiteconnect-python/5.0.1', 
                'Authorization': f'token {api_key}:{access_token}'
                    }
            try:
                r = await self.reqsession.request('POST',
                                                    'https://api.kite.trade/orders/regular',
                                                    json=None,
                                                    data=data,
                                                    params=None,
                                                    headers=headers,
                                                    timeout=7)
                
                # Any requests lib related exceptions are raised here - https://requests.readthedocs.io/en/latest/api/#exceptions
            except Exception as e:
                raise e
            
            # Validate the content type.
            if "json" in r.headers["content-type"]:
                try:
                    data = r.json()
                except ValueError:
                    raise ex.DataException("Couldn't parse the JSON response received from the server: {content}".format(
                        content=r.content))

                # api error
                if data.get("status") == "error" or data.get("error_type"):
                    # Call session hook if its registered and TokenException is raised
                    if self.session_expiry_hook and r.status_code == 403 and data["error_type"] == "TokenException":
                        self.session_expiry_hook()

                    # native Kite errors
                    exp = getattr(ex, data.get("error_type"), ex.GeneralException)
                    raise exp(data["message"], code=r.status_code)

                return data
                # return data["data"] 

    async def tkinter_square_postions_market(self,exchange, trade_symbol, qty, api_key, access_token, transaction_type):
            
            data = {'variety': 'regular', 
                    'exchange': f'{exchange}', 
                    'tradingsymbol': f'{trade_symbol}', 
                    'transaction_type': f'{transaction_type}', #BUY/SELL
                    'quantity': f'{qty}',
                    'product': 'NRML',
                    'order_type': 'MARKET', #LIMIT/MARKET
                    'validity': 'DAY',
                    }
            headers = {
                'X-Kite-Version': '3', 
                'User-Agent': 'Kiteconnect-python/5.0.1', 
                'Authorization': f'token {api_key}:{access_token}'
                    }
            
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        "https://api.kite.trade/orders/regular",
                        data=data,
                        headers=headers,
                        timeout=7
                    )
                    response.raise_for_status()  # Raise for HTTP errors

                    # Parse response and handle errors
                    if "json" in response.headers.get("content-type", ""):
                        response_data = response.json()
                        if response_data.get("status") == "error":
                            logger.error(f"Order error: {response_data.get('message')}")
                            raise Exception(response_data.get("message"))
                        return response_data

                    logger.error("Invalid response content type, expected JSON.")
                    raise Exception("Invalid response content type")

            except httpx.RequestError as e:
                logger.error(f"Request error while placing order: {e}")
                raise
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error {e.response.status_code}: {e.response.text}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise

    async def tkinter_square_known_postions(self,exchange, trade_symbol, price, qty, transaction_type, order_type):
            
            data = {'variety': 'regular', 
                    'exchange': f'{exchange}', 
                    'tradingsymbol': f'{trade_symbol}', 
                    'transaction_type': f'{transaction_type}', #BUY/SELL
                    'quantity': f'{qty}',
                    'price':f'{price}',
                    'product': 'NRML',
                    'order_type': f'{order_type}', #LIMIT/MARKET
                    'validity': 'DAY',
                    }
            headers = {
                'X-Kite-Version': '3', 
                'User-Agent': 'Kiteconnect-python/5.0.1', 
                'Authorization': f'token {api_key}:{access_token}'
                    }
            
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        "https://api.kite.trade/orders/regular",
                        data=data,
                        headers=headers,
                        timeout=7
                    )
                    response.raise_for_status()  # Raise for HTTP errors

                    # Parse response and handle errors
                    if "json" in response.headers.get("content-type", ""):
                        response_data = response.json()
                        if response_data.get("status") == "error":
                            logger.error(f"Order error: {response_data.get('message')}")
                            raise Exception(response_data.get("message"))
                        return response_data

                    logger.error("Invalid response content type, expected JSON.")
                    raise Exception("Invalid response content type")

            except httpx.RequestError as e:
                logger.error(f"Request error while placing order: {e}")
                raise
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error {e.response.status_code}: {e.response.text}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise

    
    async def hard_code_gtt_sell_order(self,exchange, trade_symbol, qty, price, trig_price, ltp, api_key, access_token, max_retries=2):
        data = {
            'condition': f'{{"exchange": "{exchange}", "tradingsymbol": "{trade_symbol}", "trigger_values": [{trig_price}], "last_price": {ltp}}}', 
            'orders': f'[{{"exchange": "{exchange}", "tradingsymbol": "{trade_symbol}", "transaction_type": "SELL", "quantity": {qty}, "order_type": "LIMIT", "product": "NRML", "price": {price}}}]', 
            'type': 'single'
            }
        
        # data = {'condition': '{"exchange": "NFO", "tradingsymbol": "BANKNIFTY2461248900PE", "trigger_values": [6], "last_price": 5}', 'orders': '[{"exchange": "NFO", "tradingsymbol": "BANKNIFTY2461248900PE", "transaction_type": "SELL", "quantity": 15, "order_type": "LIMIT", "product": "CNC", "price": 6.5}]', 'type': 'single'}

        headers = {
            'X-Kite-Version': '3', 
            'User-Agent': 'Kiteconnect-python/5.0.1', 
            'Authorization': f'token {api_key}:{access_token}'
                }
        
        attempt = 0
        r = None
        while attempt < max_retries:
            try:
                r = await self.reqsession.request('POST',
                                                    'https://api.kite.trade/gtt/triggers',
                                                    json=None,
                                                    data=data,
                                                    params=None,
                                                    headers=headers,
                                                    timeout=7)
                # If no exception was raised, break out of the retry loop
                r.raise_for_status()  # Raise an exception for HTTP errors
                break
            
                # Any requests lib related exceptions are raised here - https://requests.readthedocs.io/en/latest/api/#exceptions
            except httpx.ReadTimeout:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - Read timeout occurred.")
            except httpx.ConnectTimeout:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - Connection timeout occurred.")
            except httpx.RequestError as e:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - Request error while posting GTT order: {str(e)}")
            except Exception as e:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - Error: {str(e)}")
            
            attempt += 1
            if attempt < max_retries:
                logger.info(f"Retrying GTT sell order... (Attempt {attempt + 1})")
                await asyncio.sleep(0.5)
            else:
                logger.error("Maximum retry attempts exceeded for hard_code_gtt_sell_order")
                return {"status": "error"}

        # Validate the content type.
        if "json" in r.headers["content-type"]:
            try:
                data = r.json()
            except ValueError:
                raise ex.DataException("Couldn't parse the JSON response received from the server: {content}".format(
                    content=r.content))

            # api error
            if data.get("status") == "error" or data.get("error_type"):
                # Call session hook if its registered and TokenException is raised
                if self.session_expiry_hook and r.status_code == 403 and data["error_type"] == "TokenException":
                    self.session_expiry_hook()

                # native Kite errors
                exp = getattr(ex, data.get("error_type"), ex.GeneralException)
                raise exp(data["message"], code=r.status_code)

            logger.info(f"GTT Buy Order Status : {data['status']}")
            return data
        
        return {"status": "error"}

    async def hard_code_gtt_cancel_order(self, order_id, api_key, access_token):

        headers = {'X-Kite-Version': '3',
                'User-Agent': 'Kiteconnect-python/5.0.1',
                'Authorization': f'token {api_key}:{access_token}'}
        
        url = f'https://api.kite.trade/gtt/triggers/{order_id}'

        try:
            r = await self.reqsession.request('DELETE', 
                                            url, 
                                            headers=headers, 
                                            json=None,
                                            data=None,
                                            timeout=7)
            rep = r.json()
            logger.info(f"GTT Cancel Order Status:{rep}")
            return rep #slicing to get success
        except Exception as e:
            logger.error(f"Exception during cancel order for GTT: {e}")
            return {"status": "error", "message": str(e)}
    
    async def hard_code_gtt_modify_order(self, exchange, trade_symbol, qty, price, trig_price, ltp, order_id, access_token, api_key, max_retries=2):
        data = {
            'condition': f'{{"exchange": "{exchange}", "tradingsymbol": "{trade_symbol}", "trigger_values": [{trig_price}], "last_price": {ltp}}}', 
            'orders': f'[{{"exchange": "{exchange}", "tradingsymbol": "{trade_symbol}", "transaction_type": "SELL", "quantity": {qty}, "order_type": "LIMIT", "product": "NRML", "price": {price}}}]', 
            'type': 'single'
            }
        headers = {'X-Kite-Version': '3',
                'User-Agent': 'Kiteconnect-python/5.0.1',
                'Authorization': f'token {api_key}:{access_token}'}
        
        url = f'https://api.kite.trade/gtt/triggers/{order_id}'

        for attempt in range(max_retries):
            try:
                r = await self.reqsession.request('PUT', 
                                                url, 
                                                headers=headers, 
                                                json=None,
                                                data=data,
                                                timeout=7)
                r.raise_for_status()  # Raise an exception for HTTP errors
                rep = r.json()
                logger.info(f"GTT Order Modify Status:{rep}")

                if rep.get("status") == "success":
                    return rep  # Return the response if successful
                else:
                    logger.error(f"GTT order Modify failed with response: {rep}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying GTT Modify order... (Attempt {attempt + 2})")
                    return rep
             
            except httpx.ReadTimeout:
                logger.exception(f"Attempt {attempt + 1}/{max_retries} - Read timeout occurred.")
            except Exception as e:
                logger.error(f"Exception during modifying GTT order attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying to modify GTT order... (Attempt {attempt + 2})")
                return {"status": "error", "message": str(e)}
        return {"status": "error", "message": "Maximum retry attempts exceeded"}

    def round_to_nearest(self, number):
        # Multiply by 10, round, then divide by 10
        rounded_number = round(number * 10) / 10.0
        return rounded_number

    def define_waittime(self, hrs, min, sec=0, ms=0):
        # Define the target time with hours, minutes, seconds, and microseconds
        target_time = datetime.now(pytz.timezone('Asia/Kolkata')).replace(hour=hrs, minute=min, second=sec, microsecond=ms * 1000)

        # If the current time is past the target time for today, move to the next day
        if datetime.now(pytz.timezone('Asia/Kolkata')) > target_time:
            target_time += timedelta(days=1)

        # Calculate the waiting time in seconds
        waiting_time = (target_time - datetime.now(pytz.timezone('Asia/Kolkata'))).total_seconds()

        # Log the waiting time
        logger.info(f"Waiting for {waiting_time / 60:.2f} minutes until {target_time.strftime('%H:%M:%S.%f')} IST")

        # Wait until the target time
        time.sleep(waiting_time)

if __name__ == "__main__":
    kite = KiteConnect_custom(api_key)
    asyncio.run(kite.hardcode_orders(api_key, access_token))
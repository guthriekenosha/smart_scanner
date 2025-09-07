# ws_private.py
# BloFin PRIVATE WebSocket with heartbeat, reconnect, and channel routing
# ----------------------------------------------------------------------
# - Login per docs using HMAC-SHA256(secret, path+method+timestamp+nonce), hex -> base64
# - Heartbeat (ping) every 20s; reconnect with exponential backoff
# - Auto re-subscribe after reconnect
# - Clean async callbacks: on_order, on_position, on_orders_algo (+ listener registry)
#
# Usage:
#   ws = BlofinPrivateWS(
#       api_key=os.getenv("BLOFIN_API_KEY"),
#       api_secret=os.getenv("BLOFIN_API_SECRET"),
#       passphrase=os.getenv("BLOFIN_API_PASSPHRASE"),
#       demo=bool(int(os.getenv("BLOFIN_DEMO", "0")))
#   )
#   ws.on_order = handle_order_update
#   ws.on_position = handle_position_update
#   ws.subscribe_orders()          # all instruments (server will push updates)
#   ws.subscribe_positions()       # all instruments
#   asyncio.create_task(ws.run())
#
#   # Later:
#   await ws.stop()

from __future__ import annotations
import asyncio
import json
import os
import time
import hmac
import hashlib
import base64
from typing import Any, Awaitable, Callable, Dict, List, Optional

import websockets

LIVE_URL = "wss://openapi.blofin.com/ws/private"
DEMO_URL = "wss://demo-trading-openapi.blofin.com/ws/private"

# Type aliases for callback signatures
OrderCallback = (
    Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None]
)
PositionCallback = (
    Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None]
)
AlgoCallback = (
    Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None]
)
GenericListener = (
    Callable[[Dict[str, Any]], Awaitable[None]] | Callable[[Dict[str, Any]], None]
)


def _ws_sign(secret: str) -> tuple[str, str, str]:
    """
    WebSocket login signature per docs:
      method=GET, path=/users/self/verify
      msg = path + method + timestamp + nonce
      sign = Base64( Hex( HMAC_SHA256(secret, msg) ) )
    Returns: (sign, timestamp, nonce)
    """
    ts = str(int(time.time() * 1000))
    nonce = ts  # using ts is acceptable; UUID also fine
    path, method = "/users/self/verify", "GET"
    msg = f"{path}{method}{ts}{nonce}"
    hexdigest = (
        hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest().encode()
    )
    return base64.b64encode(hexdigest).decode(), ts, nonce


class BlofinPrivateWS:
    """
    A resilient BloFin private WebSocket client with:
      - login
      - heartbeat
      - auto reconnect + resubscribe
      - channel routing: orders, positions, orders-algo
      - simple async callback hooks
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        demo: bool = False,
        ping_interval: float = 20.0,
        max_backoff: float = 30.0,
        connect_timeout: float = 20.0,
        debug: bool = False,
    ):
        self.url = DEMO_URL if demo else LIVE_URL
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase

        self.ping_interval = ping_interval
        self.max_backoff = max_backoff
        self.connect_timeout = connect_timeout
        self.debug = debug

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._stop = asyncio.Event()
        self._connected = asyncio.Event()

        # subscriptions are stored and re-sent on reconnect
        self._subs: List[Dict[str, Any]] = []

        # optional per-channel direct handlers
        self.on_order: Optional[OrderCallback] = None
        self.on_position: Optional[PositionCallback] = None
        self.on_orders_algo: Optional[AlgoCallback] = None

        # generic listeners (receive every routed payload)
        self._listeners: List[GenericListener] = []

        # housekeeping tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._reader_task: Optional[asyncio.Task] = None

    # --------------------------
    # Public API
    # --------------------------
    def subscribe_orders(self, inst_ids: Optional[List[str]] = None):
        if inst_ids:
            for inst in inst_ids:
                self._subs.append({"channel": "orders", "instId": inst})
        else:
            self._subs.append({"channel": "orders"})

    def subscribe_positions(self, inst_ids: Optional[List[str]] = None):
        if inst_ids:
            for inst in inst_ids:
                self._subs.append({"channel": "positions", "instId": inst})
        else:
            self._subs.append({"channel": "positions"})

    def subscribe_orders_algo(self, inst_ids: Optional[List[str]] = None):
        if inst_ids:
            for inst in inst_ids:
                self._subs.append({"channel": "orders-algo", "instId": inst})
        else:
            self._subs.append({"channel": "orders-algo"})

    def add_listener(self, fn: GenericListener):
        """Receive every routed message (orders/positions/orders-algo)."""
        self._listeners.append(fn)

    async def run(self):
        """
        Main loop: connect → login → subscribe → spawn heartbeat/reader → await until stop or disconnect.
        Reconnects with exponential backoff.
        """
        backoff = 1.0
        while not self._stop.is_set():
            try:
                if self.debug:
                    print(f"[ws_private] Connecting to {self.url} ...")
                async with websockets.connect(
                    self.url, ping_interval=None, close_timeout=2, max_size=2**22
                ) as ws:
                    self._ws = ws
                    await self._login(ws)
                    await self._subscribe(ws)

                    self._connected.set()
                    if self.debug:
                        print("[ws_private] Connected, logged in, subscribed.")

                    # launch heartbeat + reader
                    self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                    self._reader_task = asyncio.create_task(self._reader_loop())

                    # wait for either stop requested or reader exits
                    done, _pending = await asyncio.wait(
                        {self._reader_task, self._heartbeat_task, asyncio.create_task(self._stop.wait())},
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    # if stop requested, break
                    if self._stop.is_set():
                        break

            except Exception as e:
                if self.debug:
                    print(f"[ws_private] Connection error: {e!r}")

            finally:
                self._connected.clear()
                # cancel tasks if still running
                for t in (self._heartbeat_task, self._reader_task):
                    if t and not t.done():
                        t.cancel()
                self._heartbeat_task = None
                self._reader_task = None
                self._ws = None

            # reconnect delay
            if not self._stop.is_set():
                if self.debug:
                    print(f"[ws_private] Reconnecting in {backoff:.1f}s ...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.7, self.max_backoff)

        if self.debug:
            print("[ws_private] Stopped.")

    async def stop(self):
        """Signal the loop to stop and close the socket."""
        self._stop.set()
        # give tasks a moment to exit gracefully
        await asyncio.sleep(0.05)
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass

    async def wait_connected(self, timeout: Optional[float] = None) -> bool:
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    # --------------------------
    # Internals
    # --------------------------
    async def _login(self, ws: websockets.WebSocketClientProtocol):
        sign, ts, nonce = _ws_sign(self.api_secret)
        payload = {
            "op": "login",
            "args": [
                {
                    "apiKey": self.api_key,
                    "passphrase": self.passphrase,
                    "timestamp": ts,
                    "sign": sign,
                    "nonce": nonce,
                }
            ],
        }
        await ws.send(json.dumps(payload))
        # wait for login ack
        msg = await asyncio.wait_for(ws.recv(), timeout=self.connect_timeout)
        data = self._ensure_json(msg)
        if not (data.get("event") == "login" and str(data.get("code")) == "0"):
            raise RuntimeError(f"Login failed: {data}")

    async def _subscribe(self, ws: websockets.WebSocketClientProtocol):
        if not self._subs:
            return
        payload = {"op": "subscribe", "args": self._subs}
        await ws.send(json.dumps(payload))

    async def _heartbeat_loop(self):
        try:
            while not self._stop.is_set():
                await asyncio.sleep(self.ping_interval)
                if self._ws is None:
                    return
                try:
                    await self._ws.send("ping")
                except Exception as e:
                    if self.debug:
                        print(f"[ws_private] Heartbeat send failed: {e!r}")
                    return
        except asyncio.CancelledError:
            return

    async def _reader_loop(self):
        ws = self._ws
        if ws is None:
            return
        try:
            async for raw in ws:
                if raw == "pong":
                    # server heartbeat response
                    continue
                data = self._ensure_json(raw)
                await self._route(data)
        except asyncio.CancelledError:
            return
        except Exception as e:
            if self.debug:
                print(f"[ws_private] Reader error: {e!r}")
            # returning will trigger reconnect in run()

    # --------------------------
    # Routing
    # --------------------------
    async def _route(self, data: Dict[str, Any]):
        """
        Route messages to per-channel handlers and broadcast to listeners.
        Expected private channel shape:
          {
            "arg": { "channel": "orders" | "positions" | "orders-algo", ... },
            "data": [ ... ],
            ...
          }
        """
        arg = data.get("arg") or {}
        ch = arg.get("channel")
        if ch == "orders":
            await self._emit(self.on_order, data)
        elif ch == "positions":
            await self._emit(self.on_position, data)
        elif ch == "orders-algo":
            await self._emit(self.on_orders_algo, data)

        # fan-out to generic listeners
        await self._fanout(data)

    async def _emit(self, cb: Optional[GenericListener], payload: Dict[str, Any]):
        if cb is None:
            return
        try:
            res = cb(payload)
            if asyncio.iscoroutine(res):
                await res
        except Exception as e:
            if self.debug:
                print(f"[ws_private] Callback error: {e!r}")

    async def _fanout(self, payload: Dict[str, Any]):
        for fn in list(self._listeners):
            try:
                res = fn(payload)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                if self.debug:
                    print(f"[ws_private] Listener error: {e!r}")

    # --------------------------
    # Helpers
    # --------------------------
    @staticmethod
    def _ensure_json(raw: Any) -> Dict[str, Any]:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                return {"raw": raw}
        if isinstance(raw, dict):
            return raw
        return {"raw": raw}


# from __future__ import annotations
# import asyncio
# import json
# import time
# from typing import Any, Dict, Optional, Tuple

# import aiohttp

# from .config import CONFIG
# from .blofin_client import _blofin_sign


# class PrivateWS:
#     def __init__(self):
#         self._session: Optional[aiohttp.ClientSession] = None
#         self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
#         self._task: Optional[asyncio.Task] = None
#         self._stop = False
#         self._events: asyncio.Queue[Tuple[str, Dict[str, Any]]] = asyncio.Queue(maxsize=10000)

#     async def start(self):
#         if self._task:
#             return
#         self._stop = False
#         self._task = asyncio.create_task(self._run(), name="ws_private")

#     async def stop(self):
#         self._stop = True
#         if self._task:
#             self._task.cancel()
#             self._task = None
#         if self._ws:
#             try:
#                 await self._ws.close()
#             except Exception:
#                 pass
#             self._ws = None
#         if self._session:
#             try:
#                 await self._session.close()
#             except Exception:
#                 pass
#             self._session = None

#     def events(self) -> "asyncio.Queue[Tuple[str, Dict[str, Any]]]":
#         return self._events

#     async def _login(self, ws: aiohttp.ClientWebSocketResponse):
#         ts = str(int(time.time() * 1000))
#         nonce = ts
#         sign = _blofin_sign(CONFIG.api_secret, "GET", "/users/self/verify", ts, nonce, None)
#         payload = {
#             "op": "login",
#             "args": [
#                 {
#                     "apiKey": CONFIG.api_key,
#                     "passphrase": CONFIG.api_passphrase,
#                     "timestamp": ts,
#                     "sign": sign,
#                     "nonce": nonce,
#                 }
#             ],
#         }
#         await ws.send_str(json.dumps(payload))

#     async def _run(self):
#         url = CONFIG.ws_private_url
#         while not self._stop:
#             try:
#                 async with aiohttp.ClientSession() as session:
#                     self._session = session
#                     async with session.ws_connect(url, heartbeat=CONFIG.ws_ping_interval_sec) as ws:
#                         self._ws = ws
#                         await self._login(ws)
#                         # Subscribes
#                         await ws.send_str(json.dumps({"op": "subscribe", "args": [{"channel": "orders"}]}))
#                         await ws.send_str(json.dumps({"op": "subscribe", "args": [{"channel": "positions"}]}))
#                         ping_at = time.time() + CONFIG.ws_ping_interval_sec
#                         while not self._stop:
#                             now = time.time()
#                             if now >= ping_at:
#                                 try:
#                                     await ws.send_str("ping")
#                                 except Exception:
#                                     break
#                                 ping_at = now + CONFIG.ws_ping_interval_sec
#                             msg = await ws.receive(timeout=30.0)
#                             if msg.type == aiohttp.WSMsgType.TEXT:
#                                 try:
#                                     m = msg.json(loads=json.loads)
#                                 except Exception:
#                                     continue
#                                 if not isinstance(m, dict):
#                                     continue
#                                 # normalize action/channel
#                                 chan = (m.get("arg") or {}).get("channel") or m.get("channel")
#                                 action = m.get("action") or "update"
#                                 if chan in {"orders", "positions"}:
#                                     if not self._events.full():
#                                         await self._events.put((chan, m))
#                             elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
#                                 break
#             except Exception:
#                 await asyncio.sleep(1.5)

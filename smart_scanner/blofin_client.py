# smart_scanner/blofin_client.py
import asyncio, aiohttp, base64, hashlib, hmac, json, time, uuid
from typing import Any, Dict, List, Optional, Tuple
from .config import CONFIG


def _normalize_bar(bar: str) -> str:
    """
    BloFin uses 1m, 3m, 5m, 15m, 1H, 1D, 1W, 1M (case-sensitive).
    Map common lowercase hour/day inputs to the proper case.
    """
    mapping = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "6h": "6H",
        "12h": "12H",
        "1d": "1D",
        "1w": "1W",
        "1M": "1M",
        "1mth": "1M",
        "1month": "1M",
        "1H": "1H",
        "1D": "1D",
        "1W": "1W",
        "1M": "1M",
    }
    b = bar.strip()
    return mapping.get(b, b)


def _blofin_sign(
    secret: str,
    method: str,
    path_with_query: str,
    timestamp_ms: str,
    nonce: str,
    body: Optional[dict],
) -> str:
    # 1) prehash = path + method + timestamp + nonce + body_json_or_empty
    msg = f"{path_with_query}{method.upper()}{timestamp_ms}{nonce}"
    if body:
        msg += json.dumps(body, separators=(",", ":"))
    # 2) HMAC-SHA256 → 3) hex → 4) base64(hex-bytes)
    hex_sig = (
        hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest().encode()
    )
    return base64.b64encode(hex_sig).decode()


def _auth_headers(
    api_key: str,
    api_secret: str,
    passphrase: str,
    method: str,
    path_with_query: str,
    body: Optional[dict],
) -> Dict[str, str]:
    ts = str(int(time.time() * 1000))
    nonce = str(uuid.uuid4())
    sign = _blofin_sign(api_secret, method, path_with_query, ts, nonce, body)
    return {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-NONCE": nonce,
        "ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "smart-scanner/1.0",
    }


class BlofinClient:
    def __init__(
        self,
        base_url: str = CONFIG.base_url,
        api_key: str = CONFIG.api_key,
        api_secret: str = CONFIG.api_secret,
        api_passphrase: str = CONFIG.api_passphrase,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key, self.api_secret, self.api_passphrase = (
            api_key,
            api_secret,
            api_passphrase,
        )
        self.session: Optional[aiohttp.ClientSession] = None
        # Simple per-process rate spacing + 429 cooldown
        self._last_req_ts = 0.0
        self._cooldown_until = 0.0
        self._spacing_lock = asyncio.Lock()

    async def __aenter__(self):
        if self.session is None or getattr(self.session, "closed", False):
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self

    async def __aexit__(self, *exc):
        if self.session:
            try:
                await self.session.close()
            except Exception:
                pass
            finally:
                self.session = None

    async def _respect_spacing(self):
        # honor global cooldown after CF/WAF hits
        now = time.time()
        if now < self._cooldown_until:
            await asyncio.sleep(self._cooldown_until - now)
        # honor minimal inter-request spacing
        async with self._spacing_lock:
            gap = CONFIG.min_req_interval_ms / 1000.0
            now = time.time()
            wait = (self._last_req_ts + gap) - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_req_ts = time.time()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        query: Dict[str, Any] | None = None,
        body: Dict[str, Any] | None = None,
        auth: bool = False,
        retries: int | None = None,
    ) -> Any:
        # Ensure session exists and is open
        if self.session is None or getattr(self.session, "closed", False):
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        retries = CONFIG.max_retries if retries is None else retries
        url_path = path  # keep path for signing
        url = f"{self.base_url}{path}"
        backoff = CONFIG.base_backoff

        for attempt in range(1, retries + 1):
            await self._respect_spacing()

            headers = {}
            # Prepare body string exactly as used for signing and sending
            upper_method = method.upper()
            body_str: Optional[str] = None
            if upper_method != "GET" and body is not None:
                # Minified JSON (no extra spaces) as required by BloFin docs
                body_str = json.dumps(body, separators=(",", ":"))
            if auth:
                # include query in the path for signing
                if query:
                    # order-independent in docs; sending in any order is fine
                    from urllib.parse import urlencode

                    url_path_with_qs = f"{url_path}?{urlencode(query)}"
                else:
                    url_path_with_qs = url_path
                headers = _auth_headers(
                    self.api_key,
                    self.api_secret,
                    self.api_passphrase,
                    upper_method,
                    url_path_with_qs,
                    body,
                )
                # For auth GETs with query, ensure the actual request URL matches the signed path
                if upper_method == "GET":
                    url = f"{self.base_url}{url_path_with_qs}"
                    query = None
            else:
                headers = {
                    "Accept": "application/json",
                    "User-Agent": "smart-scanner/1.0",
                }

            try:
                # Defensive: if session closed mid-loop, recreate and retry immediately
                if self.session is None or getattr(self.session, "closed", False):
                    self.session = aiohttp.ClientSession(
                        timeout=aiohttp.ClientTimeout(total=30)
                    )
                async with self.session.request(
                    upper_method,
                    url,
                    params=query,
                    # Send the exact minified JSON string we signed
                    data=body_str if upper_method != "GET" else None,
                    headers=headers,
                ) as resp:
                    text = await resp.text()
                    if resp.status == 429 or (
                        "rate limited" in text.lower() or "error 1015" in text.lower()
                    ):
                        # trip a cool-off window to avoid CF WAF bans
                        self._cooldown_until = time.time() + CONFIG.cf_429_cooldown_sec
                        if attempt == retries:
                            raise aiohttp.ClientResponseError(
                                resp.request_info,
                                resp.history,
                                status=resp.status,
                                message=text,
                            )
                        await asyncio.sleep(min(backoff, CONFIG.max_backoff))
                        backoff *= 1.7
                        continue

                    if resp.status >= 400:
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=resp.status,
                            message=text,
                        )

                    # success
                    self._cooldown_until = max(
                        self._cooldown_until - 0.5, 0
                    )  # gradually decay cooldown
                    return json.loads(text)
            except RuntimeError as e:
                # e.g., "Session is closed" — recreate session and retry
                if "Session is closed" in str(e):
                    try:
                        if self.session:
                            await self.session.close()
                    except Exception:
                        pass
                    self.session = aiohttp.ClientSession(
                        timeout=aiohttp.ClientTimeout(total=30)
                    )
                    if attempt == retries:
                        raise
                    await asyncio.sleep(min(backoff, CONFIG.max_backoff))
                    backoff *= 1.7
                    continue
                if attempt == retries:
                    raise
                await asyncio.sleep(min(backoff, CONFIG.max_backoff))
                backoff *= 1.7
                continue
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == retries:
                    raise
                await asyncio.sleep(min(backoff, CONFIG.max_backoff))
                backoff *= 1.7

    # -------- Public Endpoints --------

    async def get_instruments(self, inst_type: Optional[str] = None) -> List[Dict[str, Any]]:
        q: Dict[str, Any] = {}
        if inst_type:
            q["instType"] = inst_type
        data = await self._request(
            "GET", "/api/v1/market/instruments", query=q or None, auth=False
        )
        items = data.get("data") or data.get("result") or []
        out = []
        for it in items:
            inst_id = it.get("instId") or it.get("instid") or it.get("symbol")
            if isinstance(inst_id, str) and inst_id.endswith("-USDT"):
                out.append(it)
        return out

    async def get_tickers(self, inst_type: Optional[str] = None) -> List[Dict[str, Any]]:
        # Keep for fallback only (WS should feed universe)
        q: Dict[str, Any] = {}
        if inst_type:
            q["instType"] = inst_type
        data = await self._request(
            "GET", "/api/v1/market/tickers", query=q or None, auth=False
        )
        return data.get("data") or data.get("result") or []

    async def get_candles(
        self, inst_id: str, bar: str, limit: int = 200
    ) -> List[List[str]]:
        bar = _normalize_bar(bar)
        q = {"instId": inst_id, "bar": bar}
        # 'limit' not documented; most clients support it, but it’s optional.
        if limit and isinstance(limit, int):
            q["limit"] = str(limit)
        data = await self._request("GET", "/api/v1/market/candles", query=q, auth=False)
        return data.get("data") or data.get("result") or []

    # -------- Trading (auth) --------

    async def get_account_balance(self, product_type: Optional[str] = None) -> Dict[str, Any]:
        q: Dict[str, Any] = {}
        if product_type:
            q["productType"] = product_type
        data = await self._request("GET", "/api/v1/account/balance", query=q or None, auth=True)
        return data.get("data") or {}

    async def get_positions(self, inst_id: Optional[str] = None) -> List[Dict[str, Any]]:
        q: Dict[str, Any] = {}
        if inst_id:
            q["instId"] = inst_id
        data = await self._request("GET", "/api/v1/account/positions", query=q or None, auth=True)
        return data.get("data") or []

    async def get_position_mode(self) -> Dict[str, Any]:
        data = await self._request("GET", "/api/v1/account/position-mode", auth=True)
        return data.get("data") or {}

    async def set_position_mode(self, mode: str) -> Dict[str, Any]:
        body = {"positionMode": mode}
        data = await self._request("POST", "/api/v1/account/set-position-mode", body=body, auth=True)
        return data.get("data") or {}

    async def set_margin_mode(self, mode: str) -> Dict[str, Any]:
        body = {"marginMode": mode}
        data = await self._request("POST", "/api/v1/account/set-margin-mode", body=body, auth=True)
        return data.get("data") or {}

    async def set_leverage(self, inst_id: str, leverage: str, margin_mode: str = "cross", position_side: Optional[str] = None) -> Dict[str, Any]:
        body: Dict[str, Any] = {"instId": inst_id, "leverage": str(leverage), "marginMode": margin_mode}
        if position_side:
            body["positionSide"] = position_side
        data = await self._request("POST", "/api/v1/account/set-leverage", body=body, auth=True)
        return data.get("data") or {}

        # NOTE: Do NOT pass tp/sl fields here for brackets. Use submit_bracket() instead
    async def place_order(
        self,
        inst_id: str,
        side: str,
        order_type: str,
        size: str,
        *,
        price: Optional[str] = None,
        margin_mode: str = "cross",
        position_side: str = "net",
        reduce_only: Optional[bool] = None,
        client_order_id: Optional[str] = None,
        tp_trigger_price: Optional[str] = None,
        tp_order_price: Optional[str] = None,
        sl_trigger_price: Optional[str] = None,
        sl_order_price: Optional[str] = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "instId": inst_id,
            "marginMode": margin_mode,
            "positionSide": position_side,
            "side": side,
            "orderType": order_type,
            "size": str(size),
        }
        if price is not None:
            body["price"] = str(price)
        if reduce_only is not None:
            body["reduceOnly"] = "true" if reduce_only else "false"
        if client_order_id:
            body["clientOrderId"] = client_order_id
        if tp_trigger_price is not None:
            body["tpTriggerPrice"] = str(tp_trigger_price)
            if tp_order_price is not None:
                body["tpOrderPrice"] = str(tp_order_price)
        if sl_trigger_price is not None:
            body["slTriggerPrice"] = str(sl_trigger_price)
            if sl_order_price is not None:
                body["slOrderPrice"] = str(sl_order_price)
        data = await self._request("POST", "/api/v1/trade/order", body=body, auth=True)
        return data

    async def place_tpsl(
        self,
        inst_id: str,
        side: str,
        *,
        margin_mode: str = "cross",
        position_side: str = "net",
        size: str = "-1",
        tp_trigger_price: str | None = None,
        tp_order_price: str | None = "-1",
        sl_trigger_price: str | None = None,
        sl_order_price: str | None = "-1",
        trigger_price_type: str | None = "last",
        reduce_only: bool = True,
        client_order_id: str | None = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "instId": inst_id,
            "marginMode": margin_mode,
            "positionSide": position_side,
            "side": side,
            "size": size,
            "reduceOnly": "true" if reduce_only else "false",
        }
        if tp_trigger_price is not None:
            body["tpTriggerPrice"] = str(tp_trigger_price)
            if tp_order_price is not None:
                body["tpOrderPrice"] = str(tp_order_price)
            if trigger_price_type is not None:
                body["tpTriggerPriceType"] = str(trigger_price_type)
        if sl_trigger_price is not None:
            body["slTriggerPrice"] = str(sl_trigger_price)
            if sl_order_price is not None:
                body["slOrderPrice"] = str(sl_order_price)
            if trigger_price_type is not None:
                body["slTriggerPriceType"] = str(trigger_price_type)
        if client_order_id:
            body["clientOrderId"] = client_order_id
        raw = await self._request("POST", "/api/v1/trade/order-tpsl", body=body, auth=True)
        # Normalize response: prefer the inner object (dict or first list item),
        # then carry top-level code/msg so callers can inspect errors easily.
        try:
            code = str(raw.get("code", ""))
            msg = raw.get("msg")
            inner = raw.get("data")
            item: Dict[str, Any] = {}
            if isinstance(inner, dict):
                item = dict(inner)
            elif isinstance(inner, list) and inner:
                first = inner[0]
                if isinstance(first, dict):
                    item = dict(first)
            # Attach code/msg for callers to inspect
            if code:
                item.setdefault("code", code)
            if msg is not None:
                item.setdefault("msg", msg)
            return item or {"code": code, "msg": msg}
        except Exception:
            return {}

    async def place_order_algo(
        self,
        inst_id: str,
        side: str,
        *,
        margin_mode: str = "cross",
        position_side: str = "net",
        size: str = "-1",
        order_price: str | None = "-1",
        order_type: str = "trigger",
        trigger_price: str | None = None,
        trigger_price_type: str = "last",
        attach_tp_trigger_price: str | None = None,
        attach_tp_order_price: str | None = "-1",
        attach_sl_trigger_price: str | None = None,
        attach_sl_order_price: str | None = "-1",
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "instId": inst_id,
            "marginMode": margin_mode,
            "positionSide": position_side,
            "side": side,
            "size": size,
            "orderType": order_type,
        }
        if order_price is not None:
            body["orderPrice"] = str(order_price)
        if trigger_price is not None:
            body["triggerPrice"] = str(trigger_price)
            body["triggerPriceType"] = trigger_price_type
        attach = {}
        if attach_tp_trigger_price is not None:
            attach["tpTriggerPrice"] = str(attach_tp_trigger_price)
            if attach_tp_order_price is not None:
                attach["tpOrderPrice"] = str(attach_tp_order_price)
                attach["tpTriggerPriceType"] = trigger_price_type
        if attach_sl_trigger_price is not None:
            attach["slTriggerPrice"] = str(attach_sl_trigger_price)
            if attach_sl_order_price is not None:
                attach["slOrderPrice"] = str(attach_sl_order_price)
                attach["slTriggerPriceType"] = trigger_price_type
        if attach:
            body["attachAlgoOrders"] = [attach]
        data = await self._request("POST", "/api/v1/trade/order-algo", body=body, auth=True)
        return data.get("data") or {}

    async def cancel_order(self, order_id: str, inst_id: Optional[str] = None, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        body: Dict[str, Any] = {"orderId": order_id}
        if inst_id:
            body["instId"] = inst_id
        if client_order_id:
            body["clientOrderId"] = client_order_id
        data = await self._request("POST", "/api/v1/trade/cancel-order", body=body, auth=True)
        return data.get("data") or {}

    async def cancel_tpsl(self, inst_id: Optional[str] = None, tpsl_id: Optional[str] = None, client_order_id: Optional[str] = None) -> Dict[str, Any] | List[Dict[str, Any]]:
        # API supports batch cancel via list; we provide single-item convenience
        body: Any
        item: Dict[str, Any] = {}
        if inst_id:
            item["instId"] = inst_id
        if tpsl_id:
            item["tpslId"] = tpsl_id
        if client_order_id:
            item["clientOrderId"] = client_order_id
        body = [item] if item else []
        data = await self._request("POST", "/api/v1/trade/cancel-tpsl", body=body, auth=True)
        return data.get("data") or {}

    async def orders_pending(self, inst_id: Optional[str] = None, order_type: Optional[str] = None, state: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        q: Dict[str, Any] = {}
        if inst_id:
            q["instId"] = inst_id
        if order_type:
            q["orderType"] = order_type
        if state:
            q["state"] = state
        if limit:
            q["limit"] = str(limit)
        data = await self._request("GET", "/api/v1/trade/orders-pending", query=q or None, auth=True)
        return data.get("data") or []

    async def orders_tpsl_pending(self, inst_id: Optional[str] = None, tpsl_id: Optional[str] = None, client_order_id: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        q: Dict[str, Any] = {}
        if inst_id:
            q["instId"] = inst_id
        if tpsl_id:
            q["tpslId"] = tpsl_id
        if client_order_id:
            q["clientOrderId"] = client_order_id
        if limit:
            q["limit"] = str(limit)
        data = await self._request("GET", "/api/v1/trade/orders-tpsl-pending", query=q or None, auth=True)
        return data.get("data") or []

    async def fills_history(self, inst_id: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        q: Dict[str, Any] = {}
        if inst_id:
            q["instId"] = inst_id
        if limit:
            q["limit"] = str(limit)
        data = await self._request("GET", "/api/v1/trade/fills-history", query=q or None, auth=True)
        return data.get("data") or []

    async def get_price_range(self, inst_id: str, side: str) -> Dict[str, Any]:
        q = {"instId": inst_id, "side": side}
        data = await self._request("GET", "/api/v1/trade/order/price-range", query=q, auth=True)
        return data.get("data") or {}

    async def submit_bracket(
        self,
        *,
        inst_id: str,
        side: str,                 # "buy" or "sell" for entry
        size: str,                 # base size as string
        margin_mode: str = "cross",
        position_side: str = "net",
        # --- entry params ---
        entry_type: str = "market",   # "market" | "limit" | "trigger"
        price: str | None = None,      # required if entry_type == "limit"
        trigger_price: str | None = None,  # required if entry_type == "trigger"
        trigger_price_type: str = "last",
        # --- exits (tp/sl) ---
        tp_trigger: str | None = None,
        tp_order: str | None = "-1",  # -1 => market on trigger per BloFin
        sl_trigger: str | None = None,
        sl_order: str | None = "-1",
        # behavior
        client_order_id: str | None = None,
        attach_via_algo: bool = True,
        post_fill_delay_sec: float = 0.8,
    ) -> Dict[str, Any]:
        """
        Safe bracket submit for BloFin.
        Prefers a single ALGO order with attached TP/SL (atomic). If not using trigger
        entries, submits entry first and then places a TPSL strategy order, forcing
        reduceOnly on exits to prevent accidental openers that can flatten positions.
        """
        # --- Path 1: Use ALGO with attachAlgoOrders (best when using trigger-based entries)
        if attach_via_algo and entry_type == "trigger":
            return await self.place_order_algo(
                inst_id=inst_id,
                side=side,
                margin_mode=margin_mode,
                position_side=position_side,
                size=size,
                order_price="-1",                # market on trigger
                order_type="trigger",
                trigger_price=trigger_price,
                trigger_price_type=trigger_price_type,
                attach_tp_trigger_price=tp_trigger,
                attach_tp_order_price=tp_order,
                attach_sl_trigger_price=sl_trigger,
                attach_sl_order_price=sl_order,
            )

        # --- Path 2: Place entry first (market/limit), then TPSL strategy order
        # NOTE: We DO NOT include tp/sl fields in the entry request to avoid detached children.
        if entry_type not in {"market", "limit"}:
            raise ValueError("submit_bracket: entry_type must be 'market', 'limit', or use attach_via_algo with 'trigger'.")

        entry_order_type = "market" if entry_type == "market" else "limit"
        entry = await self.place_order(
            inst_id=inst_id,
            side=side,
            order_type=entry_order_type,
            size=size,
            price=price if entry_order_type == "limit" else None,
            margin_mode=margin_mode,
            position_side=position_side,
            reduce_only=None,                 # entry must NOT be reduceOnly
            client_order_id=client_order_id,
            tp_trigger_price=None,            # do NOT attach tp/sl here
            sl_trigger_price=None,
        )

        # Give the account/position snapshot a moment to reflect the fill
        try:
            if post_fill_delay_sec and post_fill_delay_sec > 0:
                await asyncio.sleep(post_fill_delay_sec)
        except Exception:
            pass

        # Submit TPSL strategy as a separate call, forcing reduceOnly
        # In net mode, size "-1" means auto-close up to full position on trigger.
        if tp_trigger is None and sl_trigger is None:
            # No exits requested; return entry result
            return entry

        tpsl = await self.place_tpsl(
            inst_id=inst_id,
            side=side,                        # same side; exchange interprets with reduceOnly+size
            margin_mode=margin_mode,
            position_side=position_side,
            size="-1",
            tp_trigger_price=tp_trigger,
            tp_order_price=tp_order,
            sl_trigger_price=sl_trigger,
            sl_order_price=sl_order,
            trigger_price_type=trigger_price_type,
            reduce_only=True,
            client_order_id=None,
        )

        # Bundle a concise response for callers
        return {"entry": entry, "tpsl": tpsl}
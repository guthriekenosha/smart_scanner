# smart_scanner/ws_universe.py
from __future__ import annotations
import asyncio, json, time
from typing import Any, Dict, List, Tuple, Optional
import fnmatch
import aiohttp

from .config import CONFIG
from .blofin_client import BlofinClient

_TICKER_FIELDS_LAST = (
    "last",
    "lastPrice",
    "lastPr",
    "lastPx",
    "lastTradedPx",
    "close",
    "c",
    "px",
)
_TICKER_FIELDS_QVOL = (
    # Prefer direct quote-volume keys first (OKX/Blofin style)
    "volCcy24h",
    # Common aliases across venues
    "volQuote",
    "quoteVol",
    "volCcyQuote",
    "vol24hQuote",
    "quoteVolume",
    # USD-denominated turnover if provided
    "volUsd24h",
    "turnoverUsd24h",
)


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _last_price(t: Dict[str, Any]) -> float:
    for k in _TICKER_FIELDS_LAST:
        if k in t:
            return _f(t[k])
    for k in ("askPrice", "askPx", "ask", "bestAsk", "a"):
        if k in t:
            return _f(t[k])
    for k in ("bidPrice", "bidPx", "bid", "bestBid", "b"):
        if k in t:
            return _f(t[k])
    return 0.0


def _qvol_with_key(t: Dict[str, Any]) -> tuple[float, str]:
    """
    Compute 24h quote volume and return the numeric value along with the
    source key used. Falls back to base-volume * last price.
    """
    for k in (
        *_TICKER_FIELDS_QVOL,
        "quoteVolume24h",
        "volUsd24h",
        "turnover24h",
        "turnoverUsd24h",
        "turnover",
        "notional24h",
        "value24h",
        "quoteTurnover",
    ):
        if k in t and t[k] is not None:
            return _f(t[k]), k
    # Base-volume fallbacks (we will multiply by last)
    base_val = 0.0
    base_key = ""
    for bk in (
        "volCurrency24h",  # Blofin docs: base currency volume
        "vol",
        "volume",
        "baseVolume",
        "vol24h",
        "baseVol24h",
        "volCcy",
        "volCcy24h",
        "amount24h",
        "amount",
    ):
        if bk in t and t[bk] is not None:
            base_val = _f(t[bk])
            base_key = bk
            break
    base = base_val
    last = _last_price(t)
    label = f"derived:{base_key or 'base'}*last"
    return base * last, label


def _qvol(t: Dict[str, Any]) -> float:
    v, _ = _qvol_with_key(t)
    return v


class WSTickersUniverse:
    def __init__(self):
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self._tickers: Dict[str, Dict[str, Any]] = {}
        self._ready = asyncio.Event()
        self._stop = False

    def is_ready(self) -> bool:
        # Consider "ready" when we’ve seen a decent number of instruments
        return len(self._tickers) >= max(20, CONFIG.top_symbols_by_quote_vol // 2)

    def tickers_seen_count(self) -> int:
        """
        How many distinct instruments have published a ticker since connect.
        """
        return len(self._tickers)

    # ---- Debug helpers ----
    def get_raw_ticker(self, inst: str) -> Optional[Dict[str, Any]]:
        return self._tickers.get(inst)

    def find_candidates(self, symbol: str) -> List[str]:
        """
        Return matching instrument ids for a human-entered symbol.
        Normalizes by stripping trailing -SWAP/-PERP for comparison.
        """
        def _norm(s: str) -> str:
            parts = s.split("-")
            if parts and parts[-1].upper() in {"SWAP", "PERP"}:
                parts = parts[:-1]
            return "-".join(parts).upper()

        target = _norm(symbol)
        exact = []
        partial = []
        for k in self._tickers.keys():
            if _norm(k) == target:
                exact.append(k)
            elif target in k.upper():
                partial.append(k)
        return exact or partial

    def gating_stats(self) -> Dict[str, int]:
        """
        Return counters for universe gating based on current WS tickers.
        Keys: total, usdt, price_ok, vol_ok, final
        """
        total = len(self._tickers)
        usdt = 0
        price_ok = 0
        vol_ok = 0
        items: List[Tuple[str, float]] = []
        for inst, t in self._tickers.items():
            parts = inst.split("-")
            if "USDT" not in parts:
                continue
            usdt += 1
            last = _last_price(t)
            if last < CONFIG.min_last_price:
                continue
            if CONFIG.max_last_price > 0 and last > CONFIG.max_last_price:
                continue
            price_ok += 1
            qv = _qvol(t)
            if qv < CONFIG.min_quote_vol_usdt:
                continue
            vol_ok += 1
            items.append((inst, qv))
        items.sort(key=lambda x: x[1], reverse=True)
        final = len(items[: max(10, CONFIG.top_symbols_by_quote_vol)])
        return {
            "total": total,
            "usdt": usdt,
            "price_ok": price_ok,
            "vol_ok": vol_ok,
            "final": final,
        }

    def top_qvol_samples(self, n: int = 10) -> List[Tuple[str, float, str, float]]:
        """
        Return top-N USDT instruments by computed quote volume for debug.
        Each entry is: (instId, qvol, source_key, last_price).
        Obeys basic price gate to match universe shaping.
        """
        items: List[Tuple[str, float, str, float]] = []
        for inst, t in self._tickers.items():
            parts = inst.split("-")
            if "USDT" not in parts:
                continue
            last = _last_price(t)
            if last < CONFIG.min_last_price:
                continue
            if CONFIG.max_last_price > 0 and last > CONFIG.max_last_price:
                continue
            qv, key = _qvol_with_key(t)
            items.append((inst, qv, key, last))
        items.sort(key=lambda x: x[1], reverse=True)
        return items[: max(1, n)]

    async def start(self):
        if self._task:
            return
        self._stop = False
        self._task = asyncio.create_task(self._run(), name="ws_universe")

    async def stop(self):
        self._stop = True
        if self._task:
            self._task.cancel()
            self._task = None
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None

    async def _run(self):
        while not self._stop:
            try:
                async with aiohttp.ClientSession() as session:
                    self._session = session
                    async with session.ws_connect(
                        CONFIG.ws_public_url, heartbeat=CONFIG.ws_ping_interval_sec
                    ) as ws:
                        self._ws = ws
                        # Subscribe to all tickers by instType to avoid REST dependency
                        try:
                            await ws.send_str(
                                json.dumps(
                                    {
                                        "op": "subscribe",
                                        "args": [
                                            {
                                                "channel": "tickers",
                                                "instType": CONFIG.ws_inst_type,
                                            }
                                        ],
                                    }
                                )
                            )
                        except Exception:
                            pass

                        # Bootstrap instruments once (best-effort) and subscribe by instId too
                        insts = await self._bootstrap_instruments()
                        # Keep USDT-quoted instruments (BTC-USDT, BTC-USDT-SWAP, ...)
                        def _is_usdt_market(inst: str) -> bool:
                            parts = inst.split("-")
                            return "USDT" in parts

                        inst_ids = [x for x in insts if _is_usdt_market(x)]
                        # subscribe in batches (100 per call is safe)
                        batch = 100
                        for i in range(0, len(inst_ids), batch):
                            args = [
                                {"channel": "tickers", "instId": sid}
                                for sid in inst_ids[i : i + batch]
                            ]
                            await ws.send_str(
                                json.dumps({"op": "subscribe", "args": args})
                            )
                            await asyncio.sleep(0.05)

                        # read loop
                        ping_at = time.time() + CONFIG.ws_ping_interval_sec
                        while not self._stop:
                            # keepalive: send text 'ping' as the docs suggest
                            now = time.time()
                            if now >= ping_at:
                                try:
                                    await ws.send_str("ping")
                                except Exception:
                                    break
                                ping_at = now + CONFIG.ws_ping_interval_sec

                            msg = await ws.receive(timeout=30.0)
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                m = msg.json(loads=json.loads)
                                # tickers update
                                if (
                                    isinstance(m, dict)
                                    and m.get("arg", {}).get("channel") == "tickers"
                                ):
                                    data = m.get("data") or []
                                    for t in data:
                                        inst = (
                                            t.get("instId")
                                            or t.get("instid")
                                            or t.get("symbol")
                                        )
                                        if not isinstance(inst, str):
                                            continue
                                        self._tickers[inst] = t
                                    if (
                                        not self._ready.is_set()
                                        and len(self._tickers) > 10
                                    ):
                                        self._ready.set()
                                # pong
                                elif m == "pong":
                                    pass
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.ERROR,
                            ):
                                break
            except Exception:
                # small backoff before reconnect
                await asyncio.sleep(1.5)

    async def _bootstrap_instruments(self) -> List[str]:
        # one REST call at startup (should be well within limits)
        async with BlofinClient() as c:
            try:
                items = await c.get_instruments(CONFIG.ws_inst_type)
            except Exception:
                items = await c.get_instruments()
        out = []
        for it in items:
            inst = it.get("instId") or it.get("symbol")
            if isinstance(inst, str):
                out.append(inst)
        # prepend fallback majors to speed up early readiness
        if CONFIG.fallback_universe:
            majors = [s for s in CONFIG.fallback_universe if s in out]
            # move majors to front
            uniq = list(dict.fromkeys(majors + out))
            return uniq
        return out

    def build_universe(self) -> List[str]:
        # rank by 24h quote vol with price and include/exclude gates
        items: List[Tuple[str, float]] = []
        for inst, t in self._tickers.items():
            parts = inst.split("-")
            if "USDT" not in parts:
                continue
            # Blue-chip base filter (by base asset name) if enabled
            if CONFIG.bluechip_only:
                try:
                    base = inst.split("-")[0].upper()
                except Exception:
                    continue
                if base not in {s.upper() for s in CONFIG.bluechip_bases}:
                    continue
            # SWAP-only gating if configured
            if CONFIG.swap_only:
                inst_type_val = t.get("instType") or t.get("inst_type") or t.get("type")
                if inst_type_val:
                    if str(inst_type_val).upper() != "SWAP":
                        continue
                else:
                    if "SWAP" not in [p.upper() for p in parts]:
                        continue
            last = _last_price(t)
            qv = _qvol(t)
            if last < CONFIG.min_last_price:
                continue
            if CONFIG.max_last_price > 0 and last > CONFIG.max_last_price:
                continue
            if qv < CONFIG.min_quote_vol_usdt:
                continue
            items.append((inst, qv))
        items.sort(key=lambda x: x[1], reverse=True)
        base = [x[0] for x in items[: max(10, CONFIG.top_symbols_by_quote_vol)]]

        # If nothing passed the volume gate, degrade to price-only ranking so WS can still feed
        if not base and self._tickers:
            price_rank: List[Tuple[str, float]] = []
            for inst, t in self._tickers.items():
                parts = inst.split("-")
                if "USDT" not in parts:
                    continue
                last = _last_price(t)
                if last < CONFIG.min_last_price:
                    continue
                if CONFIG.max_last_price > 0 and last > CONFIG.max_last_price:
                    continue
                price_rank.append((inst, last))
            price_rank.sort(key=lambda x: x[1], reverse=True)
            base = [x[0] for x in price_rank[: max(10, CONFIG.top_symbols_by_quote_vol)]]

        # include/exclude shaping
        if CONFIG.include_symbols:
            base = list(dict.fromkeys([*CONFIG.include_symbols, *base]))
        if CONFIG.exclude_symbols:
            base = [s for s in base if s not in CONFIG.exclude_symbols]
        # pattern excludes
        if CONFIG.exclude_patterns:
            base = [s for s in base if not any(fnmatch.fnmatch(s, p) for p in CONFIG.exclude_patterns)]

        # truncate to exactly top_symbols_by_quote_vol
        return base[: CONFIG.top_symbols_by_quote_vol]

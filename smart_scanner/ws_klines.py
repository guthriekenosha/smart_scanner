from __future__ import annotations
import asyncio
import json
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp

from .config import CONFIG
from .blofin_client import _normalize_bar


Row = List[str]  # ['ts','o','h','l','c','vol','volCcy','volQuote','confirm']


class _Series:
    def __init__(self, maxlen: int):
        self.maxlen = maxlen
        self._ts: List[int] = []  # ascending
        self._rows: Dict[int, Row] = {}

    def upsert(self, row: Row):
        try:
            ts = int(row[0])
        except Exception:
            return
        if ts in self._rows:
            self._rows[ts] = row
            return
        # insert keeping ascending order
        from bisect import bisect_right

        i = bisect_right(self._ts, ts)
        self._ts.insert(i, ts)
        self._rows[ts] = row
        # trim to capacity
        while len(self._ts) > self.maxlen:
            old = self._ts.pop(0)
            self._rows.pop(old, None)

    def last_n(self, n: int) -> List[Row]:
        if not self._ts:
            return []
        ts = self._ts[-n:]
        return [self._rows[t] for t in ts]

    def size(self) -> int:
        return len(self._ts)


def _ensure_row_shape(raw: List[Any]) -> Row:
    # Convert to strings and pad to 9 elements to mimic REST shape
    row = [str(x) for x in raw]
    if len(row) < 9:
        # pad volCcy, volQuote, confirm
        row += ["0"] * (9 - len(row))
    return row[:9]


class WSKlinesStore:
    """
    Maintains rolling kline bars per (symbol, timeframe) via WS.
    - Subscribes for requested (instId, bar) pairs
    - Stores up to CONFIG.ws_backfil_bars per pair
    - Exposes get_rows(symbol, timeframe, limit)
    """

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._task: Optional[asyncio.Task] = None

        self._series: Dict[Tuple[str, str], _Series] = {}
        self._maxlen = max(CONFIG.ws_backfil_bars, CONFIG.candles_limit)

        # subscription management
        self._desired: Set[Tuple[str, str]] = set()  # (instId, barNormalized)
        self._subscribed: Set[Tuple[str, str]] = set()
        self._pending: Set[Tuple[str, str]] = set()
        self._lock = asyncio.Lock()
        self._stop = False
        # event queue for closed bars: tuples (symbol, timeframe, ts)
        self._events: asyncio.Queue[Tuple[str, str, int]] = asyncio.Queue(maxsize=10000)

    async def start(self):
        if self._task:
            return
        self._stop = False
        self._task = asyncio.create_task(self._run(), name="ws_klines")

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

    async def ensure_subscriptions(self, symbols: List[str], timeframes: List[str]):
        """
        Ensure we are subscribed to given (symbol, timeframe) pairs.
        Subset symbols to your chosen cap before calling this.
        """
        bars = [_normalize_bar(tf) for tf in timeframes]
        desired = {(s, b) for s in symbols for b in bars}
        async with self._lock:
            # Remove no-longer-desired
            self._desired = desired
            # Queue new subs
            new = desired - self._subscribed
            self._pending |= new

        # Try flushing immediately if connected
        await self._flush_pending()

    def events(self) -> "asyncio.Queue[Tuple[str,str,int]]":
        return self._events

    async def _flush_pending(self):
        if not self._ws:
            return
        async with self._lock:
            if not self._pending:
                return
            pend = list(self._pending)
            self._pending.clear()
        # send in small batches; try both subscription styles for compatibility
        batch = 80
        for i in range(0, len(pend), batch):
            slice_pairs = pend[i : i + batch]
            args_a: List[Dict[str, Any]] = []  # channel "candle" with bar arg
            args_b: List[Dict[str, Any]] = []  # channel name including bar (e.g., candle1m)
            for inst, bar in slice_pairs:
                args_a.append({"channel": "candle", "instId": inst, "bar": bar})
                args_b.append({"channel": f"candle{bar}", "instId": inst})
            try:
                await self._ws.send_str(json.dumps({"op": "subscribe", "args": args_a}))
                await asyncio.sleep(0.02)
                await self._ws.send_str(json.dumps({"op": "subscribe", "args": args_b}))
                async with self._lock:
                    self._subscribed |= set(slice_pairs)
            except Exception:
                # If send fails, put back into pending for next connect
                async with self._lock:
                    self._pending |= set(slice_pairs)
                break
            await asyncio.sleep(0.08)

    def _series_for(self, inst: str, bar: str) -> _Series:
        key = (inst, bar)
        s = self._series.get(key)
        if s is None:
            s = _Series(self._maxlen)
            self._series[key] = s
        return s

    def get_rows(self, symbol: str, timeframe: str, limit: int) -> List[Row]:
        bar = _normalize_bar(timeframe)
        s = self._series.get((symbol, bar))
        if not s:
            return []
        return s.last_n(limit)

    def ingest_rows(self, symbol: str, timeframe: str, rows: List[Row]):
        """
        Seed or update the store for (symbol, timeframe) using historical rows
        (e.g., from REST). Ensures ascending time order and REST-like shape.
        """
        bar = _normalize_bar(timeframe)
        s = self._series_for(symbol, bar)
        seq = rows
        try:
            if len(seq) >= 2 and int(seq[1][0]) < int(seq[0][0]):
                seq = list(reversed(seq))
        except Exception:
            pass
        for r in seq:
            s.upsert(_ensure_row_shape(r))

    def key_sizes(self) -> Dict[Tuple[str, str], int]:
        return {k: v.size() for k, v in self._series.items()}

    async def _run(self):
        while not self._stop:
            try:
                async with aiohttp.ClientSession() as session:
                    self._session = session
                    async with session.ws_connect(
                        CONFIG.ws_public_url, heartbeat=CONFIG.ws_ping_interval_sec
                    ) as ws:
                        self._ws = ws
                        # reset subscriptions on new connection
                        async with self._lock:
                            self._subscribed.clear()
                            self._pending |= self._desired

                        await self._flush_pending()

                        ping_at = time.time() + CONFIG.ws_ping_interval_sec
                        while not self._stop:
                            # periodic ping (text)
                            now = time.time()
                            if now >= ping_at:
                                try:
                                    await ws.send_str("ping")
                                except Exception:
                                    break
                                ping_at = now + CONFIG.ws_ping_interval_sec

                            msg = await ws.receive(timeout=30.0)
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    m = msg.json(loads=json.loads)
                                except Exception:
                                    continue
                                if not isinstance(m, dict):
                                    continue
                                arg = m.get("arg") or {}
                                channel = arg.get("channel")
                                if channel and channel.startswith("candle"):
                                    inst = arg.get("instId") or arg.get("symbol")
                                    if not isinstance(inst, str):
                                        continue
                                    # derive bar
                                    bar = arg.get("bar")
                                    if not bar:
                                        # Okx-style: candle1m, candle15m, candle1H
                                        bar = channel.replace("candle", "") or "1m"
                                    bar = _normalize_bar(str(bar))

                                    data = m.get("data") or []
                                    series = self._series_for(inst, bar)
                                    for raw in data:
                                        row = _ensure_row_shape(raw)
                                        series.upsert(row)
                                        # emit event when confirmed/closed bar arrives
                                        try:
                                            confirm_val = str(row[-1]).lower()
                                            if confirm_val in ("1", "true", "yes"):
                                                ts = int(row[0])
                                                if not self._events.full():
                                                    self._events.put_nowait((inst, bar, ts))
                                        except Exception:
                                            pass
                                elif m == "pong":
                                    pass
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.ERROR,
                            ):
                                break
            except Exception:
                await asyncio.sleep(1.5)

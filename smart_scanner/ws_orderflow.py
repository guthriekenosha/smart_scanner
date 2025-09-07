from __future__ import annotations
import asyncio
import json
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

import aiohttp

from .config import CONFIG


class _TradeBuf:
    def __init__(self, window_sec: int):
        self.window = window_sec
        self.q: Deque[Tuple[float, float, int]] = deque()  # (ts, size, is_buy)
        self._last_ts: float = 0.0

    def add(self, ts_ms: int, size: float, side: str | None):
        is_buy = 1 if (side or "").lower() in ("buy", "buy_taker", "b") else 0
        ts = ts_ms / 1000.0
        self.q.append((ts, size, is_buy))
        self._last_ts = max(self._last_ts, ts)
        self._trim()

    def _trim(self):
        cutoff = time.time() - self.window
        while self.q and self.q[0][0] < cutoff:
            self.q.popleft()

    def stats(self) -> Dict[str, float]:
        self._trim()
        total_sz = sum(sz for _, sz, _ in self.q)
        buys = sum(sz for _, sz, b in self.q if b)
        sells = total_sz - buys
        n = len(self.q)
        per_min = n * (60.0 / max(self.window, 1))
        buy_ratio = buys / total_sz if total_sz > 0 else 0.0
        notional_per_min = (total_sz / max(self.window, 1.0)) * 60.0
        return {
            "taker_buy_ratio": buy_ratio,
            "trade_rate_per_min": per_min,
            "taker_buy_vol": buys,
            "taker_sell_vol": sells,
            "notional_per_min": notional_per_min,
            "trades_count": float(n),
            "last_trade_ts": float(self._last_ts),
            "trades_window": float(self.window),
        }


class WSOrderflowStore:
    """
    Maintains orderbook top-of-book + rolling trade stats per symbol via WS.
    - books: tracks best bid/ask, spread, top-N depth sums, imbalance
    - trades: rolling window taker buy/sell volume and trade rate
    """

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = False

        self._book: Dict[str, Dict[str, float]] = {}
        self._trades: Dict[str, _TradeBuf] = {}
        self._desired: List[str] = []
        self._lock = asyncio.Lock()
        self._seen_channels: set[str] = set()

    @staticmethod
    def _norm_inst(inst: str) -> str:
        s = str(inst).replace("_", "-")
        parts = s.split("-")
        if parts and parts[-1].upper() in {"SWAP", "PERP"}:
            parts = parts[:-1]
        return "-".join(parts)

    async def start(self):
        if self._task:
            return
        self._stop = False
        self._task = asyncio.create_task(self._run(), name="ws_orderflow")

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

    async def ensure_subscriptions(self, symbols: List[str]):
        async with self._lock:
            self._desired = list(symbols)
        await self._flush_subs()

    def snapshot(self, symbol: str) -> Dict[str, float]:
        out = {}
        b = self._book.get(symbol) or {}
        out.update(b)
        tb = self._trades.get(symbol)
        if tb:
            out.update(tb.stats())
        return out

    async def _flush_subs(self):
        if not self._ws:
            return
        async with self._lock:
            syms = list(self._desired)
        # subscribe books + trades in batches
        batch = 100
        for i in range(0, len(syms), batch):
            subs = syms[i : i + batch]
            # Try multiple styles to maximize WS compatibility
            args_books_a = [
                {"channel": f"books{CONFIG.ws_book_depth}", "instId": s, "instType": CONFIG.ws_inst_type}
                for s in subs
            ]
            args_books_b = [
                {
                    "channel": "books",
                    "instId": s,
                    "instType": CONFIG.ws_inst_type,
                    "depth": CONFIG.ws_book_depth,
                }
                for s in subs
            ]
            args_trades_a = [
                {"channel": "trades", "instId": s, "instType": CONFIG.ws_inst_type}
                for s in subs
            ]
            args_books_c = [
                {"channel": "books5", "instId": s, "instType": CONFIG.ws_inst_type}
                for s in subs
            ]
            args_books_d = [
                {"channel": "books50-l2-tbt", "instId": s, "instType": CONFIG.ws_inst_type}
                for s in subs
            ]
            args_trades_b = [
                {"channel": "trades-all", "instId": s, "instType": CONFIG.ws_inst_type}
                for s in subs
            ]
            try:
                # Per-instId books (style A: books{N})
                await self._ws.send_str(
                    json.dumps({"op": "subscribe", "args": args_books_a})
                )
                await asyncio.sleep(0.02)
                # Per-instId books (style B: books + depth)
                await self._ws.send_str(
                    json.dumps({"op": "subscribe", "args": args_books_b})
                )
                await asyncio.sleep(0.02)
                # Per-instId trades
                await self._ws.send_str(
                    json.dumps({"op": "subscribe", "args": args_trades_a})
                )
                await asyncio.sleep(0.02)
                # Alternate book/trade channels if supported
                await self._ws.send_str(
                    json.dumps({"op": "subscribe", "args": args_books_c})
                )
                await asyncio.sleep(0.02)
                await self._ws.send_str(
                    json.dumps({"op": "subscribe", "args": args_books_d})
                )
                await asyncio.sleep(0.02)
                await self._ws.send_str(
                    json.dumps({"op": "subscribe", "args": args_trades_b})
                )
            except Exception:
                break
            await asyncio.sleep(0.08)
        # Best-effort aggregate by instType (if server supports it)
        try:
            args_books_type = [
                {"channel": "books", "instType": CONFIG.ws_inst_type, "depth": CONFIG.ws_book_depth}
            ]
            await self._ws.send_str(
                json.dumps({"op": "subscribe", "args": args_books_type})
            )
            await asyncio.sleep(0.02)
        except Exception:
            pass
        try:
            args_trades_type = [
                {"channel": "trades", "instType": CONFIG.ws_inst_type}
            ]
            await self._ws.send_str(
                json.dumps({"op": "subscribe", "args": args_trades_type})
            )
        except Exception:
            pass

    async def _run(self):
        while not self._stop:
            try:
                async with aiohttp.ClientSession() as session:
                    self._session = session
                    async with session.ws_connect(
                        CONFIG.ws_public_url, heartbeat=CONFIG.ws_ping_interval_sec
                    ) as ws:
                        self._ws = ws
                        await self._flush_subs()
                        ping_at = time.time() + CONFIG.ws_ping_interval_sec
                        while not self._stop:
                            now = time.time()
                            if now >= ping_at:
                                try:
                                    await ws.send_str("ping")
                                except Exception:
                                    break
                                ping_at = now + CONFIG.ws_ping_interval_sec
                            msg = await ws.receive(timeout=30.0)
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                if msg.type in (
                                    aiohttp.WSMsgType.CLOSED,
                                    aiohttp.WSMsgType.ERROR,
                                ):
                                    break
                                continue
                            try:
                                m = msg.json(loads=json.loads)
                            except Exception:
                                continue
                            if not isinstance(m, dict):
                                continue
                            arg = m.get("arg") or {}
                            channel = arg.get("channel")
                            if not channel:
                                continue
                            if CONFIG.print_of_debug and channel and channel not in self._seen_channels:
                                self._seen_channels.add(channel)
                                print(f"[of/ws] channel seen: {channel}")
                            if channel.startswith("books"):
                                raw = m.get("data")
                                # BloFin pushes books as an object {asks:[..], bids:[..], ts, ...}
                                data_list = [raw] if isinstance(raw, dict) else (raw or [])
                                for d in data_list:
                                    inst_raw = (
                                        d.get("instId")
                                        or d.get("symbol")
                                        or arg.get("instId")
                                        or arg.get("symbol")
                                    )
                                    inst = self._norm_inst(inst_raw) if isinstance(inst_raw, str) else None
                                    if not isinstance(inst, str):
                                        continue
                                    try:
                                        bids = d.get("bids") or d.get("b") or d.get("bid") or []
                                        asks = d.get("asks") or d.get("a") or d.get("ask") or []
                                        # entries like [px, sz, ...]
                                        def _sum(side):
                                            tot_px = 0.0
                                            tot_sz = 0.0
                                            for e in side[: CONFIG.ws_book_depth]:
                                                px = float(e[0])
                                                sz = float(e[1])
                                                tot_px += px * sz
                                                tot_sz += sz
                                            return tot_px, tot_sz

                                        bpx = float(bids[0][0]) if bids else 0.0
                                        apx = float(asks[0][0]) if asks else 0.0
                                        mid = (bpx + apx) / 2.0 if (bpx > 0 and apx > 0) else 0.0
                                        spread_bps = (
                                            ((apx - bpx) / mid * 10000.0) if mid > 0 else 0.0
                                        )
                                        b_notional, bsz = _sum(bids)
                                        a_notional, asz = _sum(asks)
                                        imb = (bsz - asz) / max(bsz + asz, 1e-9)
                                        self._book[inst] = {
                                            "spread_bps": float(spread_bps),
                                            "best_bid": float(bpx),
                                            "best_ask": float(apx),
                                            "depth_bid_sz": float(bsz),
                                            "depth_ask_sz": float(asz),
                                            "depth_bid_notional": float(b_notional),
                                            "depth_ask_notional": float(a_notional),
                                            "depth_imbalance": float(imb),
                                            "book_ts": float(time.time()),
                                        }
                                    except Exception:
                                        pass
                            elif channel == "trades" or channel.startswith("trades"):
                                data = m.get("data") or []
                                for d in data:
                                    inst_raw = (
                                        d.get("instId")
                                        or d.get("symbol")
                                        or arg.get("instId")
                                        or arg.get("symbol")
                                    )
                                    inst = self._norm_inst(inst_raw) if isinstance(inst_raw, str) else None
                                    if not isinstance(inst, str):
                                        continue
                                    try:
                                        px = float(d.get("px") or d.get("price") or d.get("p") or 0.0)
                                        sz = float(d.get("sz") or d.get("size") or d.get("s") or 0.0)
                                        side = d.get("side") or d.get("S") or d.get("sd")
                                        ts = int(d.get("ts") or d.get("timestamp") or d.get("t") or time.time() * 1000)
                                        tb = self._trades.get(inst)
                                        if tb is None:
                                            tb = _TradeBuf(CONFIG.orderflow_window_sec)
                                            self._trades[inst] = tb
                                        tb.add(ts, sz * px, side)  # use notional size for ratios
                                    except Exception:
                                        pass
            except Exception:
                await asyncio.sleep(1.5)

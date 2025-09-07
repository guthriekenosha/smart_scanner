"""
Simple Bot: EMA Cross

Creates minimal trading logic independent of the scanner strategies.
- Builds a liquid universe (reuses scanner's universe builder)
- Fetches candles for a chosen timeframe
- Detects EMA(21)/EMA(50) cross events
- Emits Signal(s) and routes them to AutoTrader

Run (once):
  python -m smart_scanner.simple_bot

Run (loop):
  python -m smart_scanner.simple_bot --loop

Env knobs (optional):
  BOT_TIMEFRAME=5m
  BOT_EMA_FAST=21
  BOT_EMA_SLOW=50
  BOT_SCORE=4.2
  BOT_PROB=0.65

AutoTrader gates apply (score/prob/cooldowns/exposure). Configure in .env.
"""

from __future__ import annotations
import asyncio
import os
from typing import Any, Dict, List, Tuple

import numpy as np

from .blofin_client import BlofinClient
from .config import CONFIG
from .indicators import ema
from .signal_types import Signal
from .trader import GLOBAL_TRADER
from .metrics import emit as emit_metric
from .scanner_runner import _get_universe  # reuse robust universe builder


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


BOT_TIMEFRAME = _env_str("BOT_TIMEFRAME", "5m")
BOT_EMA_FAST = _env_int("BOT_EMA_FAST", 21)
BOT_EMA_SLOW = _env_int("BOT_EMA_SLOW", 50)
BOT_SCORE = _env_float("BOT_SCORE", 4.2)
BOT_PROB = _env_float("BOT_PROB", 0.65)


async def _fetch_candles_chunk(
    client: BlofinClient, symbols: List[str], timeframe: str
) -> Dict[Tuple[str, str], List[List[str]]]:
    out: Dict[Tuple[str, str], List[List[str]]] = {}
    sem = asyncio.Semaphore(CONFIG.candles_concurrency)

    async def _one(sym: str):
        rows: List[List[str]] = []
        try:
            rows = await client.get_candles(sym, timeframe, limit=CONFIG.candles_limit)
        except Exception:
            rows = []
        if rows:
            out[(sym, timeframe)] = rows

    tasks = []
    for s in symbols:
        tasks.append(asyncio.create_task(_guarded(_one, s, sem)))
    await asyncio.gather(*tasks)
    return out


async def _guarded(fn, arg, sem: asyncio.Semaphore):
    async with sem:
        return await fn(arg)


def _cross_signal(o: np.ndarray, h: np.ndarray, l: np.ndarray, c: np.ndarray) -> Tuple[bool, str]:
    if len(c) < max(BOT_EMA_SLOW + 2, 10):
        return False, "buy"
    ef = ema(c, BOT_EMA_FAST)
    es = ema(c, BOT_EMA_SLOW)
    # Use last two bars to detect a fresh cross
    now_up = ef[-1] > es[-1]
    prev_up = ef[-2] > es[-2]
    if now_up and not prev_up:
        return True, "buy"
    if (not now_up) and prev_up:
        return True, "sell"
    return False, "buy"


def _build_signal(symbol: str, timeframe: str, c: np.ndarray, side: str) -> Signal:
    price = float(c[-1])
    # Small magnitude bonus based on EMA gap
    # Note: We recompute small arrays here for simplicity; cost is negligible vs IO
    ef = ema(c, BOT_EMA_FAST)
    es = ema(c, BOT_EMA_SLOW)
    gap = float(abs(ef[-1] - es[-1]) / max(c[-1], 1e-9))
    score = float(BOT_SCORE + min(1.2, max(0.0, gap * 30.0)))
    prob = float(min(0.95, BOT_PROB + min(0.12, gap * 10.0)))
    ev = (prob * 1.0) - ((1 - prob) * 0.5)
    return Signal(
        symbol=symbol,
        timeframe=timeframe,
        side=side,
        score=score,
        price=price,
        components=["ema_cross"],
        prob=prob,
        ev=ev,
        confirmation="confirmed",
        tags=["ema_cross", "trend_up" if side == "buy" else "trend_down"],
        level=None,
        meta={"source": "simple_bot", "fast": BOT_EMA_FAST, "slow": BOT_EMA_SLOW},
    )


async def run_once(client: BlofinClient) -> List[Signal]:
    # Get universe from the scanner's WS + fallbacks
    uni, src = await _get_universe(client)
    if not uni:
        return []
    # Fetch candles for our bot timeframe
    data_map = await _fetch_candles_chunk(client, uni, BOT_TIMEFRAME)
    out: List[Signal] = []
    for (sym, tf), rows in data_map.items():
        if not rows or len(rows) < max(BOT_EMA_SLOW + 2, 60):
            continue
        # ensure ascending order
        try:
            if int(rows[1][0]) < int(rows[0][0]):
                rows = list(reversed(rows))
        except Exception:
            pass
        # parse arrays
        o = np.array([float(x[1]) for x in rows], dtype=float)
        h = np.array([float(x[2]) for x in rows], dtype=float)
        l = np.array([float(x[3]) for x in rows], dtype=float)
        c = np.array([float(x[4]) for x in rows], dtype=float)

        fresh, side = _cross_signal(o, h, l, c)
        if not fresh:
            continue
        sig = _build_signal(sym, tf, c, side)
        out.append(sig)
    return out


async def loop_forever():
    print(f"[bot] timeframe={BOT_TIMEFRAME} fast={BOT_EMA_FAST} slow={BOT_EMA_SLOW}")
    async with BlofinClient() as client:
        while True:
            try:
                sigs = await run_once(client)
                if not sigs:
                    print("[bot] No signals this round.")
                for s in sigs:
                    print(f"[bot] {s.symbol} {s.timeframe} {s.side} score={s.score:.2f} prob={s.prob:.2f} price={s.price:.6g}")
                    try:
                        emit_metric("signal", s.__dict__)
                    except Exception:
                        pass
                    try:
                        GLOBAL_TRADER.on_signal(s, client)
                    except Exception:
                        pass
            except Exception as e:
                print(f"[bot/ERR] {type(e).__name__}: {e}")
            await asyncio.sleep(CONFIG.loop_sleep_sec)


def main():
    import argparse

    p = argparse.ArgumentParser(description="Simple EMA Cross Bot")
    p.add_argument("--loop", action="store_true", help="Run continuous loop")
    args = p.parse_args()
    if args.loop:
        asyncio.run(loop_forever())
    else:
        async def _once():
            async with BlofinClient() as client:
                sigs = await run_once(client)
                for s in sigs:
                    print(s.to_json())
                    try:
                        GLOBAL_TRADER.on_signal(s, client)
                    except Exception:
                        pass
        asyncio.run(_once())


if __name__ == "__main__":
    main()


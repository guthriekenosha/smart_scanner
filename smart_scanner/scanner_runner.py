"""
Main scanner:
- Build the trading universe (prefers WebSocket public tickers, avoids REST tickers)
- Pull candles in chunks (rate-limit friendly)
- Evaluate signals per timeframe
- Keep top-N per timeframe, dedupe across TFs (best per symbol), cap per loop
- Optional JSONL output for ingestion
"""

from __future__ import annotations
import asyncio
import json
import os
import sys
import time
from typing import Any, Dict, List, Tuple
import fnmatch

from .config import CONFIG
from .blofin_client import BlofinClient  # REST client (still used for candles)
from .pretty import PR
from .signal_engine import evaluate_symbol_timeframe
from .lifecycle import dedupe, on_cooldown, touch_cooldown
from .signal_types import Signal
from .metrics import emit as emit_metric
from .bandit import GLOBAL_BANDIT
from .labeler import GLOBAL_LABELER
from .trader import GLOBAL_TRADER
from .risk_manager import GLOBAL_RISK_MANAGER
from .emergency import kill_active, panic_flatten


# WS-driven universe (no REST tickers)
from .ws_universe import WSTickersUniverse, _qvol_with_key
from .ws_klines import WSKlinesStore
from .ws_orderflow import WSOrderflowStore
# from .ws_private import BlofinPrivateWS as PrivateWS

# --------------------------- WS Universe --------------------------------------

_ws_uni = WSTickersUniverse()
_ws_started = False
_ws_kl = WSKlinesStore()
_ws_kl_started = False
_ws_of = WSOrderflowStore()
_ws_of_started = False
# _ws_priv = PrivateWS()
_ws_priv_started = False
_bandit_last_save_ts = 0.0
_exposure_last_refresh_ts = 0.0


class _BTC15mCache:
    def __init__(self):
        self._ts = 0.0
        self._data: List[float] = []

    async def get(self, client: BlofinClient, ttl_sec: float = 45.0) -> List[float]:
        now = time.time()
        if (now - self._ts) > ttl_sec or not self._data:
            self._data = await _fetch_btc_15m_close(client)
            self._ts = now
        return self._data


_btc_cache = _BTC15mCache()

# --------------------------- Utilities ---------------------------------------


def _fmt_list(items: List[str], n: int = 6) -> str:
    if len(items) <= n:
        return str(items)
    return str(items[:n] + ["..."])


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _get_last_price(t: Dict[str, Any]) -> float:
    for k in (
        "last",
        "lastPrice",
        "lastPr",
        "lastPx",
        "lastTradedPx",
        "close",
        "c",
        "px",
    ):
        if k in t:
            return _f(t[k])
    bid = _f(t.get("bidPrice") or t.get("bidPx") or t.get("bid"), 0.0)
    ask = _f(t.get("askPrice") or t.get("askPx") or t.get("ask"), 0.0)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return 0.0


def _get_quote_vol(t: Dict[str, Any]) -> float:
    # Try common quote-volume fields (various APIs)
    for k in (
        # Prefer direct quote-volume keys first
        "volCcy24h",
        # Common aliases across venues
        "volQuote",
        "quoteVol",
        "volCcyQuote",
        "vol24hQuote",
        "quoteVolume",
        "quoteVolume24h",
        # USD-denominated turnover
        "volUsd24h",
        "turnoverUsd24h",
        "turnover24h",
        # Other notional synonyms
        "turnover",
        "notional24h",
        "value24h",
        "quoteTurnover",
    ):
        if k in t and t[k] is not None:
            return _f(t[k])
    # Fallback: compute from base-volume * last price
    base = _f(
        t.get("volCurrency24h")
        or t.get("vol")
        or t.get("volume")
        or t.get("baseVolume")
        or t.get("vol24h")
        or t.get("baseVol24h")
        or t.get("volCcy")
        or t.get("volCcy24h")
        or t.get("amount24h")
        or t.get("amount")
    )
    last = _get_last_price(t)
    return base * last


def _select_universe(
    tickers: List[Dict[str, Any]], include: List[str], exclude: List[str], k: int
) -> List[str]:
    """
    REST fallback universe selection (kept for completeness).
    Filters by -USDT suffix, min price, min quote vol; sorts by liquidity.
    """
    items = []
    def _is_usdt_market(inst: str) -> bool:
        parts = inst.split("-")
        return "USDT" in parts

    def _is_swap(inst: str, inst_type_val: Any) -> bool:
        if not CONFIG.swap_only:
            return True
        t = str(inst_type_val or "").upper()
        if t:
            return t == "SWAP"
        parts = inst.split("-")
        return "SWAP" in [p.upper() for p in parts]

    def _excluded(inst: str) -> bool:
        # explicit excludes win
        if inst in set(exclude):
            return True
        # pattern excludes from config
        for pat in CONFIG.exclude_patterns:
            if fnmatch.fnmatch(inst, pat):
                return True
        return False

    def _bluechip_ok(inst: str) -> bool:
        if not CONFIG.bluechip_only:
            return True
        try:
            base = inst.split("-")[0].upper()
        except Exception:
            return False
        return base in set(s.upper() for s in CONFIG.bluechip_bases)

    for t in tickers:
        inst = t.get("instId") or t.get("instid") or t.get("symbol")
        inst_type_val = t.get("instType") or t.get("inst_type") or t.get("type")
        if not inst or not isinstance(inst, str):
            continue
        if not _is_usdt_market(inst):
            continue
        if not _is_swap(inst, inst_type_val):
            continue
        if _excluded(inst):
            continue
        if not _bluechip_ok(inst):
            continue
        last = _get_last_price(t)
        qv = _get_quote_vol(t)
        if last < CONFIG.min_last_price:
            continue
        if CONFIG.max_last_price > 0 and last > CONFIG.max_last_price:
            continue
        if qv < CONFIG.min_quote_vol_usdt:
            continue
        items.append((inst, qv))
    items.sort(key=lambda x: x[1], reverse=True)
    base = [x[0] for x in items[: max(10, k)]]

    if include:
        base = list(dict.fromkeys([*include, *base]))
    if exclude:
        base = [s for s in base if s not in set(exclude)]
    # Apply pattern excludes again after shaping, just in case
    base = [s for s in base if not _excluded(s)]
    return base[:k]


async def _fetch_btc_15m_close(client: BlofinClient) -> List[float]:
    rows = await client.get_candles(
        "BTC-USDT", "15m", limit=min(400, CONFIG.candles_limit)
    )
    if not rows:
        return []
    try:
        if int(rows[1][0]) < int(rows[0][0]):
            rows = list(reversed(rows))
    except Exception:
        rows = list(reversed(rows))
    return [float(r[4]) for r in rows]


async def _fetch_candles_chunk(
    client: BlofinClient,
    symbols: List[str],
    timeframe: str,
) -> Dict[Tuple[str, str], List[List[str]]]:
    out: Dict[Tuple[str, str], List[List[str]]] = {}
    missing: List[str] = []

    # Prefer WS klines if enabled; fall back to REST for gaps
    if CONFIG.use_ws_candles:
        for sym in symbols:
            rows = _ws_kl.get_rows(sym, timeframe, CONFIG.candles_limit)
            if rows and len(rows) >= 60:
                out[(sym, timeframe)] = rows
            else:
                missing.append(sym)
    else:
        missing = list(symbols)

    if missing:
        if CONFIG.print_diagnostics:
            try:
                print(
                    f"[fetch] {timeframe}: WS={len(out)} ready, REST missing={len(missing)}"
                )
            except Exception:
                pass
        sem = asyncio.Semaphore(CONFIG.candles_concurrency)
        done = 0
        total_rest = len(missing)
        bar_width = 28

        # throttle progress updates roughly every ~10% or 0.5s
        bar_step = max(1, total_rest // 10)
        last_update_ts = 0.0

        async def _one(sym: str):
            nonlocal done, last_update_ts
            async with sem:
                try:
                    rows = await client.get_candles(
                        sym, timeframe, limit=CONFIG.candles_limit
                    )
                    if rows:
                        out[(sym, timeframe)] = rows
                        if CONFIG.use_ws_candles:
                            try:
                                _ws_kl.ingest_rows(sym, timeframe, rows)
                            except Exception:
                                pass
                except Exception:
                    pass
                if CONFIG.print_diagnostics:
                    done += 1
                    try:
                        now = time.time()
                        if (done % bar_step == 0) or (now - last_update_ts >= 0.5) or (done >= total_rest):
                            filled = int(bar_width * done / max(total_rest, 1))
                            bar = "#" * filled + "-" * (bar_width - filled)
                            sys.stdout.write(
                                f"\r[fetch] {timeframe}: [{bar}] REST {done}/{total_rest} | WS {len(out)} ready"
                            )
                            sys.stdout.flush()
                            last_update_ts = now
                    except Exception:
                        pass

        tasks = [_one(s) for s in missing]
        await asyncio.gather(*tasks)
        if CONFIG.print_diagnostics:
            try:
                sys.stdout.write("\n")
                sys.stdout.flush()
            except Exception:
                pass
    return out


# --------------------------- Universe loading --------------------------------


# Optional: disk cache for the universe in case WS is cold on startup
def _load_universe_from_disk() -> List[str]:
    try:
        if os.path.exists(CONFIG.universe_cache_path):
            with open(CONFIG.universe_cache_path, "r") as f:
                data = json.load(f)
            if isinstance(data, list):
                return [s for s in data if isinstance(s, str)]
    except Exception:
        pass
    return []


def _save_universe_to_disk(symbols: List[str]) -> None:
    try:
        with open(CONFIG.universe_cache_path, "w") as f:
            json.dump(symbols, f)
    except Exception:
        pass


async def _get_universe(client: BlofinClient | None = None) -> Tuple[List[str], str]:
    """
    Primary path: build from WS tickers.
    Fallback: disk cache; final fallback: static majors.
    """
    # Ensure WS task is running
    global _ws_started
    if CONFIG.universe_from_ws and not _ws_started:
        await _ws_uni.start()
        _ws_started = True
    # Start private WS for orders/positions if trading enabled (optional)
    global _ws_priv_started
    # if CONFIG.enable_autotrade and not CONFIG.paper_trading and CONFIG.enable_private_ws and not _ws_priv_started:
    #     try:
    #         await _ws_priv.start()
    #         _ws_priv_started = True
    #     except Exception:
    #         pass

    universe: List[str] = []
    source: str = "none"
    if CONFIG.universe_from_ws:
        # Give WS a brief moment to accumulate
        t0 = time.time()
        while not _ws_uni.is_ready() and (time.time() - t0) < max(
            1.0, CONFIG.ws_universe_ready_wait_sec + 2.0
        ):
            await asyncio.sleep(0.2)
        universe = _ws_uni.build_universe()
        if universe:
            source = "ws"

    if not universe:
        # disk cache
        universe = _load_universe_from_disk()
        if universe and source == "none":
            source = "disk"

    # Try REST tickers to build/refresh universe if needed (even if disk had a small set)
    if not universe or len(universe) < max(10, CONFIG.top_symbols_by_quote_vol // 2):
        try:
            if client is None:
                async with BlofinClient() as c:
                    tickers = await c.get_tickers(CONFIG.ws_inst_type)
            else:
                tickers = await client.get_tickers(CONFIG.ws_inst_type)
            from_rest = _select_universe(
                tickers,
                list(CONFIG.include_symbols),
                list(CONFIG.exclude_symbols),
                CONFIG.top_symbols_by_quote_vol,
            )
            if from_rest:
                universe = from_rest
                source = "rest"
            # Fallback: try SPOT if SWAP yielded few/no symbols
            if not universe or len(universe) < max(10, CONFIG.top_symbols_by_quote_vol // 2):
                try:
                    if client is None:
                        async with BlofinClient() as c:
                            tickers2 = await c.get_tickers("SPOT")
                    else:
                        tickers2 = await client.get_tickers("SPOT")
                    from_rest2 = _select_universe(
                        tickers2,
                        list(CONFIG.include_symbols),
                        list(CONFIG.exclude_symbols),
                        CONFIG.top_symbols_by_quote_vol,
                    )
                    if from_rest2:
                        universe = from_rest2
                        source = "rest"
                except Exception:
                    pass
        except Exception:
            # keep whatever we have
            pass

    if not universe:
        # static majors as final safety net
        universe = list(CONFIG.fallback_universe)
        source = "static"

    # Persist what we’ll use this round (helps next cold start)
    _save_universe_to_disk(universe)
    return universe, source


# --------------------------- Core scan ---------------------------------------


async def scan_once(client: BlofinClient | None = None) -> List[Signal]:
    # Allow caller to pass a shared client to avoid reopening sessions each cycle
    owns_client = client is None
    ctx: BlofinClient | None = None
    client_cm: Any | None = None
    if owns_client:
        ctx = BlofinClient()
    try:
        if owns_client and ctx is not None:
            client_cm = ctx
            client = await client_cm.__aenter__()
        assert client is not None
        universe, uni_src = await _get_universe(client)
        if CONFIG.print_liquidity_debug:
            print(
                f"[liquidity] Universe (active): {len(universe)} (example: {_fmt_list(universe, 12)}) [source={uni_src}]"
            )
            # Also show how many WS tickers we've seen so far
            try:
                if CONFIG.universe_from_ws:
                    print(f"[ws] tickers seen: {_ws_uni.tickers_seen_count()}")
                    # Gating counters from WS to help diagnose why universe is small
                    stats = _ws_uni.gating_stats()
                    print(
                        f"[ws] gating: total={stats['total']} usdt={stats['usdt']} price_ok={stats['price_ok']} vol_ok={stats['vol_ok']} final={stats['final']}"
                    )
                    # Show top-by-quote-volume samples for debug (key used + value)
                    try:
                        top = _ws_uni.top_qvol_samples(8)
                        if top:
                            def _fmt(v: float) -> str:
                                # compact human-readable number
                                if v >= 1e9:
                                    return f"{v/1e9:.1f}B"
                                if v >= 1e6:
                                    return f"{v/1e6:.1f}M"
                                if v >= 1e3:
                                    return f"{v/1e3:.1f}K"
                                return f"{v:.0f}"

                            parts = [f"{inst}:{_fmt(qv)}({key})" for inst, qv, key, _ in top]
                            print("[ws] top qvol: " + ", ".join(parts))
                    except Exception:
                        pass
            except Exception:
                pass

        # Start WS klines and subscribe to the active universe (capped)
        global _ws_kl_started
        if CONFIG.use_ws_candles and not _ws_kl_started:
            await _ws_kl.start()
            _ws_kl_started = True
        if CONFIG.use_ws_candles:
            sub_symbols = universe[: CONFIG.ws_max_symbols]
            try:
                await _ws_kl.ensure_subscriptions(sub_symbols, list(CONFIG.timeframes))
            except Exception:
                pass
            # Small status line per loop: how many pairs are ready per timeframe
            try:
                total = len(sub_symbols)
                if total > 0:
                    parts: List[str] = []
                    for tf in CONFIG.timeframes:
                        ready = 0
                        for sym in sub_symbols:
                            rows = _ws_kl.get_rows(sym, tf, 60)
                            if rows and len(rows) >= 60:
                                ready += 1
                        parts.append(f"{tf}:{ready}/{total}")
                    print("[ws] klines ready: " + ", ".join(parts))
            except Exception:
                pass

            # Ensure orderflow WS is running and subscribed
            global _ws_of_started
            if CONFIG.use_ws_orderflow and not _ws_of_started:
                try:
                    await _ws_of.start()
                    _ws_of_started = True
                except Exception:
                    pass
            if CONFIG.use_ws_orderflow:
                try:
                    await _ws_of.ensure_subscriptions(universe[: CONFIG.ws_max_symbols])
                except Exception:
                    pass

            # Orderflow gating summary (diagnostic)
            try:
                if CONFIG.print_liquidity_debug and CONFIG.use_ws_orderflow:
                    subs = universe[: CONFIG.ws_max_symbols]
                    total = len(subs)
                    if total > 0:
                        now = time.time()
                        book = book_fresh = trades_ready = trades_fresh = 0
                        spread_ok = spread_seen = 0
                        rate_ok = rate_seen = 0
                        np_ok = np_seen = 0
                        depth_ok = depth_seen = 0
                        for sym in subs:
                            of = _ws_of.snapshot(sym)
                            if not of:
                                continue
                            has_book = bool(of.get("best_bid") and of.get("best_ask"))
                            if has_book:
                                book += 1
                                if (now - float(of.get("book_ts", 0.0))) <= CONFIG.of_ready_max_age_sec:
                                    book_fresh += 1
                            trc = float(of.get("trades_count", 0.0))
                            if trc:
                                if trc >= CONFIG.of_ready_min_trades:
                                    trades_ready += 1
                                if (now - float(of.get("last_trade_ts", 0.0))) <= CONFIG.of_ready_max_age_sec:
                                    trades_fresh += 1
                            sp_val = of.get("spread_bps")
                            if sp_val is not None:
                                spread_seen += 1
                                if float(sp_val) <= CONFIG.max_spread_bps or CONFIG.max_spread_bps <= 0:
                                    spread_ok += 1
                            tr_val = of.get("trade_rate_per_min")
                            if tr_val is not None:
                                rate_seen += 1
                                if float(tr_val) >= CONFIG.min_trades_per_min:
                                    rate_ok += 1
                            npmin = of.get("notional_per_min")
                            if npmin is not None:
                                np_seen += 1
                                if float(npmin) >= CONFIG.min_notional_per_min_usd:
                                    np_ok += 1
                            dbn = of.get("depth_bid_notional")
                            dan = of.get("depth_ask_notional")
                            if dbn is not None and dan is not None:
                                depth_seen += 1
                                if (
                                    float(dbn) >= CONFIG.min_depth_notional_usd
                                    and float(dan) >= CONFIG.min_depth_notional_usd
                                ):
                                    depth_ok += 1
                        print(
                            f"[of] gating: book={book_fresh}/{total} trades={trades_ready}/{total} spread_ok={spread_ok}/{spread_seen} rate_ok={rate_ok}/{rate_seen} npmin_ok={np_ok}/{np_seen} depth_ok={depth_ok}/{depth_seen}"
                        )
            except Exception:
                pass

        btc_15m_close = await _btc_cache.get(client)

        per_tf_bests: Dict[str, List[Signal]] = {}
        diag_total_candidates = 0
        diag_kept_after_tf_topN = 0

        for tf in CONFIG.timeframes:
            tf_candidates: List[Signal] = []
            # chunked candles fetch
            for i in range(0, len(universe), CONFIG.candles_chunk):
                batch = universe[i : i + CONFIG.candles_chunk]
                if not batch:
                    continue
                data_map = await _fetch_candles_chunk(client, batch, tf)
                for (sym, timeframe), rows in data_map.items():
                    cd_key = f"{sym}|{timeframe}"
                    if on_cooldown(cd_key, CONFIG.cooldown_sec):
                        continue
                    # Optional qVol gate at signal time to avoid low-liquidity prints
                    ticker = None
                    last_price: float | None = None
                    try:
                        ticker = _ws_uni.get_raw_ticker(sym)
                        if ticker is None:
                            ticker = _get_cached_ticker(sym)
                        if CONFIG.signal_min_qvol_usdt > 0:
                            if ticker is not None:
                                qv, _ = _qvol_with_key(ticker)
                                if qv < CONFIG.signal_min_qvol_usdt:
                                    continue
                        # Optional max price gate at signal time
                        if CONFIG.max_last_price > 0:
                            if ticker is not None:
                                last_price = _get_last_price(ticker)
                                if last_price > CONFIG.max_last_price:
                                    continue
                        if last_price is None and ticker is not None:
                            last_price = _get_last_price(ticker)
                    except Exception:
                        pass
                    # orderflow gating (symbol-level)
                    extra_feats = None
                    if CONFIG.use_ws_orderflow:
                        of = _ws_of.snapshot(sym)
                        extra_feats = of
                        # Strict readiness gate (optional)
                        if CONFIG.orderflow_strict:
                            has_book = of.get("best_bid") and of.get("best_ask")
                            trades_ready = (
                                float(of.get("trades_count", 0.0)) >= CONFIG.of_ready_min_trades
                            )
                            fresh_book = (
                                (time.time() - float(of.get("book_ts", 0.0)))
                                <= CONFIG.of_ready_max_age_sec
                            )
                            fresh_trades = (
                                (time.time() - float(of.get("last_trade_ts", 0.0)))
                                <= CONFIG.of_ready_max_age_sec
                            )
                            if not (has_book and trades_ready and fresh_book and fresh_trades):
                                continue
                        # Apply value thresholds when present
                        if CONFIG.max_spread_bps > 0:
                            sp_val = of.get("spread_bps") if of else None
                            if sp_val is not None and float(sp_val) > CONFIG.max_spread_bps:
                                continue
                        if CONFIG.min_trades_per_min > 0:
                            tr_val = of.get("trade_rate_per_min") if of else None
                            if tr_val is not None and float(tr_val) < CONFIG.min_trades_per_min:
                                continue
                        if CONFIG.min_notional_per_min_usd > 0:
                            npmin = of.get("notional_per_min") if of else None
                            if npmin is not None and float(npmin) < CONFIG.min_notional_per_min_usd:
                                continue
                        if CONFIG.min_depth_notional_usd > 0:
                            dbn = of.get("depth_bid_notional") if of else None
                            dan = of.get("depth_ask_notional") if of else None
                            if (
                                dbn is not None
                                and dan is not None
                                and (
                                    float(dbn) < CONFIG.min_depth_notional_usd
                                    or float(dan) < CONFIG.min_depth_notional_usd
                                )
                            ):
                                continue
                    sigs = evaluate_symbol_timeframe(
                        sym,
                        timeframe,
                        rows,
                        btc_15m_close=btc_15m_close,
                        extra_features=extra_feats,
                    )
                    for s in sigs:
                        if last_price and last_price > 0:
                            s.price = float(last_price)
                        diag_total_candidates += 1
                        if dedupe(s, CONFIG.dedupe_ttl_sec):
                            continue
                        tf_candidates.append(s)
                        touch_cooldown(cd_key)
                await asyncio.sleep(CONFIG.candles_inter_chunk_sleep)

            tf_candidates.sort(key=lambda s: (s.score, s.ev, s.prob), reverse=True)
            kept = tf_candidates[: CONFIG.max_candidates_per_tf]
            per_tf_bests[tf] = kept
            diag_kept_after_tf_topN += len(kept)

            # small pause between timeframe batches to reduce request bursts
            await asyncio.sleep(CONFIG.tf_sleep_sec)

        # best timeframe per symbol
        best_per_symbol: Dict[str, Signal] = {}
        for tf, arr in per_tf_bests.items():
            for s in arr:
                prev = best_per_symbol.get(s.symbol)
                if (prev is None) or (s.score, s.ev, s.prob) > (
                    prev.score,
                    prev.ev,
                    prev.prob,
                ):
                    best_per_symbol[s.symbol] = s

        final = sorted(
            best_per_symbol.values(),
            key=lambda s: (s.score, s.ev, s.prob),
            reverse=True,
        )
        if len(final) > CONFIG.max_candidates_per_loop:
            final = final[: CONFIG.max_candidates_per_loop]

        if CONFIG.print_diagnostics:
            print(
                f"[diag] totals: evaluated={diag_total_candidates} kept_after_tf={diag_kept_after_tf_topN} final={len(final)}"
            )

        return final
    finally:
        if client_cm is not None:
            try:
                await client_cm.__aexit__(None, None, None)
            except Exception:
                pass


async def ws_event_loop():
    # Ensure WS subs
    global _ws_kl_started, _bandit_last_save_ts
    async with BlofinClient() as client:
        # load bandit state if present
        try:
            GLOBAL_BANDIT.load_from(CONFIG.bandit_state_path)
        except Exception:
            pass

        uni, src = await _get_universe(client)
        if CONFIG.print_liquidity_debug:
            try:
                if CONFIG.pretty:
                    PR.universe_summary(uni, src, sample=12)
                else:
                    print(
                        f"[liquidity] Universe (active): {len(uni)} (example: {_fmt_list(uni, 12)}) [source={src}]"
                    )
            except Exception:
                print(
                    f"[liquidity] Universe (active): {len(uni)} (example: {_fmt_list(uni, 12)}) [source={src}]"
                )
        if CONFIG.use_ws_candles and not _ws_kl_started:
            await _ws_kl.start()
            _ws_kl_started = True
        if CONFIG.use_ws_candles:
            await _ws_kl.ensure_subscriptions(uni[: CONFIG.ws_max_symbols], list(CONFIG.timeframes))
        # Orderflow WS
        global _ws_of_started
        if CONFIG.use_ws_orderflow and not _ws_of_started:
            try:
                await _ws_of.start()
                _ws_of_started = True
            except Exception:
                pass
        if CONFIG.use_ws_orderflow:
            try:
                await _ws_of.ensure_subscriptions(uni[: CONFIG.ws_max_symbols])
            except Exception:
                pass

        # Start risk manager if trading live and enabled (for trailing or min-margin enforcement)
        try:
            if CONFIG.enable_autotrade and not CONFIG.paper_trading and (CONFIG.enable_trailing or CONFIG.enforce_min_margin):
                await GLOBAL_RISK_MANAGER.start()
        except Exception:
            pass

        q = _ws_kl.events()
        panic_done = False
        while True:
            try:
                sym, tf, ts_bar = await q.get()
            except Exception:
                continue
            # Emergency: auto-panic flatten if kill file/env is active (one-shot)
            if (
                CONFIG.enable_autotrade
                and not CONFIG.paper_trading
                and CONFIG.auto_panic_on_file
                and not panic_done
                and kill_active()
            ):
                try:
                    emit_metric("panic_trigger", {"source": "ws_event_loop"})
                except Exception:
                    pass
                try:
                    await panic_flatten(client)
                except Exception:
                    pass
                panic_done = True
            # Periodically refresh live exposure from positions (prevents stale max_exposure gating)
            global _exposure_last_refresh_ts
            if (time.time() - _exposure_last_refresh_ts) > 20:
                try:
                    await _refresh_exposure(client)
                except Exception:
                    pass
                _exposure_last_refresh_ts = time.time()
            # Only act on current universe
            if sym not in uni:
                continue
            rows = _ws_kl.get_rows(sym, tf, CONFIG.candles_limit)
            if not rows or len(rows) < 60:
                continue
            cd_key = f"{sym}|{tf}"
            if on_cooldown(cd_key, CONFIG.cooldown_sec):
                continue
            # Optional qVol gate at signal time
            ticker = None
            last_price: float | None = None
            try:
                ticker = _ws_uni.get_raw_ticker(sym)
                if ticker is None:
                    ticker = _get_cached_ticker(sym)
                if CONFIG.signal_min_qvol_usdt > 0:
                    if ticker is not None:
                        qv, _ = _qvol_with_key(ticker)
                        if qv < CONFIG.signal_min_qvol_usdt:
                            continue
                if ticker is not None:
                    last_price = _get_last_price(ticker)
            except Exception:
                pass
            btc_15m_close = await _btc_cache.get(client)
            # orderflow gating + extra features
            extra_feats = None
            if CONFIG.use_ws_orderflow:
                of = _ws_of.snapshot(sym)
                extra_feats = of
                if CONFIG.orderflow_strict:
                    has_book = of.get("best_bid") and of.get("best_ask")
                    trades_ready = (
                        float(of.get("trades_count", 0.0)) >= CONFIG.of_ready_min_trades
                    )
                    fresh_book = (
                        (time.time() - float(of.get("book_ts", 0.0)))
                        <= CONFIG.of_ready_max_age_sec
                    )
                    fresh_trades = (
                        (time.time() - float(of.get("last_trade_ts", 0.0)))
                        <= CONFIG.of_ready_max_age_sec
                    )
                    if not (has_book and trades_ready and fresh_book and fresh_trades):
                        continue
                if CONFIG.max_spread_bps > 0:
                    sp_val = of.get("spread_bps") if of else None
                    if sp_val is not None and float(sp_val) > CONFIG.max_spread_bps:
                        continue
                if CONFIG.min_trades_per_min > 0:
                    tr_val = of.get("trade_rate_per_min") if of else None
                    if tr_val is not None and float(tr_val) < CONFIG.min_trades_per_min:
                        continue
                if CONFIG.min_notional_per_min_usd > 0:
                    npmin = of.get("notional_per_min") if of else None
                    if npmin is not None and float(npmin) < CONFIG.min_notional_per_min_usd:
                        continue
                if CONFIG.min_depth_notional_usd > 0:
                    dbn = of.get("depth_bid_notional") if of else None
                    dan = of.get("depth_ask_notional") if of else None
                    if (
                        dbn is not None
                        and dan is not None
                        and (
                            float(dbn) < CONFIG.min_depth_notional_usd
                            or float(dan) < CONFIG.min_depth_notional_usd
                        )
                    ):
                        continue
            sigs = evaluate_symbol_timeframe(
                sym, tf, rows, btc_15m_close=btc_15m_close, extra_features=extra_feats
            )
            for s in sigs:
                if last_price and last_price > 0:
                    s.price = float(last_price)
                if dedupe(s, CONFIG.dedupe_ttl_sec):
                    continue
                touch_cooldown(cd_key)
                # print and emit metrics
                if CONFIG.print_json_lines:
                    _print_jsonl(s)
                else:
                    _print_human(s)
                try:
                    emit_metric("signal", s.__dict__)
                except Exception:
                    pass
                # Auto-trade on printed signals (if enabled)
                try:
                    GLOBAL_TRADER.on_signal(s)
                except Exception as e:
                    try:
                        emit_metric("autotrade_error", {"symbol": s.symbol, "err": repr(e)})
                    except Exception:
                        pass

            # periodically refresh subscriptions and save bandit state
            now = time.time()
            if (now - _bandit_last_save_ts) > 300:
                try:
                    GLOBAL_BANDIT.save_to(CONFIG.bandit_state_path)
                except Exception:
                    pass
                _bandit_last_save_ts = now


# --------------------------- Loop / Output -----------------------------------


def _print_human(s: Signal):
    components = ", ".join(s.components)
    tag = "🟢" if s.side == "buy" else "🔴"
    # Include 24h quote volume from WS tickers if available
    qv_str = ""
    try:
        t = _ws_uni.get_raw_ticker(s.symbol)
        if t:
            qv, key = _qvol_with_key(t)
            # compact formatting
            if qv >= 1e9:
                qv_str = f" | qVol {qv/1e9:.1f}B"
            elif qv >= 1e6:
                qv_str = f" | qVol {qv/1e6:.1f}M"
            elif qv >= 1e3:
                qv_str = f" | qVol {qv/1e3:.1f}K"
            elif qv > 0:
                qv_str = f" | qVol {qv:.0f}"
            # append source key label
            if key:
                label = "derived" if key == "derived_base*last" else str(key)
                qv_str += f" ({label})"
    except Exception:
        pass
    print(
        f"[{tag}] {s.symbol} {s.timeframe} | Score {s.score:.2f} | P {s.prob*100:.1f}% | EV {s.ev:.2f} | {components}{qv_str} | price {s.price:.6g}"
    )


def _print_jsonl(s: Signal):
    print(s.to_json())


async def loop_forever():
    try:
        if CONFIG.pretty:
            PR.config_table(CONFIG)
        else:
            print(
                f"[cfg] timeframes={list(CONFIG.timeframes)} min_score={CONFIG.min_score} top_symbols={CONFIG.top_symbols_by_quote_vol}"
            )
    except Exception:
        print(
            f"[cfg] timeframes={list(CONFIG.timeframes)} min_score={CONFIG.min_score} top_symbols={CONFIG.top_symbols_by_quote_vol}"
        )
    # load bandit state
    try:
        GLOBAL_BANDIT.load_from(CONFIG.bandit_state_path)
    except Exception:
        pass
    async with BlofinClient() as client:
        # one-time account sanity if trading is enabled (real)
        if CONFIG.run_account_sanity and not CONFIG.paper_trading and CONFIG.enable_autotrade:
            try:
                print("[startup] Running account sanity…")
                await asyncio.wait_for(_account_sanity(client), timeout=getattr(CONFIG, "account_sanity_timeout_sec", 20))
                print("[startup] Account sanity done.")
            except Exception as e:
                print(f"[startup] Account sanity skipped: {type(e).__name__}: {e}")
        else:
            print("[startup] Account sanity disabled or not needed.")
        # Initial exposure refresh
        try:
            await _refresh_exposure(client)
        except Exception:
            pass
        # start private WS risk manager (trailing/breakeven/min-margin)
        try:
            if CONFIG.enable_autotrade and not CONFIG.paper_trading and (CONFIG.enable_trailing or CONFIG.enforce_min_margin):
                await GLOBAL_RISK_MANAGER.start()
        except Exception:
            pass
        panic_done = False
        while True:
            try:
                # Periodically refresh exposure from live positions (for REST/WS loop)
                global _exposure_last_refresh_ts
                if (time.time() - _exposure_last_refresh_ts) > 20:
                    try:
                        await _refresh_exposure(client)
                    except Exception:
                        pass
                    _exposure_last_refresh_ts = time.time()
                # Emergency: auto-panic flatten if kill file/env is active (one-shot)
                if (
                    CONFIG.enable_autotrade
                    and not CONFIG.paper_trading
                    and CONFIG.auto_panic_on_file
                    and not panic_done
                    and kill_active()
                ):
                    try:
                        emit_metric("panic_trigger", {"source": "loop_forever"})
                    except Exception:
                        pass
                    try:
                        await panic_flatten(client)
                    except Exception:
                        pass
                    panic_done = True
                signals = await scan_once(client)
                if not signals:
                    print("[scan] No candidates this round.")
                else:
                    # Pretty table for this batch (if enabled and not JSON lines)
                    if CONFIG.pretty and not CONFIG.print_json_lines:
                        try:
                            PR.signals_table(signals)
                        except Exception:
                            pass
                    # keep best timeframe per symbol (already applied in scan_once)
                    for s in signals:
                        if CONFIG.print_json_lines:
                            _print_jsonl(s)
                        elif not CONFIG.pretty:
                            _print_human(s)
                        try:
                            emit_metric("signal", s.__dict__)
                        except Exception:
                            pass
                        try:
                            GLOBAL_LABELER.register(s)
                        except Exception:
                            pass
                        try:
                            GLOBAL_TRADER.on_signal(s, client)
                        except Exception as e:
                            try:
                                emit_metric("autotrade_error", {"symbol": s.symbol, "err": repr(e)})
                            except Exception:
                                pass
            except Exception as e:
                print(f"[ERR] {type(e).__name__}: {e}")

            # periodic bandit save
            global _bandit_last_save_ts
            now = time.time()
            if (now - _bandit_last_save_ts) > 300:
                try:
                    GLOBAL_BANDIT.save_to(CONFIG.bandit_state_path)
                except Exception:
                    pass
                _bandit_last_save_ts = now

            # process any matured labels and update bandit weights
            try:
                labels = await GLOBAL_LABELER.process(client)
                if labels:
                    for L in labels:
                        try:
                            ret = float(L.get("ret", 0.0))
                            reward = 1.0 if ret > 0 else 0.0
                            comps = L.get("components") or []
                            for arm in comps:
                                GLOBAL_BANDIT.update(str(arm), float(reward))
                        except Exception:
                            pass
            except Exception:
                pass
            # # drain private WS events (orders/positions)
            # try:
            #     if _ws_priv_started:
            #         q = _ws_priv.events()
            #         for _ in range(32):  # soft cap per loop
            #             try:
            #                 chan, evt = q.get_nowait()
            #             except Exception:
            #                 break
            #             try:
            #                 emit_metric(f"ws_{chan}", evt)
            #             except Exception:
            #                 pass
            # except Exception:
            #     pass
            # await asyncio.sleep(CONFIG.loop_sleep_sec)


def main():
    import argparse

    p = argparse.ArgumentParser(description="Smart Scanner")
    p.add_argument("--loop", action="store_true", help="Run polling loop (REST/WS mix)")
    p.add_argument(
        "--event",
        action="store_true",
        help="Run event-driven loop (WS candle closes)",
    )
    args = p.parse_args()

    if args.event:
        asyncio.run(ws_event_loop())
    elif args.loop:
        asyncio.run(loop_forever())
    else:
        out = asyncio.run(scan_once(None))
        if not out:
            print("[scan] No candidates.")
        else:
            for s in out:
                if CONFIG.print_json_lines:
                    _print_jsonl(s)
                else:
                    _print_human(s)

async def _refresh_exposure(client: BlofinClient) -> None:
    """Recompute live exposure from open positions and set in GLOBAL_TRADER.
    Exposure is the sum of abs(position_size) * markPrice (fallback averagePrice).
    """
    try:
        items = await client.get_positions()
    except Exception:
        items = []
    total = 0.0
    try:
        for it in items or []:
            try:
                sz = abs(float(it.get("positions") or it.get("pos") or 0.0))
                px = None
                for k in ("markPrice", "averagePrice", "avgPx", "lastPx"):
                    v = it.get(k)
                    if v is not None:
                        try:
                            px = float(v)
                            break
                        except Exception:
                            pass
                if px is None:
                    continue
                total += sz * px
            except Exception:
                continue
    except Exception:
        total = 0.0
    try:
        total_f = float(total)
        GLOBAL_TRADER._exposure = total_f
        try:
            emit_metric("exposure", {"total": total_f})
        except Exception:
            pass
    except Exception:
        pass

async def _account_sanity(client: BlofinClient) -> None:
    if not CONFIG.enable_autotrade or CONFIG.paper_trading:
        return
    # Set position/margin modes and record balance
    try:
        bal = await client.get_account_balance("USDT-FUTURES")
        details = (bal.get("details") or [])
        if details:
            d0 = details[0]
            print(f"[acct] equityUsd={d0.get('equityUsd','?')} available={d0.get('available','?')}")
    except Exception:
        pass
    # Position mode
    try:
        pm = await client.get_position_mode()
        want = CONFIG.trading_position_mode
        current = pm.get("positionMode") if isinstance(pm, dict) else None
        want_api = "net_mode" if want == "net" else "long_short_mode"
        if current and current != want_api:
            await client.set_position_mode(want_api)
    except Exception:
        pass
    # Margin mode (cross/isolated)
    try:
        # API returns marginMode via GET margin-mode; reuse set blindly
        await client.set_margin_mode(CONFIG.trading_margin_mode)
    except Exception:
        pass
    # Optional: set leverage for a common instrument (helps some venues)
    try:
        await client.set_leverage("BTC-USDT", CONFIG.trading_leverage, CONFIG.trading_margin_mode, None)
    except Exception:
        pass
  


if __name__ == "__main__":
    main()

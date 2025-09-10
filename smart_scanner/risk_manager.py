from __future__ import annotations
"""
Risk Manager: Trailing stops and break-even via private WS positions updates.

Listens to private positions channel and adjusts SL using REST tpsl endpoints.
Rules (configurable):
- Break-even: once PnL reaches N*R, move SL to entry +/- buffer.
- Trailing: maintain stop at high-water minus trail distance (ATR or bps).

Notes:
- Computes R from current SL vs entry if ATR is unknown (R = |entry - SL|).
- Uses markPrice from positions updates for PnL and trailing decisions.
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Optional, Any, Union, cast

from .config import CONFIG
from .metrics import emit as emit_metric
from .blofin_client import BlofinClient
try:
    # optional; only needed when trailing is enabled and deps installed
    from .ws_private import BlofinPrivateWS  # type: ignore
except Exception:  # pragma: no cover
    BlofinPrivateWS = None  # type: ignore


@dataclass
class PosState:
    entry: float
    side: str  # long/short/net
    size: float
    high_water: float
    low_water: float
    mark: float
    sl_px: Optional[float] = None
    be_done: bool = False
    margin_mode: str = "cross"
    open_ts: float = 0.0


class RiskManager:
    def __init__(self):
        self._ws: Optional[Any] = None  # runtime type; ws class may be None when optional
        self._task: Optional[asyncio.Task] = None
        self._states: Dict[str, PosState] = {}
        self._enforce_ts: Dict[str, float] = {}
        self._last_close: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def start(self):
        if self._task or not CONFIG.enable_trailing or CONFIG.paper_trading:
            return
        if BlofinPrivateWS is None:
            return
        self._ws = BlofinPrivateWS(
            api_key=CONFIG.api_key,
            api_secret=CONFIG.api_secret,
            passphrase=CONFIG.api_passphrase,
            demo=("demo" in CONFIG.base_url)
        )
        ws = self._ws
        if ws is None:
            return
        ws.on_position = self._on_position  # type: ignore[attr-defined]
        ws.subscribe_positions()             # type: ignore[attr-defined]
        # Also watch orders stream to log TP/SL closes
        try:
            ws.on_order = self._on_order      # type: ignore[attr-defined]
            ws.subscribe_orders()             # type: ignore[attr-defined]
        except Exception:
            pass
        self._task = asyncio.create_task(ws.run(), name="risk_manager_ws")  # type: ignore[attr-defined]

    async def stop(self):
        if self._ws:
            await self._ws.stop()
        if self._task:
            self._task.cancel()
            self._task = None

    async def _on_position(self, payload: Dict):
        data = payload.get("data") or []
        for p in data:
            try:
                inst = p.get("instId") or p.get("symbol")
                if not inst:
                    continue
                pos_raw = p.get("positions") or p.get("pos") or "0"
                sz = float(pos_raw)
                # Determine side per mode
                mode = str(CONFIG.trading_position_mode or "net").lower()
                ps_field = (p.get("positionSide") or "").lower()
                if mode == "long_short" and ps_field in ("long", "short"):
                    side = ps_field
                else:
                    side = "long" if sz > 0 else "short"
                key = f"{inst}|{side}" if mode == "long_short" else inst

                if abs(sz) <= 0:
                    # flat: clear state
                    # Emit manual close if no recent tp/sl close was seen for this (symbol,side)
                    try:
                        mode = str(CONFIG.trading_position_mode or "net").lower()
                        key = f"{inst}|{side}" if mode == "long_short" else inst
                        ts_last = self._last_close.get(key)
                        now = time.time()
                        if ts_last is None or (now - ts_last) > 5.0:
                            info = {"instId": inst, "side": side, "reason": "manual", "updateTime": int(now * 1000)}
                            try:
                                st_prev = self._states.get(key)
                                if st_prev and float(st_prev.open_ts or 0) > 0:
                                    info["duration_sec"] = float(now - float(st_prev.open_ts))
                            except Exception:
                                pass
                            emit_metric("trade_close", info)
                        # cleanup last_close entry on flat
                        self._last_close.pop(key, None)
                    except Exception:
                        pass
                    # Best-effort: cancel any lingering TP/SL algos for this instrument/side
                    try:
                        async with BlofinClient() as c:
                            items = await c.orders_tpsl_pending(inst_id=inst, limit=100)
                            for it in items or []:
                                try:
                                    # If in hedge mode, match the same positionSide; in net mode, cancel all for inst
                                    if str(CONFIG.trading_position_mode or "net").lower() == "long_short":
                                        ps = (it.get("positionSide") or "").lower()
                                        if ps and ps in ("long", "short") and ps != side:
                                            continue
                                    tid = it.get("tpslId") or it.get("algoId")
                                    if tid:
                                        resx = await c.cancel_tpsl(inst_id=inst, tpsl_id=str(tid))
                                        try:
                                            emit_metric("risk_cancel_tpsl_on_flat", {"instId": inst, "tpslId": str(tid), "resp": resx})
                                        except Exception:
                                            pass
                                except Exception:
                                    continue
                    except Exception:
                        pass
                    try:
                        emit_metric("trade_flat", {"instId": inst, "side": side, "reason": "position_zero"})
                    except Exception:
                        pass
                    async with self._lock:
                        self._states.pop(key, None)
                    # Recompute exposure when a symbol goes flat
                    await self._recompute_exposure()
                    continue
                avg = float(p.get("averagePrice") or 0.0)
                mark = float(p.get("markPrice") or avg)
                mm = str(p.get("marginMode") or CONFIG.trading_margin_mode)
                # venue-reported initial margin (or free-form 'margin')
                try:
                    pos_margin = float(p.get("initialMargin") or p.get("margin") or 0.0)
                except Exception:
                    pos_margin = 0.0
                # leverage (fallback env)
                try:
                    lev = float(p.get("leverage") or CONFIG.trading_leverage)
                except Exception:
                    lev = float(CONFIG.trading_leverage)

                async with self._lock:
                    st = self._states.get(key)
                    if st is None:
                        st = PosState(entry=avg, side=side, size=abs(sz), high_water=mark, low_water=mark, mark=mark, margin_mode=mm, open_ts=time.time())
                        self._states[key] = st
                    else:
                        st.size = abs(sz)
                        st.side = side
                        st.margin_mode = mm
                        st.mark = mark
                    if side == "long":
                        st.high_water = max(st.high_water, mark)
                    else:
                        st.low_water = min(st.low_water, mark)
                # Emit a lightweight position snapshot for the dashboard
                try:
                    emit_metric("position", {
                        "instId": inst,
                        "side": side,
                        "size": abs(sz),
                        "entry": avg,
                        "mark": mark,
                        "sl": getattr(st, 'sl_px', None),
                        "ts": time.time(),
                    })
                except Exception:
                    pass
                await self._maybe_adjust_sl(inst, side)
                # Update global exposure on every positions push
                await self._recompute_exposure()

                # Venue-side min margin enforcement (optional)
                try:
                    if (
                        CONFIG.enforce_min_margin
                        and not CONFIG.paper_trading
                        and float(pos_margin) > 0.0
                        and float(pos_margin) < float(CONFIG.min_initial_margin_usd)
                    ):
                        await self._enforce_min_margin(inst, side, mm, abs(sz), mark, pos_margin, lev)
                except Exception:
                    pass
            except Exception:
                continue

    async def _maybe_adjust_sl(self, inst: str, side: str):
        mode = str(CONFIG.trading_position_mode or "net").lower()
        key = f"{inst}|{side}" if mode == "long_short" else inst
        async with self._lock:
            st = self._states.get(key)
        if not st:
            return

        # compute desired SL based on rules
        entry = st.entry
        side = st.side
        # Obtain current SL from pending TPSL to derive R
        sl_active_px = await self._current_sl_px(inst, side)
        R = None
        if sl_active_px:
            R = abs(entry - sl_active_px)
        if not R or R <= 0:
            # fallback to ATR ref
            R = max(entry, 1e-9) * CONFIG.atr_ref_pct * CONFIG.atr_sl_mult

        # mark for computation: use high/low water
        if side == "long":
            hw = st.high_water
            profit = hw - entry
        else:
            lw = st.low_water
            profit = entry - lw

        be_px = None
        if not st.be_done and profit >= CONFIG.break_even_after_R * R:
            # Move to break-even +/- small buffer
            buf = entry * (CONFIG.break_even_buffer_bps / 10000.0)
            be_px = (entry + buf) if side == "long" else (entry - buf)

        # trailing target
        trail_dist = None
        if CONFIG.trail_mode == "atr":
            trail_dist = max(entry, 1e-9) * CONFIG.atr_ref_pct * CONFIG.trail_atr_mult
        else:
            trail_dist = entry * (CONFIG.trail_bps / 10000.0)

        trail_px = None
        if side == "long":
            trail_px = st.high_water - trail_dist
        else:
            trail_px = st.low_water + trail_dist

        # Choose the tighter (more protective) between be_px and trail_px versus current SL
        desired_sl = None
        if be_px is not None:
            desired_sl = be_px
        if trail_px is not None:
            if desired_sl is None:
                desired_sl = trail_px
            else:
                desired_sl = max(desired_sl, trail_px) if side == "long" else min(desired_sl, trail_px)

        if desired_sl is None:
            return

        # min step filter to avoid noise
        min_step = entry * (CONFIG.trail_min_step_bps / 10000.0)
        if st.sl_px is not None and abs(desired_sl - st.sl_px) < min_step:
            return

        # Apply new SL: cancel old SL (if any) and place new reduce-only SL for entire position
        try:
            async with BlofinClient() as client:
                # cancel previous SL (best-effort)
                try:
                    await self._cancel_existing_sl(client, inst, side)
                except Exception:
                    pass
                dp = max(0, int(CONFIG.price_round_dp))
                fmt = f"{{:.{dp}f}}"
                sl_trigger = fmt.format(desired_sl)
                reduce_side = "sell" if side == "long" else "buy"
                mm = st.margin_mode or CONFIG.trading_margin_mode
                pos_side_param = side if mode == "long_short" else "net"
                res = await client.place_tpsl(
                    inst,
                    reduce_side,
                    margin_mode=mm,
                    position_side=pos_side_param,
                    size="-1",
                    sl_trigger_price=sl_trigger,
                    sl_order_price="-1",
                    trigger_price_type="last",
                    reduce_only=True,
                )
                emit_metric("risk_trail_sl", {"instId": inst, "side": side, "new_sl": sl_trigger, "res": res})
                ok = False
                try:
                    code_ok = (str(res.get("code", "0")) == "0") if isinstance(res, dict) else False
                    has_id = False
                    if isinstance(res, dict):
                        has_id = bool(res.get("tpslId") or res.get("algoId"))
                    ok = code_ok or has_id
                except Exception:
                    ok = False
                if ok:
                    async with self._lock:
                        st.sl_px = desired_sl
                        if be_px is not None:
                            st.be_done = True
                else:
                    try:
                        emit_metric("risk_trail_sl_nack", {"instId": inst, "side": side, "new_sl": sl_trigger, "res": res})
                    except Exception:
                        pass
        except Exception as e:
            emit_metric("risk_trail_sl_error", {"instId": inst, "err": repr(e)})
            return

    async def _current_sl_px(self, inst: str, side: str) -> Optional[float]:
        # Query pending tpsl and pick SL with reduce side matching position
        try:
            async with BlofinClient() as client:
                items = await client.orders_tpsl_pending(inst_id=inst, limit=100)
        except Exception:
            items = []
        sls = []
        reduce_side = "sell" if side == "long" else "buy"
        mode = str(CONFIG.trading_position_mode or "net").lower()
        for it in items:
            try:
                # Filter by reduce side and positionSide when in hedge mode
                if (it.get("side") or reduce_side) != reduce_side:
                    continue
                if mode == "long_short":
                    ps = (it.get("positionSide") or "").lower()
                    if ps and ps not in ("long", "short"):
                        pass
                    elif ps and ps != side:
                        continue
                slp = it.get("slTriggerPrice")
                if slp is None:
                    continue
                sls.append(float(slp))
            except Exception:
                continue
        if not sls:
            return None
        # For long, protective SL is the max of SLs; for short, it's the min
        return max(sls) if side == "long" else min(sls)

    async def _cancel_existing_sl(self, client: BlofinClient, inst: str, side: str) -> None:
        items = await client.orders_tpsl_pending(inst_id=inst, limit=100)
        # cancel only SL orders (with slTriggerPrice present). Keep TPs intact.
        reduce_side = "sell" if side == "long" else "buy"
        mode = str(CONFIG.trading_position_mode or "net").lower()
        for it in items:
            slp = it.get("slTriggerPrice")
            tpp = it.get("tpTriggerPrice")
            if slp is None:
                continue
            if (it.get("side") or reduce_side) != reduce_side:
                continue
            if mode == "long_short":
                ps = (it.get("positionSide") or "").lower()
                if ps and ps not in ("long", "short"):
                    pass
                elif ps and ps != side:
                    continue
            # If both tp and sl present in one tpsl, prefer not to cancel to avoid nuking TP
            if tpp is not None:
                continue
            tpsl_id = it.get("tpslId") or it.get("algoId")
            if tpsl_id:
                try:
                    res = await client.cancel_tpsl(inst_id=inst, tpsl_id=str(tpsl_id))
                    # If API returns error structure, log it
                    def _is_err(r) -> bool:
                        try:
                            if isinstance(r, dict):
                                c = str(r.get("code", "0"))
                                return c != "0"
                            if isinstance(r, list):
                                return any(str(x.get("code", "0")) != "0" for x in r if isinstance(x, dict))
                        except Exception:
                            return False
                        return False
                    if _is_err(res):
                        emit_metric("risk_cancel_sl_error", {"instId": inst, "tpslId": str(tpsl_id), "resp": res})
                except Exception:
                    pass

    async def _on_order(self, payload: Dict):
        """Log TP/SL closes from private WS orders stream.
        We rely on 'orderCategory' values like 'tp'/'sl' published on updates.
        """
        try:
            data = payload.get("data") or []
            for it in data:
                try:
                    cat = (it.get("orderCategory") or "").lower()
                    if cat not in ("tp", "sl"):
                        continue
                    inst = it.get("instId") or it.get("symbol")
                    reason = "take_profit" if cat == "tp" else "stop_loss"
                    # Determine side for BE logic (hedge aware)
                    ps = (it.get("positionSide") or "").lower()
                    if ps not in ("long", "short"):
                        side_field = (it.get("side") or "").lower()
                        ps = "long" if side_field == "sell" else "short"
                    info = {
                        "instId": inst,
                        "reason": reason,
                        "orderId": it.get("orderId"),
                        "clientOrderId": it.get("clientOrderId"),
                        "filledSize": it.get("filledSize"),
                        "avgPrice": it.get("averagePrice"),
                        "pnl": it.get("pnl"),
                        "updateTime": it.get("updateTime") or it.get("createTime"),
                        "reduceOnly": it.get("reduceOnly"),
                    }
                    emit_metric("trade_close", info)
                    try:
                        print(f"[close] {inst} {reason} filledSize={it.get('filledSize')} avg={it.get('averagePrice')}")
                    except Exception:
                        pass
                    # Optionally move SL to break-even on first TP
                    try:
                        if cat == "tp" and CONFIG.move_sl_be_on_tp1:
                            await self._move_sl_to_break_even(inst, ps)
                        # Refresh exposure after tp/sl
                        await self._recompute_exposure()
                        # Record last close ts for manual-close detection
                        try:
                            mode = str(CONFIG.trading_position_mode or "net").lower()
                            key = f"{inst}|{ps}" if mode == "long_short" else inst
                            self._last_close[key] = time.time()
                        except Exception:
                            pass
                    except Exception:
                        pass
                except Exception:
                    continue
        except Exception:
            pass

    async def _recompute_exposure(self) -> None:
        """Compute total exposure from current states and publish to GLOBAL_TRADER.
        Exposure = sum(|size| * markPrice) across symbols.
        """
        try:
            total = 0.0
            async with self._lock:
                for st in self._states.values():
                    px = st.mark if st.mark else st.entry
                    total += abs(float(st.size)) * float(px)
        except Exception:
            total = 0.0
        # Late import to avoid circular dependency
        try:
            from .trader import GLOBAL_TRADER  # type: ignore
            total_f = float(total)
            GLOBAL_TRADER._exposure = total_f
            try:
                emit_metric("exposure", {"total": total_f})
            except Exception:
                pass
        except Exception:
            pass

    async def _instrument_specs(self, client: BlofinClient, inst: str) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        step = None
        min_sz = None
        quote_per_size = None
        base_per_size = None
        try:
            items = await client.get_instruments(CONFIG.ws_inst_type)
            for it in items:
                iid = it.get("instId") or it.get("symbol")
                if iid == inst:
                    for k in ("lotSz", "lotSize", "sizeIncrement", "minSizeIncrement", "stepSize"):
                        if it.get(k) is not None:
                            try:
                                step = float(it[k])
                            except Exception:
                                pass
                    for k in ("minSz", "minSize", "minTradeSize"):
                        if it.get(k) is not None:
                            try:
                                min_sz = float(it[k])
                            except Exception:
                                pass
                    # multipliers
                    try:
                        ctval = it.get("ctVal") or it.get("contractVal") or it.get("faceValue")
                        ccy = (it.get("ctValCcy") or it.get("contractValCcy") or "").upper()
                        if ctval is not None:
                            v = float(ctval)
                            if ccy in ("USDT", "USD"):
                                quote_per_size = v
                            else:
                                base_per_size = v
                    except Exception:
                        pass
                    try:
                        if base_per_size is None:
                            for k in ("contractSize", "multiplier", "qtyMultiplier"):
                                val = it.get(k)
                                if val is None:
                                    continue
                                try:
                                    if isinstance(val, (int, float, str)) and str(val) != "":
                                        vv = float(val)
                                        if vv > 0:
                                            base_per_size = vv
                                            break
                                except (TypeError, ValueError):
                                    continue
                    except Exception:
                        pass
                    break
        except Exception:
            pass
        if not CONFIG.contract_size_aware:
            quote_per_size = None
            base_per_size = None
        return step, min_sz, quote_per_size, base_per_size

    async def _enforce_min_margin(self, inst: str, side: str, margin_mode: str, pos_size: float, mark: float, pos_margin: float, lev: float) -> None:
        # simple cooldown per (symbol,side)
        key = f"{inst}|{side}"
        now = asyncio.get_event_loop().time()
        last = self._enforce_ts.get(key, 0.0)
        if (now - last) < 2.0:  # avoid spamming during rapid updates
            return
        self._enforce_ts[key] = now

        # Grace period right after open: avoid enforcing while fills/position snapshots settle
        try:
            grace_sec = float(getattr(CONFIG, "min_margin_grace_sec", 1.2))
        except Exception:
            grace_sec = 1.2
        try:
            st = self._states.get(key)
            if st and float(st.open_ts or 0) > 0:
                if (time.time() - float(st.open_ts)) < grace_sec:
                    emit_metric("min_margin_grace", {"instId": inst, "side": side, "since_open_sec": time.time() - float(st.open_ts)})
                    return
        except Exception:
            pass

        action = (CONFIG.enforce_min_margin_action or "close").lower()
        # If action is 'skip', do not close/top-up; only signal and return
        if action == "skip":
            try:
                emit_metric("min_margin_skip", {
                    "instId": inst,
                    "side": side,
                    "margin": float(pos_margin),
                    "min": float(CONFIG.min_initial_margin_usd),
                    "lev": float(lev),
                })
            except Exception:
                pass
            return

        dp = max(0, int(CONFIG.price_round_dp))
        try:
            emit_metric("min_margin_detect", {
                "instId": inst,
                "side": side,
                "margin": float(pos_margin),
                "min": float(CONFIG.min_initial_margin_usd),
                "lev": float(lev),
            })
        except Exception:
            pass

        reduce_side = "sell" if side == "long" else "buy"
        entry_side = "buy" if side == "long" else "sell"
        pos_side_param = side if (str(CONFIG.trading_position_mode or "net").lower() == "long_short") else "net"

        async with BlofinClient() as client:
            step, min_sz, qps, bps = await self._instrument_specs(client, inst)
            # Defaults to satisfy type checker even if balance fetch fails
            avail: Optional[float] = None
            safety: float = 0.0

            def _adapt(q: float) -> float:
                adj = max(0.0, float(q))
                if step and step > 0:
                    import math as _math
                    adj = _math.floor(adj / step) * step
                if min_sz and adj < min_sz:
                    # For close, if adj < min and we have min size, use min
                    adj = float(min_sz)
                return float(adj)

            if action == "topup":
                try:
                    need_margin = float(CONFIG.min_initial_margin_usd) - float(pos_margin)
                    add_notional = max(0.0, need_margin * max(1.0, float(lev)))
                    denom = float(qps) if qps is not None else max(float(mark) * (float(bps) if bps is not None else 1.0), 1e-9)
                    add_qty_raw = add_notional / denom
                    add_qty = _adapt(add_qty_raw)
                    if add_qty <= 0:
                        emit_metric("min_margin_enforce_skip", {"instId": inst, "side": side, "reason": "rounding_zero"})
                        return
                    # Optional balance check with safety
                    ok_bal = True
                    if CONFIG.check_balance_before_order:
                        try:
                            bal = await client.get_account_balance("USDT-FUTURES")
                            details = bal.get("details") or bal.get("data") or []
                            if isinstance(details, dict):
                                details = [details]
                            for d in details:
                                cur = (d.get("ccy") or d.get("currency") or "").upper()
                                if cur and cur != "USDT":
                                    continue
                                for k in ("available", "availableUsd", "availableEquity", "cashAvailable", "availEq"):
                                    val = d.get(k)
                                    if isinstance(val, (int, float, str)) and str(val) != "":
                                        try:
                                            avail = float(val)
                                            break
                                        except (TypeError, ValueError):
                                            continue
                                if avail is not None:
                                    break
                        except Exception:
                            pass
                        safety = float(getattr(CONFIG, "margin_safety_bps", 0.0)) / 10000.0
                    req_notional = float(add_qty) * (float(qps) if qps is not None else float(mark) * (float(bps) if bps is not None else 1.0))
                    req = req_notional / max(float(lev), 1e-9)
                    if avail is not None and avail < req * (1.0 + safety):
                        ok_bal = False
                    if not ok_bal:
                        emit_metric("min_margin_enforce_skip", {"instId": inst, "side": side, "reason": "insufficient_balance"})
                        return
                    res = await client.place_order(
                        inst,
                        entry_side,
                        "market",
                        f"{add_qty:.6f}",
                        margin_mode=margin_mode,
                        position_side=pos_side_param,
                        reduce_only=False,
                    )
                    emit_metric("min_margin_topup", {"instId": inst, "side": side, "qty": add_qty, "res": res})
                    return
                except Exception as e:
                    emit_metric("min_margin_topup_error", {"instId": inst, "side": side, "err": repr(e)})
                    return

            # default action: close
            try:
                # Use availablePositions if provided for exact closable size
                # pos_size is assumed in base coin units; convert to size units if needed
                if bps is not None and bps > 0:
                    close_qty_raw = float(pos_size) / float(bps)
                else:
                    close_qty_raw = float(pos_size)
                try:
                    # we don't have availablePositions here; pos_size is abs(positions)
                    pass
                except Exception:
                    pass
                close_qty = _adapt(close_qty_raw)
                if close_qty <= 0:
                    emit_metric("min_margin_enforce_skip", {"instId": inst, "side": side, "reason": "close_qty_zero"})
                    return
                res = await client.place_order(
                    inst,
                    reduce_side,
                    "market",
                    f"{close_qty:.6f}",
                    margin_mode=margin_mode,
                    position_side=pos_side_param,
                    reduce_only=True,
                )
                emit_metric("min_margin_close", {"instId": inst, "side": side, "qty": close_qty, "res": res})
            except Exception as e:
                emit_metric("min_margin_close_error", {"instId": inst, "side": side, "err": repr(e)})

    async def _move_sl_to_break_even(self, inst: str, side: str) -> None:
        """Cancel current SL and set break-even SL after first TP trigger.
        Break-even = entry +/- buffer (bps) depending on side.
        """
        mode = str(CONFIG.trading_position_mode or "net").lower()
        key = f"{inst}|{side}" if mode == "long_short" else inst
        async with self._lock:
            st = self._states.get(key)
        if not st:
            return
        if st.be_done:
            return
        try:
            buf_bps = CONFIG.break_even_buffer_bps / 10000.0
            entry = float(st.entry)
            be_px = entry + entry * buf_bps if st.side == "long" else entry - entry * buf_bps
            dp = max(0, int(CONFIG.price_round_dp))
            sl_trigger = f"{float(be_px):.{dp}f}"
            reduce_side = "sell" if st.side == "long" else "buy"
            mm = st.margin_mode or CONFIG.trading_margin_mode
        except Exception:
            return

        try:
            async with BlofinClient() as client:
                try:
                    await self._cancel_existing_sl(client, inst, st.side)
                except Exception:
                    pass
                pos_side_param = st.side if mode == "long_short" else "net"
                res = await client.place_tpsl(
                    inst,
                    reduce_side,
                    margin_mode=mm,
                    position_side=pos_side_param,
                    size="-1",
                    sl_trigger_price=sl_trigger,
                    sl_order_price="-1",
                    trigger_price_type="last",
                    reduce_only=True,
                )
                emit_metric("risk_be_on_tp", {"instId": inst, "side": st.side, "be_px": sl_trigger, "res": res})
                ok = False
                try:
                    code_ok = (str(res.get("code", "0")) == "0") if isinstance(res, dict) else False
                    has_id = False
                    if isinstance(res, dict):
                        has_id = bool(res.get("tpslId") or res.get("algoId"))
                    ok = code_ok or has_id
                except Exception:
                    ok = False
                if ok:
                    async with self._lock:
                        st.sl_px = float(sl_trigger)
                        st.be_done = True
                else:
                    try:
                        emit_metric("risk_be_on_tp_nack", {"instId": inst, "side": st.side, "be_px": sl_trigger, "res": res})
                    except Exception:
                        pass
        except Exception as e:
            emit_metric("risk_be_on_tp_error", {"instId": inst, "err": repr(e)})


GLOBAL_RISK_MANAGER = RiskManager()

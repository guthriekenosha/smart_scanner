from __future__ import annotations
import time
from dataclasses import dataclass
import math
from typing import Dict, Optional
import asyncio

from .config import CONFIG
from .metrics import emit as emit_metric
from .signal_types import Signal
from .blofin_client import BlofinClient
import os


@dataclass
class Position:
    symbol: str
    side: str  # buy/sell
    qty: float
    entry: float
    notional: float
    opened_ts: int


class PaperBroker:
    """
    Minimal paper broker: immediate fills at signal price +/- slippage.
    - Records faux fills to metrics: kind="paper_fill"
    - Maintains positions until closed by opposite signal
    """

    def __init__(self, slippage_bps: float = 8.0):
        self.slippage = float(slippage_bps)
        self.positions: Dict[str, Position] = {}

    def _apply_slippage(self, price: float, side: str) -> float:
        bps = self.slippage / 10000.0
        if side == "buy":
            return price * (1.0 + bps)
        else:
            return price * (1.0 - bps)

    def market(self, symbol: str, side: str, notional_usd: float, ref_price: float) -> Position:
        px = self._apply_slippage(ref_price, side)
        qty = max(notional_usd, 0.0) / max(px, 1e-9)
        pos = Position(symbol=symbol, side=side, qty=qty, entry=px, notional=notional_usd, opened_ts=int(time.time()))
        # replace existing opposite position (flat then open)
        self.positions[symbol] = pos
        emit_metric("paper_fill", {
            "symbol": symbol,
            "side": side,
            "price": px,
            "qty": qty,
            "notional": notional_usd,
        })
        return pos


class AutoTrader:
    """
    Converts high-confidence signals into trades.
    Defaults to paper trading. Real trading uses BlofinClient private endpoints.
    """

    def __init__(self):
        self.enabled = bool(CONFIG.enable_autotrade)
        self.paper = bool(CONFIG.paper_trading)
        self.notional = float(CONFIG.trade_notional_usd)
        self.cooldown = int(CONFIG.trade_cooldown_sec)
        self.min_score = float(CONFIG.trade_min_score)
        self.min_prob = float(CONFIG.trade_min_prob)
        self.min_qvol = float(CONFIG.min_qvol_trade_usdt)
        self.max_positions = int(CONFIG.trade_max_positions)
        self.max_exposure = float(CONFIG.trade_max_exposure_usd)
        self._last_trade_ts: Dict[str, int] = {}
        self._exposure = 0.0
        self.broker = PaperBroker(CONFIG.order_slippage_bps)
        # Serialize live order placement to avoid overcommitting balance
        self._order_lock: asyncio.Lock = asyncio.Lock()

    def _eligibility(self, s: Signal) -> tuple[bool, str]:
        # Global kill-switch: block all new orders immediately
        try:
            if CONFIG.kill_switch or (getattr(CONFIG, "emergency_file", None) and os.path.exists(CONFIG.emergency_file)):
                return False, "kill_switch"
        except Exception:
            pass
        if not self.enabled:
            return False, "disabled"
        if s.score < self.min_score:
            return False, "min_score"
        if s.prob < self.min_prob:
            return False, "min_prob"
        # qVol gate using attached meta (if present) else allow
        try:
            qv = float(s.meta.get("features", {}).get("qvol", 0.0)) if isinstance(s.meta, dict) else 0.0
        except Exception:
            qv = 0.0
        if self.min_qvol > 0 and qv > 0 and qv < self.min_qvol:
            return False, "min_qvol"
        # per-symbol cooldown
        now = int(time.time())
        lt = self._last_trade_ts.get(s.symbol, 0)
        if (now - lt) < self.cooldown:
            return False, "cooldown"
        # global exposure/position caps
        if len(self.broker.positions) >= self.max_positions:
            return False, "max_positions"
        # consider dynamic notional if smart risk is enabled
        try:
            plan = self._plan_risk(s)
            planned_notional = float(plan.get("notional_usd", self.notional))
        except Exception:
            planned_notional = self.notional
        if (self._exposure + planned_notional) > self.max_exposure:
            try:
                emit_metric("exposure_block", {
                    "symbol": s.symbol,
                    "exposure": self._exposure,
                    "planned": planned_notional,
                    "cap": self.max_exposure,
                })
            except Exception:
                pass
            return False, "max_exposure"
        return True, "ok"

    def _plan_risk(self, s: Signal) -> Dict[str, object]:
        """
        Decide margin_mode, leverage, and notional multiplier based on signal features.
        Returns dict: {margin_mode:str, leverage:str, notional_usd:float}
        """
        # Defaults
        margin_mode = CONFIG.trading_margin_mode
        lev = float(CONFIG.trading_leverage or 1)
        notional_mult = 1.0

        if CONFIG.enable_smart_risk:
            # Inputs
            atr_pct = None
            try:
                atr_pct = float(s.meta.get("features", {}).get("atr14_pct")) if isinstance(s.meta, dict) else None
            except Exception:
                atr_pct = None
            atr_pct = atr_pct if (atr_pct is not None and atr_pct > 0) else CONFIG.atr_ref_pct
            # confidence ~ blend of score and prob (0..1)
            score_norm = max(0.0, min(1.0, (float(s.score) - 3.0) / max(1e-9, (CONFIG.score_max - 3.0))))
            prob = float(s.prob)
            conf = 0.5 * score_norm + 0.5 * max(0.0, min(1.0, prob))
            regime = 1.0
            try:
                regime = float(s.meta.get("regime_mult", 1.0))
            except Exception:
                pass

            # Leverage: increase with confidence/regime, decrease with vol
            vol_term = 1.0 + CONFIG.k_vol * max(0.0, (atr_pct / max(CONFIG.atr_ref_pct, 1e-9) - 1.0))
            conf_term = 1.0 + CONFIG.k_conf * conf
            lev_target = CONFIG.lev_base * conf_term * regime / max(vol_term, 1e-6)
            lev = max(CONFIG.lev_min, min(CONFIG.lev_max, float(lev_target)))

            # Margin mode: cross if vol high; isolated if very high confidence and vol moderate
            if atr_pct >= CONFIG.cross_if_atr_gt:
                margin_mode = "cross"
            elif conf >= CONFIG.iso_if_conf_gt and atr_pct < (CONFIG.cross_if_atr_gt * 0.8):
                margin_mode = "isolated"
            else:
                margin_mode = CONFIG.trading_margin_mode

            # Dynamic notional scaling (optional)
            if CONFIG.enable_dynamic_notional:
                # Increase notional with confidence; decrease with vol
                mult = (1.0 + 0.6 * conf) / max(1.0, vol_term)
                mult *= max(0.85, min(1.15, regime))
                notional_mult = max(CONFIG.dyn_notional_min_mult, min(CONFIG.dyn_notional_max_mult, mult))

        notional_usd = max(0.0, float(self.notional) * float(notional_mult))
        lev_str = str(int(round(lev)))  # API expects integer leverage
        return {
            "margin_mode": margin_mode,
            "leverage": lev_str,
            "notional_usd": notional_usd,
        }

    def on_signal(self, s: Signal, client: Optional[BlofinClient] = None) -> Optional[Position]:
        ok, reason = self._eligibility(s)
        if not ok:
            try:
                emit_metric("trade_skip", {"symbol": s.symbol, "reason": reason, "score": s.score, "prob": s.prob})
            except Exception:
                pass
            if CONFIG.log_trade_skips:
                try:
                    print(f"[trade/SKIP] {s.symbol} reason={reason} score={s.score:.2f} prob={s.prob:.2f}")
                except Exception:
                    pass
            return None
        side = s.side
        plan = self._plan_risk(s)
        try:
            emit_metric("risk_plan", {"symbol": s.symbol, "plan": plan, "score": s.score, "prob": s.prob})
        except Exception:
            pass
        # Choose broker: paper or real
        if self.paper:
            pos = self.broker.market(s.symbol, side, float(plan["notional_usd"]), s.price)
        else:
            # fire-and-forget async order to avoid blocking the event loop caller
            try:
                import asyncio

                if client is None:
                    async def _go_tmp():
                        async with BlofinClient() as c:
                            await self._place_real_market_async(s, c, plan)

                    asyncio.create_task(_go_tmp())
                else:
                    asyncio.create_task(self._place_real_market_async(s, client, plan))
            except Exception:
                pass
            # return a stub position (unknown fill); exposure accounted optimistically
            stub_notional = float(plan["notional_usd"]) if plan else self.notional
            pos = Position(symbol=s.symbol, side=s.side, qty=stub_notional / max(s.price, 1e-9), entry=float(s.price), notional=stub_notional, opened_ts=int(time.time()))
        # Update exposure only for paper immediately (live exposure is refreshed via WS/REST)
        if self.paper:
            try:
                # approximate exposure as sum of paper positions' notionals
                self._exposure = sum(p.notional for p in self.broker.positions.values())
            except Exception:
                pass
        self._last_trade_ts[s.symbol] = int(time.time())
        try:
            emit_metric("order", {
                "symbol": s.symbol,
                "signal_id": s.id,
                "side": side,
                "timeframe": s.timeframe,
                "price": s.price,
                "score": s.score,
                "prob": s.prob,
                "ev": s.ev,
                "notional": float(plan["notional_usd"]) if plan else self.notional,
                "paper": self.paper,
            })
        except Exception:
            pass
        return pos

    async def _place_real_market_async(self, s: Signal, client: BlofinClient, plan: Dict[str, object] | None = None) -> None:
        async with self._order_lock:
            # NOTE: Serialize order placement; body currently runs outside the lock.
            # If needed, wrap the remainder of this method inside this lock.
            pass
        # place market order and attach smart TP/SL if enabled
        order_notional = float(plan.get("notional_usd", self.notional)) if plan else self.notional
        margin_mode = str(plan.get("margin_mode", CONFIG.trading_margin_mode)) if plan else CONFIG.trading_margin_mode
        leverage = str(plan.get("leverage", CONFIG.trading_leverage)) if plan else CONFIG.trading_leverage

        # Optional: pre-trade balance check and dynamic notional downsize
        if not self.paper and CONFIG.check_balance_before_order:
            try:
                bal = await client.get_account_balance("USDT-FUTURES")
            except Exception:
                bal = {}
            avail = None
            try:
                # Robust extract across possible schemas
                details = bal.get("details") or bal.get("data") or []
                if isinstance(details, dict):
                    details = [details]
                for d in details:
                    cur = (d.get("ccy") or d.get("currency") or "").upper()
                    if cur and cur != "USDT":
                        continue
                    for k in ("available", "availableUsd", "availableEquity", "cashAvailable", "availEq"):
                        v = d.get(k)
                        if v is not None:
                            try:
                                avail = float(v)
                                break
                            except Exception:
                                pass
                    if avail is not None:
                        break
            except Exception:
                avail = None
            # If still None, try top-level common keys
            if avail is None:
                try:
                    for k in ("available", "availableUsd", "availableEquity", "availEq"):
                        v = bal.get(k)
                        if v is not None:
                            avail = float(v)
                            break
                except Exception:
                    pass

            # Emit a parsed balance snapshot for quick verification (pre-size)
            try:
                snap = {"symbol": s.symbol, "signal_id": s.id, "stage": "pre_size"}
                # extract a few helpful fields if present
                eq_usd = None
                try:
                    details = bal.get("details") or bal.get("data") or []
                    if isinstance(details, dict):
                        details = [details]
                    if details:
                        d0 = details[0]
                        for k in ("equityUsd", "equity", "totalEquity"):
                            if d0.get(k) is not None:
                                eq_usd = float(d0.get(k))
                                break
                except Exception:
                    eq_usd = None
                if avail is not None:
                    snap["available"] = float(avail)
                if eq_usd is not None:
                    snap["equityUsd"] = float(eq_usd)
                # include current margin requirement estimate at this stage
                try:
                    lev_tmp = max(1.0, float(leverage))
                except Exception:
                    lev_tmp = 1.0
                snap["required_margin_est"] = float(order_notional) / max(lev_tmp, 1e-9)
                snap["notional_est"] = float(order_notional)
                try:
                    emit_metric("order_balance_snapshot", snap)
                except Exception:
                    pass
            except Exception:
                pass

            if avail is not None:
                try:
                    lev = max(1.0, float(leverage))
                except Exception:
                    lev = 1.0
                safety = CONFIG.margin_safety_bps / 10000.0
                required_margin = order_notional / max(lev, 1e-9)
                # If not enough available, downsize notional to what fits
                if avail < required_margin * (1.0 + safety):
                    # Target notional so that required margin ~= avail / (1+safety)
                    target_margin = max(0.0, avail / (1.0 + safety))
                    new_notional = max(0.0, target_margin * lev)
                    # Enforce minimum initial margin even after downsize
                    if (new_notional / max(lev, 1e-9)) < CONFIG.min_initial_margin_usd:
                        try:
                            print(f"[trade/SKIP_BAL_MIN] {s.symbol} available margin too small for min margin {CONFIG.min_initial_margin_usd}")
                        except Exception:
                            pass
                        try:
                            emit_metric("trade_skip", {"symbol": s.symbol, "reason": "insufficient_balance_min_margin", "available": avail, "min_margin": CONFIG.min_initial_margin_usd})
                        except Exception:
                            pass
                        return
                    if new_notional <= 0:
                        try:
                            print(f"[trade/SKIP_BAL] {s.symbol} insufficient available margin: {avail}")
                        except Exception:
                            pass
                        try:
                            emit_metric("trade_skip", {"symbol": s.symbol, "reason": "insufficient_balance", "available": avail, "required": required_margin})
                        except Exception:
                            pass
                        return
                    try:
                        emit_metric("order_balance_adjust", {"symbol": s.symbol, "from_notional": order_notional, "to_notional": new_notional, "available": avail, "lev": lev})
                    except Exception:
                        pass
                    order_notional = new_notional

        # Determine API positionSide (net vs long/short)
        pos_mode = str(CONFIG.trading_position_mode or "net").lower()
        pos_side_entry = "net" if pos_mode != "long_short" else ("long" if s.side == "buy" else "short")

        # Guard: one open position per symbol or per side (depending on mode)
        if not self.paper and CONFIG.single_position_per_symbol:
            try:
                ps = await client.get_positions(s.symbol)
                if isinstance(ps, list) and ps:
                    # In net mode: any non-zero blocks. In long_short: block only same-side.
                    for p0 in ps:
                        try:
                            raw = p0.get("positions") or p0.get("pos") or "0"
                            sz = abs(float(raw))
                            if sz <= 0:
                                continue
                            if pos_mode != "long_short":
                                blocked = True
                            else:
                                cur_ps = (p0.get("positionSide") or "").lower()
                                blocked = (cur_ps == pos_side_entry)
                            if blocked:
                                try:
                                    print(f"[trade/SKIP_OPEN] {s.symbol} position exists (mode={pos_mode}, side={pos_side_entry}); skip new order")
                                except Exception:
                                    pass
                                try:
                                    emit_metric("trade_skip", {"symbol": s.symbol, "reason": "open_position", "mode": pos_mode, "side": pos_side_entry})
                                except Exception:
                                    pass
                                return
                        except Exception:
                            continue
            except Exception:
                pass

        # sync leverage/margin (best-effort)
        try:
            await client.set_margin_mode(margin_mode)
        except Exception as e:
            emit_metric("order_api_error", {"stage": "set_margin_mode", "symbol": s.symbol, "margin_mode": margin_mode, "err": repr(e)})
        try:
            await client.set_leverage(s.symbol, leverage, margin_mode, pos_side_entry if pos_mode == "long_short" else None)
        except Exception as e:
            emit_metric("order_api_error", {"stage": "set_leverage", "symbol": s.symbol, "leverage": leverage, "err": repr(e)})

        qty = max(order_notional, 0.0) / max(float(s.price), 1e-9)

        async def _adapt_size(q_coins: float, price: float) -> tuple[str, float, dict]:
            """Adapt order size to venue constraints and correct size units.
            Input q_coins is the desired base-coin quantity (notional/price).
            We convert to exchange "size units" (contracts or coins), apply step/min,
            and return:
              - size_str: rounded size in exchange units to send to API
              - adj_qty: same as float
              - info: { step, min, quote_per_size?, base_per_size?, size_unit }
            """
            step = None
            min_sz = None
            quote_per_size = None  # USDT (or USD) value per size unit
            base_per_size = None   # base coin amount per size unit
            info: dict = {}
            try:
                items = await client.get_instruments(CONFIG.ws_inst_type)
                for it in items:
                    inst_id = it.get("instId") or it.get("symbol")
                    if inst_id == s.symbol:
                        # Common keys across venues for size granularity
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
                        # Per-size multipliers (contract value); prefer explicit quote value, else base value
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
                                    if it.get(k) is not None:
                                        vv = float(it.get(k))
                                        if vv > 0:
                                            base_per_size = vv
                                            break
                        except Exception:
                            pass
                        break
            except Exception:
                pass

            # Convert desired coin quantity into exchange size units (contracts or coins)
            q_size = float(q_coins)
            size_unit = "coin"
            try:
                if CONFIG.contract_size_aware:
                    if quote_per_size is not None:
                        # size unit represents fixed USDT value
                        notional = float(q_coins) * float(price)
                        denom = max(float(quote_per_size), 1e-12)
                        q_size = notional / denom
                        size_unit = "contract"
                    elif base_per_size is not None:
                        # size unit represents fixed base coin amount
                        denom = max(float(base_per_size), 1e-12)
                        q_size = float(q_coins) / denom
                        size_unit = "contract"
            except Exception:
                pass

            # Apply step/min in size units
            adj = q_size
            if step and step > 0:
                adj = math.floor(adj / step) * step
            if (not step or step <= 0) and min_sz:
                # if only min given, floor to reasonable precision
                prec = 6
                adj = float(f"{adj:.{prec}f}")
            if min_sz and adj < min_sz:
                adj = min_sz

            size_s = f"{adj:.6f}"
            info = {"step": step, "min": min_sz, "size_unit": size_unit}
            if quote_per_size is not None:
                info["quote_per_size"] = float(quote_per_size)
            if base_per_size is not None:
                info["base_per_size"] = float(base_per_size)
            return size_s, float(adj), info

        size_str, adj_qty, norm_info = await _adapt_size(qty, float(s.price))
        if abs(adj_qty - qty) > 1e-12:
            try:
                print(
                    f"[order] Adjusted size for {s.symbol}: {qty:.6f} -> {adj_qty:.6f} (min={norm_info.get('min')}, step={norm_info.get('step')})"
                )
            except Exception:
                pass
            try:
                emit_metric("order_size_adjust", {"symbol": s.symbol, "from": qty, "to": adj_qty, **{k: v for k, v in norm_info.items() if v is not None}})
            except Exception:
                pass
        # Enforce minimum initial margin after final sizing
        try:
            px = float(s.price)
        except Exception:
            px = s.price
        try:
            lev_f = max(1.0, float(leverage))
        except Exception:
            lev_f = 1.0
        qps = norm_info.get("quote_per_size")
        bps = norm_info.get("base_per_size") or 1.0
        if not CONFIG.contract_size_aware:
            qps = None
            bps = 1.0
        # adj_qty is now in exchange size units; convert back to notional
        if qps is not None:
            final_notional = float(adj_qty) * float(qps)
        else:
            final_notional = float(adj_qty) * float(bps) * float(px)
        required_margin = final_notional / max(lev_f, 1e-9)
        if required_margin < CONFIG.min_initial_margin_usd:
            upsized = False
            if not self.paper and CONFIG.upsize_to_min_margin:
                # Attempt to increase quantity to meet the minimum margin floor,
                # respecting step/min, balance (with safety), and exposure caps.
                try:
                    step = None
                    min_sz = None
                    try:
                        step = norm_info.get("step")
                        min_sz = norm_info.get("min")
                    except Exception:
                        pass
                    denom = float(qps) if qps is not None else max(float(px) * float(bps), 1e-9)
                    target_qty_raw = (CONFIG.min_initial_margin_usd * lev_f) / denom
                    target_qty = target_qty_raw
                    import math as _math
                    if step and step > 0:
                        target_qty = _math.ceil(target_qty_raw / step) * step
                    if min_sz and target_qty < float(min_sz):
                        target_qty = float(min_sz)
                    target_notional = float(target_qty) * (float(qps) if qps is not None else float(bps) * float(px))
                    target_required_margin = target_notional / max(lev_f, 1e-9)
                    # Check exposure cap
                    if (self._exposure + target_notional) <= self.max_exposure:
                        # Check available balance if enabled
                        ok_bal = True
                        if CONFIG.check_balance_before_order:
                            try:
                                bal = await client.get_account_balance("USDT-FUTURES")
                                details = bal.get("details") or bal.get("data") or []
                                if isinstance(details, dict):
                                    details = [details]
                                avail = None
                                for d in details:
                                    cur = (d.get("ccy") or d.get("currency") or "").upper()
                                    if cur and cur != "USDT":
                                        continue
                                    for k in ("available", "availableUsd", "availableEquity", "cashAvailable", "availEq"):
                                        v = d.get(k)
                                        if v is not None:
                                            try:
                                                avail = float(v)
                                                break
                                            except Exception:
                                                pass
                                    if avail is not None:
                                        break
                                if avail is None:
                                    for k in ("available", "availableUsd", "availableEquity", "availEq"):
                                        v = bal.get(k)
                                        if v is not None:
                                            avail = float(v)
                                            break
                            except Exception:
                                avail = None
                            safety = CONFIG.margin_safety_bps / 10000.0
                            if avail is not None and avail < target_required_margin * (1.0 + safety):
                                ok_bal = False
                        if ok_bal:
                            # Accept upsize
                            prev_required = required_margin
                            adj_qty = float(target_qty)
                            size_str = f"{adj_qty:.6f}"
                            final_notional = target_notional
                            required_margin = target_required_margin
                            upsized = True
                            try:
                                emit_metric("order_upsize_to_min", {"symbol": s.symbol, "from_margin": prev_required, "to_margin": target_required_margin, "min_margin": CONFIG.min_initial_margin_usd})
                            except Exception:
                                pass
                except Exception:
                    upsized = False
            if not upsized:
                try:
                    print(f"[order/ERR] {s.symbol} required margin {required_margin:.2f} < min {CONFIG.min_initial_margin_usd:.2f}; skipping")
                except Exception:
                    pass
                try:
                    emit_metric(
                        "trade_skip",
                        {
                            "symbol": s.symbol,
                            "reason": "min_margin",
                            "required_margin": required_margin,
                            "min_margin": CONFIG.min_initial_margin_usd,
                            "final_notional": final_notional,
                            "leverage": lev_f,
                        },
                    )
                except Exception:
                    pass
                return
        if adj_qty <= 0:
            try:
                print(f"[order/ERR] {s.symbol} {s.side} adjusted size <= 0; skipping")
            except Exception:
                pass
            emit_metric("order_api_error", {"stage": "size_adjust", "symbol": s.symbol, "qty": qty, "adj": adj_qty, "info": norm_info})
            return
        # Log computed margin for visibility
        try:
            emit_metric("order_margin_info", {"symbol": s.symbol, "signal_id": s.id, "leverage": lev_f, "required_margin": required_margin, "final_notional": final_notional})
        except Exception:
            pass

        # Second-chance balance check right before sending the order (after rounding)
        if not self.paper and CONFIG.check_balance_before_order:
            try:
                bal2 = await client.get_account_balance("USDT-FUTURES")
            except Exception:
                bal2 = {}
            avail2 = None
            try:
                details = bal2.get("details") or bal2.get("data") or []
                if isinstance(details, dict):
                    details = [details]
                for d in details:
                    cur = (d.get("ccy") or d.get("currency") or "").upper()
                    if cur and cur != "USDT":
                        continue
                    for k in ("available", "availableUsd", "availableEquity", "cashAvailable", "availEq"):
                        v = d.get(k)
                        if v is not None:
                            try:
                                avail2 = float(v)
                                break
                            except Exception:
                                pass
                    if avail2 is not None:
                        break
            except Exception:
                avail2 = None
            if avail2 is None:
                try:
                    for k in ("available", "availableUsd", "availableEquity", "availEq"):
                        v = bal2.get(k)
                        if v is not None:
                            avail2 = float(v)
                            break
                except Exception:
                    pass
            try:
                safety = CONFIG.margin_safety_bps / 10000.0
                # Emit snapshot before decision
                try:
                    snap2 = {"symbol": s.symbol, "signal_id": s.id, "stage": "pre_send"}
                    if avail2 is not None:
                        snap2["available"] = float(avail2)
                    # equityUsd if present
                    eq2 = None
                    try:
                        details2 = bal2.get("details") or bal2.get("data") or []
                        if isinstance(details2, dict):
                            details2 = [details2]
                        if details2:
                            d0 = details2[0]
                            for k in ("equityUsd", "equity", "totalEquity"):
                                if d0.get(k) is not None:
                                    eq2 = float(d0.get(k))
                                    break
                    except Exception:
                        eq2 = None
                    if eq2 is not None:
                        snap2["equityUsd"] = float(eq2)
                    snap2["required_margin"] = float(required_margin)
                    snap2["final_notional"] = float(final_notional)
                    snap2["leverage"] = float(lev_f)
                    emit_metric("order_balance_snapshot", snap2)
                except Exception:
                    pass

                if avail2 is not None and avail2 < required_margin * (1.0 + safety):
                    emit_metric("trade_skip", {"symbol": s.symbol, "reason": "insufficient_balance_after_round", "available": avail2, "required": required_margin})
                    return
            except Exception:
                pass

        resp = await client.place_order(
            s.symbol,
            s.side,
            "market",
            size_str,
            margin_mode=margin_mode,
            position_side=pos_side_entry,
        )
        emit_metric("order_api", {"symbol": s.symbol, "signal_id": s.id, "side": s.side, "resp": resp})

        # Inspect response for explicit rejection and log
        def _resp_error(r) -> tuple[bool, dict | None]:
            try:
                if isinstance(r, dict):
                    code = str(r.get("code", "0"))
                    msg = r.get("msg")
                    if code and code != "0":
                        return True, {"code": code, "msg": msg}
                    data = r.get("data")
                    if isinstance(data, list):
                        for it in data:
                            c = str(it.get("code", "0"))
                            if c and c != "0":
                                return True, {"code": c, "msg": it.get("msg"), "item": it}
                return False, None
            except Exception:
                return False, None

        is_err, info = _resp_error(resp)
        if is_err:
            emit_metric("order_api_error", {"stage": "place_order", "symbol": s.symbol, "side": s.side, "info": info, "resp": resp})
            try:
                code = info.get("code") if isinstance(info, dict) else None
                msg = (info or {}).get("msg") if isinstance(info, dict) else None
                print(f"[order/REJ] {s.symbol} {s.side} code={code} msg={msg}")
            except Exception:
                pass
            # One retry: reduce size by 10% (respecting step/min if available)
            try:
                step = norm_info.get("step")
                min_sz = norm_info.get("min")
            except Exception:
                step = None
                min_sz = None
            try:
                retry_qty = adj_qty * 0.9
                if step and step > 0:
                    retry_qty = math.floor(retry_qty / step) * step
                if min_sz and retry_qty < min_sz:
                    retry_qty = min_sz
                retry_size = f"{retry_qty:.6f}"
                print(f"[order/RETRY] {s.symbol} {s.side} size={retry_size}")
                emit_metric("order_api_retry", {"symbol": s.symbol, "side": s.side, "size": retry_size})
                # Re-check min margin on retry
                try:
                    px = float(s.price)
                except Exception:
                    px = s.price
                try:
                    lev_f = max(1.0, float(leverage))
                except Exception:
                    lev_f = 1.0
                retry_notional = float(retry_qty) * (float(qps) if qps is not None else float(bps) * float(px))
                retry_required_margin = retry_notional / max(lev_f, 1e-9)
                if retry_required_margin < CONFIG.min_initial_margin_usd:
                    try:
                        print(f"[order/ERR] {s.symbol} retry margin {retry_required_margin:.2f} < min {CONFIG.min_initial_margin_usd:.2f}; abort retry")
                    except Exception:
                        pass
                    try:
                        emit_metric("trade_skip", {"symbol": s.symbol, "reason": "min_margin_retry", "required_margin": retry_required_margin, "min_margin": CONFIG.min_initial_margin_usd})
                    except Exception:
                        pass
                    return
                # Optional snapshot prior to retry send
                try:
                    if not self.paper and CONFIG.check_balance_before_order:
                        bal3 = await client.get_account_balance("USDT-FUTURES")
                        avail3 = None
                        try:
                            details3 = bal3.get("details") or bal3.get("data") or []
                            if isinstance(details3, dict):
                                details3 = [details3]
                            for d in details3:
                                cur = (d.get("ccy") or d.get("currency") or "").upper()
                                if cur and cur != "USDT":
                                    continue
                                for k in ("available", "availableUsd", "availableEquity", "cashAvailable", "availEq"):
                                    v = d.get(k)
                                    if v is not None:
                                        avail3 = float(v)
                                        break
                                if avail3 is not None:
                                    break
                        except Exception:
                            avail3 = None
                        snap3 = {"symbol": s.symbol, "signal_id": s.id, "stage": "pre_send_retry", "retry_size": retry_qty}
                        if avail3 is not None:
                            snap3["available"] = float(avail3)
                        snap3["required_margin"] = float(retry_notional) / max(lev_f, 1e-9)
                        snap3["final_notional"] = float(retry_notional)
                        snap3["leverage"] = float(lev_f)
                        emit_metric("order_balance_snapshot", snap3)
                except Exception:
                    pass

                resp2 = await client.place_order(
                    s.symbol,
                    s.side,
                    "market",
                    retry_size,
                    margin_mode=margin_mode,
                    position_side=pos_side_entry,
                )
                emit_metric("order_api", {"symbol": s.symbol, "signal_id": s.id, "side": s.side, "resp": resp2, "retry": 1})
                is_err2, info2 = _resp_error(resp2)
                if is_err2:
                    emit_metric("order_api_error", {"stage": "place_order_retry", "symbol": s.symbol, "side": s.side, "info": info2, "resp": resp2})
                    try:
                        code2 = info2.get("code") if isinstance(info2, dict) else None
                        msg2 = (info2 or {}).get("msg") if isinstance(info2, dict) else None
                        print(f"[order/REJ] {s.symbol} {s.side} retry code={code2} msg={msg2}")
                    except Exception:
                        pass
                    return
                else:
                    # update pos_size base on retried qty
                    size_str = retry_size
                    adj_qty = retry_qty
            except Exception as e:
                emit_metric("order_api_error", {"stage": "retry_exception", "symbol": s.symbol, "err": repr(e)})
                return
        else:
            # Print basic OK line once on success
            try:
                print(f"[order/OK] {s.symbol} {s.side} size={size_str}")
            except Exception:
                pass

        # Determine final filled size from positions (safer for scale-outs)
        pos_size = None
        try:
            ps = await client.get_positions(s.symbol)
            if isinstance(ps, list) and ps:
                p0 = ps[0]
                raw = p0.get("positions") or p0.get("pos") or "0"
                pos_size = abs(float(raw))
        except Exception:
            pos_size = None
        if pos_size is None or pos_size <= 0:
            try:
                pos_size = float(size_str)
            except Exception:
                pos_size = qty

        # Smart TP/SL planning & placement
        try:
            if CONFIG.enable_tpsl:
                px = float(s.price)
                dp = max(0, int(CONFIG.price_round_dp))

                def _round(p: float) -> str:
                    fmt = f"{{:.{dp}f}}"
                    return fmt.format(float(p))

                reduce_side = "sell" if s.side == "buy" else "buy"
                pos_side_reduce = pos_side_entry
                tp1 = tp2 = slp = None

                if CONFIG.enable_smart_tpsl and CONFIG.tpsl_mode in ("atr", "level_atr"):
                    # Use ATR% from signal features if available
                    atr_pct = None
                    try:
                        atr_pct = float(s.meta.get("features", {}).get("atr14_pct"))
                    except Exception:
                        atr_pct = None
                    if atr_pct is None or atr_pct <= 0:
                        # fallback to bps if ATR missing
                        bps_tp = CONFIG.tp_bps / 10000.0
                        bps_sl = CONFIG.sl_bps / 10000.0
                        if s.side == "buy":
                            tp1 = px * (1.0 + bps_tp)
                            slp = px * (1.0 - bps_sl)
                        else:
                            tp1 = px * (1.0 - bps_tp)
                            slp = px * (1.0 + bps_sl)
                    else:
                        # Risk unit based on ATR and optional regime multiplier
                        r_mult = CONFIG.atr_sl_mult
                        try:
                            if CONFIG.regime_scale_risk:
                                rm = float(s.meta.get("regime_mult", 1.0))
                                r_mult *= max(0.7, min(1.3, rm))
                        except Exception:
                            pass
                        R = atr_pct * px * float(r_mult)
                        lvl = s.level if CONFIG.use_signal_level else None
                        if s.side == "buy":
                            raw_sl = px - R
                            if lvl:
                                buf = px * (CONFIG.level_buffer_bps / 10000.0)
                                raw_sl = min(raw_sl, float(lvl) - buf)
                            slp = raw_sl
                            tp1 = px + (CONFIG.atr_tp1_mult * R)
                            tp2 = px + (CONFIG.atr_tp2_mult * R)
                        else:
                            raw_sl = px + R
                            if lvl:
                                buf = px * (CONFIG.level_buffer_bps / 10000.0)
                                raw_sl = max(raw_sl, float(lvl) + buf)
                            slp = raw_sl
                            tp1 = px - (CONFIG.atr_tp1_mult * R)
                            tp2 = px - (CONFIG.atr_tp2_mult * R)
                else:
                    # Legacy fixed bps mode
                    bps_tp = CONFIG.tp_bps / 10000.0
                    bps_sl = CONFIG.sl_bps / 10000.0
                    if s.side == "buy":
                        tp1 = px * (1.0 + bps_tp)
                        slp = px * (1.0 - bps_sl)
                    else:
                        tp1 = px * (1.0 - bps_tp)
                        slp = px * (1.0 + bps_sl)

                # Ensure triggers satisfy strict venue inequality vs latest price
                # Fetch latest price (best effort) and nudge triggers by a small epsilon if needed.
                try:
                    last_px = None
                    try:
                        tks = await client.get_tickers(CONFIG.ws_inst_type)
                        for t in tks or []:
                            inst = t.get("instId") or t.get("symbol") or t.get("instid")
                            if inst == s.symbol:
                                for k in ("last", "lastPrice", "lastPr", "lastPx", "lastTradedPx", "close", "c", "px"):
                                    v = t.get(k)
                                    if v is not None:
                                        try:
                                            last_px = float(v)
                                            break
                                        except Exception:
                                            pass
                                break
                    except Exception:
                        last_px = None
                    if last_px is not None:
                        eps_bps = max(1.0, float(CONFIG.trigger_eps_bps))
                        eps = float(last_px) * (eps_bps / 10000.0)
                        # Stop-loss: long => sl < last; short => sl > last
                        if slp is not None:
                            orig = float(slp)
                            if s.side == "buy" and float(slp) >= float(last_px):
                                slp = max(0.0, float(last_px) - float(eps))
                            elif s.side == "sell" and float(slp) <= float(last_px):
                                slp = float(last_px) + float(eps)
                            if abs(float(slp) - orig) > 0:
                                try:
                                    emit_metric("sl_adjust_for_last", {"symbol": s.symbol, "from": orig, "to": float(slp), "last": float(last_px), "eps_bps": eps_bps})
                                except Exception:
                                    pass
                        # Take-profit: long => tp > last; short => tp < last
                        def _tp_adjust(label: str, val: float | None) -> float | None:
                            if val is None:
                                return None
                            o = float(val)
                            n = o
                            if s.side == "buy" and o <= float(last_px):
                                n = float(last_px) + float(eps)
                            elif s.side == "sell" and o >= float(last_px):
                                n = float(last_px) - float(eps)
                            if abs(n - o) > 0:
                                try:
                                    emit_metric("tp_adjust_for_last", {"symbol": s.symbol, "which": label, "from": o, "to": n, "last": float(last_px), "eps_bps": eps_bps})
                                except Exception:
                                    pass
                            return n
                        tp1 = _tp_adjust("tp1", tp1)
                        tp2 = _tp_adjust("tp2", tp2)
                except Exception:
                    pass

                # Adjust SL towards a valid trigger relative to the latest price (venue requires strict inequality)
                try:
                    last_px = None
                    try:
                        tks = await client.get_tickers(CONFIG.ws_inst_type)
                        for t in tks or []:
                            inst = t.get("instId") or t.get("symbol") or t.get("instid")
                            if inst == s.symbol:
                                for k in ("last", "lastPrice", "lastPr", "lastPx", "lastTradedPx", "close", "c", "px"):
                                    v = t.get(k)
                                    if v is not None:
                                        try:
                                            last_px = float(v)
                                            break
                                        except Exception:
                                            pass
                                break
                    except Exception:
                        last_px = None
                    if last_px is not None and slp is not None:
                        eps_bps = max(1.0, float(CONFIG.trail_min_step_bps)) / 2.0
                        eps = float(last_px) * (eps_bps / 10000.0)
                        orig = float(slp)
                        if s.side == "buy" and float(slp) >= float(last_px):
                            slp = max(0.0, float(last_px) - float(eps))
                        elif s.side == "sell" and float(slp) <= float(last_px):
                            slp = float(last_px) + float(eps)
                        if abs(float(slp) - float(orig)) > 0:
                            try:
                                emit_metric("sl_adjust_for_last", {"symbol": s.symbol, "from": orig, "to": float(slp), "last": float(last_px)})
                            except Exception:
                                pass
                except Exception:
                    pass

                # Existing pending TP/SL (avoid duplicates)
                existing_tp_size = 0.0
                has_existing_sl = False
                try:
                    pend = await client.orders_tpsl_pending(inst_id=s.symbol, limit=100)
                    reduce_side = "sell" if s.side == "buy" else "buy"
                    for it in pend or []:
                        if (it.get("side") or reduce_side) != reduce_side:
                            continue
                        try:
                            if it.get("tpTriggerPrice") is not None:
                                sz = it.get("size")
                                if sz is not None:
                                    val = float(str(sz))
                                    if val < 0:
                                        # entire position covered
                                        existing_tp_size = max(existing_tp_size, float(pos_size)) if 'pos_size' in locals() and pos_size is not None else existing_tp_size
                                    else:
                                        existing_tp_size += abs(val)
                            if it.get("slTriggerPrice") is not None:
                                has_existing_sl = True
                        except Exception:
                            continue
                    try:
                        if has_existing_sl:
                            emit_metric("tpsl_skip_existing_sl", {"symbol": s.symbol})
                        if existing_tp_size > 0:
                            emit_metric("tpsl_existing_tp_size", {"symbol": s.symbol, "covered": existing_tp_size, "pos_size": float(pos_size) if 'pos_size' in locals() and pos_size is not None else None})
                    except Exception:
                        pass
                except Exception:
                    pass

                # Place one global SL for full remaining position (only if not already present)
                try:
                    if slp is not None and not has_existing_sl:
                        tpsl_sl = await client.place_tpsl(
                            s.symbol,
                            reduce_side,
                            margin_mode=margin_mode,
                            position_side=pos_side_reduce,
                            size="-1",
                            sl_trigger_price=_round(slp),
                            sl_order_price="-1",
                            trigger_price_type="last",
                            reduce_only=True,
                        )
                        ok_sl = bool(tpsl_sl and (tpsl_sl.get("tpslId") or tpsl_sl.get("algoId"))) if isinstance(tpsl_sl, dict) else False
                        if not ok_sl:
                            emit_metric("order_api_error", {"stage": "place_sl", "symbol": s.symbol, "reduce_side": reduce_side, "resp": tpsl_sl})
                            # Safety retry with wider epsilon and alternate trigger type
                            if CONFIG.tpsl_retry_on_fail:
                                try:
                                    last_px = None
                                    tks = await client.get_tickers(CONFIG.ws_inst_type)
                                    for t in tks or []:
                                        inst = t.get("instId") or t.get("symbol") or t.get("instid")
                                        if inst == s.symbol:
                                            for k in ("last","lastPrice","lastPr","lastPx","lastTradedPx","close","c","px"):
                                                v = t.get(k)
                                                if v is not None:
                                                    last_px = float(v)
                                                    break
                                            break
                                    if last_px is not None:
                                        eps = float(last_px) * (max(1.0, float(CONFIG.trigger_eps_bps)) * float(CONFIG.tpsl_retry_mult) / 10000.0)
                                        sl_fb = slp
                                        if s.side == "buy":
                                            sl_fb = max(0.0, float(last_px) - eps)
                                        else:
                                            sl_fb = float(last_px) + eps
                                        tpsl_sl2 = await client.place_tpsl(
                                            s.symbol,
                                            reduce_side,
                                            margin_mode=margin_mode,
                                            position_side=pos_side_reduce,
                                            size="-1",
                                            sl_trigger_price=_round(sl_fb),
                                            sl_order_price="-1",
                                            trigger_price_type=str(CONFIG.tpsl_retry_trigger_type or "mark"),
                                            reduce_only=True,
                                        )
                                        ok_sl2 = bool(tpsl_sl2 and (tpsl_sl2.get("tpslId") or tpsl_sl2.get("algoId"))) if isinstance(tpsl_sl2, dict) else False
                                        emit_metric("order_api_sl_retry", {"symbol": s.symbol, "ok": ok_sl2, "resp": tpsl_sl2})
                                        if ok_sl2:
                                            ok_sl = True
                                except Exception:
                                    pass
                        else:
                            emit_metric("order_api_sl", {"symbol": s.symbol, "side": reduce_side, "sl": tpsl_sl})
                except Exception as e:
                    emit_metric("order_api_error", {"stage": "place_sl", "symbol": s.symbol, "err": repr(e)})

                # Scale-out TPs (up to two). Use fractions of position size. Skip if existing TPs already cover the position.
                try:
                    remaining = float(pos_size)
                    placed_tp = 0.0
                    # Adapt TP sizes to instrument step/min just like entry orders
                    step = None
                    min_sz = None
                    try:
                        step = norm_info.get("step")
                        min_sz = norm_info.get("min")
                    except Exception:
                        pass

                    def _adapt_tp_qty(q: float) -> float:
                        adj = max(0.0, q)
                        if step and step > 0:
                            import math as _math
                            adj = _math.floor(adj / step) * step
                        if min_sz and adj < min_sz:
                            adj = 0.0
                        return float(adj)
                    # Adjust remaining by existing TP coverage
                    try:
                        if existing_tp_size > 0:
                            remaining = max(0.0, remaining - existing_tp_size)
                    except Exception:
                        pass

                    if tp1 is not None and CONFIG.scale_out_pct1 > 0 and remaining > 0:
                        raw_sz1 = max(0.0, min(remaining, remaining * CONFIG.scale_out_pct1))
                        sz1 = _adapt_tp_qty(raw_sz1)
                        if sz1 > 0:
                            tpsl_tp1 = await client.place_tpsl(
                                s.symbol,
                                reduce_side,
                                margin_mode=margin_mode,
                                position_side=pos_side_reduce,
                                size=f"{sz1:.6f}",
                                tp_trigger_price=_round(tp1),
                                tp_order_price="-1",
                                trigger_price_type="last",
                                reduce_only=True,
                            )
                            placed_tp += sz1
                            ok_tp1 = bool(tpsl_tp1 and (tpsl_tp1.get("tpslId") or tpsl_tp1.get("algoId"))) if isinstance(tpsl_tp1, dict) else False
                            if not ok_tp1:
                                emit_metric("order_api_error", {"stage": "place_tp1", "symbol": s.symbol, "resp": tpsl_tp1})
                                if CONFIG.tpsl_retry_on_fail:
                                    try:
                                        last_px = None
                                        tks = await client.get_tickers(CONFIG.ws_inst_type)
                                        for t in tks or []:
                                            inst = t.get("instId") or t.get("symbol") or t.get("instid")
                                            if inst == s.symbol:
                                                for k in ("last","lastPrice","lastPr","lastPx","lastTradedPx","close","c","px"):
                                                    v = t.get(k)
                                                    if v is not None:
                                                        last_px = float(v)
                                                        break
                                                break
                                        if last_px is not None:
                                            eps = float(last_px) * (max(1.0, float(CONFIG.trigger_eps_bps)) * float(CONFIG.tpsl_retry_mult) / 10000.0)
                                            tp1_fb = tp1
                                            if s.side == "buy" and float(tp1_fb) <= float(last_px):
                                                tp1_fb = float(last_px) + eps
                                            elif s.side == "sell" and float(tp1_fb) >= float(last_px):
                                                tp1_fb = float(last_px) - eps
                                            tpsl_tp1b = await client.place_tpsl(
                                                s.symbol,
                                                reduce_side,
                                                margin_mode=margin_mode,
                                                position_side=pos_side_reduce,
                                                size=f"{sz1:.6f}",
                                                tp_trigger_price=_round(tp1_fb),
                                                tp_order_price="-1",
                                                trigger_price_type=str(CONFIG.tpsl_retry_trigger_type or "mark"),
                                                reduce_only=True,
                                            )
                                            ok_tp1b = bool(tpsl_tp1b and (tpsl_tp1b.get("tpslId") or tpsl_tp1b.get("algoId"))) if isinstance(tpsl_tp1b, dict) else False
                                            emit_metric("order_api_tp_retry", {"symbol": s.symbol, "which": 1, "ok": ok_tp1b, "resp": tpsl_tp1b})
                                    except Exception:
                                        pass
                            else:
                                emit_metric("order_api_tp", {"symbol": s.symbol, "which": 1, "tp": tpsl_tp1})
                    if tp2 is not None and CONFIG.scale_out_pct2 > 0 and placed_tp < remaining:
                        rem = max(0.0, remaining - placed_tp)
                        raw_sz2 = max(0.0, min(rem, remaining * CONFIG.scale_out_pct2))
                        sz2 = _adapt_tp_qty(raw_sz2)
                        if sz2 > 0:
                            tpsl_tp2 = await client.place_tpsl(
                                s.symbol,
                                reduce_side,
                                margin_mode=margin_mode,
                                position_side=pos_side_reduce,
                                size=f"{sz2:.6f}",
                                tp_trigger_price=_round(tp2),
                                tp_order_price="-1",
                                trigger_price_type="last",
                                reduce_only=True,
                            )
                            placed_tp += sz2
                            ok_tp2 = bool(tpsl_tp2 and (tpsl_tp2.get("tpslId") or tpsl_tp2.get("algoId"))) if isinstance(tpsl_tp2, dict) else False
                            if not ok_tp2:
                                emit_metric("order_api_error", {"stage": "place_tp2", "symbol": s.symbol, "resp": tpsl_tp2})
                                if CONFIG.tpsl_retry_on_fail:
                                    try:
                                        last_px = None
                                        tks = await client.get_tickers(CONFIG.ws_inst_type)
                                        for t in tks or []:
                                            inst = t.get("instId") or t.get("symbol") or t.get("instid")
                                            if inst == s.symbol:
                                                for k in ("last","lastPrice","lastPr","lastPx","lastTradedPx","close","c","px"):
                                                    v = t.get(k)
                                                    if v is not None:
                                                        last_px = float(v)
                                                        break
                                                break
                                        if last_px is not None:
                                            eps = float(last_px) * (max(1.0, float(CONFIG.trigger_eps_bps)) * float(CONFIG.tpsl_retry_mult) / 10000.0)
                                            tp2_fb = tp2
                                            if s.side == "buy" and float(tp2_fb) <= float(last_px):
                                                tp2_fb = float(last_px) + eps
                                            elif s.side == "sell" and float(tp2_fb) >= float(last_px):
                                                tp2_fb = float(last_px) - eps
                                            tpsl_tp2b = await client.place_tpsl(
                                                s.symbol,
                                                reduce_side,
                                                margin_mode=margin_mode,
                                                position_side=pos_side_reduce,
                                                size=f"{sz2:.6f}",
                                                tp_trigger_price=_round(tp2_fb),
                                                tp_order_price="-1",
                                                trigger_price_type=str(CONFIG.tpsl_retry_trigger_type or "mark"),
                                                reduce_only=True,
                                            )
                                            ok_tp2b = bool(tpsl_tp2b and (tpsl_tp2b.get("tpslId") or tpsl_tp2b.get("algoId"))) if isinstance(tpsl_tp2b, dict) else False
                                            emit_metric("order_api_tp_retry", {"symbol": s.symbol, "which": 2, "ok": ok_tp2b, "resp": tpsl_tp2b})
                                    except Exception:
                                        pass
                            else:
                                emit_metric("order_api_tp", {"symbol": s.symbol, "which": 2, "tp": tpsl_tp2})
                except Exception as e:
                    emit_metric("order_api_error", {"stage": "place_tp", "symbol": s.symbol, "err": repr(e)})
        except Exception as e:
            emit_metric("order_api_error", {"stage": "attach_tpsl", "symbol": s.symbol, "err": repr(e)})


GLOBAL_TRADER = AutoTrader()

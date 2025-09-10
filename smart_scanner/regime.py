"""
Market regime detector.

Current behavior:
  - Computes BTC 15m EMA slope + realized volatility to classify: bull / bear / chop.

Smarter extensions implemented (backwards compatible):
  - Optional context features (breadth, average spread, trade rate, funding/basis proxies).
  - Composite continuous score with light smoothing + hysteresis.
  - Returns both a discrete regime and a suggested risk multiplier to scale signals.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Literal, Dict, Any, List, Optional, Union
import numpy as np

from .indicators import ema
# Accept both Python floats and NumPy floating scalars for helpers
NumberLike = Union[float, np.floating[Any]]

RegimeKind = Literal["bull", "bear", "chop"]


@dataclass
class RegimeState:
    kind: RegimeKind
    vol_annualized: float
    ema_fast_slope: float
    score: float = 0.0  # composite in [-1, 1] (bear .. bull)
    risk_mult: float = 0.9  # suggested multiplier ~ [0.7, 1.1]


_last_score: float = 0.0
_last_kind: RegimeKind = "chop"


def _tanh_clip(x: NumberLike, scale: float = 1.0) -> float:
    import math
    return math.tanh(float(x) * float(scale))


def detect_regime(
    btc_close_15m: List[float],
    ema_fast: int = 21,
    ema_slow: int = 50,
    context: Optional[Dict[str, float]] = None,
    smoothing: float = 0.7,
) -> RegimeState:
    """
    Detect market regime.

    Parameters
    - btc_close_15m: 15m closes for BTC-USDT (ascending)
    - context (optional):
        breadth_pct: 0..1 fraction of symbols above their 50-EMA
        avg_spread_bps: average top-of-book spread across universe (bps)
        avg_trade_rate: average trades/min across universe
        funding_8h: mean funding rate (decimal) if available
        basis_bps: perp basis in bps if available
    - smoothing: EMA on the composite score (0..1)

    Returns RegimeState(kind, vol_annualized, ema_fast_slope, score, risk_mult)
    """
    global _last_score, _last_kind

    arr = np.asarray(btc_close_15m, dtype=float)
    if len(arr) < ema_slow + 5:
        return RegimeState("chop", vol_annualized=0.0, ema_fast_slope=0.0, score=_last_score, risk_mult=0.9)

    ef = ema(arr, ema_fast)
    es = ema(arr, ema_slow)
    ef_last = float(ef[-1])
    ef_prev5 = float(ef[-5])
    es_last = float(es[-1])

    denom = ef_prev5 if ef_prev5 != 0.0 else 1e-9
    slope = (ef_last - ef_prev5) / max(denom, 1e-9)
    trend = _tanh_clip(slope * 6.0) + (_tanh_clip((ef_last / max(es_last, 1e-9)) - 1.0, 4.0)) * 0.5

    # Realized vol ratio (short vs long) to avoid absolute-scale dependency
    rets = np.diff(np.log(arr))
    if len(rets) >= 96:
        vol_short = float(np.std(rets[-96:]))
    else:
        vol_short = float(np.std(rets))

    if len(rets) >= 400:
        vol_long = float(np.std(rets[-400:]))
    else:
        base_long = float(np.std(rets))
        vol_long = max(base_long, 1e-9)

    vol_ratio = vol_short / max(vol_long, 1e-9)
    # Higher vol → more negative for risk
    vol_term = -_tanh_clip((vol_ratio - 1.0) * 1.5)

    if len(rets) >= 96:
        vol_ann = float(vol_short * (96 * 365) ** 0.5)
    else:
        vol_ann = float(float(np.std(rets)) * (len(rets) * 365) ** 0.5)

    # Optional context
    breadth_term = 0.0
    liq_term = 0.0
    funding_term = 0.0
    if context:
        b = float(context.get("breadth_pct", 0.5))
        breadth_term = _tanh_clip((b - 0.5) * 3.0)  # push away from 0.5
        sp = float(context.get("avg_spread_bps", 8.0))
        tr = float(context.get("avg_trade_rate", 2.0))
        # Tight spreads + higher trade rate = positive
        liq_term = (-_tanh_clip((sp - 8.0) / 10.0)) + _tanh_clip((tr - 2.0) / 3.0)
        fd = float(context.get("funding_8h", 0.0))
        bs = float(context.get("basis_bps", 0.0))
        # Mildly reward positive funding and basis in bull, penalize extreme negative
        funding_term = _tanh_clip(fd * 8.0 + bs / 300.0)

    # Composite score in [-1, 1]
    score_raw = 0.9 * trend + 0.7 * breadth_term + 0.5 * liq_term + 0.6 * vol_term + 0.3 * funding_term
    score = float(smoothing * _last_score + (1.0 - smoothing) * score_raw)

    # Hysteresis on thresholds
    hi, lo = 0.15, -0.15
    if _last_kind == "bull":
        lo -= 0.05
    elif _last_kind == "bear":
        hi += 0.05
    if score > hi:
        kind: RegimeKind = "bull"
    elif score < lo:
        kind = "bear"
    else:
        kind = "chop"

    # Map score to suggested risk multiplier ~ [0.7, 1.1]
    risk_mult = float(0.9 + 0.2 * max(-1.0, min(1.0, float(score))))

    _last_score, _last_kind = score, kind
    return RegimeState(kind=kind, vol_annualized=float(vol_ann), ema_fast_slope=float(slope), score=score, risk_mult=risk_mult)

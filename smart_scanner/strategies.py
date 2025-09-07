"""
Strategy library. Keep each strategy small, fast, and composable.
Return (passed, score, side, tags, level).
"""

from __future__ import annotations
from typing import Dict, Any, Tuple, List
import numpy as np

from .indicators import ema, rsi, atr, supertrend

StrategyResult = Tuple[bool, float, str, List[str], float | None]


def _norm(x: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (x - lo) / (hi - lo)))


def breakout_strategy(
    o: np.ndarray,
    h: np.ndarray,
    l: np.ndarray,
    c: np.ndarray,
    vol: np.ndarray,
    *,
    lookback: int = 20,
    rsi_len: int = 14,
) -> StrategyResult:
    if len(c) < max(lookback + 1, rsi_len + 5):
        return (False, 0.0, "buy", [], None)
    hh = np.max(h[-(lookback + 1) : -1])
    broke = c[-1] > hh and c[-1] > o[-1]
    _rsi = rsi(c, rsi_len)
    rs = _rsi[-1]
    vol_ok = vol[-1] > np.mean(vol[-lookback:]) * 1.3
    score = 0.0
    tags: List[str] = []
    if broke:
        score += 1.2
        tags.append("breakout")
    if vol_ok:
        score += 0.8
        tags.append("vol_spike")
    if rs > 55:
        score += _norm(rs, 55, 75) * 1.0
        tags.append("rsi_confirm")
    return (broke and (vol_ok or rs > 55), score, "buy", tags, float(hh))


def pullback_ema_strategy(
    o: np.ndarray,
    h: np.ndarray,
    l: np.ndarray,
    c: np.ndarray,
    *,
    fast: int = 21,
    slow: int = 50,
) -> StrategyResult:
    if len(c) < slow + 5:
        return (False, 0.0, "buy", [], None)
    ef = ema(c, fast)
    es = ema(c, slow)
    trend_up = ef[-1] > es[-1] and ef[-1] > ef[-5]
    touched = (l[-1] <= ef[-1] <= h[-1]) or abs(c[-1] - ef[-1]) / max(
        ef[-1], 1e-9
    ) < 0.0025
    score = 0.0
    tags: List[str] = []
    if trend_up:
        score += 0.9
        tags.append("trend_up")
    if touched:
        score += 0.8
        tags.append("ema21_touch")
    return (trend_up and touched, score, "buy", tags, float(ef[-1]))


def momentum_burst_strategy(
    o: np.ndarray, h: np.ndarray, l: np.ndarray, c: np.ndarray
) -> StrategyResult:
    if len(c) < 30:
        return (False, 0.0, "buy", [], None)
    rng = h[-1] - l[-1]
    body = abs(c[-1] - o[-1])
    body_ratio = body / max(rng, 1e-9)
    atr14 = atr(h, l, c, 14)
    atr_ratio = rng / max(atr14[-1], 1e-9)
    passed = (body_ratio > 0.6) and (atr_ratio > 1.2) and c[-1] > o[-1]
    score = (body_ratio - 0.6) * 1.5 + (atr_ratio - 1.0) * 0.8
    score = max(0.0, score)
    tags = ["momentum"] if passed else []
    return (passed, score, "buy", tags, None)


def supertrend_flip_strategy(
    h: np.ndarray, l: np.ndarray, c: np.ndarray
) -> StrategyResult:
    if len(c) < 30:
        return (False, 0.0, "buy", [], None)
    st, dirn = supertrend(h, l, c, period=10, mult=3.0)
    flip_long = dirn[-1] == 1 and dirn[-2] == -1 and c[-1] > st[-1]
    score = 1.0 if flip_long else 0.0
    return (bool(flip_long), score, "buy", ["st_flip"], float(st[-1]))


def evaluate_all(
    o: np.ndarray, h: np.ndarray, l: np.ndarray, c: np.ndarray, v: np.ndarray
) -> Dict[str, StrategyResult]:
    results: Dict[str, StrategyResult] = {}
    results["breakout"] = breakout_strategy(o, h, l, c, v)
    results["pullback"] = pullback_ema_strategy(o, h, l, c)
    results["momentum"] = momentum_burst_strategy(o, h, l, c)
    results["st_flip"] = supertrend_flip_strategy(h, l, c)
    return results

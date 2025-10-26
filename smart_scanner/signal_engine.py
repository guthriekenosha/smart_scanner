"""
Signal engine: collects candles, computes indicators/strategies, blends via bandit + regime.
"""

from __future__ import annotations
import math
from typing import Dict, Any, List, Tuple, Optional
import numpy as np

from .bandit import GLOBAL_BANDIT
from .config import CONFIG
from .signal_types import Signal
from .strategies import evaluate_all
from .features import compute_basic_features
from .calibration import GLOBAL_CALIBRATOR
from .regime import detect_regime


def _logistic(x: float) -> float:
    return 1 / (1 + math.exp(-x))


def _compose_score(
    strategy_scores: Dict[str, float], weights: Dict[str, float]
) -> float:
    total = 0.0
    for k, s in strategy_scores.items():
        w = weights.get(k, 0.0)
        total += w * s
    # rescale to [0, 7] roughly
    return max(0.0, min(CONFIG.score_max, total * 7.0))


def _prob_from_components(score: float, tags: List[str], n_comp: int, feats: Dict[str, float]) -> float:
    # Calibrate around mid-scores; reward confluence; cap realistically.
    # Base grows smoothly; extra 3-12% only if multiple strong tags/components.
    base = 1 / (1 + math.exp(-(score - 3.5) * 0.7))
    comp_bonus = min(0.06 * max(0, n_comp - 1), 0.12)
    tag_bonus = 0.03 * sum(
        1
        for t in tags
        if t in (
            # bullish tags
            "breakout", "momentum", "trend_up", "rsi_confirm", "st_flip",
            # bearish symmetric tags
            "breakdown", "momentum_down", "trend_down", "st_flip_down",
        )
    )
    # If a calibration model is available, prefer it (uses features including score)
    feats_with_score = dict(feats)
    feats_with_score["score"] = float(score)
    cal_p = GLOBAL_CALIBRATOR.prob(feats_with_score)
    if cal_p is not None:
        return max(0.05, min(0.95, cal_p))
    p = base + comp_bonus + tag_bonus
    return max(0.05, min(0.90 if n_comp < 3 else 0.93, p))


def _build_entry_hint(
    side: str,
    price: float,
    level: float | None,
    components: List[str],
    tags: List[str],
    feats: Dict[str, float] | None,
    score: float,
    prob: float,
    timeframe: str,
) -> Dict[str, Any]:
    """
    Prototype heuristics for suggesting an entry plan.
    Returns a lightweight dict describing the preferred approach.
    """
    def _confidence(s: float, p: float) -> str:
        if s >= 6.0 or p >= 0.82:
            return "high"
        if s >= 5.0 or p >= 0.7:
            return "medium"
        return "base"

    def _atr_buffer_bps(feats_map: Dict[str, float] | None) -> float:
        if not feats_map:
            return 6.0
        try:
            atr_pct = float(feats_map.get("atr14_pct", 0.0))
        except (TypeError, ValueError):
            atr_pct = 0.0
        if atr_pct <= 0:
            return 6.0
        atr_bps = atr_pct * 10000.0
        return float(max(3.0, min(18.0, atr_bps / 4.0)))

    buf_bps = _atr_buffer_bps(feats)
    price = float(price)
    hint: Dict[str, Any] = {
        "decision": "enter_now",
        "mode": "market",
        "price": price,
        "reason": "momentum continuation" if "momentum" in components or "momentum" in tags else "clean setup",
    }

    is_breakout = "breakout" in components
    is_breakdown = "breakdown" in components
    confidence = _confidence(score, prob)

    if level is not None and ((side == "buy" and is_breakout) or (side == "sell" and is_breakdown)):
        base = max(abs(level), 1e-9)
        if side == "buy" and price > level:
            gap_bps = (price - level) / base * 10000.0
            if gap_bps > buf_bps * 0.75:
                limit_price = max(level * (1.0 + buf_bps / 10000.0), 0.0)
                hint = {
                    "decision": "wait_for_retest",
                    "mode": "limit",
                    "price": limit_price,
                    "buffer_bps": buf_bps,
                    "gap_bps": gap_bps,
                    "reason": "wait for breakout retest",
                }
        elif side == "sell" and price < level:
            gap_bps = (level - price) / base * 10000.0
            if gap_bps > buf_bps * 0.75:
                limit_price = level * (1.0 - buf_bps / 10000.0)
                hint = {
                    "decision": "wait_for_retest",
                    "mode": "limit",
                    "price": limit_price,
                    "buffer_bps": buf_bps,
                    "gap_bps": gap_bps,
                    "reason": "wait for breakdown retest",
                }

    if hint["decision"] == "enter_now":
        if "ema21_touch" in tags or "pullback" in components or "retrace" in components:
            hint["reason"] = "pullback filled into support/resistance"
        elif "st_flip" in tags or "st_flip_down" in tags:
            hint["reason"] = "trend flip confirmation"

    hint["side"] = side
    hint["timeframe"] = timeframe
    hint["confidence"] = confidence
    hint["score"] = float(score)
    hint["prob"] = float(prob)
    return hint


def evaluate_symbol_timeframe(
    symbol: str,
    timeframe: str,
    candles: List[List[str]],
    btc_15m_close: List[float] | None = None,
    extra_features: Optional[Dict[str, float]] = None,
) -> List[Signal]:
    """
    Convert kline rows → ndarray → strategies → composite signal(s).
    """
    if len(candles) < 60:
        return []

    # candles: [['ts','o','h','l','c','vol','volCcy','volQuote','confirm'], ...] newest first sometimes; normalize
    # Try to detect ordering. If ts strictly decreases, reverse it.
    try:
        ts0 = int(candles[0][0])
        ts1 = int(candles[1][0])
        if ts1 < ts0:
            candles = list(reversed(candles))
    except Exception:
        candles = list(reversed(candles))

    ts = np.array([int(x[0]) for x in candles], dtype=np.int64)
    o = np.array([float(x[1]) for x in candles], dtype=float)
    h = np.array([float(x[2]) for x in candles], dtype=float)
    l = np.array([float(x[3]) for x in candles], dtype=float)
    c = np.array([float(x[4]) for x in candles], dtype=float)
    v = np.array([float(x[5]) for x in candles], dtype=float)

    # Market regime (optional, based on BTC 15m)
    regime_weight = {"bull": 1.0, "bear": 0.8, "chop": 0.7}
    if btc_15m_close and len(btc_15m_close) > 60:
        _reg = detect_regime(btc_15m_close)
        # Prefer calibrated multiplier if provided; fallback to discrete mapping
        regime_mult = float(getattr(_reg, "risk_mult", 0.0)) or regime_weight.get(_reg.kind, 0.9)
    else:
        regime_mult = 0.9

    strat_results = evaluate_all(o, h, l, c, v)
    feats = compute_basic_features(o, h, l, c, v)
    if extra_features:
        feats.update({k: float(v) for k, v in extra_features.items() if isinstance(v, (int, float))})

    components = []
    strat_scores: Dict[str, float] = {}
    side_votes = {"buy": 0, "sell": 0}
    side_score = {"buy": 0.0, "sell": 0.0}
    tags: List[str] = []
    level = None

    for name, (passed, score, side, _tags, _level) in strat_results.items():
        if passed:
            components.append(name)
            strat_scores[name] = score
            side_votes[side] = side_votes.get(side, 0) + 1
            try:
                side_score[side] = side_score.get(side, 0.0) + float(score)
            except Exception:
                pass
            tags.extend(_tags)
            if _level is not None:
                level = _level

    if not components:
        return []

    # Optional confluence gate
    if CONFIG.require_multi_components and len(components) < max(1, CONFIG.min_components):
        return []

    # Bandit weights for adaptive blending (contextual scaffold)
    # Simple contextual features
    chg = float(c[-1] - c[-5]) / max(c[-5], 1e-9)
    vol = float(np.std(np.diff(np.log(c[-30:]))) if len(c) >= 31 else 0.0)
    context = {"chg5": chg, "vol": vol}
    try:
        weights = GLOBAL_BANDIT.score_context(list(strat_results.keys()), context)
    except Exception:
        weights = GLOBAL_BANDIT.score(list(strat_results.keys()))
    raw_score = _compose_score(strat_scores, weights) * regime_mult
    score = float(max(0.0, min(CONFIG.score_max, raw_score)))
    if score < CONFIG.min_score:
        return []

    # Choose side by aggregate score first; tie-breaker by votes; final tie -> buy
    try:
        if side_score["buy"] != side_score["sell"]:
            side = "buy" if side_score["buy"] > side_score["sell"] else "sell"
        elif side_votes["buy"] != side_votes["sell"]:
            side = "buy" if side_votes["buy"] > side_votes["sell"] else "sell"
        else:
            side = "buy"
    except Exception:
        side = "buy" if side_votes["buy"] >= side_votes.get("sell", 0) else "sell"
    prob = _prob_from_components(score, tags, n_comp=len(components), feats=feats)
    ev = (prob * 1.0) - ((1 - prob) * 0.5)  # simplistic EV estimate; tune later
    entry_hint = _build_entry_hint(
        side=side,
        price=float(c[-1]),
        level=level,
        components=components,
        tags=tags,
        feats=feats,
        score=score,
        prob=prob,
        timeframe=timeframe,
    )

    sig = Signal(
        symbol=symbol,
        timeframe=timeframe,
        side=side,
        score=score,
        price=float(c[-1]),
        components=components,
        prob=float(prob),
        ev=float(ev),
        confirmation=(
            "confirmed" if CONFIG.require_confirmation == 0 else "anticipation"
        ),
        tags=list(dict.fromkeys(tags)),  # unique
        level=level,
        entry_hint=entry_hint,
        meta={
            "strat_scores": strat_scores,
            "weights": weights,
            "regime_mult": regime_mult,
            "context": context,
            "features": feats,
        },
    )
    return [sig]

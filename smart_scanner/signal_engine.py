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
        if t in ("breakout", "momentum", "trend_up", "rsi_confirm", "st_flip")
    )
    # If a calibration model is available, prefer it (uses features including score)
    feats_with_score = dict(feats)
    feats_with_score["score"] = float(score)
    cal_p = GLOBAL_CALIBRATOR.prob(feats_with_score)
    if cal_p is not None:
        return max(0.05, min(0.95, cal_p))
    p = base + comp_bonus + tag_bonus
    return max(0.05, min(0.90 if n_comp < 3 else 0.93, p))


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
    tags: List[str] = []
    level = None

    for name, (passed, score, side, _tags, _level) in strat_results.items():
        if passed:
            components.append(name)
            strat_scores[name] = score
            side_votes[side] = side_votes.get(side, 0) + 1
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

    side = "buy" if side_votes["buy"] >= side_votes.get("sell", 0) else "sell"
    prob = _prob_from_components(score, tags, n_comp=len(components), feats=feats)
    ev = (prob * 1.0) - ((1 - prob) * 0.5)  # simplistic EV estimate; tune later

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
        meta={
            "strat_scores": strat_scores,
            "weights": weights,
            "regime_mult": regime_mult,
            "context": context,
            "features": feats,
        },
    )
    return [sig]

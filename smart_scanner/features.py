from __future__ import annotations
from typing import Dict
import numpy as np

from .indicators import ema, atr


def compute_basic_features(o: np.ndarray, h: np.ndarray, l: np.ndarray, c: np.ndarray, v: np.ndarray) -> Dict[str, float]:
    n = len(c)
    feats: Dict[str, float] = {}
    if n < 10:
        return feats
    # momentum and trend proxies
    feats["ret_5"] = float((float(c[-1]) - float(c[-6])) / max(float(c[-6]), 1e-9)) if n >= 6 else 0.0
    feats["ret_20"] = float((float(c[-1]) - float(c[-21])) / max(float(c[-21]), 1e-9)) if n >= 21 else 0.0
    e21 = ema(c, 21)
    e50 = ema(c, 50)
    feats["ema21_slope_5"] = float((float(e21[-1]) - float(e21[-6])) / max(float(abs(e21[-6])), 1e-9)) if n >= 6 else 0.0
    feats["ema21_gt_ema50"] = float(1.0 if e21[-1] > e50[-1] else 0.0)
    # volatility
    a14 = atr(h, l, c, 14)
    feats["atr14_pct"] = float(float(a14[-1]) / max(float(c[-1]), 1e-9))
    # range + body
    rng = float(h[-1] - l[-1])
    body = float(abs(c[-1] - o[-1]))
    feats["body_ratio"] = float(body / max(rng, 1e-9))
    # volume burst
    feats["vol_burst_20"] = float(float(v[-1]) / max(1e-9, float(np.mean(v[-20:])))) if n >= 20 else 0.0
    return feats


"""
Vectorized, fast TA helpers (NumPy-first; no pandas dependency at callsite).
"""

from __future__ import annotations
import numpy as np
from typing import Tuple


def ema(arr: np.ndarray, length: int) -> np.ndarray:
    alpha = 2 / (length + 1)
    out = np.empty_like(arr, dtype=float)
    out[:] = np.nan
    if len(arr) == 0:
        return out
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = alpha * arr[i] + (1 - alpha) * out[i - 1]
    return out


def rsi(closes: np.ndarray, length: int = 14) -> np.ndarray:
    if len(closes) < length + 1:
        return np.full_like(closes, np.nan, dtype=float)
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    roll_up = np.empty_like(closes, dtype=float)
    roll_up[:] = np.nan
    roll_down = np.empty_like(closes, dtype=float)
    roll_down[:] = np.nan
    roll_up[length] = gains[:length].mean()
    roll_down[length] = losses[:length].mean()
    for i in range(length + 1, len(closes)):
        roll_up[i] = (roll_up[i - 1] * (length - 1) + gains[i - 1]) / length
        roll_down[i] = (roll_down[i - 1] * (length - 1) + losses[i - 1]) / length
    rs = roll_up / (roll_down + 1e-12)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def atr(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, length: int = 14
) -> np.ndarray:
    trs = np.empty_like(closes, dtype=float)
    trs[0] = highs[0] - lows[0]
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs[i] = tr
    return ema(trs, length)


def supertrend(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    period: int = 10,
    mult: float = 3.0,
) -> Tuple[np.ndarray, np.ndarray]:
    _atr = atr(highs, lows, closes, period)
    hl2 = (highs + lows) / 2.0
    upper = hl2 + mult * _atr
    lower = hl2 - mult * _atr

    st = np.empty_like(closes, dtype=float)
    dirn = np.empty_like(closes, dtype=int)
    st[0] = upper[0]
    dirn[0] = 1
    for i in range(1, len(closes)):
        prev = st[i - 1]
        if closes[i] > upper[i - 1]:
            st[i] = lower[i]
            dirn[i] = 1
        elif closes[i] < lower[i - 1]:
            st[i] = upper[i]
            dirn[i] = -1
        else:
            st[i] = prev
            dirn[i] = dirn[i - 1]
            if dirn[i] == 1 and lower[i] > st[i]:
                st[i] = lower[i]
            if dirn[i] == -1 and upper[i] < st[i]:
                st[i] = upper[i]
    return st, dirn

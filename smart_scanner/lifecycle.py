"""
In-memory lifecycle helpers: dedupe + cooldown.
"""

from __future__ import annotations
import time
from typing import Dict

from .signal_types import Signal

_DEDUPE: Dict[str, float] = {}
_COOLDOWN: Dict[str, float] = {}


def dedupe(signal: Signal, ttl_sec: int) -> bool:
    now = time.time()
    last = _DEDUPE.get(signal.id)
    _DEDUPE[signal.id] = now
    # If seen within ttl, dedupe it.
    return last is not None and (now - last) < ttl_sec


def on_cooldown(key: str, cooldown_sec: int) -> bool:
    now = time.time()
    last = _COOLDOWN.get(key)
    if last is None:
        return False
    return (now - last) < cooldown_sec


def touch_cooldown(key: str):
    _COOLDOWN[key] = time.time()

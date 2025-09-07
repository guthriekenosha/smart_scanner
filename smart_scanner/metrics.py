from __future__ import annotations
import json
import os
import time
from typing import Any, Dict

from .config import CONFIG


def emit(kind: str, payload: Dict[str, Any]) -> None:
    if not CONFIG.metrics_enabled:
        return
    rec = {"ts": int(time.time()), "kind": kind, **payload}
    try:
        with open(CONFIG.metrics_path, "a") as f:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
    except Exception:
        pass


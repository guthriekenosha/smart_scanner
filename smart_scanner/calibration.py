from __future__ import annotations
import json
from typing import Dict, Any, Optional
import math
import os

from .config import CONFIG


class Calibrator:
    def __init__(self):
        self._loaded = False
        self._bias = 0.0
        self._coef: Dict[str, float] = {}
        self._path = CONFIG.calibration_path
        # Optional: hot-reload if calibration file changes (disabled by default)
        try:
            self._reload_sec = float(os.getenv("CALIB_RELOAD_SEC", "0"))
        except Exception:
            self._reload_sec = 0.0
        self._last_check = 0.0
        self._mtime = 0.0
        self._try_load()

    def _try_load(self):
        try:
            if not os.path.exists(self._path):
                return
            with open(self._path, "r") as f:
                data = json.load(f)
            self._bias = float(data.get("bias", 0.0))
            coefs = data.get("coef", {})
            if isinstance(coefs, dict):
                self._coef = {str(k): float(v) for k, v in coefs.items()}
            self._loaded = True
            try:
                self._mtime = os.path.getmtime(self._path)
            except Exception:
                self._mtime = 0.0
        except Exception:
            self._loaded = False

    def prob(self, features: Dict[str, float]) -> Optional[float]:
        # Optional hot-reload check
        if self._reload_sec > 0:
            try:
                import time as _time

                now = _time.time()
                if (now - self._last_check) >= self._reload_sec:
                    self._last_check = now
                    mt = os.path.getmtime(self._path) if os.path.exists(self._path) else 0.0
                    if mt > self._mtime:
                        self._try_load()
            except Exception:
                pass
        if not self._loaded:
            return None
        z = self._bias
        for k, v in features.items():
            z += self._coef.get(k, 0.0) * float(v)
        p = 1.0 / (1.0 + math.exp(-z))
        return max(0.01, min(0.99, float(p)))


GLOBAL_CALIBRATOR = Calibrator()

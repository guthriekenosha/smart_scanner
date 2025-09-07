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
        except Exception:
            self._loaded = False

    def prob(self, features: Dict[str, float]) -> Optional[float]:
        if not self._loaded:
            return None
        z = self._bias
        for k, v in features.items():
            z += self._coef.get(k, 0.0) * float(v)
        p = 1.0 / (1.0 + math.exp(-z))
        return max(0.01, min(0.99, float(p)))


GLOBAL_CALIBRATOR = Calibrator()


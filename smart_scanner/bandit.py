"""
Tiny EXP3 bandit to adaptively weight strategies by recent performance.
You can plug it into your live PnL / labeler; here we persist in-memory.
"""

from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import Dict, List, Any
import json


@dataclass
class EXP3:
    tau: float = 0.07  # "temperature" / exploration
    weights: Dict[str, float] = field(default_factory=dict)

    def score(self, arms: List[str]) -> Dict[str, float]:
        # Ensure all arms exist
        for a in arms:
            self.weights.setdefault(a, 1.0)
        total = sum(self.weights.get(a, 1.0) for a in arms)
        if total <= 0:
            total = 1.0
        return {a: self.weights[a] / total for a in arms}

    def update(self, arm: str, reward: float):
        # reward in [0, 1] ideally; we’ll clip
        reward = max(0.0, min(1.0, reward))
        self.weights.setdefault(arm, 1.0)
        self.weights[arm] *= math.exp(self.tau * reward)

    # -------- Persistence --------
    def save_to(self, path: str) -> None:
        try:
            with open(path, "w") as f:
                json.dump({"tau": self.tau, "weights": self.weights}, f)
        except Exception:
            pass

    def load_from(self, path: str) -> None:
        try:
            with open(path, "r") as f:
                data = json.load(f)
            self.tau = float(data.get("tau", self.tau))
            ws = data.get("weights", {})
            if isinstance(ws, dict):
                self.weights.update({str(k): float(v) for k, v in ws.items()})
        except Exception:
            pass

    # -------- Contextual scaffold --------
    def score_context(self, arms: List[str], context: Dict[str, Any]) -> Dict[str, float]:
        # Minimal scaffold: reuse score() for now; replace with contextual model later
        return self.score(arms)


# A single global bandit for the process; swap to file-backed if you want persistence.
GLOBAL_BANDIT = EXP3()

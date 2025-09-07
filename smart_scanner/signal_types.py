from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Literal
import time
import hashlib
import json

Side = Literal["buy", "sell"]


@dataclass
class Signal:
    symbol: str
    timeframe: str
    side: Side
    score: float
    price: float
    components: List[str] = field(default_factory=list)
    type: str = "composite"
    prob: float = 0.5
    ev: float = 0.0
    confirmation: Literal["anticipation", "confirmed"] = "confirmed"
    tags: List[str] = field(default_factory=list)
    level: float | None = None
    created_ts: int = field(default_factory=lambda: int(time.time()))
    meta: Dict[str, Any] = field(default_factory=dict)

    @property
    def id(self) -> str:
        # deterministic hash for dedupe
        raw = f"{self.symbol}|{self.timeframe}|{self.side}|{int(self.price*1e8)}|{self.components}|{self.created_ts//60}"
        return hashlib.sha1(raw.encode()).hexdigest()[:16]

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

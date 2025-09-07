from __future__ import annotations
import asyncio
import json
import os
import time
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional, Tuple

from .config import CONFIG
from .metrics import emit as emit_metric
from .signal_types import Signal
from .blofin_client import BlofinClient


@dataclass
class _Track:
    symbol: str
    side: str
    entry_price: float
    created_ts: int
    timeframe: str
    components: List[str] = field(default_factory=list)
    score: float = 0.0
    prob: float = 0.0
    ev: float = 0.0
    meta: Dict[str, float | int | str] = field(default_factory=dict)
    horizons: List[int] = field(default_factory=list)  # seconds
    done: Dict[int, bool] = field(default_factory=dict)


class Labeler:
    """
    Lightweight forward-labeler for signals.
    - Registers new signals with horizons (default from env)
    - On a schedule, computes forward returns using 1m candles
    - Emits JSONL metrics rows with kind="label"
    - Persists pending state to survive restarts
    """

    def __init__(self, state_path: Optional[str] = None):
        self._pending: Dict[str, _Track] = {}
        self._state_path = state_path or os.path.join(
            os.path.dirname(CONFIG.metrics_path), "labels_state.json"
        )
        self._last_save = 0.0
        self._save_interval = 60.0
        self._horizons = self._parse_horizons(
            os.getenv("LABEL_HORIZONS_SEC", "900,3600")
        )
        self._load()

    @staticmethod
    def _parse_horizons(raw: str) -> List[int]:
        out: List[int] = []
        for part in str(raw or "").split(","):
            part = part.strip()
            if not part:
                continue
            try:
                out.append(int(float(part)))
            except Exception:
                pass
        return [x for x in out if x > 0]

    def register(self, s: Signal) -> None:
        try:
            tid = s.id
            if tid in self._pending:
                return
            tr = _Track(
                symbol=s.symbol,
                side=s.side,
                entry_price=float(s.price),
                created_ts=int(s.created_ts),
                timeframe=s.timeframe,
                components=list(s.components),
                score=float(s.score),
                prob=float(s.prob),
                ev=float(s.ev),
                meta={
                    "regime_mult": float(s.meta.get("regime_mult", 0.0)) if isinstance(s.meta, dict) else 0.0,
                },
                horizons=list(self._horizons),
                done={h: False for h in self._horizons},
            )
            self._pending[tid] = tr
        except Exception:
            pass

    async def process(self, client: BlofinClient) -> list[dict]:
        """
        Check pending signals and emit labels whose horizon has elapsed.
        """
        out: list[dict] = []
        if not self._pending:
            return out
        now = int(time.time())
        remove_after: List[str] = []
        for tid, tr in list(self._pending.items()):
            all_done = True
            for h in tr.horizons:
                if tr.done.get(h):
                    continue
                target_ts = tr.created_ts + h
                if now < target_ts:
                    all_done = False
                    continue
                # compute label
                try:
                    end_ts, end_price = await self._price_at_or_before(client, tr.symbol, target_ts)
                    if end_price is None:
                        all_done = False
                        continue
                    ret = (end_price - tr.entry_price) / max(tr.entry_price, 1e-9)
                    if tr.side == "sell":
                        ret = -ret
                    payload = {
                        "signal_id": tid,
                        "symbol": tr.symbol,
                        "side": tr.side,
                        "timeframe": tr.timeframe,
                        "created_ts": tr.created_ts,
                        "entry_price": tr.entry_price,
                        "horizon_sec": h,
                        "end_ts": end_ts,
                        "end_price": end_price,
                        "ret": float(ret),
                        "components": tr.components,
                        "score": tr.score,
                        "prob": tr.prob,
                        "ev": tr.ev,
                        "meta": tr.meta,
                    }
                    emit_metric("label", payload)
                    out.append(payload)
                    tr.done[h] = True
                except Exception:
                    all_done = False
                    continue
            if all_done:
                remove_after.append(tid)
        for tid in remove_after:
            self._pending.pop(tid, None)
        self._maybe_save()
        return out

    async def _price_at_or_before(
        self, client: BlofinClient, symbol: str, target_ts: int
    ) -> Tuple[int | None, float | None]:
        """
        Use 1m candles to find the most recent confirmed close at or before target_ts.
        Falls back to last available close if no exact match.
        """
        try:
            rows = await client.get_candles(symbol, "1m", limit=300)
        except Exception:
            rows = []
        if not rows:
            return None, None
        # ensure ascending
        try:
            if int(rows[1][0]) < int(rows[0][0]):
                rows = list(reversed(rows))
        except Exception:
            pass
        best_ts = None
        best_close = None
        t_ms = int(target_ts) * 1000
        for r in rows:
            try:
                ts = int(r[0])
                if ts <= t_ms:
                    best_ts, best_close = ts, float(r[4])
                else:
                    break
            except Exception:
                continue
        if best_ts is None:
            # earliest bar is after target; use first bar close
            try:
                best_ts, best_close = int(rows[0][0]), float(rows[0][4])
            except Exception:
                return None, None
        return best_ts // 1000, best_close

    def _maybe_save(self):
        now = time.time()
        if (now - self._last_save) < self._save_interval:
            return
        self._last_save = now
        try:
            data = {tid: asdict(tr) for tid, tr in self._pending.items()}
            with open(self._state_path, "w") as f:
                json.dump(data, f)
        except Exception:
            pass

    def _load(self):
        try:
            if not os.path.exists(self._state_path):
                return
            with open(self._state_path, "r") as f:
                data = json.load(f)
            for tid, obj in data.items():
                self._pending[tid] = _Track(
                    symbol=obj.get("symbol", ""),
                    side=obj.get("side", "buy"),
                    entry_price=float(obj.get("entry_price", 0.0)),
                    created_ts=int(obj.get("created_ts", 0)),
                    timeframe=obj.get("timeframe", "1m"),
                    components=list(obj.get("components", [])),
                    score=float(obj.get("score", 0.0)),
                    prob=float(obj.get("prob", 0.0)),
                    ev=float(obj.get("ev", 0.0)),
                    meta=obj.get("meta", {}),
                    horizons=list(obj.get("horizons", self._horizons)),
                    done={int(k): bool(v) for k, v in (obj.get("done", {}) or {}).items()},
                )
        except Exception:
            pass


GLOBAL_LABELER = Labeler()

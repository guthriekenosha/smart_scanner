from __future__ import annotations

"""
Daily realized PnL report from JSONL metrics.

Usage:
  python -m smart_scanner.pnl_report \
      --metrics artifacts/scanner_metrics.jsonl \
      --days 30 --tz 0

Notes:
  - Aggregates only events with kind="trade_close" and a numeric "pnl" field.
  - For most venues the "pnl" is reported in quote currency (e.g., USDT).
  - Timestamps: prefers event["updateTime"] (ms or sec) when present, else event["ts"].
  - If no trade_close with pnl exist, prints a friendly message and exits 0.
"""

import argparse
import json
import math
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .config import CONFIG


def _read_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    try:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    yield obj
    except FileNotFoundError:
        return


def _to_seconds(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        try:
            v = float(str(x).strip())
        except Exception:
            return None
    # Heuristic: treat large values as milliseconds
    if v > 1e12:
        return v / 1000.0
    # Some APIs send 13-digit ms; some send seconds
    if v > 1e10:
        return v / 1000.0
    return v


def _event_time(ev: Dict[str, Any]) -> Optional[float]:
    # Prefer venue updateTime when available; else use metrics ts
    t = ev.get("updateTime")
    if t is None:
        t = ev.get("ts")
    return _to_seconds(t)


def _pnl_value(ev: Dict[str, Any]) -> Optional[float]:
    v = ev.get("pnl")
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        try:
            return float(str(v).replace(",", ""))
        except Exception:
            return None


def _iso_day(ts_sec: float, tz_hours: float) -> str:
    tz = timezone(timedelta(hours=float(tz_hours)))
    return datetime.fromtimestamp(ts_sec, tz=tz).date().isoformat()


def _fmt_usd(x: float) -> str:
    # Compact formatting with sign
    s = "-" if x < 0 else ""
    ax = abs(x)
    if ax >= 1000:
        return f"{s}{ax:,.2f}"
    if ax >= 1:
        return f"{s}{ax:.2f}"
    if ax >= 0.01:
        return f"{s}{ax:.4f}"
    return f"{s}{ax:.6f}"


def report(metrics_path: str, days: int = 0, tz_hours: float = 0.0, csv: bool = False) -> int:
    rows: List[Tuple[str, float]] = []  # (iso_day, pnl)

    # Collect closes with realized pnl
    for ev in _read_jsonl(metrics_path):
        if (ev.get("kind") or ev.get("event")) != "trade_close":
            continue
        pnl = _pnl_value(ev)
        if pnl is None:
            continue
        t = _event_time(ev)
        if t is None:
            continue
        d = _iso_day(t, tz_hours)
        rows.append((d, pnl))

    if not rows:
        print("[pnl] No realized PnL found (no trade_close events with pnl).")
        return 0

    # Aggregate by day
    agg = defaultdict(list)
    for d, pnl in rows:
        agg[d].append(float(pnl))

    days_sorted = sorted(agg.keys())
    if days and days > 0:
        days_sorted = days_sorted[-int(days) :]

    # Print
    total = 0.0
    n_trades = 0
    if csv:
        print("date,trades,realized_pnl,total_to_date")
    for d in days_sorted:
        vals = agg[d]
        s = float(sum(vals))
        c = len(vals)
        total += s
        n_trades += c
        if csv:
            print(f"{d},{c},{s:.6f},{total:.6f}")
        else:
            print(f"{d}  trades={c:3d}  realized={_fmt_usd(s):>10}  total_to_date={_fmt_usd(total):>10}")

    if not csv:
        print("-" * 60)
        avg = total / max(n_trades, 1)
        print(f"Total trades: {n_trades}")
        print(f"Total realized: {_fmt_usd(total)}")
        print(f"Avg per trade: {_fmt_usd(avg)}")
    return 0


def main():
    p = argparse.ArgumentParser(description="Daily realized PnL report from metrics JSONL")
    p.add_argument("--metrics", type=str, default=CONFIG.metrics_path, help="Path to scanner_metrics.jsonl")
    p.add_argument("--days", type=int, default=0, help="Limit to last N days (0 = all)")
    p.add_argument("--tz", type=float, default=0.0, help="Timezone offset in hours from UTC (e.g., -5, 8)")
    p.add_argument("--csv", action="store_true", help="Output CSV instead of pretty text")
    args = p.parse_args()
    raise SystemExit(report(args.metrics, args.days, args.tz, args.csv))


if __name__ == "__main__":
    main()


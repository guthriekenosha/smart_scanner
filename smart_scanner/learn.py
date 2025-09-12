from __future__ import annotations

"""
Offline learning utilities.

What this provides right now:
- Calibration trainer: learns a logistic mapping from signal features (+score)
  to success probability using historical labels in the JSONL metrics file.

Outputs a calibration.json compatible with calibration.Calibrator, which the
signal engine will use automatically if present.

Usage:
  python -m smart_scanner.learn calibrate \
      --metrics scanner_metrics.jsonl \
      --out calibration.json \
      --horizon 900

Notes:
- The trainer matches each label to its originating signal by
  (symbol, timeframe, side, created_ts). Make sure your metrics contain
  both "signal" and "label" events.
- Only features from meta["features"] and the scalar "score" are used,
  which matches what the online Calibrator receives at inference.
"""

import argparse
import json
import math
import os
from dataclasses import dataclass
from typing import Dict, Any, Iterable, List, Tuple, Optional

import numpy as np

from .config import CONFIG


# ----------------------------- IO helpers ------------------------------------


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


def _sig_key(symbol: str, timeframe: str, side: str, created_ts: int) -> str:
    return f"{symbol}|{timeframe}|{side}|{int(created_ts)}"


@dataclass
class Sample:
    x: Dict[str, float]
    y: int  # 0/1 label (ret>0)
    ret: float  # actual forward return
    score: float
    tags: List[str]
    n_comp: int
    w: float = 1.0  # optional weight


def build_calibration_dataset(
    metrics_path: str,
    horizon_sec: Optional[int] = None,
    min_abs_ret: float = 0.0,
    max_samples: Optional[int] = None,
) -> Tuple[List[Sample], List[str]]:
    """
    Build a dataset by joining signal features to their labels.
    Returns (samples, feature_names).
    """
    # Collect latest signal -> features map keyed by (sym, tf, side, ts)
    sig_map: Dict[str, Dict[str, Any]] = {}
    # We'll also keep a small cache of seen keys to avoid huge memory when log is large
    # While ensuring we can still match labels later in the file.

    # First pass: we will buffer signal features in-memory; that is typically manageable
    # for a few hundred thousand rows of JSONL. If your file is much larger, consider
    # tailing or filtering the metrics file before running training.

    for rec in _read_jsonl(metrics_path):
        kind = rec.get("kind") or rec.get("event")
        if kind == "signal":
            try:
                sym = str(rec.get("symbol"))
                tf = str(rec.get("timeframe"))
                side = str(rec.get("side"))
                ts = int(rec.get("created_ts") or 0)
                if not (sym and tf and side and ts):
                    continue
                feats: Dict[str, float] = {}
                meta = rec.get("meta") or {}
                if isinstance(meta, dict):
                    F = meta.get("features") or {}
                    if isinstance(F, dict):
                        for k, v in F.items():
                            try:
                                feats[str(k)] = float(v)
                            except Exception:
                                continue
                # Always include score as a feature
                try:
                    sc = float(rec.get("score", 0.0))
                    feats["score"] = sc
                except Exception:
                    continue
                tags = rec.get("tags") or []
                if not isinstance(tags, list):
                    tags = []
                comps = rec.get("components") or []
                if not isinstance(comps, list):
                    comps = []
                key = _sig_key(sym, tf, side, ts)
                sig_map[key] = {
                    "feats": feats,
                    "score": sc,
                    "tags": [str(t) for t in tags if isinstance(t, (str, int, float))],
                    "n_comp": int(len(comps)),
                }
            except Exception:
                continue

    # Second pass: collect labels and join
    samples: List[Sample] = []
    feat_names_set: set[str] = set()
    for rec in _read_jsonl(metrics_path):
        kind = rec.get("kind") or rec.get("event")
        if kind != "label":
            continue
        try:
            sym = str(rec.get("symbol"))
            tf = str(rec.get("timeframe"))
            side = str(rec.get("side"))
            ts = int(rec.get("created_ts") or 0)
            hz = int(rec.get("horizon_sec") or 0)
            if horizon_sec and hz != int(horizon_sec):
                continue
            ret = float(rec.get("ret", 0.0))
            if abs(ret) < float(min_abs_ret):
                # skip near-zero outcomes if requested
                continue
            key = _sig_key(sym, tf, side, ts)
            it = sig_map.get(key)
            if not it:
                continue
            feats = it.get("feats", {})
            if not isinstance(feats, dict):
                continue
            y = 1 if ret > 0 else 0
            # optional sample weights: scale by |ret| but cap to avoid outliers
            w = min(2.0, 1.0 + abs(ret) * 4.0)
            score = float(it.get("score", float(feats.get("score", 0.0))))
            tags = it.get("tags") or []
            if not isinstance(tags, list):
                tags = []
            n_comp = int(it.get("n_comp", 0))
            samples.append(Sample(x=feats, y=y, ret=float(ret), score=score, tags=[str(t) for t in tags], n_comp=n_comp, w=w))
            feat_names_set.update(feats.keys())
            if max_samples and len(samples) >= int(max_samples):
                break
        except Exception:
            continue

    feat_names = sorted(list(feat_names_set))
    return samples, feat_names


# -------------------------- Logistic regression ------------------------------


def _sigmoid(z: np.ndarray) -> np.ndarray:
    # numerically stable sigmoid
    return 1.0 / (1.0 + np.exp(-np.clip(z, -40.0, 40.0)))


@dataclass
class LogRegModel:
    bias: float
    coef: Dict[str, float]


def train_logreg(
    samples: List[Sample],
    feat_names: List[str],
    l2: float = 1e-3,
    lr: float = 0.05,
    epochs: int = 200,
    batch_size: int = 256,
    pos_weight: float = 1.0,
) -> LogRegModel:
    if not samples or not feat_names:
        return LogRegModel(0.0, {})

    # Build dense matrices
    n = len(samples)
    d = len(feat_names)
    X = np.zeros((n, d), dtype=np.float64)
    y = np.zeros((n,), dtype=np.float64)
    w = np.ones((n,), dtype=np.float64)
    idx = {f: i for i, f in enumerate(feat_names)}
    for i, s in enumerate(samples):
        for k, v in s.x.items():
            j = idx.get(k)
            if j is None:
                continue
            try:
                X[i, j] = float(v)
            except Exception:
                X[i, j] = 0.0
        y[i] = 1.0 if s.y else 0.0
        w[i] = float(s.w) if s.w is not None else 1.0

    # Optionally balance classes via pos_weight
    if pos_weight != 1.0:
        pos_idx = (y >= 0.5)
        w[pos_idx] *= float(pos_weight)

    # Initialize
    rng = np.random.default_rng(42)
    W = rng.normal(loc=0.0, scale=0.01, size=(d,))
    b = 0.0

    # Mini-batch gradient descent
    order = np.arange(n)
    for ep in range(epochs):
        rng.shuffle(order)
        total_loss = 0.0
        for start in range(0, n, batch_size):
            sel = order[start : start + batch_size]
            Xb = X[sel]
            yb = y[sel]
            wb = w[sel]
            z = Xb @ W + b
            p = _sigmoid(z)
            # Weighted logistic loss + L2
            eps = 1e-9
            loss = -np.sum(wb * (yb * np.log(p + eps) + (1 - yb) * np.log(1 - p + eps)))
            loss += 0.5 * l2 * np.sum(W * W)
            total_loss += float(loss)
            # Gradients
            diff = (p - yb) * wb
            gW = Xb.T @ diff + l2 * W
            gb = np.sum(diff)
            # Step
            W -= lr * gW / max(len(sel), 1)
            b -= lr * gb / max(len(sel), 1)
        # Simple anneal
        if (ep + 1) % 60 == 0:
            lr *= 0.7
        # Optional early stop could be added; we keep things simple.

    coef = {feat_names[i]: float(W[i]) for i in range(d)}
    return LogRegModel(bias=float(b), coef=coef)


def evaluate_model(model: LogRegModel, samples: List[Sample], feat_names: List[str]) -> Dict[str, float]:
    if not samples or not feat_names:
        return {"n": 0}
    n = len(samples)
    d = len(feat_names)
    idx = {f: i for i, f in enumerate(feat_names)}
    X = np.zeros((n, d), dtype=np.float64)
    y = np.zeros((n,), dtype=np.float64)
    for i, s in enumerate(samples):
        for k, v in s.x.items():
            j = idx.get(k)
            if j is None:
                continue
            X[i, j] = float(v)
        y[i] = 1.0 if s.y else 0.0
    W = np.array([model.coef.get(f, 0.0) for f in feat_names], dtype=np.float64)
    b = float(model.bias)
    p = _sigmoid(X @ W + b)
    # Metrics
    pred = (p >= 0.5).astype(np.float64)
    acc = float(np.mean(pred == y))
    # AUC (naive implementation)
    try:
        order = np.argsort(p)
        cum_pos = 0.0
        auc_area = 0.0
        pos = float(np.sum(y))
        neg = float(n - pos)
        for i in order:
            if y[i] > 0.5:
                cum_pos += 1.0
            else:
                auc_area += cum_pos
        auc = auc_area / max(pos * neg, 1.0)
    except Exception:
        auc = 0.0
    return {"n": n, "acc@0.5": acc, "auc": float(auc)}


def save_calibration(path: str, model: LogRegModel) -> None:
    obj = {"bias": float(model.bias), "coef": {k: float(v) for k, v in model.coef.items()}}
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


# -------------------------- Thresholds & Reports -----------------------------


_TAG_BONUS_SET = set(
    [
        # bullish tags
        "breakout",
        "momentum",
        "trend_up",
        "rsi_confirm",
        "st_flip",
        # bearish symmetric
        "breakdown",
        "momentum_down",
        "trend_down",
        "st_flip_down",
    ]
)


def baseline_prob(score: float, tags: List[str], n_comp: int, feats: Optional[Dict[str, float]] = None) -> float:
    base = 1.0 / (1.0 + math.exp(-(float(score) - 3.5) * 0.7))
    comp_bonus = min(0.06 * max(0, int(n_comp) - 1), 0.12)
    tag_bonus = 0.03 * sum(1 for t in (tags or []) if str(t) in _TAG_BONUS_SET)
    p = base + comp_bonus + tag_bonus
    return max(0.05, min(0.90 if int(n_comp) < 3 else 0.93, float(p)))


def predict_model_probs(model: LogRegModel, samples: List[Sample], feat_names: List[str]) -> np.ndarray:
    if not samples or not feat_names:
        return np.zeros((0,), dtype=np.float64)
    n = len(samples)
    d = len(feat_names)
    idx = {f: i for i, f in enumerate(feat_names)}
    X = np.zeros((n, d), dtype=np.float64)
    for i, s in enumerate(samples):
        for k, v in s.x.items():
            j = idx.get(k)
            if j is None:
                continue
            X[i, j] = float(v)
    W = np.array([model.coef.get(f, 0.0) for f in feat_names], dtype=np.float64)
    b = float(model.bias)
    return _sigmoid(X @ W + b)


def metrics_from_probs(y: np.ndarray, p: np.ndarray) -> Dict[str, float]:
    n = len(y)
    eps = 1e-12
    # Accuracy at 0.5
    acc = float(np.mean((p >= 0.5).astype(np.float64) == y)) if n > 0 else 0.0
    # Brier score
    brier = float(np.mean((p - y) ** 2)) if n > 0 else 0.0
    # Log loss
    logloss = float(-np.mean(y * np.log(p + eps) + (1 - y) * np.log(1 - p + eps))) if n > 0 else 0.0
    # AUC (same naive impl)
    try:
        order = np.argsort(p)
        cum_pos = 0.0
        auc_area = 0.0
        pos = float(np.sum(y))
        neg = float(n - pos)
        for i in order:
            if y[i] > 0.5:
                cum_pos += 1.0
            else:
                auc_area += cum_pos
        auc = auc_area / max(pos * neg, 1.0)
    except Exception:
        auc = 0.0
    return {"n": float(n), "acc@0.5": acc, "brier": brier, "logloss": logloss, "auc": float(auc)}


def decile_lift(y: np.ndarray, p_base: np.ndarray, p_cal: np.ndarray, rets: np.ndarray) -> Dict[str, float]:
    n = len(y)
    if n == 0:
        return {}
    k = max(1, int(0.1 * n))
    # Top decile by baseline
    idx_base = np.argsort(-p_base)[:k]
    idx_cal = np.argsort(-p_cal)[:k]
    win_base = float(np.mean(y[idx_base])) if k > 0 else 0.0
    win_cal = float(np.mean(y[idx_cal])) if k > 0 else 0.0
    ret_base = float(np.mean(rets[idx_base])) if k > 0 else 0.0
    ret_cal = float(np.mean(rets[idx_cal])) if k > 0 else 0.0
    return {
        "decile": float(k) / float(n),
        "winrate_base_top10": win_base,
        "winrate_cal_top10": win_cal,
        "winrate_lift_pts": (win_cal - win_base),
        "avgret_base_top10": ret_base,
        "avgret_cal_top10": ret_cal,
        "avgret_lift": (ret_cal - ret_base),
    }


def tune_thresholds(
    samples: List[Sample],
    probs: np.ndarray,
    p_grid: Tuple[float, float, int] = (0.5, 0.85, 36),
    score_grid: Optional[Tuple[float, float, int]] = None,
    objective: str = "ev",
    min_trades: int = 50,
) -> Dict[str, Any]:
    # Build vectors
    y = np.array([1.0 if s.y else 0.0 for s in samples], dtype=np.float64)
    rets = np.array([float(s.ret) for s in samples], dtype=np.float64)
    scores = np.array([float(s.score) for s in samples], dtype=np.float64)

    # Default score grid from data range
    if score_grid is None:
        lo = float(np.percentile(scores, 5)) if len(scores) else 0.0
        hi = float(np.percentile(scores, 95)) if len(scores) else 1.0
        steps = 24
        score_grid = (lo, hi, steps)

    p_lo, p_hi, p_steps = p_grid
    s_lo, s_hi, s_steps = score_grid
    p_vals = np.linspace(p_lo, p_hi, int(p_steps))
    s_vals = np.linspace(s_lo, s_hi, int(s_steps))

    best = None
    def _score(sel: np.ndarray) -> Tuple[float, Dict[str, float]]:
        if sel.sum() <= 0:
            return -1e18, {"n": 0}
        rr = rets[sel]
        yy = y[sel]
        n = int(sel.sum())
        win = float(np.mean(yy)) if n > 0 else 0.0
        avg_ret = float(np.mean(rr)) if n > 0 else 0.0
        # Objectives
        if objective == "winrate":
            obj = win
        elif objective == "sharpe":
            mu = float(np.mean(rr))
            sd = float(np.std(rr))
            obj = mu / (sd + 1e-9)
        else:  # "ev" default: average realized return
            obj = avg_ret
        return obj, {"n": n, "winrate": win, "avg_ret": avg_ret}

    for pt in p_vals:
        for st in s_vals:
            sel = (probs >= pt) & (scores >= st)
            if sel.sum() < int(min_trades):
                continue
            obj, stats = _score(sel)
            if (best is None) or (obj > best[0]):
                best = (obj, float(pt), float(st), stats)
    if best is None:
        return {"ok": 0, "reason": "no combination met min_trades"}
    _, p_best, s_best, stats = best
    return {"ok": 1, "prob_min": p_best, "score_min": s_best, **stats}


# ------------------------------- CLI -----------------------------------------


def _cmd_calibrate(args: argparse.Namespace) -> None:
    mpath = args.metrics or CONFIG.metrics_path
    horizon = args.horizon
    min_abs_ret = args.min_abs_ret
    max_samples = args.max_samples
    print(f"[learn] Building dataset from {mpath} (horizon={horizon}, min_abs_ret={min_abs_ret})...")
    samples, feat_names = build_calibration_dataset(
        mpath, horizon_sec=horizon, min_abs_ret=min_abs_ret, max_samples=max_samples
    )
    if not samples:
        print("[learn] No samples found. Ensure your metrics contain 'signal' and 'label' rows.")
        return
    print(f"[learn] Samples={len(samples)} features={len(feat_names)} -> {feat_names}")
    model = train_logreg(
        samples,
        feat_names,
        l2=float(args.l2),
        lr=float(args.lr),
        epochs=int(args.epochs),
        batch_size=int(args.batch),
        pos_weight=float(args.pos_weight),
    )
    stats = evaluate_model(model, samples, feat_names)
    print(f"[learn] Train metrics: n={stats.get('n')} acc@0.5={stats.get('acc@0.5'):.3f} auc={stats.get('auc'):.3f}")
    out = args.out or CONFIG.calibration_path
    save_calibration(out, model)
    print(f"[learn] Wrote calibration to {out}")


def _load_model(path: str) -> Optional[LogRegModel]:
    try:
        with open(path, "r") as f:
            obj = json.load(f)
        bias = float(obj.get("bias", 0.0))
        coef = {str(k): float(v) for k, v in (obj.get("coef", {}) or {}).items()}
        return LogRegModel(bias=bias, coef=coef)
    except Exception:
        return None


def _cmd_report(args: argparse.Namespace) -> None:
    mpath = args.metrics or CONFIG.metrics_path
    horizon = args.horizon
    min_abs_ret = args.min_abs_ret
    max_samples = args.max_samples
    samples, feat_names = build_calibration_dataset(
        mpath, horizon_sec=horizon, min_abs_ret=min_abs_ret, max_samples=max_samples
    )
    if not samples:
        print("[report] No samples found.")
        return

    # Base and calibrated probabilities
    p_base = np.array([baseline_prob(s.score, s.tags, s.n_comp, s.x) for s in samples], dtype=np.float64)
    model = _load_model(args.calibration or CONFIG.calibration_path)
    if model is not None and feat_names:
        p_cal = predict_model_probs(model, samples, feat_names)
    else:
        p_cal = p_base.copy()
    y = np.array([1.0 if s.y else 0.0 for s in samples], dtype=np.float64)
    rets = np.array([float(s.ret) for s in samples], dtype=np.float64)

    M_base = metrics_from_probs(y, p_base)
    M_cal = metrics_from_probs(y, p_cal)
    L = decile_lift(y, p_base, p_cal, rets)
    print("[report] Dataset:", int(M_base.get("n", 0)))
    print(
        f"[report] AUC: base={M_base['auc']:.3f} cal={M_cal['auc']:.3f} | lift=+{(M_cal['auc']-M_base['auc']):.3f}"
    )
    print(
        f"[report] Brier: base={M_base['brier']:.4f} cal={M_cal['brier']:.4f} | delta={(M_cal['brier']-M_base['brier']):.4f}"
    )
    print(
        f"[report] LogLoss: base={M_base['logloss']:.4f} cal={M_cal['logloss']:.4f} | delta={(M_cal['logloss']-M_base['logloss']):.4f}"
    )
    print(
        f"[report] Top10% lift: winrate +{L.get('winrate_lift_pts',0.0):.3f} pts | avgret +{L.get('avgret_lift',0.0):.5f}"
    )


def _cmd_tune(args: argparse.Namespace) -> None:
    mpath = args.metrics or CONFIG.metrics_path
    horizon = args.horizon
    min_abs_ret = args.min_abs_ret
    max_samples = args.max_samples
    samples, feat_names = build_calibration_dataset(
        mpath, horizon_sec=horizon, min_abs_ret=min_abs_ret, max_samples=max_samples
    )
    if not samples:
        print("[tune] No samples found.")
        return
    use_cal = bool(args.use_calibrated)
    if use_cal:
        model = _load_model(args.calibration or CONFIG.calibration_path)
        if model is None:
            print("[tune] No calibration found; falling back to baseline probabilities.")
            use_cal = False
    if use_cal and feat_names:
        probs = predict_model_probs(model, samples, feat_names)
    else:
        probs = np.array([baseline_prob(s.score, s.tags, s.n_comp, s.x) for s in samples], dtype=np.float64)

    # Grids
    p_min = float(args.p_min)
    p_max = float(args.p_max)
    p_steps = int(args.p_steps)
    s_min = float(args.s_min) if args.s_min is not None else None
    s_max = float(args.s_max) if args.s_max is not None else None
    s_steps = int(args.s_steps) if args.s_steps else 24
    score_grid = None
    if s_min is not None and s_max is not None:
        score_grid = (s_min, s_max, s_steps)
    res = tune_thresholds(
        samples,
        probs,
        p_grid=(p_min, p_max, p_steps),
        score_grid=score_grid,
        objective=str(args.objective),
        min_trades=int(args.min_trades),
    )
    if not res.get("ok"):
        print("[tune] Could not find thresholds meeting constraints:", res.get("reason"))
        return
    print(
        f"[tune] Recommended: TRADE_MIN_PROB={res['prob_min']:.3f} TRADE_MIN_SCORE={res['score_min']:.2f}"
    )
    print(
        f"[tune] Stats at threshold: n={int(res['n'])} winrate={res['winrate']:.3f} avg_ret={res['avg_ret']:.5f}"
    )


def main():
    p = argparse.ArgumentParser(description="Learning utilities for Smart Scanner")
    sub = p.add_subparsers(dest="cmd")

    pc = sub.add_parser("calibrate", help="Train probability calibration from metrics")
    pc.add_argument("--metrics", type=str, default=CONFIG.metrics_path, help="Path to JSONL metrics (signals + labels)")
    pc.add_argument("--out", type=str, default=CONFIG.calibration_path, help="Output calibration JSON path")
    pc.add_argument("--horizon", type=int, default=900, help="Horizon seconds to use (e.g., 900)")
    pc.add_argument("--min-abs-ret", type=float, default=0.0, help="Ignore labels with |ret| below this threshold")
    pc.add_argument("--max-samples", type=int, default=0, help="Optional cap on samples (0=all)")
    pc.add_argument("--l2", type=float, default=1e-3, help="L2 regularization strength")
    pc.add_argument("--lr", type=float, default=0.05, help="Learning rate")
    pc.add_argument("--epochs", type=int, default=200, help="Training epochs")
    pc.add_argument("--batch", type=int, default=256, help="Mini-batch size")
    pc.add_argument("--pos-weight", type=float, default=1.0, help="Weight multiplier for positive class")
    pc.set_defaults(func=_cmd_calibrate)

    pr = sub.add_parser("report", help="Quantify lift/improvement from calibration vs baseline")
    pr.add_argument("--metrics", type=str, default=CONFIG.metrics_path)
    pr.add_argument("--calibration", type=str, default=CONFIG.calibration_path)
    pr.add_argument("--horizon", type=int, default=900)
    pr.add_argument("--min-abs-ret", type=float, default=0.0)
    pr.add_argument("--max-samples", type=int, default=0)
    pr.set_defaults(func=_cmd_report)

    tn = sub.add_parser("tune", help="Tune trade thresholds (prob/score) to maximize objective")
    tn.add_argument("--metrics", type=str, default=CONFIG.metrics_path)
    tn.add_argument("--calibration", type=str, default=CONFIG.calibration_path)
    tn.add_argument("--horizon", type=int, default=900)
    tn.add_argument("--min-abs-ret", type=float, default=0.0)
    tn.add_argument("--max-samples", type=int, default=0)
    tn.add_argument("--use-calibrated", type=int, default=1, help="1=use calibrated probs if available, else baseline")
    tn.add_argument("--objective", type=str, default="ev", choices=["ev", "winrate", "sharpe"])
    tn.add_argument("--min-trades", type=int, default=50)
    tn.add_argument("--p-min", type=float, default=0.50)
    tn.add_argument("--p-max", type=float, default=0.85)
    tn.add_argument("--p-steps", type=int, default=36)
    tn.add_argument("--s-min", type=float, help="Optional score min for grid (else data-driven)")
    tn.add_argument("--s-max", type=float, help="Optional score max for grid (else data-driven)")
    tn.add_argument("--s-steps", type=int, default=24)
    tn.set_defaults(func=_cmd_tune)

    args = p.parse_args()
    if not args.cmd:
        p.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()

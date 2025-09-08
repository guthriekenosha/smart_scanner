from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import orjson
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates


METRICS_PATH = os.getenv("METRICS_PATH", "/data/scanner_metrics.jsonl")


app = FastAPI(title="Smart Scanner Dashboard")
base_dir = Path(__file__).parent
templates = Jinja2Templates(directory=str(base_dir / "templates"))
app.mount("/static", StaticFiles(directory=str(base_dir / "static")), name="static")


def _ago(ts: Optional[float]) -> str:
    try:
        if ts is None:
            return "—"
        now = time.time()
        d = max(0, now - float(ts))
        if d < 60:
            return f"{int(d)}s ago"
        m = int(d // 60)
        if m < 60:
            return f"{m}m ago"
        h = int(m // 60)
        return f"{h}h ago"
    except Exception:
        return "—"


templates.env.filters["ago"] = _ago  # type: ignore[attr-defined]


def _read_events(max_lines: int = 20000) -> List[Dict[str, Any]]:
    try:
        p = Path(METRICS_PATH)
        if not p.exists():
            return []
        data = p.read_bytes()
        if len(data) > 2 * 1024 * 1024:
            data = data[-2 * 1024 * 1024 :]
        lines = data.decode(errors="ignore").splitlines()[-max_lines:]
        out: List[Dict[str, Any]] = []
        for ln in lines:
            ln = ln.strip()
            if not ln:
                continue
            try:
                ev = orjson.loads(ln)
                if isinstance(ev, dict):
                    out.append(ev)
            except Exception:
                continue
        return out
    except Exception:
        return []


def _partition(evts: List[Dict[str, Any]]):
    signals: List[Dict[str, Any]] = []
    orders: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    positions_map: Dict[str, Dict[str, Any]] = {}
    for ev in evts:
        k = str(ev.get("kind") or "")
        if k == "signal":
            signals.append(ev)
        elif k.startswith("order") or k.startswith("risk_") or k == "trade_close" or k == "exposure":
            orders.append(ev)
        elif k.endswith("error"):
            errors.append(ev)
        elif k == "position":
            try:
                inst = ev.get("instId") or ev.get("symbol")
                side = (ev.get("side") or "net").lower()
                size = float(ev.get("size") or 0.0)
                key = inst if side in ("", None, "net") else f"{inst}|{side}"
                if size <= 0:
                    positions_map.pop(key, None)
                else:
                    positions_map[key] = {
                        "instId": inst,
                        "side": side or "net",
                        "size": size,
                        "entry": float(ev.get("entry") or 0.0),
                        "mark": float(ev.get("mark") or 0.0),
                        "sl": ev.get("sl"),
                        "ts": float(ev.get("ts") or time.time()),
                    }
            except Exception:
                pass
    return signals, orders, errors, positions_map


def _positions_list(positions_map: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for st in positions_map.values():
        try:
            entry = float(st.get("entry") or 0.0)
            mark = float(st.get("mark") or 0.0)
            size = float(st.get("size") or 0.0)
            side = (st.get("side") or "net").lower()
            pnl_u = 0.0
            if entry > 0 and mark > 0 and size > 0:
                pnl_u = (mark - entry) * size if side != "short" else (entry - mark) * size
        except Exception:
            pnl_u = 0.0
        out.append({
            "symbol": st.get("instId"),
            "side": st.get("side"),
            "size": st.get("size"),
            "entry": st.get("entry"),
            "mark": st.get("mark"),
            "sl": st.get("sl"),
            "pnl_u": pnl_u,
            "ts": st.get("ts"),
        })
    out.sort(key=lambda x: (x.get("symbol") or "", x.get("side") or ""))
    return out


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    evts = _read_events()
    signals, orders, errors, positions_map = _partition(evts)
    ctx = {
        "request": request,
        "signals": list(reversed(signals[-30:])),
        "orders": list(reversed(orders[-50:])),
        "errors": list(reversed(errors[-50:])),
    }
    return templates.TemplateResponse("index.html", ctx)


@app.get("/partials/positions", response_class=HTMLResponse)
def partial_positions(request: Request):
    evts = _read_events()
    _, _, _, positions_map = _partition(evts)
    rows = _positions_list(positions_map)
    return templates.TemplateResponse("_positions.html", {"request": request, "positions": rows})


@app.get("/partials/signals", response_class=HTMLResponse)
def partial_signals(request: Request):
    evts = _read_events()
    signals, _, _, _ = _partition(evts)
    rows = list(reversed(signals[-50:]))
    return templates.TemplateResponse("_signals_tbody.html", {"request": request, "signals": rows})


@app.get("/partials/orders", response_class=HTMLResponse)
def partial_orders(request: Request):
    evts = _read_events()
    _, orders, _, _ = _partition(evts)
    rows = list(reversed(orders[-50:]))
    return templates.TemplateResponse("_orders_tbody.html", {"request": request, "orders": rows})


@app.get("/partials/errors", response_class=HTMLResponse)
def partial_errors(request: Request):
    evts = _read_events()
    _, _, errors, _ = _partition(evts)
    rows = list(reversed(errors[-100:]))
    return templates.TemplateResponse("_errors_tbody.html", {"request": request, "errors": rows})


@app.get("/api/summary")
def api_summary():
    evts = _read_events()
    signals, orders, errors, positions_map = _partition(evts)
    def _ts(xs: List[Dict[str, Any]]):
        try:
            return float(xs[-1].get("ts")) if xs else 0.0
        except Exception:
            return 0.0
    last_pos_ts = 0.0
    try:
        for st in positions_map.values():
            t = float(st.get("ts") or 0.0)
            if t > last_pos_ts:
                last_pos_ts = t
    except Exception:
        pass
    out = {
        "signals": len(signals),
        "orders": len(orders),
        "errors": len(errors),
        "last_signal_ts": _ts(signals),
        "last_order_ts": _ts(orders),
        "last_error_ts": _ts(errors),
        "last_position_ts": last_pos_ts,
        "server_now": time.time(),
    }
    return JSONResponse(out)


@app.get("/partials/summary", response_class=HTMLResponse)
def partial_summary(request: Request):
    # Keep it simple: reuse api_summary
    from starlette.requests import Request as SR
    summary = api_summary().body
    try:
        data = orjson.loads(summary)
    except Exception:
        data = {}
    return templates.TemplateResponse("_summary.html", {"request": request, "summary": data})


@app.get("/partials/details", response_class=HTMLResponse)
def partial_details(request: Request, bucket: str, ts: float):
    evts = _read_events()
    best = None
    best_dt = 1e9
    for ev in evts:
        try:
            t = float(ev.get("ts"))
            dt = abs(t - ts)
            if dt < best_dt:
                best = ev
                best_dt = dt
        except Exception:
            continue
    title = f"{bucket.title()} details"
    if not best:
        body = "No data"
    else:
        try:
            body = orjson.dumps(best, option=orjson.OPT_INDENT_2).decode()
        except Exception:
            import json as _json
            body = _json.dumps(best, indent=2, ensure_ascii=False)
    return templates.TemplateResponse("_details.html", {"request": request, "title": title, "body": body})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8081")), reload=False)


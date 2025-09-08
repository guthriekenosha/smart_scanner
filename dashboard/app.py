from __future__ import annotations

import asyncio
import io
import os
import time
from collections import deque
from pathlib import Path
from typing import Any, AsyncGenerator, Deque, Dict, List, Optional, Set, Tuple

import orjson
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles


METRICS_PATH = os.getenv("METRICS_PATH", "/data/scanner_metrics.jsonl")
MAX_BUF = int(os.getenv("DASHBOARD_MAX_BUF", "500"))


app = FastAPI(title="Smart Scanner Dashboard")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")


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


# expose as a Jinja filter
templates.env.filters["ago"] = _ago  # type: ignore[attr-defined]


# Ring buffers (in-memory recent history)
signals: Deque[Dict[str, Any]] = deque(maxlen=MAX_BUF)
orders: Deque[Dict[str, Any]] = deque(maxlen=MAX_BUF)
errors: Deque[Dict[str, Any]] = deque(maxlen=MAX_BUF)
# Live positions keyed by symbol (net) or symbol|side in hedge mode
positions_map: Dict[str, Dict[str, Any]] = {}


class Hub:
    def __init__(self) -> None:
        self.clients: Set[asyncio.Queue[str]] = set()
        self._lock = asyncio.Lock()

    async def subscribe(self) -> asyncio.Queue[str]:
        q: asyncio.Queue[str] = asyncio.Queue(maxsize=1024)
        async with self._lock:
            self.clients.add(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue[str]) -> None:
        async with self._lock:
            self.clients.discard(q)

    async def broadcast(self, event: str, html: str) -> None:
        # SSE format: event: <name>\n data: <payload>\n\n
        msg = f"event: {event}\ndata: {html}\n\n"
        async with self._lock:
            dead: List[asyncio.Queue[str]] = []
            for q in list(self.clients):
                try:
                    q.put_nowait(msg)
                except asyncio.QueueFull:
                    dead.append(q)
            for q in dead:
                self.clients.discard(q)


hub = Hub()


def _parse_line(raw: str) -> Optional[Dict[str, Any]]:
    raw = raw.strip()
    if not raw:
        return None
    try:
        return orjson.loads(raw)
    except Exception:
        return None


def _last_lines(path: str, max_lines: int = 10000) -> List[str]:
    try:
        with open(path, "rb") as f:
            data = f.read()
        # Limit to last ~2MB to avoid huge reads
        if len(data) > 2 * 1024 * 1024:
            data = data[-2 * 1024 * 1024 :]
        text = data.decode(errors="ignore")
        lines = text.splitlines()
        return lines[-max_lines:]
    except Exception:
        return []


def _classify(ev: Dict[str, Any]) -> str:
    k = str(ev.get("kind") or "")
    return k


def _normalize_order_view(ev: Dict[str, Any]) -> Dict[str, Any]:
    kind = str(ev.get("kind") or "")
    symbol = ev.get("symbol") or ev.get("instId") or "?"
    side = ev.get("side") or ""
    ts = ev.get("ts")
    label = "Order"
    info = ""
    info_full = ""
    status = "ok"

    try:
        if kind == "order_api_tp":
            which = ev.get("which")
            label = f"TP{which}" if which else "TP"
            obj = ev.get("tp") or {}
            code = str(obj.get("code", "0"))
            tid = obj.get("tpslId") or obj.get("algoId")
            status = "ok" if (code == "0" or tid) else "err"
            info = f"id {tid}" if tid else (obj.get("msg") or "")
            info_full = orjson.dumps(obj).decode()
        elif kind == "order_api_sl":
            label = "SL"
            obj = ev.get("sl") or {}
            code = str(obj.get("code", "0"))
            sid = obj.get("tpslId") or obj.get("algoId")
            status = "ok" if (code == "0" or sid) else "err"
            info = f"id {sid}" if sid else (obj.get("msg") or "")
            info_full = orjson.dumps(obj).decode()
        elif kind == "order_api":
            label = "Order"
            obj = ev.get("resp") or {}
            code = str(obj.get("code", "0"))
            status = "ok" if code == "0" else "err"
            info = (obj.get("msg") or "placed") if code == "0" else obj.get("msg")
            info_full = orjson.dumps(obj).decode()
        elif kind == "risk_trail_sl":
            label = "Trail SL"
            side = ev.get("side") or side
            new_sl = ev.get("new_sl")
            info = f"new {new_sl}" if new_sl is not None else "trail"
            res = ev.get("res") or {}
            code = str(res.get("code", "0"))
            status = "ok" if code == "0" else "err"
            info_full = orjson.dumps(res).decode()
        elif kind == "trade_close":
            label = "Close"
            reason = ev.get("reason") or "close"
            pnl = ev.get("pnl")
            dur = ev.get("duration_sec")
            parts = [reason]
            if pnl is not None:
                parts.append(f"pnl {pnl}")
            if dur is not None:
                m, s = int(dur // 60), int(dur % 60)
                parts.append(f"dur {m}m{s}s")
            info = ", ".join(parts)
            status = "ok"
        else:
            # fallback: compact string
            msg = ev.get("msg") or ev.get("info") or ""
            info = str(msg)[:180]
            info_full = orjson.dumps(ev).decode()
    except Exception:
        pass

    return {
        "ts": ts,
        "label": label,
        "symbol": symbol,
        "side": side,
        "info": info,
        "status": status,
        "info_full": info_full,
    }


def _render_row(request: Request, kind: str, ev: Dict[str, Any]) -> str:
    template = None
    if kind == "signal":
        template = "_row_signal.html"
    elif kind.startswith("order_api") or kind.startswith("order") or kind.startswith("risk_") or kind == "trade_close":
        template = "_row_order.html"
    else:
        template = "_row_error.html"
    # Prepare view model for orders
    ctx = {"ev": ev}
    if template == "_row_order.html":
        ctx["view"] = _normalize_order_view(ev)
    # Render to a string
    html = templates.get_template(template).render(**ctx, request=request)
    # Ensure no stray newlines break SSE payloads
    return html.replace("\n", " ")


def _window(items: Deque[Dict[str, Any]], secs: int) -> List[Dict[str, Any]]:
    if not items:
        return []
    cutoff = time.time() - secs
    out: List[Dict[str, Any]] = []
    for ev in reversed(items):  # newest first
        try:
            ts = float(ev.get("ts"))
        except Exception:
            ts = 0.0
        if ts and ts >= cutoff:
            out.append(ev)
        else:
            break
    return list(reversed(out))


def _compute_summary(window_mins: int = 60) -> Dict[str, Any]:
    last_bal = None
    for ev in reversed(orders):
        if ev.get("kind") == "order_balance_snapshot":
            last_bal = ev
            break
    last_equity = None
    last_available = None
    if last_bal:
        try:
            last_equity = float(last_bal.get("equityUsd")) if last_bal.get("equityUsd") is not None else None
        except Exception:
            last_equity = None
        try:
            last_available = float(last_bal.get("available")) if last_bal.get("available") is not None else None
        except Exception:
            last_available = None

    # Exposure metric (if emitted by scanner)
    last_exposure = None
    for ev in reversed(orders):
        if ev.get("kind") == "exposure":
            try:
                last_exposure = float(ev.get("total"))
            except Exception:
                last_exposure = None
            break

    # Error rate in last N minutes
    secs = max(60, int(window_mins) * 60)
    orders_n = [e for e in _window(orders, secs) if str(e.get("kind")).startswith("order_")]
    errors_n = _window(errors, secs)
    err_rate = 0.0
    denom = max(1, len(orders_n) + len(errors_n))
    err_rate = (len(errors_n) / denom) * 100.0

    # Freshness
    last_sig_ts = signals[-1].get("ts") if signals else None
    last_ord_ts = orders[-1].get("ts") if orders else None
    last_err_ts = errors[-1].get("ts") if errors else None
    # Realized PnL and hold durations (today)
    pnl_today = 0.0
    hold_avg_sec = 0.0
    b_lt1 = b_1_5 = b_5_15 = b_gt15 = 0
    try:
        start = _start_of_day()
        durs = []
        for ev in reversed(orders):
            ts = float(ev.get("ts", 0))
            if ts < start:
                break
            if ev.get("kind") == "trade_close":
                v = ev.get("pnl")
                if v is not None:
                    try: pnl_today += float(v)
                    except Exception: pass
                dur = ev.get("duration_sec")
                if dur is not None:
                    try:
                        dv = float(dur)
                        durs.append(dv)
                        m = dv/60.0
                        if m < 1: b_lt1 += 1
                        elif m < 5: b_1_5 += 1
                        elif m < 15: b_5_15 += 1
                        else: b_gt15 += 1
                    except Exception:
                        pass
        if durs:
            hold_avg_sec = sum(durs)/len(durs)
    except Exception:
        pass

    return {
        "equity": last_equity,
        "available": last_available,
        "exposure": last_exposure,
        "err_rate": err_rate,
        "last_signal_ts": last_sig_ts,
        "last_order_ts": last_ord_ts,
        "last_error_ts": last_err_ts,
        "window_mins": window_mins,
        "pnl_today": pnl_today,
        "hold_avg_sec": hold_avg_sec,
        "hold_buckets": {"lt1": b_lt1, "m1_5": b_1_5, "m5_15": b_5_15, "+15": b_gt15},
    }


async def _tailer_task() -> None:
    path = METRICS_PATH
    # Prime buffers with recent history
    for line in _last_lines(path, 20000):
        ev = _parse_line(line)
        if not ev:
            continue
        k = _classify(ev)
        if k == "signal":
            signals.append(ev)
        elif k.startswith("order") or k.startswith("risk_"):
            orders.append(ev)
        elif k == "trade_close":
            orders.append(ev)
        elif k.endswith("error"):
            errors.append(ev)
        elif k == "position":
            try:
                inst = ev.get("instId") or ev.get("symbol")
                side = (ev.get("side") or "").lower()
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
                        "ts": ev.get("ts") or time.time(),
                    }
            except Exception:
                pass

    # Tail the file
    pos = 0
    try:
        pos = os.path.getsize(path)
    except Exception:
        pos = 0

    while True:
        try:
            size = os.path.getsize(path)
            if size < pos:
                # rotated or truncated
                pos = 0
            if size > pos:
                with open(path, "rb") as f:
                    f.seek(pos)
                    chunk = f.read(size - pos)
                pos = size
                text = chunk.decode(errors="ignore")
                for line in io.StringIO(text):
                    ev = _parse_line(line)
                    if not ev:
                        continue
                    k = _classify(ev)
                    if k == "signal":
                        signals.append(ev)
                    elif k.startswith("order") or k.startswith("risk_"):
                        orders.append(ev)
                    elif k.endswith("error"):
                        errors.append(ev)
                    # Broadcast as SSE (render on server for HTMX sse-swap)
                    # Use a fake request object for template rendering context
                    req = Request(scope={"type": "http"})
                    if k == "signal":
                        html = _render_row(req, "signal", ev)
                        await hub.broadcast("signal", html)
                    elif k.startswith("order") or k.startswith("risk_"):
                        html = _render_row(req, "order", ev)
                        await hub.broadcast("orders", html)
                    elif k.endswith("error"):
                        html = _render_row(req, "error", ev)
                        await hub.broadcast("errors", html)
                    elif k == "trade_close":
                        html = _render_row(req, "order", ev)
                        await hub.broadcast("orders", html)
                    if k == "position":
                        try:
                            inst = ev.get("instId") or ev.get("symbol")
                            side = (ev.get("side") or "").lower()
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
                                    "ts": ev.get("ts") or time.time(),
                                }
                        except Exception:
                            pass
        except Exception:
            # sleep and retry on any I/O parse errors
            await asyncio.sleep(0.5)
        await asyncio.sleep(0.5)


@app.on_event("startup")
async def _on_start() -> None:
    asyncio.create_task(_tailer_task())


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    # Prepare initial slices
    sigs = list(signals)[-30:][::-1]
    ords = list(orders)[-50:][::-1]
    errs = list(errors)[-50:][::-1]
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "signals": sigs,
            "orders": ords,
            "errors": errs,
            "summary": _compute_summary(60),
        },
    )


@app.get("/api/summary")
async def summary():
    def _ts(ev: Dict[str, Any]) -> float:
        t = ev.get("ts")
        try:
            return float(t)
        except Exception:
            return 0.0

    # last position ts from live positions map
    try:
        last_pos_ts = 0.0
        for st in positions_map.values():
            t = float(st.get("ts") or 0.0)
            if t > last_pos_ts:
                last_pos_ts = t
    except Exception:
        last_pos_ts = 0.0

    out = {
        "signals": len(signals),
        "orders": len(orders),
        "errors": len(errors),
        "last_signal_ts": _ts(signals[-1]) if signals else 0,
        "last_order_ts": _ts(orders[-1]) if orders else 0,
        "last_error_ts": _ts(errors[-1]) if errors else 0,
        "last_position_ts": float(last_pos_ts),
        "server_now": time.time(),
    }
    return JSONResponse(out)


@app.get("/partials/summary", response_class=HTMLResponse)
async def partial_summary(request: Request, mins: int = 60):
    return templates.TemplateResponse("_summary.html", {"request": request, "summary": _compute_summary(mins)})


def _filter_signals(side: str = "all", tf: str = "all", limit: int = 50, sort: str = "ts", direction: str = "desc") -> List[Dict[str, Any]]:
    rows = list(signals)[-200:]
    out = []
    side = side.lower()
    tf = tf.lower()
    for ev in reversed(rows):
        s_ok = (side == "all" or str(ev.get("side")).lower() == side)
        tf_ok = (tf == "all" or str(ev.get("timeframe")).lower() == tf)
        if s_ok and tf_ok:
            out.append(ev)
        if len(out) >= limit:
            break
    key = sort.lower()
    reverse = (str(direction or "desc").lower() != "asc")
    def _k(e: Dict[str, Any]):
        try:
            if key == "symbol": return str(e.get("symbol") or "")
            if key == "tf" or key == "timeframe": return str(e.get("timeframe") or "")
            if key == "side": return str(e.get("side") or "")
            if key == "score": return float(e.get("score") or 0.0)
            if key == "prob": return float(e.get("prob") or 0.0)
            if key == "price": return float(e.get("price") or 0.0)
            # default ts
            return float(e.get("ts") or 0.0)
        except Exception:
            return 0
    try:
        out.sort(key=_k, reverse=reverse)
    except Exception:
        pass
    return out


@app.get("/partials/signals", response_class=HTMLResponse)
async def partial_signals(request: Request, side: str = "all", tf: str = "all", limit: int = 50, sort: str = "ts", dir: str = "desc"):
    rows = _filter_signals(side, tf, limit, sort, dir)
    return templates.TemplateResponse("_signals_tbody.html", {"request": request, "signals": rows})


def _filter_orders(kind: str = "all", limit: int = 50, sort: str = "ts", direction: str = "desc") -> List[Dict[str, Any]]:
    rows = list(orders)[-300:]
    out: List[Dict[str, Any]] = []
    k = kind.lower()
    for ev in reversed(rows):
        sk = str(ev.get("kind") or "").lower()
        take = (
            k == "all"
            or (k == "tp" and sk == "order_api_tp")
            or (k == "sl" and sk == "order_api_sl")
            or (k == "order" and (sk == "order_api" or sk == "trade_close"))
            or (k == "trail" and sk == "risk_trail_sl")
            or (k == "error" and sk.endswith("error"))
            or (sk == "trade_close")
        )
        if take:
            out.append(ev)
        if len(out) >= limit:
            break
    key = sort.lower()
    reverse = (str(direction or "desc").lower() != "asc")
    def _k(e: Dict[str, Any]):
        try:
            if key == "symbol": return str(e.get("symbol") or e.get("instId") or "")
            if key == "side": return str(e.get("side") or "")
            if key == "kind": return str(e.get("kind") or "")
            return float(e.get("ts") or 0.0)
        except Exception:
            return 0
    try:
        out.sort(key=_k, reverse=reverse)
    except Exception:
        pass
    return out


@app.get("/partials/orders", response_class=HTMLResponse)
async def partial_orders(request: Request, kind: str = "all", limit: int = 50, sort: str = "ts", dir: str = "desc"):
    rows = _filter_orders(kind, limit, sort, dir)
    return templates.TemplateResponse("_orders_tbody.html", {"request": request, "orders": rows})


def _start_of_day() -> float:
    t = time.localtime()
    sod = time.struct_time((t.tm_year, t.tm_mon, t.tm_mday, 0, 0, 0, t.tm_wday, t.tm_yday, t.tm_isdst))
    return time.mktime(sod)


def _positions_list() -> List[Dict[str, Any]]:
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


@app.get("/partials/positions", response_class=HTMLResponse)
async def partial_positions(request: Request, sort: str = "symbol", dir: str = "asc"):
    rows = _positions_list()
    key = sort.lower()
    reverse = (str(dir or "asc").lower() != "asc")
    def _k(e: Dict[str, Any]):
        try:
            if key == "symbol": return str(e.get("symbol") or "")
            if key == "side": return str(e.get("side") or "")
            if key == "size": return float(e.get("size") or 0.0)
            if key == "entry": return float(e.get("entry") or 0.0)
            if key == "mark": return float(e.get("mark") or 0.0)
            if key == "pnl": return float(e.get("pnl_u") or 0.0)
            return float(e.get("ts") or 0.0)
        except Exception:
            return 0
    try:
        rows.sort(key=_k, reverse=reverse)
    except Exception:
        pass
    return templates.TemplateResponse("_positions.html", {"request": request, "positions": rows})


def _pnl_series(window_mins: int = 240, points: int = 100) -> List[tuple[float, float]]:
    cutoff = time.time() - (window_mins * 60)
    closes = [ev for ev in orders if ev.get("kind") == "trade_close" and float(ev.get("ts", 0)) >= cutoff]
    closes.sort(key=lambda e: float(e.get("ts", 0)))
    series: List[tuple[float, float]] = []
    acc = 0.0
    for ev in closes:
        pnl = ev.get("pnl")
        try:
            acc += float(pnl) if pnl is not None else 0.0
        except Exception:
            pass
        series.append((float(ev.get("ts", time.time())), acc))
    if len(series) > points and points > 0:
        step = len(series) / points
        series = [series[int(i*step)] for i in range(points)]
    return series


@app.get("/partials/pnl", response_class=HTMLResponse)
async def partial_pnl(request: Request, mins: int = 240):
    series = _pnl_series(mins, 120)
    return templates.TemplateResponse("_pnl.html", {"request": request, "series": series, "mins": mins})


@app.get("/stream")
async def sse_stream() -> StreamingResponse:
    async def _gen() -> AsyncGenerator[bytes, None]:
        q = await hub.subscribe()
        try:
            # initial heartbeat so HTMX connects
            yield b"event: ping\ndata: ok\n\n"
            while True:
                msg = await q.get()
                yield msg.encode()
        finally:
            await hub.unsubscribe(q)

    return StreamingResponse(_gen(), media_type="text/event-stream")


def _find_event(bucket: str, ts: float) -> Optional[Dict[str, Any]]:
    src: List[Dict[str, Any]]
    if bucket == "signals":
        src = list(signals)
    elif bucket == "orders":
        src = list(orders)
    else:
        src = list(errors)
    # find nearest matching ts
    best = None
    best_dt = 1e9
    for ev in src:
        try:
            t = float(ev.get("ts"))
            dt = abs(t - ts)
            if dt < best_dt:
                best = ev
                best_dt = dt
        except Exception:
            continue
    return best


@app.get("/partials/details", response_class=HTMLResponse)
async def partial_details(request: Request, bucket: str, ts: float):
    ev = _find_event(bucket, ts)
    title = f"{bucket.title()} details"
    if not ev:
        return templates.TemplateResponse("_details.html", {"request": request, "title": title, "body": "No data"})
    try:
        body = orjson.dumps(ev, option=orjson.OPT_INDENT_2).decode()
    except Exception:
        import json as _json
        body = _json.dumps(ev, indent=2, ensure_ascii=False)
    return templates.TemplateResponse("_details.html", {"request": request, "title": title, "body": body})


def _filter_errors(limit: int = 100, sort: str = "ts", direction: str = "desc") -> List[Dict[str, Any]]:
    rows = list(errors)[-400:]
    out = list(reversed(rows))  # newest first
    key = sort.lower()
    reverse = (str(direction or "desc").lower() != "asc")
    def _k(e: Dict[str, Any]):
        try:
            if key == "stage": return str(e.get("stage") or "")
            if key == "symbol": return str(e.get("symbol") or "")
            return float(e.get("ts") or 0.0)
        except Exception:
            return 0
    try:
        out.sort(key=_k, reverse=reverse)
    except Exception:
        pass
    return out[:limit]


@app.get("/partials/errors", response_class=HTMLResponse)
async def partial_errors(request: Request, limit: int = 100, sort: str = "ts", dir: str = "desc"):
    rows = _filter_errors(limit, sort, dir)
    return templates.TemplateResponse("_errors_tbody.html", {"request": request, "errors": rows})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8081")), reload=False)

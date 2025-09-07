from __future__ import annotations

import asyncio
import io
import os
import time
from collections import deque
from pathlib import Path
from typing import Any, AsyncGenerator, Deque, Dict, List, Optional, Set

import orjson
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates


METRICS_PATH = os.getenv("METRICS_PATH", "/data/scanner_metrics.jsonl")
MAX_BUF = int(os.getenv("DASHBOARD_MAX_BUF", "500"))


app = FastAPI(title="Smart Scanner Dashboard")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


# Ring buffers (in-memory recent history)
signals: Deque[Dict[str, Any]] = deque(maxlen=MAX_BUF)
orders: Deque[Dict[str, Any]] = deque(maxlen=MAX_BUF)
errors: Deque[Dict[str, Any]] = deque(maxlen=MAX_BUF)


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


def _render_row(request: Request, kind: str, ev: Dict[str, Any]) -> str:
    template = None
    if kind == "signal":
        template = "_row_signal.html"
    elif kind.startswith("order_api") or kind.startswith("order") or kind.startswith("risk_"):
        template = "_row_order.html"
    else:
        template = "_row_error.html"
    # Render to a string
    html = templates.get_template(template).render(ev=ev, request=request)
    # Ensure no stray newlines break SSE payloads
    return html.replace("\n", " ")


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
        elif k.endswith("error"):
            errors.append(ev)

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

    out = {
        "signals": len(signals),
        "orders": len(orders),
        "errors": len(errors),
        "last_signal_ts": _ts(signals[-1]) if signals else 0,
        "last_order_ts": _ts(orders[-1]) if orders else 0,
        "last_error_ts": _ts(errors[-1]) if errors else 0,
    }
    return JSONResponse(out)


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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8081")), reload=False)


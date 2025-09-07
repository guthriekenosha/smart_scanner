"""
Optional: public WS to stream tickers (if you want sub-second updates).
Kept simple for now; scanner uses REST. Extend later as needed.
"""

from __future__ import annotations
import asyncio
import json
import websockets


# Placeholder—wire up when you want live streaming.
async def watch_tickers():
    ws_url = "wss://openapi.blofin.com/ws/public"
    sub = {"op": "subscribe", "args": [{"channel": "tickers"}]}
    async with websockets.connect(ws_url, ping_interval=20) as ws:
        await ws.send(json.dumps(sub))
        async for msg in ws:
            _ = json.loads(msg)  # do something

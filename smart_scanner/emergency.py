from __future__ import annotations
import asyncio
import os
from typing import Iterable, List, Optional

from .config import CONFIG
from .metrics import emit as emit_metric
from .blofin_client import BlofinClient


def kill_active() -> bool:
    try:
        if CONFIG.kill_switch:
            return True
    except Exception:
        pass
    try:
        p = getattr(CONFIG, "emergency_file", ".panic")
        if p and os.path.exists(p):
            return True
    except Exception:
        pass
    return False


async def panic_flatten(
    client: BlofinClient,
    symbols: Optional[Iterable[str]] = None,
    cancel_open: Optional[bool] = None,
    close_positions: Optional[bool] = None,
) -> None:
    """
    Cancel all open orders and close all open positions (reduce-only market).
    If 'symbols' provided, limits the operation to those instruments.
    """
    cancel_open = bool(CONFIG.panic_cancel_open) if cancel_open is None else bool(cancel_open)
    close_positions = bool(CONFIG.panic_close_positions) if close_positions is None else bool(close_positions)

    allow_symbols: Optional[List[str]] = None
    if symbols is not None:
        allow_symbols = [s for s in symbols]

    async def _allow(inst: Optional[str]) -> bool:
        if allow_symbols is None:
            return True
        try:
            return inst in allow_symbols
        except Exception:
            return False

    # 1) Cancel open algo TP/SL and regular pending orders
    if cancel_open:
        try:
            pend = await client.orders_pending()
        except Exception:
            pend = []
        for it in pend or []:
            try:
                inst = it.get("instId") or it.get("symbol")
                if not await _allow(inst):
                    continue
                oid = str(it.get("orderId")) if it.get("orderId") is not None else None
                if oid:
                    res = await client.cancel_order(oid, inst_id=inst)
                    try:
                        emit_metric("panic_cancel_order", {"instId": inst, "orderId": oid, "resp": res})
                    except Exception:
                        pass
            except Exception:
                continue
        try:
            algo = await client.orders_tpsl_pending(limit=100)
        except Exception:
            algo = []
        for it in algo or []:
            try:
                inst = it.get("instId") or it.get("symbol")
                if not await _allow(inst):
                    continue
                aid = it.get("tpslId") or it.get("algoId")
                if aid:
                    res2 = await client.cancel_tpsl(inst_id=inst, tpsl_id=str(aid))
                    try:
                        emit_metric("panic_cancel_tpsl", {"instId": inst, "tpslId": str(aid), "resp": res2})
                    except Exception:
                        pass
            except Exception:
                continue

    # 2) Close all open positions using reduce-only market orders
    if close_positions:
        try:
            poss = await client.get_positions()
        except Exception:
            poss = []
        for p in poss or []:
            try:
                inst = p.get("instId") or p.get("symbol")
                if not inst:
                    continue
                if not await _allow(inst):
                    continue
                raw = p.get("positions") or p.get("pos") or p.get("position") or 0
                sz = float(raw)
                if abs(sz) <= 0:
                    continue
                side = "sell" if sz > 0 else "buy"
                pos_side = (p.get("positionSide") or "net").lower()
                mm = (p.get("marginMode") or CONFIG.trading_margin_mode)
                size_s = f"{abs(sz):.6f}"
                res3 = await client.place_order(
                    str(inst),
                    side,
                    "market",
                    size_s,
                    margin_mode=str(mm),
                    position_side=pos_side,
                    reduce_only=bool(CONFIG.panic_reduce_only),
                )
                try:
                    emit_metric("panic_close", {"instId": inst, "side": side, "size": size_s, "resp": res3})
                except Exception:
                    pass
            except Exception:
                continue


def main():
    import argparse

    p = argparse.ArgumentParser(description="Emergency controls")
    p.add_argument("--cancel", action="store_true", help="Cancel open orders only")
    p.add_argument("--close", action="store_true", help="Close positions only")
    p.add_argument("--all", action="store_true", help="Cancel + Close (default)")
    p.add_argument("--symbols", type=str, default="", help="Comma-separated instIds to limit")
    args = p.parse_args()

    syms = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    do_cancel = args.cancel or args.all or (not args.cancel and not args.close and not args.all)
    do_close = args.close or args.all or (not args.cancel and not args.close and not args.all)

    async def _run():
        async with BlofinClient() as c:
            await panic_flatten(c, symbols=syms, cancel_open=do_cancel, close_positions=do_close)

    asyncio.run(_run())


if __name__ == "__main__":
    main()


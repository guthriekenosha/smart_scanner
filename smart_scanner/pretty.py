"""
Lightweight pretty-print helpers for terminal output.

Uses Rich when available, falls back to plain prints otherwise.
Enable via env PRETTY=1 (Config.pretty).
"""
from __future__ import annotations

from typing import Any, Iterable, Optional, cast

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box
    _RICH_OK = True
except Exception:  # pragma: no cover
    Console = None  # type: ignore
    Table = None  # type: ignore
    Panel = None  # type: ignore
    box = None  # type: ignore
    _RICH_OK = False


class PrettyPrinter:
    def __init__(self) -> None:
        self.enabled = _RICH_OK
        self._console: Optional[Any] = Console() if _RICH_OK else None

    def _print(self, s: str) -> None:
        if self._console is None:
            print(s)
        else:
            self._console.print(s)

    def _entry_summary(self, s: Any) -> str:
        hint = getattr(s, "entry_hint", None)
        if not isinstance(hint, dict):
            return ""
        mode = str(hint.get("mode", "market"))
        decision = str(hint.get("decision", mode))
        price = hint.get("price")
        price_str = ""
        if isinstance(price, (int, float)):
            try:
                price_str = f" @{float(price):.6g}"
            except Exception:
                price_str = ""
        parts = [f"{mode}{price_str}".strip()]
        if decision == "wait_for_retest":
            parts.append("retest")
        elif decision and decision != mode:
            parts.append(decision)
        reason = hint.get("reason")
        if isinstance(reason, str) and reason and len(reason) <= 40:
            parts.append("·")
            parts.append(reason)
        return " ".join(part for part in parts if part)

    # -------- High-level helpers --------
    def config_table(self, cfg: Any) -> None:
        if not self.enabled or self._console is None:
            print(
                f"[cfg] timeframes={list(cfg.timeframes)} min_score={cfg.min_score} top_symbols={cfg.top_symbols_by_quote_vol}"
            )
            return
        # Rich-only path (satisfy type checker)
        RTable = cast(Any, Table)
        RBox = cast(Any, box)
        t = RTable(title="Scanner Config", box=RBox.SIMPLE_HEAVY)
        t.add_column("Key", style="bold cyan")
        t.add_column("Value", style="white")
        t.add_row("Timeframes", ", ".join(map(str, cfg.timeframes)))
        t.add_row("Min Score", f"{cfg.min_score}")
        t.add_row("Top Symbols", f"{cfg.top_symbols_by_quote_vol}")
        t.add_row("Universe from WS", "Yes" if cfg.universe_from_ws else "No")
        t.add_row("Paper Trading", "Yes" if cfg.paper_trading else "No")
        t.add_row("Autotrade", "Yes" if cfg.enable_autotrade else "No")
        t.add_row("TPSL", "On" if cfg.enable_tpsl else "Off")
        t.add_row("Leverage", str(cfg.trading_leverage))
        t.add_row("Margin Mode", str(cfg.trading_margin_mode))
        self._console.print(t)

    def universe_summary(self, universe: Iterable[str], source: str, sample: int = 12) -> None:
        uni = list(universe)
        if not self.enabled or self._console is None:
            from itertools import islice
            sample_items = ", ".join(list(islice(uni, sample)))
            print(f"[liquidity] Universe (active): {len(uni)} (example: {sample_items}) [source={source}]")
            return
        RTable = cast(Any, Table)
        RBox = cast(Any, box)
        title = f"Active Universe: {len(uni)} symbols [source={source}]"
        t = RTable(title=title, box=RBox.MINIMAL_HEAVY_HEAD)
        t.add_column("#", justify="right", style="dim")
        t.add_column("Symbol", style="bold")
        for i, sym in enumerate(uni[:sample], 1):
            t.add_row(str(i), sym)
        self._console.print(t)

    def signals_table(self, signals: list[Any]) -> None:
        if not signals:
            return
        if not self.enabled or self._console is None:
            for s in signals:
                side_tag = "🟢" if getattr(s, "side", "buy") == "buy" else "🔴"
                comps = ", ".join(getattr(s, "components", []) or [])
                entry = self._entry_summary(s)
                entry_txt = f" | entry {entry}" if entry else ""
                print(
                    f"[{side_tag}] {s.symbol} {s.timeframe} | Score {s.score:.2f} | P {getattr(s,'prob',0.0)*100:.1f}% | EV {getattr(s,'ev',0.0):.2f} | {comps} | price {s.price:.6g}{entry_txt}"
                )
            return
        RTable = cast(Any, Table)
        RBox = cast(Any, box)
        t = RTable(title="Signals", box=RBox.SIMPLE_HEAVY)
        t.add_column("Symbol", style="bold")
        t.add_column("TF", style="cyan")
        t.add_column("Side")
        t.add_column("Score", justify="right")
        t.add_column("Prob %", justify="right")
        t.add_column("EV", justify="right")
        t.add_column("Price", justify="right")
        t.add_column("Components", overflow="fold")
        t.add_column("Entry", style="dim", overflow="fold")
        for s in signals:
            side = getattr(s, "side", "buy")
            side_style = "green" if side == "buy" else "red"
            comps = ", ".join(getattr(s, "components", []) or [])
            prob = getattr(s, "prob", 0.0) * 100.0
            ev = getattr(s, "ev", 0.0)
            entry = self._entry_summary(s)
            t.add_row(
                str(getattr(s, "symbol", "?")),
                str(getattr(s, "timeframe", "?")),
                f"[{side_style}]{side}[/]",
                f"{float(getattr(s, 'score', 0.0)):.2f}",
                f"{prob:.1f}",
                f"{ev:.2f}",
                f"{float(getattr(s, 'price', 0.0)):.6g}",
                comps,
                entry,
            )
        self._console.print(t)

    def info(self, msg: str) -> None:
        if not self.enabled or self._console is None:
            print(msg)
        else:
            self._console.print(f"[bold cyan]{msg}[/]")

    def warn(self, msg: str) -> None:
        if not self.enabled or self._console is None:
            print(msg)
        else:
            self._console.print(f"[bold yellow]{msg}[/]")

    def err(self, msg: str) -> None:
        if not self.enabled or self._console is None:
            print(msg)
        else:
            self._console.print(f"[bold red]{msg}[/]")


# Singleton printer
PR = PrettyPrinter()

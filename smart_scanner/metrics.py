from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict

from .config import CONFIG

_FALLBACK_WARNED = False


def _ensure_parent_dir(filepath: str) -> None:
    parent = os.path.dirname(filepath)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def _write_line(path: str, line: str) -> bool:
    try:
        _ensure_parent_dir(path)
        with open(path, "a") as fh:
            fh.write(line)
        return True
    except (FileNotFoundError, PermissionError, OSError):
        return False


def _candidate_paths() -> list[str]:
    primary = CONFIG.metrics_path
    paths = [primary]
    if os.path.isabs(primary):
        fallback = os.path.join(os.getcwd(), os.path.basename(primary))
        if fallback not in paths:
            paths.append(fallback)
    return paths


def emit(kind: str, payload: Dict[str, Any]) -> None:
    if not CONFIG.metrics_enabled:
        return
    line = json.dumps({"ts": int(time.time()), "kind": kind, **payload}, separators=(",", ":")) + "\n"
    global _FALLBACK_WARNED
    for idx, candidate in enumerate(_candidate_paths()):
        if _write_line(candidate, line):
            if idx > 0 and not _FALLBACK_WARNED:
                try:
                    print(
                        f"[metrics/WARN] primary path '{CONFIG.metrics_path}' unavailable, using '{candidate}' instead",
                        file=sys.stderr,
                    )
                except Exception:
                    pass
                _FALLBACK_WARNED = True
            return

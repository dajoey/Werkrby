"""Small shared helpers. Stdlib only."""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime


def log(msg: str) -> None:
    """One line per event, flushed: systemd timers capture stdout per run."""
    print("[tick %s] %s" % (datetime.now().strftime("%H:%M:%S"), msg), flush=True)


def clip(text, n: int) -> str:
    text = "" if text is None else str(text)
    return text if len(text) <= n else text[: n - 1] + "…"


def read_text(path: str, limit: int | None = None) -> str:
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            s = fh.read()
        return s[:limit] if limit else s
    except OSError:
        return ""


def read_json(path: str):
    raw = read_text(path)
    if not raw.strip():
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None


def write_atomic(path: str, data: str, mode: int = 0o644) -> None:
    """tmp + rename so a reader never sees a half-written file."""
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.chmod(tmp, mode)
    os.replace(tmp, path)


def write_json_atomic(path: str, obj, mode: int = 0o644) -> None:
    write_atomic(path, json.dumps(obj, indent=2, sort_keys=True) + "\n", mode)


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts).isoformat(timespec="seconds")


def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, "") or default)
    except ValueError:
        return default


def now() -> float:
    return time.time()

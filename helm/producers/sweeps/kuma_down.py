#!/usr/bin/env python3
"""Deterministic sweep: Uptime Kuma liveness watcher (fetch -> shape -> decide -> apply).

Contract P4 §3:
Zero LLM on critical path.
Hysteresis: alerts only after ALERT_AFTER (default 3) consecutive failed checks.
Dry-run purity: --dry-run writes NOTHING to state, disk, or network.
Heartbeat on every run: --heartbeat touches .kuma-down.heartbeat.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent.parent
sys.path.insert(0, str(REPO_ROOT))
_EXTRA_PATH = os.environ.get("HELM_EXTRA_PATH")
if _EXTRA_PATH:
    sys.path.insert(0, _EXTRA_PATH)

from producers.common import put_thread, settle_thread, touch_heartbeat

# Probe target is deployment data: set KUMA_URL, or pass --url.
DEFAULT_KUMA_URL = os.environ.get("KUMA_URL", "http://127.0.0.1:3031/")
OPS_DIR = Path(os.environ.get("HELM_OPS_DIR") or (Path.home() / "ops"))
DEFAULT_STATE_FILE = OPS_DIR / ".kuma-down.state.json"
DEFAULT_HEARTBEAT_FILE = OPS_DIR / ".kuma-down.heartbeat"
ALERT_AFTER = 3


def fetch_kuma_status(url: str = DEFAULT_KUMA_URL, timeout: int = 10) -> tuple[int, str]:
    """Fetch raw HTTP status code from Uptime Kuma endpoint."""
    req = urllib.request.Request(url, headers={"User-Agent": "Helm-Sweep/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, ""
    except urllib.error.HTTPError as e:
        return e.code, str(e)
    except Exception as e:
        return 0, str(e)


def shape_kuma_status(status_code: int, err_msg: str, now: float | None = None) -> dict:
    """Shape raw probe results into structured health facts."""
    ts = now if now is not None else time.time()
    up = status_code in (200, 302)
    return {
        "kuma_up": up,
        "status_code": status_code,
        "error": err_msg,
        "timestamp": ts
    }


def decide_kuma(shaped: dict, state: dict, alert_after: int = ALERT_AFTER,
                now_dt: datetime | None = None) -> dict:
    """Pure decision function. Returns decision data structure without side effects."""
    now_dt = now_dt or datetime.now(timezone.utc)
    date_str = now_dt.strftime("%Y%m%d")
    tid = f"sweep-kuma-down-{date_str}"

    was_alerted = state.get("alerted", False)
    fails = state.get("fails", 0)

    if shaped["kuma_up"]:
        if was_alerted:
            return {
                "action": "resolve",
                "thread_id": state.get("active_thread_id", tid),
                "reason": "Kuma HTTP endpoint answering again",
                "new_state": {"fails": 0, "alerted": False, "active_thread_id": None, "last_up": shaped["timestamp"]}
            }
        return {
            "action": "noop",
            "reason": "Kuma healthy",
            "new_state": {"fails": 0, "alerted": False, "active_thread_id": None, "last_up": shaped["timestamp"]}
        }

    # Kuma is down
    new_fails = fails + 1
    if new_fails >= alert_after:
        action = "update" if was_alerted else "raise"
        thread_payload = {
            "thread_id": tid,
            "title": f"[Uptime Kuma DOWN] Container unreachable ({new_fails} fails)",
            "summary": (
                f"Uptime Kuma has failed {new_fails} consecutive HTTP checks.\n"
                f"Last HTTP code: {shaped['status_code']} ({shaped['error']}).\n"
                f"Impact: every backup and push monitor it watches is dark."
            ),
            "category": "Fleet health",
            "urgency": "immediate",
            "state": "needs-you",
            "ask": {
                "kind": "choice",
                "prompt": "Uptime Kuma is down. What action should be taken?",
                "options": [
                    {
                        "label": "Restart container",
                        "value": "restart",
                        "effect": "I restart the uptime-kuma container"
                    },
                    {
                        "label": "Acknowledge outage",
                        "value": "ack",
                        "effect": "I acknowledge the outage and inspect manually"
                    }
                ],
                "requires": ["decision"]
            },
            "sources": [{"source": "sweep", "id": "kuma-down", "note": "Deterministic liveness check"}]
        }
        return {
            "action": action,
            "thread_id": tid,
            "thread": thread_payload,
            "new_state": {
                "fails": new_fails,
                "alerted": True,
                "active_thread_id": tid,
                "last_fail": shaped["timestamp"]
            }
        }

    return {
        "action": "noop",
        "reason": f"Fails streak ({new_fails}) below threshold ({alert_after})",
        "new_state": {
            "fails": new_fails,
            "alerted": False,
            "active_thread_id": None,
            "last_fail": shaped["timestamp"]
        }
    }


def load_state(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"fails": 0, "alerted": False, "active_thread_id": None}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def run_sweep(url: str = DEFAULT_KUMA_URL, state_file: Path = DEFAULT_STATE_FILE,
              heartbeat_file: Path = DEFAULT_HEARTBEAT_FILE, dry_run: bool = False,
              heartbeat: bool = True) -> dict:
    """Run full sweep pipeline."""
    code, err = fetch_kuma_status(url)
    shaped = shape_kuma_status(code, err)
    state = load_state(state_file)
    decision = decide_kuma(shaped, state)

    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "shaped": shaped,
            "decision": decision
        }

    # Apply decision
    action = decision["action"]
    if action in ("raise", "update"):
        put_thread(decision["thread"], dry_run=False)
    elif action == "resolve":
        settle_thread(decision["thread_id"], note=decision["reason"], dry_run=False)

    save_state(state_file, decision["new_state"])
    if heartbeat:
        touch_heartbeat(heartbeat_file, dry_run=False)

    return {
        "ok": True,
        "dry_run": False,
        "action": action,
        "shaped": shaped,
        "decision": decision
    }


def selftest() -> int:
    """Offline selftest verifying fetch/shape/decide purity and hysteresis."""
    s_up = shape_kuma_status(200, "")
    assert s_up["kuma_up"] is True, "HTTP 200 must be up"

    s_down = shape_kuma_status(500, "Internal Server Error")
    assert s_down["kuma_up"] is False, "HTTP 500 must be down"

    st = {"fails": 0, "alerted": False}
    # 1 fail -> noop
    d1 = decide_kuma(s_down, st)
    assert d1["action"] == "noop", "First failure must not alarm"
    assert d1["new_state"]["fails"] == 1

    # 2 fails -> noop
    d2 = decide_kuma(s_down, d1["new_state"])
    assert d2["action"] == "noop", "Second failure must not alarm"
    assert d2["new_state"]["fails"] == 2

    # 3 fails -> raise
    d3 = decide_kuma(s_down, d2["new_state"])
    assert d3["action"] == "raise", "Third failure must raise alarm"
    assert d3["thread"]["ask"]["kind"] == "choice"
    assert len(d3["thread"]["ask"]["options"]) >= 2

    # Recover -> resolve
    d_rec = decide_kuma(s_up, d3["new_state"])
    assert d_rec["action"] == "resolve", "Recovery must resolve active alert"

    print("kuma_down: selftest passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Uptime Kuma down sweep")
    parser.add_argument("--dry-run", action="store_true", help="Evaluate without network/disk side effects")
    parser.add_argument("--selftest", action="store_true", help="Run offline logic selftest")
    parser.add_argument("--heartbeat", action="store_true", default=True, help="Update heartbeat file")
    parser.add_argument("--url", default=DEFAULT_KUMA_URL, help="Uptime Kuma URL")
    args = parser.parse_args(argv)

    if args.selftest:
        return selftest()

    res = run_sweep(url=args.url, dry_run=args.dry_run, heartbeat=args.heartbeat)
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

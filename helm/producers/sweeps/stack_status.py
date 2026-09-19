#!/usr/bin/env python3
"""Deterministic sweep: Docker stack health status watcher (fetch -> shape -> decide -> apply).

Contract P4 §3:
Zero LLM on critical path.
Concurrently probes core Docker infrastructure services.
Hysteresis: alerts only after threshold consecutive failures.
Dry-run purity: --dry-run writes NOTHING to state, disk, or network.
Heartbeat on every run: --heartbeat touches .stack-status.heartbeat.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent.parent
sys.path.insert(0, str(REPO_ROOT))
_EXTRA_PATH = os.environ.get("HELM_EXTRA_PATH")
if _EXTRA_PATH:
    sys.path.insert(0, _EXTRA_PATH)

from producers.common import put_thread, settle_thread, touch_heartbeat

OPS_DIR = Path(os.environ.get("HELM_OPS_DIR") or (Path.home() / "ops"))
DEFAULT_STATE_FILE = OPS_DIR / ".stack-status.state.json"
DEFAULT_HEARTBEAT_FILE = OPS_DIR / ".stack-status.heartbeat"
ALERT_THRESHOLD = 2

# Services whose failure is immediate rather than this-week.
_CRITICAL = {s.strip() for s in (os.environ.get("HELM_CRITICAL_SERVICES")
             or "helm-gateway,gitea,vaultwarden").split(",") if s.strip()}

# Probe targets are deployment data, not source. Point the sweep at your own
# stack with HELM_CORE_SERVICES, a JSON list of [name, url, [valid_codes]]:
#   HELM_CORE_SERVICES='[["wiki","http://wiki.example:3032/.ping",[200]]]'
# There is no built-in list: an unconfigured sweep probes nothing rather
# than alarming on unreachable defaults.
FALLBACK_CORE_SERVICES: list[tuple[str, str, list[int]]] = []


def _load_core_services() -> list[tuple[str, str, list[int]]]:
    """Read probe targets from HELM_CORE_SERVICES, else use the fallback list."""
    raw = os.environ.get("HELM_CORE_SERVICES", "").strip()
    if not raw:
        return list(FALLBACK_CORE_SERVICES)
    try:
        parsed = json.loads(raw)
        services = [(str(n), str(u), [int(c) for c in codes]) for n, u, codes in parsed]
    except Exception as e:
        print(f"WARN: ignoring malformed HELM_CORE_SERVICES: {e}", file=sys.stderr)
        return list(FALLBACK_CORE_SERVICES)
    return services or list(FALLBACK_CORE_SERVICES)


DEFAULT_CORE_SERVICES = _load_core_services()


def probe_service(name: str, url: str, valid_codes: list[int], timeout: int = 5) -> dict:
    """Probe a single service endpoint."""
    req = urllib.request.Request(url, headers={"User-Agent": "Helm-StackSweep/1.0"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ok = resp.status in valid_codes
            return {
                "name": name,
                "url": url,
                "ok": ok,
                "status_code": resp.status,
                "latency_ms": int((time.time() - t0) * 1000),
                "error": ""
            }
    except urllib.error.HTTPError as e:
        ok = e.code in valid_codes
        return {
            "name": name,
            "url": url,
            "ok": ok,
            "status_code": e.code,
            "latency_ms": int((time.time() - t0) * 1000),
            "error": str(e)
        }
    except Exception as e:
        return {
            "name": name,
            "url": url,
            "ok": False,
            "status_code": 0,
            "latency_ms": int((time.time() - t0) * 1000),
            "error": str(e)
        }


def fetch_stack_status(services: list[tuple[str, str, list[int]]] | None = None) -> list[dict]:
    """Fetch health status for all target services concurrently."""
    services = services or DEFAULT_CORE_SERVICES
    results = []
    if not services:
        return results
    with ThreadPoolExecutor(max_workers=len(services)) as pool:
        futures = [pool.submit(probe_service, name, url, codes) for name, url, codes in services]
        for f in futures:
            results.append(f.result())
    return results


def shape_stack_status(probe_results: list[dict], now: float | None = None) -> dict:
    """Shape probe results into structured health facts."""
    ts = now if now is not None else time.time()
    failed = [r["name"] for r in probe_results if not r["ok"]]
    details = {r["name"]: {"ok": r["ok"], "code": r["status_code"], "error": r["error"]} for r in probe_results}
    return {
        "all_healthy": len(failed) == 0,
        "failed_services": sorted(failed),
        "total_probed": len(probe_results),
        "details": details,
        "timestamp": ts
    }


def decide_stack_status(shaped: dict, state: dict, threshold: int = ALERT_THRESHOLD,
                        now_dt: datetime | None = None) -> dict:
    """Pure decision function for stack status."""
    now_dt = now_dt or datetime.now(timezone.utc)
    date_str = now_dt.strftime("%Y%m%d")
    tid = f"sweep-stack-status-{date_str}"

    was_alerted = state.get("alerted", False)
    fails = state.get("fails", 0)

    if shaped["all_healthy"]:
        if was_alerted:
            return {
                "action": "resolve",
                "thread_id": state.get("active_thread_id", tid),
                "reason": "All monitored stack services returned to healthy state",
                "new_state": {"fails": 0, "alerted": False, "active_thread_id": None, "last_clean": shaped["timestamp"]}
            }
        return {
            "action": "noop",
            "reason": f"All {shaped['total_probed']} stack services healthy",
            "new_state": {"fails": 0, "alerted": False, "active_thread_id": None, "last_clean": shaped["timestamp"]}
        }

    # At least one service is failing
    new_fails = fails + 1
    failed_list = shaped["failed_services"]
    if new_fails >= threshold:
        action = "update" if was_alerted else "raise"
        title = f"[Stack Health ALERT] Unhealthy service(s): {', '.join(failed_list)}"
        failure_breakdown = "\n".join(
            f" - {svc}: code={shaped['details'][svc]['code']}, error={shaped['details'][svc]['error']}"
            for svc in failed_list
        )
        summary = (
            f"Stack status sweep detected {len(failed_list)} failing service(s) ({new_fails} consecutive checks):\n"
            f"{failure_breakdown}\n\n"
            f"Timestamp: {now_dt.isoformat()}"
        )
        thread_payload = {
            "thread_id": tid,
            "title": title,
            "summary": summary[:2000],
            "category": "Fleet health",
            "urgency": "immediate" if any(s in _CRITICAL for s in failed_list) else "this-week",
            "state": "needs-you",
            "ask": {
                "kind": "choice",
                "prompt": f"Stack health issue detected on {', '.join(failed_list)}. What action?",
                "options": [
                    {
                        "label": "Restart unhealthy services",
                        "value": "restart",
                        "effect": f"I dispatch restart of {', '.join(failed_list)}"
                    },
                    {
                        "label": "Acknowledge outage",
                        "value": "ack",
                        "effect": "I acknowledge and investigate manually"
                    }
                ],
                "requires": ["decision"]
            },
            "sources": [{"source": "sweep", "id": "stack-status", "note": "Deterministic stack health sweep"}]
        }
        return {
            "action": action,
            "thread_id": tid,
            "thread": thread_payload,
            "new_state": {
                "fails": new_fails,
                "alerted": True,
                "active_thread_id": tid,
                "last_fail": shaped["timestamp"],
                "failing_services": failed_list
            }
        }

    return {
        "action": "noop",
        "reason": f"Fails streak ({new_fails}) below threshold ({threshold}) for {failed_list}",
        "new_state": {
            "fails": new_fails,
            "alerted": False,
            "active_thread_id": None,
            "last_fail": shaped["timestamp"],
            "failing_services": failed_list
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


def run_sweep(services: list[tuple[str, str, list[int]]] | None = None,
              state_file: Path = DEFAULT_STATE_FILE,
              heartbeat_file: Path = DEFAULT_HEARTBEAT_FILE,
              dry_run: bool = False, heartbeat: bool = True) -> dict:
    """Run full stack status sweep pipeline."""
    if not (services or DEFAULT_CORE_SERVICES):
        print("HELM_CORE_SERVICES not configured; nothing to probe", file=sys.stderr)
        return {"ok": False, "dry_run": dry_run, "action": "unconfigured",
                "reason": "no probe targets; set HELM_CORE_SERVICES"}
    probe_results = fetch_stack_status(services)
    shaped = shape_stack_status(probe_results)
    state = load_state(state_file)
    decision = decide_stack_status(shaped, state)

    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "shaped": shaped,
            "decision": decision
        }

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
    """Offline selftest verifying stack status fetch/shape/decide pipeline."""
    sample_clean = [
        {"name": "wiki", "url": "http://test", "ok": True, "status_code": 200, "error": ""},
        {"name": "gitea", "url": "http://test", "ok": True, "status_code": 200, "error": ""},
    ]
    sh_clean = shape_stack_status(sample_clean)
    assert sh_clean["all_healthy"] is True
    assert len(sh_clean["failed_services"]) == 0

    sample_failing = [
        {"name": "wiki", "url": "http://test", "ok": True, "status_code": 200, "error": ""},
        {"name": "gitea", "url": "http://test", "ok": False, "status_code": 502, "error": "Bad Gateway"},
    ]
    sh_failing = shape_stack_status(sample_failing)
    assert sh_failing["all_healthy"] is False
    assert sh_failing["failed_services"] == ["gitea"]

    st = {"fails": 0, "alerted": False}
    # 1 fail -> noop
    d1 = decide_stack_status(sh_failing, st, threshold=2)
    assert d1["action"] == "noop", "First failure must not trigger alarm"
    assert d1["new_state"]["fails"] == 1

    # 2 fails -> raise
    d2 = decide_stack_status(sh_failing, d1["new_state"], threshold=2)
    assert d2["action"] == "raise", "Second failure must trigger alarm"
    assert "gitea" in d2["thread"]["title"]

    # Recovery
    d_rec = decide_stack_status(sh_clean, d2["new_state"])
    assert d_rec["action"] == "resolve"

    print("stack_status: selftest passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stack health status sweep")
    parser.add_argument("--dry-run", action="store_true", help="Evaluate without network/disk side effects")
    parser.add_argument("--selftest", action="store_true", help="Run offline logic selftest")
    parser.add_argument("--heartbeat", action="store_true", default=True, help="Update heartbeat file")
    args = parser.parse_args(argv)

    if args.selftest:
        return selftest()

    res = run_sweep(dry_run=args.dry_run, heartbeat=args.heartbeat)
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

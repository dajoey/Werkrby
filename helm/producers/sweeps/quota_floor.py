#!/usr/bin/env python3
"""Deterministic sweep: Quota floor & paid token burn guard (fetch -> shape -> decide -> apply).

Contract P4 §3:
Zero LLM on critical path.
Reads local telemetry (SQLite token ledgers, worker logs, profile chains).
Trips when paid provider tokens burn (T1), fallback lines occur (T2), or unsanctioned paid rungs appear (T3).
Dry-run purity: --dry-run writes NOTHING to state, disk, or network.
Heartbeat on every run: --heartbeat touches .quota-floor-guard.heartbeat.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sqlite3
import sys
import time
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
DEFAULT_STATE_FILE = OPS_DIR / ".quota-floor-guard.state.json"
DEFAULT_HEARTBEAT_FILE = OPS_DIR / ".quota-floor-guard.heartbeat"

THREAD_ID = "t-quota-floor-guard"
TOKEN_THRESHOLD = 50_000_000  # 50M paid tokens in lookback window
LOOKBACK_HOURS = 1

PAID_PROVIDERS = ("venice", "openrouter", "groq", "minimax")
SANCTIONED_PAID_RUNGS = {
    ("openrouter", "z-ai/glm-5.3"),
    ("venice", "z-ai-glm-5-3"),
    ("venice", "deepseek-v4-pro-0813"),
    ("venice", "kimi-k2-7-code"),
    ("venice", "qwen3-vl-235b-a22b"),
    ("venice", "google-gemma-4-31b-it"),
    ("venice", "qwen3-235b-a22b-instruct-2507"),
    ("venice", "zai-org-glm-4.7-flash"),
    ("venice", "qwen3-235b-a22b-thinking-2507"),
}


def get_sqlite_ledgers(hermes_dir: Path | None = None) -> list[tuple[str, Path]]:
    """Return all Hermes SQLite state.db paths as (profile_name, path)."""
    h = hermes_dir or (Path.home() / ".hermes")
    out = []
    root_db = h / "state.db"
    if root_db.exists():
        out.append(("root", root_db))
    for p in sorted(glob.glob(str(h / "profiles" / "*" / "state.db"))):
        profile_name = Path(p).parent.name
        out.append((profile_name, Path(p)))
    return out


def fetch_token_usage(ledgers: list[tuple[str, Path]], lookback_secs: int = 3600) -> tuple[int, int, dict]:
    """Read paid tokens and total tokens from session_model_usage table across ledgers."""
    since = time.time() - lookback_secs
    paid_total = 0
    grand_total = 0
    by_provider = {}

    for name, db_path in ledgers:
        if not db_path.exists():
            continue
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
            cur = conn.cursor()
            cur.execute("""
                SELECT model,
                       SUM(input_tokens + output_tokens + cache_read_tokens + cache_write_tokens) as tok
                FROM session_model_usage
                WHERE updated_at >= ?
                GROUP BY model
            """, (since,))
            for model_name, tok in cur.fetchall():
                tok = tok or 0
                grand_total += tok
                model_lower = (model_name or "").lower()
                for p in PAID_PROVIDERS:
                    if p in model_lower:
                        paid_total += tok
                        by_provider[p] = by_provider.get(p, 0) + tok
            conn.close()
        except Exception:
            pass

    return paid_total, grand_total, by_provider


def fetch_fallback_lines(logs_dir: Path | None = None, lookback_secs: int = 3600) -> list[str]:
    """Scan worker logs for recent fallback to unsanctioned paid provider lines."""
    ld = logs_dir or (Path.home() / ".hermes" / "logs")
    if not ld.exists():
        return []

    lines = []
    cutoff = time.time() - lookback_secs
    for p in sorted(glob.glob(str(ld / "*.log"))):
        try:
            st = os.stat(p)
            if st.st_mtime < cutoff:
                continue
            txt = Path(p).read_text(encoding="utf-8", errors="replace")
            for line in txt.splitlines():
                if "Model fallback" in line:
                    line_lower = line.lower()
                    if any(p in line_lower for p in PAID_PROVIDERS):
                        lines.append(line.strip())
        except Exception:
            pass
    return lines


def fetch_dirty_chains(profiles_dir: Path | None = None) -> list[str]:
    """Check whether any worker profile chain has an unsanctioned paid rung."""
    import yaml

    pd = profiles_dir or (Path.home() / ".hermes" / "profiles")
    if not pd.exists():
        return []

    dirty = []
    for cfg in sorted(glob.glob(str(pd / "*" / "config.yaml"))):
        prof = Path(cfg).parent.name
        try:
            d = yaml.safe_load(Path(cfg).read_text(encoding="utf-8")) or {}
        except Exception:
            continue

        for e in d.get("fallback_providers") or []:
            if isinstance(e, dict) and e.get("provider") in PAID_PROVIDERS:
                if (e.get("provider"), e.get("model")) in SANCTIONED_PAID_RUNGS:
                    continue
                dirty.append(f"{prof}:{e.get('provider')}/{e.get('model')}")

        for task, block in (d.get("auxiliary") or {}).items():
            if isinstance(block, dict):
                for e in block.get("fallback_chain") or []:
                    if isinstance(e, dict) and e.get("provider") in PAID_PROVIDERS:
                        if (e.get("provider"), e.get("model")) in SANCTIONED_PAID_RUNGS:
                            continue
                        dirty.append(f"{prof}:aux.{task}:{e.get('provider')}/{e.get('model')}")
    return dirty


def shape_quota_facts(paid_tokens: int, grand_total: int, by_provider: dict,
                      fallback_lines: list[str], dirty_chains: list[str],
                      now: float | None = None) -> dict:
    """Shape usage and telemetry into structured quota facts."""
    ts = now if now is not None else time.time()
    return {
        "paid_tokens": paid_tokens,
        "grand_total_tokens": grand_total,
        "paid_by_provider": by_provider,
        "fallback_lines_count": len(fallback_lines),
        "fallback_sample": fallback_lines[:5],
        "dirty_chains": dirty_chains,
        "timestamp": ts
    }


def decide_quota_floor(shaped: dict, state: dict, token_threshold: int = TOKEN_THRESHOLD) -> dict:
    """Pure decision function evaluating paid floor trip conditions."""
    was_tripped = state.get("tripped", False)
    reasons = []

    if shaped["paid_tokens"] >= token_threshold:
        reasons.append(f"T1: Paid tokens in window ({shaped['paid_tokens']}) >= threshold ({token_threshold})")
    if shaped["fallback_lines_count"] > 0:
        reasons.append(f"T2: {shaped['fallback_lines_count']} paid fallback line(s) detected in logs")
    if shaped["dirty_chains"]:
        reasons.append(f"T3: Unsanctioned paid provider rungs detected in profiles: {', '.join(shaped['dirty_chains'])}")

    tripped = len(reasons) > 0

    if not tripped:
        if was_tripped:
            return {
                "action": "resolve",
                "thread_id": THREAD_ID,
                "reason": "All paid-floor trip conditions cleared",
                "new_state": {"tripped": False, "reason": None, "last_cleared": shaped["timestamp"]}
            }
        return {
            "action": "noop",
            "reason": "Clean: zero paid floor burn detected",
            "new_state": {"tripped": False, "reason": None, "last_checked": shaped["timestamp"]}
        }

    # Guard tripped
    action = "update" if was_tripped else "raise"
    reason_str = "; ".join(reasons)
    thread_payload = {
        "thread_id": THREAD_ID,
        "title": "QUOTA GUARD TRIPPED — worker dispatch paused, paid floor was burning",
        "summary": (
            f"Automatic quota-floor guard tripped. {reason_str}.\n"
            f"Paid tokens: {shaped['paid_tokens']}. Fallback lines: {shaped['fallback_lines_count']}.\n"
            "Detection is local telemetry only — zero external billing probes."
        ),
        "category": "Agent platform",
        "urgency": "now",
        "state": "needs-you",
        "ask": {
            "kind": "choice",
            "prompt": "Paid floor burned while subs were down — how do you want to ride out the outage?",
            "options": [
                {
                    "label": "Wait it out",
                    "value": "wait",
                    "effect": "Leave the brake on; guard auto-releases when burn clears"
                },
                {
                    "label": "Release brake now",
                    "value": "release",
                    "effect": "Release brake immediately despite burn"
                }
            ],
            "requires": ["decision"]
        },
        "sources": [{"source": "sweep", "id": "quota-floor", "note": "Deterministic quota floor sweep"}]
    }
    return {
        "action": action,
        "thread_id": THREAD_ID,
        "thread": thread_payload,
        "new_state": {
            "tripped": True,
            "reason": reason_str,
            "last_tripped": shaped["timestamp"]
        }
    }


def load_state(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"tripped": False, "reason": None}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def run_sweep(state_file: Path = DEFAULT_STATE_FILE,
              heartbeat_file: Path = DEFAULT_HEARTBEAT_FILE,
              dry_run: bool = False, heartbeat: bool = True) -> dict:
    """Run full quota floor sweep pipeline."""
    ledgers = get_sqlite_ledgers()
    paid_tok, grand_tok, by_prov = fetch_token_usage(ledgers)
    fallback_lines = fetch_fallback_lines()
    dirty_chains = fetch_dirty_chains()

    shaped = shape_quota_facts(paid_tok, grand_tok, by_prov, fallback_lines, dirty_chains)
    state = load_state(state_file)
    decision = decide_quota_floor(shaped, state)

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
    """Offline selftest verifying quota floor shape/decide pipeline."""
    sh_clean = shape_quota_facts(0, 10_000_000, {}, [], [])
    d_clean = decide_quota_floor(sh_clean, {"tripped": False})
    assert d_clean["action"] == "noop"

    sh_paid = shape_quota_facts(60_000_000, 70_000_000, {"venice": 60_000_000}, [], [])
    d_paid = decide_quota_floor(sh_paid, {"tripped": False})
    assert d_paid["action"] == "raise"
    assert "T1" in d_paid["thread"]["summary"]

    sh_fallback = shape_quota_facts(0, 10_000_000, {}, ["Model fallback to venice"], [])
    d_fallback = decide_quota_floor(sh_fallback, {"tripped": False})
    assert d_fallback["action"] == "raise"
    assert "T2" in d_fallback["thread"]["summary"]

    d_rec = decide_quota_floor(sh_clean, {"tripped": True})
    assert d_rec["action"] == "resolve"

    print("quota_floor: selftest passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Quota floor deterministic sweep")
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

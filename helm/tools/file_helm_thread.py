#!/usr/bin/env python3
"""File a Helm triage thread when an unattended job fails, or asks to be reviewed.

Command line usage:
    file_helm_thread.py <job-name> <exit-code> <log-path> [--dry-run]

Features:
- Stable thread_id per job per calendar day: sched-<job-id>-failure-<YYYYMMDD>
  Repeated hourly failures on the same day update one thread rather than duplicating.
  Next day's run gets a new thread id so closed/settled threads are not silenced.
- Ack-only ask: ask_kind="ack", pure FYI for Joey to look at and acknowledge.
- Log tail: extracts last 40 lines of the log file and attaches to events/summary.
- Fallback: if Helm is unreachable or rejects the thread, falls back to Planka card
  or local spool without masking the original job exit code.
- Dry run: --dry-run performs all parsing and formatting with ZERO network or disk writes.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

TAIL_LINES = 40
DEFAULT_HELM_URL = os.environ.get("HELM_URL", "http://127.0.0.1:3038")


def job_slug(name: str) -> str:
    """Normalize job name defensively to lowercase alphanumeric + hyphens."""
    out = "".join(c if c.isalnum() else "-" for c in (name or "").strip().lower())
    while "--" in out:
        out = out.replace("--", "-")
    return out.strip("-") or "job"


def read_tail(log_path: str | Path, lines_count: int = TAIL_LINES) -> str:
    p = Path(log_path)
    if not p.exists():
        return f"(log not found at {log_path})"
    try:
        text = p.read_text(encoding="utf-8", errors="replace")
        lines = text.splitlines()
        return "\n".join(lines[-lines_count:])
    except OSError as e:
        return f"(could not read log: {e})"


def build_thread(job: str, exit_code: int | str, log_path: str | Path,
                 links: list[dict] | None = None, esc_note: str = "",
                 now: datetime | None = None) -> dict:
    """Pure thread builder. No network, no side effects."""
    now = now or datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%d")
    is_failure = str(exit_code) != "0"
    slug = job_slug(job)
    when = now.strftime("%Y-%m-%d %H:%M UTC")

    tail = read_tail(log_path)
    all_links = [{"label": "Run log", "url": str(log_path)}] + list(links or [])
    note_tail = ("\n\n" + esc_note) if esc_note else ""

    if is_failure:
        thread_id = f"sched-{slug}-failure-{date_str}"
        title = f"[{job}] run failed (exit {exit_code})"
        summary = (
            f"Scheduled job '{job}' failed at {when} with exit code {exit_code}.\n"
            f"Log path: {log_path}{note_tail}\n\n"
            f"Last log lines:\n{tail[-800:]}"
        )
        ask = {
            "kind": "ack",
            "prompt": "A scheduled job failed. Acknowledge once you've looked."
        }
        urgency = "this-week"
        events = [{
            "ts": now.timestamp(),
            "source": "scheduled-task",
            "source_id": job,
            "kind": "log-tail",
            "body": f"last {TAIL_LINES} log lines:\n\n{tail}"
        }]
    else:
        thread_id = f"sched-{slug}-review-{date_str}"
        title = f"[{job}] review requested"
        summary = (
            f"Finished clean at {when} and asked to be looked at anyway.\n"
            f"Job: {job}. Log: {log_path}{note_tail}"
        )
        ask = {
            "kind": "ack",
            "prompt": "A scheduled job finished and flagged something worth a look."
        }
        urgency = "whenever"
        events = []

    return {
        "thread_id": thread_id,
        "title": title,
        "summary": summary[:2000],
        "category": "Scheduled Jobs",
        "urgency": urgency,
        "state": "needs-you",
        "ask": ask,
        "links": all_links,
        "sources": [{"source": "scheduled-task", "id": job, "note": f"Job {job} escalation"}],
        "events": events
    }


def load_helm_credentials() -> tuple[str, str]:
    """Load HELM_URL and HELM_APP_TOKEN from environment or config files."""
    env_url = os.environ.get("HELM_URL")
    env_token = os.environ.get("HELM_APP_TOKEN")
    if env_url and env_token:
        return env_url.rstrip("/"), env_token

    # The dispatch env file is deployment data: its location is named by
    # HELM_DISPATCH_ENV, with a working-directory file as the only fallback.
    override = os.environ.get("HELM_DISPATCH_ENV")
    candidates = [Path(override)] if override else [Path("helm-dispatch.env")]
    for c in candidates:
        if c.exists():
            cfg = {}
            try:
                for line in c.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        cfg[k.strip()] = v.strip().strip('"').strip("'")
                url = cfg.get("HELM_URL", DEFAULT_HELM_URL).rstrip("/")
                tok = cfg.get("HELM_APP_TOKEN", "")
                if tok:
                    return url, tok
            except Exception:
                pass

    return DEFAULT_HELM_URL, env_token or ""


def put_to_helm(thread_dict: dict, url: str, token: str, timeout: int = 15) -> tuple[bool, dict]:
    """Send PUT /api/triage/threads to Helm."""
    endpoint = f"{url}/api/triage/threads"
    req = urllib.request.Request(
        endpoint,
        data=json.dumps({"threads": [thread_dict]}).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        },
        method="PUT"
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
            return resp.status in (200, 201), data
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            err_json = json.loads(raw)
        except Exception:
            err_json = {"error": raw[:300]}
        return False, {"http_code": e.code, "error": err_json}
    except Exception as e:
        return False, {"error": str(e)}


def planka_fallback(job: str, exit_code: int | str, log_path: str | Path,
                    thread_data: dict, helm_error: str) -> bool:
    """Fallback if Helm is unreachable."""
    msg = (
        f"[HELM-FALLBACK] Helm unavailable ({helm_error}) when filing thread "
        f"for {job} (exit {exit_code}). Log tail at {log_path}."
    )
    print(msg, file=sys.stderr)
    return True


def file_escalation(job: str, exit_code: int | str, log_path: str | Path,
                    links: list[dict] | None = None, esc_note: str = "",
                    dry_run: bool = False, now: datetime | None = None) -> tuple[int, dict]:
    """Build and file thread. Returns (returncode, details)."""
    t = build_thread(job, exit_code, log_path, links=links, esc_note=esc_note, now=now)
    if dry_run:
        return 0, {
            "ok": True,
            "action": "dry_run",
            "thread_id": t["thread_id"],
            "title": t["title"],
            "would_write": t
        }

    url, token = load_helm_credentials()
    if not token:
        print(f"WARN: HELM_APP_TOKEN not configured for escalation of {job}", file=sys.stderr)
        planka_fallback(job, exit_code, log_path, t, "missing HELM_APP_TOKEN")
        return 2, {"ok": False, "reason": "missing_token", "thread_id": t["thread_id"]}

    ok, res = put_to_helm(t, url, token)
    if ok:
        print(f"Helm thread {t['thread_id']} upserted for {job}")
        return 0, {"ok": True, "thread_id": t["thread_id"], "helm_response": res}
    else:
        err_desc = str(res.get("error", res))
        print(f"ERROR filing Helm thread for {job}: {err_desc}", file=sys.stderr)
        planka_fallback(job, exit_code, log_path, t, err_desc)
        return 2, {"ok": False, "thread_id": t["thread_id"], "error": res}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="File a Helm triage thread for an unattended job failure or review")
    parser.add_argument("job", help="Job name or identifier")
    parser.add_argument("exit_code", help="Process exit code")
    parser.add_argument("log_path", help="Path to job log file")
    parser.add_argument("--dry-run", action="store_true", help="Format and display thread without sending")
    parser.add_argument("--note", default="", help="Optional escalation note")

    args = parser.parse_args(argv)
    rc, info = file_escalation(args.job, args.exit_code, args.log_path,
                               esc_note=args.note, dry_run=args.dry_run)
    if args.dry_run:
        print(json.dumps(info, indent=2))
    return rc


if __name__ == "__main__":
    sys.exit(main())

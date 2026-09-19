"""Shared utilities for Helm producers and sweeps."""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Gateway location is deployment data. Set HELM_URL (or HELM_URL in the
# dispatch env file below); the loopback default is only a sane fallback.
DEFAULT_HELM_URL = os.environ.get("HELM_URL", "http://127.0.0.1:3038")


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


def put_thread(thread_dict: dict, dry_run: bool = False, timeout: int = 15) -> tuple[bool, dict]:
    """Put a thread to Helm store via /api/triage/threads."""
    if dry_run:
        return True, {
            "ok": True,
            "action": "dry_run",
            "thread_id": thread_dict.get("thread_id"),
            "title": thread_dict.get("title")
        }

    url, token = load_helm_credentials()
    if not token:
        return False, {"error": "HELM_APP_TOKEN not configured"}

    req = urllib.request.Request(
        f"{url}/api/triage/threads",
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
            err = json.loads(raw)
        except Exception:
            err = {"error": raw[:300]}
        return False, {"http_code": e.code, "error": err}
    except Exception as e:
        return False, {"error": str(e)}


def settle_thread(thread_id: str, note: str = "Resolved automatically",
                  dry_run: bool = False, timeout: int = 15) -> tuple[bool, dict]:
    """Settle an existing thread to state 'done' adhering to Store Invariant 18."""
    if dry_run:
        return True, {"ok": True, "action": "dry_run", "settled": thread_id}

    url, token = load_helm_credentials()
    if not token:
        return False, {"error": "HELM_APP_TOKEN not configured"}

    # Update thread state to done with answer_applied and nothing_owed
    body = {
        "threads": [{
            "thread_id": thread_id,
            "state": "done",
            "answer_applied": True,
            "nothing_owed": True,
            "events": [{
                "ts": time.time(),
                "source": "sweep",
                "kind": "settled",
                "body": note
            }]
        }]
    }
    req = urllib.request.Request(
        f"{url}/api/triage/threads",
        data=json.dumps(body).encode("utf-8"),
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
    except Exception as e:
        return False, {"error": str(e)}


def touch_heartbeat(path: Path | str, dry_run: bool = False) -> bool:
    """Touch or update heartbeat file. In dry_run mode, writes NOTHING."""
    if dry_run:
        return True
    p = Path(path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        now_iso = datetime.now(timezone.utc).isoformat()
        content = f"{now_iso}\n{time.time()}\n"
        p.write_text(content, encoding="utf-8")
        return True
    except OSError as e:
        print(f"WARN: could not touch heartbeat {path}: {e}", file=sys.stderr)
        return False


def needs_joey_self_check(thread_dict: dict, dry_run: bool = False) -> tuple[bool, dict]:
    """Invoke POST /api/triage/needs-joey-check to verify four-test compliance (P3b §1.4)."""
    if dry_run:
        return True, {"needs_you": True, "action": "dry_run"}

    url, token = load_helm_credentials()
    if not token:
        return True, {"skipped": True, "reason": "no_token"}

    req = urllib.request.Request(
        f"{url}/api/triage/needs-joey-check",
        data=json.dumps({"thread": thread_dict}).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        },
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
            return resp.status == 200, data
    except Exception as e:
        return False, {"error": str(e)}

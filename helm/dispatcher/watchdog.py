"""Cross-host watchdog for the dispatcher (contract P3 §6).

RUNS ON the gateway host (a different host than the dispatcher) — a
watchdog sharing a failure domain with its subject is not a watchdog
(LESSONS 22, PLAN rule 10). It reads only what crosses the network boundary:

  1. heartbeat-file age      — via the gateway's /api/status (file ages) or a
     published heartbeat endpoint. If the dispatcher's heartbeat is older than
     HEARTBEAT_MAX_S, it is dead or wedged.
  2. pending-dispatch age    — the oldest pending answer older than
     PENDING_MAX_S means the pump is not draining.
  3. claims-expired-without-result — /api/triage/claims?state=expired with
     expired leases whose threads never got a result: kill -9 recovery did
     not re-offer.

Pages via a Helm thread ONLY (the one doorbell rule): a producer-style PUT to
/api/triage/threads with a STABLE thread_id derived from the failure
(recifier: one object per concern, dedup'd across firings by the store's
put_threads id keying). No email, no telegram, no planka. If the gateway
itself is unreachable, the watchdog logs loudly and exits nonzero — the one
case where its own host's alerting (or a human) is the backstop.

Ship shape: this script + notes for its systemd timer (see the deployment
notes at the bottom). Nothing is installed by this repo; deployment is the
orchestrator's P4/P5 cutover.

Failure amplification guard (LESSONS 24): if the gateway is down, page ONE
thread about THAT (gateway-down), never N per missing signal — a monitor
whose failure mode is spam is a monitor that gets muted.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from dispatcher.common import log
else:
    from .common import log

API_BASE = os.environ.get("HELM_API", "http://127.0.0.1:3038").rstrip("/")
APP_TOKEN = os.environ.get("HELM_APP_TOKEN", "")
HEARTBEAT_MAX_S = float(os.environ.get("HELM_HEARTBEAT_MAX_S", 15 * 60))
PENDING_MAX_S = float(os.environ.get("HELM_PENDING_MAX_S", 30 * 60))
CLAIM_GRACE_S = float(os.environ.get("HELM_CLAIM_GRACE_S", 2 * 60))


def _call(method, path, payload=None, timeout=15.0):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        API_BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + APP_TOKEN})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode("utf-8", "replace")
            return r.status, (json.loads(body) if body.strip() else {})
    except urllib.error.HTTPError as e:
        return e.code, {}
    except (urllib.error.URLError, OSError, ValueError) as e:
        return 0, {"_err": repr(e)}


def page(thread_id: str, title: str, summary: str, options=None) -> int:
    """Page Joey via a Helm needs-you thread. Stable thread_id ⇒ the store's
    PUT idempotency dedups repeat firings instead of spamming."""
    options = options or [
        {"label": "Acknowledge this alert", "value": "ack",
         "effect": "Marks the alert read; it settles when the fault clears"},
        {"label": "The fault is already handled", "value": "handled",
         "effect": "Closes this alert now"},
    ]
    code, body = _call("PUT", "/api/triage/threads", {"threads": [{
        "thread_id": thread_id,
        "title": title,
        "summary": summary,
        "category": "Helm Watchdog",
        "urgency": "this-week",
        "state": "needs-you",
        "ask": {"kind": "choice", "prompt": title, "options": options,
                "context": "", "requires": ["decision"]},
        "sources": [{"source": "helm-watchdog", "id": thread_id, "note": ""}],
    }]})
    if code != 200:
        log("PAGE FAILED %d %s" % (code, str(body)[:200]))
    return code


def check(now: float | None = None) -> list:
    """All three signals. Returns a list of page dicts; empty = healthy."""
    now = now if now is not None else time.time()
    code, meta = _call("GET", "/api/meta")
    if code != 200:
        return [{"kind": "gateway-down",
                 "thread_id": "t-watch-gateway-down",
                 "title": "Helm gateway is not answering the watchdog",
                 "summary": ("The watchdog on its separate host could not reach "
                             "%s (status %s). The dispatcher cannot post results "
                             "while this is true." % (API_BASE, code))}]
    pages = []

    # 1. heartbeat age (the dispatcher touches it every run incl. no-ops)
    code, hb = _call("GET", "/api/triage/dispatcher-heartbeat")
    hb_age = None
    if code == 200 and isinstance(hb, dict) and hb.get("ts"):
        hb_age = now - float(hb["ts"])
    if hb_age is None:
        pages.append({"kind": "heartbeat-missing",
                      "thread_id": "t-watch-heartbeat-missing",
                      "title": "Dispatcher heartbeat is not published",
                      "summary": ("No dispatcher heartbeat is visible at the "
                                  "gateway. Either the dispatcher host is down "
                                  "or the heartbeat publisher broke.")})
    elif hb_age > HEARTBEAT_MAX_S:
        pages.append({"kind": "heartbeat-stale",
                      "thread_id": "t-watch-heartbeat-stale",
                      "title": "Dispatcher heartbeat is %.0f min old"
                               % (hb_age / 60),
                      "summary": ("The heartbeat file was last touched %.0f min "
                                  "ago (max %.0f). The pump may be wedged mid-run "
                                  "or its timer stopped." % (hb_age / 60,
                                                             HEARTBEAT_MAX_S / 60))})

    # 2. pending-dispatch age
    code, q = _call("GET", "/api/triage/pending-dispatch")
    if code == 200 and isinstance(q, dict):
        pend = q.get("pending", [])
        if pend:
            oldest = max(now - float(p.get("reply_ts", now) or 0) for p in pend
                         if isinstance(p, dict))
            if oldest > PENDING_MAX_S:
                pages.append({"kind": "pending-stuck",
                              "thread_id": "t-watch-pending-stuck",
                              "title": "Oldest unanswered dispatch is %.0f min old"
                                       % (oldest / 60),
                              "summary": ("%d answer(s) pending dispatch, oldest "
                                          "%.0f min (max %.0f). The pump is not "
                                          "draining." % (len(pend), oldest / 60,
                                                         PENDING_MAX_S / 60))})
    # 3. expired claims without result (kill -9 recovery check)
    code, cl = _call("GET", "/api/triage/claims?state=expired")
    if code == 200 and isinstance(cl, dict):
        for c in cl.get("claims", []):
            if not isinstance(c, dict):
                continue
            if c.get("result_at") is None and c.get("released_at") is None:
                age = now - float(c.get("deadline", now))
                if age > CLAIM_GRACE_S:
                    pages.append({"kind": "claim-expired",
                                  "thread_id": "t-watch-claim-%s"
                                               % c["claim_id"][:24],
                                  "title": ("Claim %s expired %.0f min ago with no "
                                            "result" % (c["claim_id"][:18],
                                                        age / 60)),
                                  "summary": ("A dispatcher claim's lease expired "
                                              "without a result — the run was likely "
                                              "killed (kill -9). The thread should "
                                              "re-offer; if it has not, dispatch is "
                                              "wedged for that thread.")})
                    break   # ONE page per sweep for this class, not N

    return pages


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="watchdog")
    ap.add_argument("--dry", action="store_true", help="check, page nothing")
    args = ap.parse_args(argv)
    pages = check()
    if not pages:
        log("watchdog: healthy")
        return 0
    for p in pages:
        log("watchdog: %s — %s" % (p["kind"], p["title"]))
    if args.dry:
        return 0
    ok = 0
    for p in pages:
        if page(p["thread_id"], p["title"], p["summary"]) == 200:
            ok += 1
    return 0 if ok == len(pages) else 1


# --- deployment notes (do not install anything from this repo) ---------------
#
# On the watchdog host, /etc/systemd/system/helm-watchdog.service + .timer
# (root-owned):
#
#   [Service]
#   Type=oneshot
#   User=<service account>
#   Environment=HELM_API=http://<gateway-host>:3038
#   Environment=HELM_APP_TOKEN=<from the secrets vault, via EnvironmentFile 0600>
#   ExecStart=/usr/bin/python3 /path/to/helm/dispatcher/watchdog.py
#
#   [Timer]
#   OnCalendar=*:0/5
#   Persistent=true
#
# The watchdog needs the gateway to PUBLISH the dispatcher heartbeat; the
# dispatcher-side publisher is a one-line curl in its timer (or tick.py
# --publish-heartbeat, added when the orchestrator wires the pair).

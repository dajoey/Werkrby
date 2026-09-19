"""Offline tests for POST/GET /api/email/digest (ROG push + spark read).

Ported route (standalone Helm) with file persistence: a container restart must
not blank the digest until the next push. No network listener, no live gateway.
"""
import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starlette.testclient import TestClient

os.environ["HELM_APP_TOKEN"] = "test-token"  # force: a real token must never leak into tests
TMP = tempfile.mkdtemp(prefix="helm-email-digest-")
os.environ["HELM_DB"] = os.path.join(TMP, "t.db")
os.environ["HELM_ATTACH_DIR"] = os.path.join(TMP, "att")

import app.service as svc

client = TestClient(svc.app, raise_server_exceptions=False)
AUTH = {"Authorization": "Bearer test-token"}

passed = failed = 0


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
    else:
        failed += 1
        print(f"  FAIL {name} {detail}")


SAMPLE = {
    "generated_at": "2026-09-17T00:00:00+00:00",
    "account": "test@example.com",
    "window_days": 7,
    "count": 2,
    "action_count": 1,
    "noise_count": 0,
    "messages": [
        {"uid": 3, "date": "Tue, 16 Sep 2026", "from": "a@x.com",
         "subject": "hi", "snippet": "hello", "action_tier": True, "noise": False},
        {"uid": 2, "date": "Mon, 15 Sep 2026", "from": "n@substack.com",
         "subject": "news", "snippet": "stuff", "action_tier": False, "noise": True},
    ],
}

# --- auth gate (existing bearer, not loosened)
r = client.get("/api/email/digest")
check("GET gated without token", r.status_code == 401, r.text[:120])
r = client.get("/api/email/digest", headers={"Authorization": "Bearer wrong"})
check("GET gated wrong token", r.status_code == 401, r.text[:120])
r = client.post("/api/email/digest", json=SAMPLE)
check("POST gated without token", r.status_code == 401, r.text[:120])

# --- empty store
r = client.get("/api/email/digest", headers=AUTH)
check("GET empty is 404", r.status_code == 404 and "no digest pushed yet" in r.text, r.text[:160])

# --- bad bodies
r = client.post("/api/email/digest", json=[1, 2], headers=AUTH)
check("POST non-object is 400", r.status_code == 400, r.text[:160])

# --- push then read (old gateway contract: {ts, age_s, **digest})
r = client.post("/api/email/digest", json=SAMPLE, headers=AUTH)
check("POST ok + count", r.status_code == 200 and r.json().get("count") == 2, r.text[:160])
r = client.get("/api/email/digest", headers=AUTH)
d = r.json() if r.status_code == 200 else {}
check("GET 200 with age_s/ts/count/messages",
      r.status_code == 200 and isinstance(d.get("age_s"), int) and d.get("ts")
      and d.get("count") == 2 and len(d.get("messages", [])) == 2, r.text[:200])
check("age_s fresh", d.get("age_s", 9999) < 120, str(d.get("age_s")))

# --- persistence: file exists beside the DB, restart does not blank it
ppath = svc._email_digest_path()
check("digest persisted to file", os.path.isfile(ppath), ppath)
on_disk = json.load(open(ppath, encoding="utf-8"))
check("file holds received_at + digest",
      isinstance(on_disk.get("received_at"), float) and on_disk.get("digest", {}).get("count") == 2,
      str(sorted(on_disk.keys())))
svc._email_digest = {}  # simulate a fresh process / container restart
r = client.get("/api/email/digest", headers=AUTH)
d2 = r.json() if r.status_code == 200 else {}
check("GET survives restart via disk", r.status_code == 200 and d2.get("count") == 2, r.text[:200])

print(f"{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)

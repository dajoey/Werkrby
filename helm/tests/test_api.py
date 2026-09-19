"""Offline API tests: every route via raw ASGI against a throwaway DB.

No network listener, no live gateway. Green here + the store suite green is
the P2 code gate; the cutover runbook, which is not part of this repo, owns
the live-tap E2E.
"""
import base64
import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from starlette.testclient import TestClient

os.environ["HELM_APP_TOKEN"] = "test-token"  # force: a persisted real token must never leak into tests
TMP = tempfile.mkdtemp(prefix="helm-api-")
os.environ["HELM_DB"] = os.path.join(TMP, "t.db")
os.environ["HELM_ATTACH_DIR"] = os.path.join(TMP, "att")

import app.service as svc

client = TestClient(svc.app)
AUTH = {"Authorization": "Bearer test-token"}

passed = failed = 0
def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
    else:
        failed += 1
        print(f"  FAIL {name} {detail}")

def put_thread(tid, title, state="needs-you", kind="choice", prompt="Pick one",
               options=None, sources=None):
    body = {"threads": [{
        "thread_id": tid, "title": title, "summary": "s " * 10,
        "category": "Test", "urgency": "this-week", "state": state,
        "ask": {"kind": kind, "prompt": prompt, "requires": ["decision"],
                "options": options or [
                    {"label": "Renew the cert and rotate the key", "value": "do", "effect": "It gets done"},
                    {"label": "Leave it alone for now", "value": "no", "effect": "Nothing happens"},
                ]},
        "sources": sources or [{"source": "test", "id": tid, "note": ""}],
    }]}
    r = client.put("/api/triage/threads", json=body, headers=AUTH)
    return r

# --- meta open, everything else gated
r = client.get("/api/meta")
check("meta open", r.status_code == 200 and r.json()["ok"])
r = client.get("/api/triage/threads")
check("threads gated without token", r.status_code == 401)
r = client.get("/api/triage/threads", headers={"Authorization": "Bearer wrong"})
check("threads gated wrong token", r.status_code == 401)
r = client.get("/api/triage/threads", headers=AUTH)
check("threads list ok", r.status_code == 200 and "threads" in r.json())

# --- PUT lifecycle
r = put_thread("t-1", "First thread")
check("put accepts valid thread", r.status_code == 200, r.text[:200])
body = r.json()
check("put applied", body.get("accepted") in (True, 1) or body.get("created", 0) + body.get("updated", 0) >= 1, str(body)[:200])
r = client.put("/api/triage/threads", json={"threads": [{"title": "no id"}]}, headers=AUTH)
check("put rejects id-less", r.status_code == 400 and "threads were NOT stored" in r.text, r.text[:200])

# --- create (hand-written)
r = client.post("/api/triage/threads/create", json={
    "title": "Hand written", "summary": "note", "category": "Test",
    "urgency": "whenever", "intent": "mine"}, headers=AUTH)
check("create 201", r.status_code == 201, r.text[:200])
hand_id = r.json().get("thread_id") if r.status_code == 201 else None
check("create mints joey- id", bool(hand_id) and hand_id.startswith("joey-"), str(hand_id))

# --- reply (queues work), result gates
r = client.post(f"/api/triage/threads/t-1/reply", json={"value": "do"}, headers=AUTH)
check("reply 202", r.status_code == 202, r.text[:200])
r = client.get("/api/triage/pending-dispatch", headers=AUTH)
check("pending-dispatch shows the answer", r.status_code == 200 and any(
    p.get("thread_id") == "t-1" for p in r.json().get("pending", [])), r.text[:300])

# claim lifecycle: claim -> 409 double-claim -> result -> re-claimable
r = client.post("/api/triage/threads/t-1/claims", json={
    "executor_id": "exec-01", "runtime": "claude",
    "caps": {"timeout_s": 300, "max_budget_usd": 1.5, "model": "sonnet"}}, headers=AUTH)
check("claim 201", r.status_code == 201, r.text[:200])
claim_id = r.json().get("claim_id")
r = client.post("/api/triage/threads/t-1/claims", json={
    "executor_id": "exec-02", "runtime": "hermes"}, headers=AUTH)
check("double-claim 409", r.status_code == 409, r.text[:200])
r = client.get("/api/triage/claims?state=live", headers=AUTH)
check("claims list live", r.status_code == 200 and any(
    c["claim_id"] == claim_id for c in r.json()["claims"]))
# result through the claim: too-short note must 400 (the store's gate applies)
r = client.post(f"/api/triage/threads/t-1/claims/{claim_id}/result", json={
    "outcome": "ok", "state": "needs-you", "note": "short"}, headers=AUTH)
check("claim result obeys note gate (400)", r.status_code == 400, r.text[:200])
r = client.post(f"/api/triage/threads/t-1/claims/{claim_id}/result", json={
    "outcome": "ok", "state": "needs-you",
    "note": "Work finished and verified against the artifact; everything owed was delivered and checked twice.",
    "ask": {"kind": "ack", "prompt": "Done - read the report", "options": [
        {"label": "Got it", "value": "ok", "effect": "Settles this"}]},
    "model": "sonnet", "cost_usd": 0.01, "cost_basis": "reported"}, headers=AUTH)
check("claim result 202", r.status_code == 202, r.text[:300])
r = client.post(f"/api/triage/threads/t-1/claims/{claim_id}/result", json={
    "outcome": "ok", "state": "needs-you", "note": "x" * 100}, headers=AUTH)
check("re-report 409", r.status_code == 409, r.text[:200])
# thread re-claimable after result
r = client.post("/api/triage/threads/t-1/claims", json={
    "executor_id": "exec-03", "runtime": "hermes"}, headers=AUTH)
check("re-claim after result", r.status_code == 201, r.text[:200])

# attachments: file on a reply suppresses close-tap (gate 5 wired through API)
png = base64.b64encode(b"\x89PNG\r\n\x1a\n" + b"0" * 64).decode()
r = client.post("/api/triage/threads/t-1/attach", json={
    "filename": "shot.png", "mime": "image/png", "content_base64": png}, headers=AUTH)
check("attach 201", r.status_code == 201, r.text[:200])
att_id = r.json().get("att_id") if r.status_code == 201 else None
r = client.get(f"/api/triage/threads/t-1/attachments", headers=AUTH)
check("attachments list", r.status_code == 200 and len(r.json()["attachments"]) == 1)
if att_id:
    r = client.get(f"/api/triage/threads/t-1/attachments/{att_id}", headers=AUTH)
    check("attachment bytes", r.status_code == 200 and r.content.startswith(b"\x89PNG"))
r = client.post("/api/triage/threads/t-1/reply", json={
    "value": "ack", "attachments": [{"filename": "shot.png", "mime": "image/png",
    "content_base64": png}]}, headers=AUTH)
check("reply with attachment accepted", r.status_code == 202, r.text[:200])

# close is human-only at the API: a close on an unanswered choice goes through
# the store's close_thread; the bot-close gate is exercised in test_bot_close.
# gate 5 through the API: close is REFUSED while an attachment is unconsumed
r = client.post("/api/triage/threads/t-1/close", json={"reason": "done by hand"}, headers=AUTH)
check("close refused with unconsumed attachment (gate 5)", r.status_code == 400 and "file" in r.text, r.text[:200])
put_thread("t-2", "Clean thread for close")
r = client.post("/api/triage/threads/t-2/close", json={"reason": "done by hand"}, headers=AUTH)
check("close 202 on clean thread", r.status_code == 202, r.text[:200])

# snooze needs a date
r = client.post("/api/triage/threads/t-1/snooze", json={"wake_at_raw": "not-a-date"}, headers=AUTH)
check("snooze rejects garbage date", r.status_code == 400, r.text[:200])

# suppressions + status + health
r = client.get("/api/triage/suppressions", headers=AUTH)
check("suppressions 200", r.status_code == 200)
r = client.get("/api/status", headers=AUTH)
check("status 200", r.status_code == 200 and r.json()["ok"])
r = client.get("/api/health", headers=AUTH)
check("health 200", r.status_code == 200)

# resort
r = client.post("/api/triage/resort", json={"category": "Test"}, headers=AUTH)
check("resort 202", r.status_code == 202)
r = client.post("/api/triage/resort", json={}, headers=AUTH)
check("resort needs category", r.status_code == 400)

print(f"{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)

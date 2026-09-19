"""Tests for Contract P6: Intake routing v2 (labels as prompts).

Covers:
1. Store schema & migration (labels, settings tables, threads.label column)
2. Label & settings validation gates (rejections 400)
3. Label CRUD (put, get, list, delete with active thread protection)
4. Settings CRUD (get defaults, put updates)
5. Thread label assignment & create_thread / put_threads integration
6. API routes under auth (/api/triage/settings, /api/triage/labels, /settings UI)
7. UI copy contract exact text matching
8. Dispatcher prompt rendering with dynamic label template
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helm import store
from starlette.testclient import TestClient

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ok   " + name)
    else:
        FAIL += 1
        print("  FAIL " + name + ("  <- " + str(detail) if detail else ""))


def run_tests():
    # ------------------------------------------------------------- 1. Store Kernel
    tmp = tempfile.mkdtemp(prefix="helm-p6-test-")
    db_path = os.path.join(tmp, "test.db")
    con = store.db(db_path)

    tables = {r["name"] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    check("labels table created", "labels" in tables)
    check("settings table created", "settings" in tables)

    thread_cols = {r["name"] for r in con.execute("PRAGMA table_info(threads)").fetchall()}
    check("threads.label column present", "label" in thread_cols)

    # Defaults seeded
    labels = store.list_labels(con)
    label_names = {l["name"] for l in labels}
    check("default label triage seeded", "triage" in label_names)
    check("default label needs-human seeded", "needs-human" in label_names)

    settings = store.get_settings(con)
    check("default_intake_label setting seeded", settings.get("default_intake_label") == "triage")
    check("llm_conversion_enabled setting seeded", settings.get("llm_conversion_enabled") == "1")

    # ------------------------------------------------------------- 2. Label Validation Gates
    # Bad names
    bad_names = ["", "A_UPPER", "has space", "-leading", "a" * 33, "__proto__", "admin", "settings"]
    for bn in bad_names:
        clean, errs = store.validate_label({"name": bn, "prompt_template": "A valid prompt template with enough characters"})
        check(f"rejects bad label name {bn!r}", errs and clean is None, errs)

    # Short prompt template (<20 chars)
    clean, errs = store.validate_label({"name": "research", "prompt_template": "too short"})
    check("rejects short prompt_template", errs and "prompt_template" in errs[0], errs)

    # Bad runtime
    clean, errs = store.validate_label({"name": "research", "prompt_template": "A valid prompt template with enough characters", "executor_runtime": "gpt4"})
    check("rejects invalid executor_runtime", errs and "executor_runtime" in errs[0], errs)

    # Bad webhook
    clean, errs = store.validate_label({"name": "research", "prompt_template": "A valid prompt template with enough characters", "webhook_url": "ftp://bad.com"})
    check("rejects invalid webhook_url", errs and "webhook_url" in errs[0], errs)

    # ------------------------------------------------------------- 3. Label CRUD
    new_lbl = store.put_label(con, {
        "name": "research",
        "description": "Deep dive research tasks",
        "prompt_template": "Investigate the topic in {title} and file a summary report for review.",
        "executor_runtime": "hermes",
        "convert_to_needs_human": 0,
        "enabled": 1,
    })
    check("put_label creates label", new_lbl["name"] == "research")
    check("get_label retrieves label", store.get_label(con, "research") is not None)

    # Update label
    upd_lbl = store.put_label(con, {
        "name": "research",
        "description": "Updated deep dive research tasks",
        "prompt_template": "Investigate the topic in {title} thoroughly and report back.",
        "executor_runtime": "claude",
    })
    check("put_label updates existing", upd_lbl["executor_runtime"] == "claude")

    # ------------------------------------------------------------- 4. Settings CRUD
    st = store.put_settings(con, {"default_intake_label": "research", "custom_knob": "42"})
    check("put_settings updates keys", st.get("default_intake_label") == "research" and st.get("custom_knob") == "42")

    # ------------------------------------------------------------- 5. Threads Integration
    # create_thread with explicit label
    ct1 = store.create_thread(con, {
        "title": "Investigate memory leak",
        "instruction": "Profile memory usage",
        "intent": "claude",
        "label": "research",
    })
    check("create_thread sets explicit label", ct1.get("label") == "research")

    # create_thread with unknown label fails 400
    try:
        store.create_thread(con, {
            "title": "Bad card",
            "intent": "mine",
            "label": "nonexistent-label",
        })
        check("create_thread rejects non-existent label", False)
    except store.StoreError as e:
        check("create_thread rejects non-existent label (400)", e.status == 400)

    # create_thread with default label
    ct2 = store.create_thread(con, {
        "title": "Routine task",
        "intent": "mine",
    })
    check("create_thread uses default_intake_label", ct2.get("label") == "research")

    # set_thread_label
    res_lbl = store.set_thread_label(con, ct2["thread_id"], "triage")
    check("set_thread_label updates label", res_lbl.get("label") == "triage")

    # Delete label: protected when active thread references it
    del_res = store.delete_label(con, "research")
    check("delete_label disables when referenced by active thread", del_res["disabled"] and not del_res["deleted"])

    del_clean = store.delete_label(con, "needs-human")
    check("delete_label removes when not referenced", del_clean["deleted"])

    # put_threads preserves label
    pt_res = store.put_threads(con, {"threads": [{
        "thread_id": "pt-label-1",
        "title": "PUT thread with label",
        "category": "Test",
        "urgency": "whenever",
        "state": "needs-you",
        "label": "triage",
        "ask": {"kind": "ack", "prompt": "Review this?"},
        "sources": [{"source": "test", "id": "pt-label-1", "note": ""}],
    }]})
    check("put_threads accepts label", pt_res["accepted"] == 1)
    thr = con.execute("SELECT * FROM threads WHERE thread_id=?", ("pt-label-1",)).fetchone()
    check("thread carries label from PUT", thr["label"] == "triage")

    # ------------------------------------------------------------- 6. API Tests
    os.environ["HELM_APP_TOKEN"] = "test-token"
    os.environ["HELM_DB"] = db_path
    os.environ["HELM_ATTACH_DIR"] = os.path.join(tmp, "att")

    import app.service as svc
    client = TestClient(svc.app)
    AUTH = {"Authorization": "Bearer test-token"}

    # Unauthorized requests fail 401
    r = client.get("/api/triage/settings")
    check("unauthorized settings request 401", r.status_code == 401)

    # GET /api/triage/settings
    r = client.get("/api/triage/settings", headers=AUTH)
    check("GET /api/triage/settings 200", r.status_code == 200 and "settings" in r.json() and "labels" in r.json())

    # POST /api/triage/settings
    r = client.post("/api/triage/settings", json={"default_intake_label": "triage"}, headers=AUTH)
    check("POST /api/triage/settings 200", r.status_code == 200 and r.json()["settings"]["default_intake_label"] == "triage")

    # GET /api/triage/labels
    r = client.get("/api/triage/labels", headers=AUTH)
    check("GET /api/triage/labels 200", r.status_code == 200 and isinstance(r.json().get("labels"), list))

    # POST /api/triage/labels
    r = client.post("/api/triage/labels", json={
        "name": "qa-check",
        "description": "QA verification runs",
        "prompt_template": "Verify QA test results for {title} and ensure all tests pass.",
        "executor_runtime": "hermes",
    }, headers=AUTH)
    check("POST /api/triage/labels 200", r.status_code == 200 and r.json()["ok"])

    # POST /api/triage/labels 400 on invalid
    r = client.post("/api/triage/labels", json={"name": "INVALID NAME", "prompt_template": "short"}, headers=AUTH)
    check("POST /api/triage/labels 400 on invalid", r.status_code == 400)

    # POST /api/triage/threads/{thread_id}/label
    r = client.post("/api/triage/threads/pt-label-1/label", json={"label": "qa-check"}, headers=AUTH)
    check("POST thread label 202", r.status_code == 202 and r.json()["label"] == "qa-check")

    # ------------------------------------------------------------- 7. UI Copy Contract
    r_ui = client.get("/settings", headers=AUTH)
    check("GET /settings 200 HTML", r_ui.status_code == 200 and "text/html" in r_ui.headers.get("content-type", ""))
    html = r_ui.text

    copy1 = "Intake cards enter via the API or app. Each card is tagged with a label."
    copy2 = "A label defines what instructions the autonomous worker receives and which model runtime executes it."
    copy3 = "Cards labeled 'needs-human' are converted into structured decision cards for Joey with multiple-choice options and no context assumed."

    check("UI copy contains contract sentence 1", copy1 in html)
    check("UI copy contains contract sentence 2", copy2 in html)
    check("UI copy contains contract sentence 3", copy3 in html)

    # ------------------------------------------------------------- 8. Dispatcher Prompt Integration
    from dispatcher import tick
    lbl_obj = {
        "name": "custom-tmpl",
        "prompt_template": "CUSTOM PROMPT FOR {title}: Perform detailed checks on {category}.",
        "convert_to_needs_human": 1,
    }
    item = {
        "thread_id": "t-disp-lbl",
        "title": "Test Dispatcher Label",
        "category": "DevOps",
        "urgency": "today",
        "reply_value": "do-it",
        "reply_note": "Proceed immediately",
        "label": "custom-tmpl",
    }
    rendered = tick.build_prompt(item, "/tmp/rundir", rounds=0, label_data=lbl_obj)
    check("dispatcher uses custom label prompt_template", "CUSTOM PROMPT FOR Test Dispatcher Label" in rendered)
    check("dispatcher formats category in custom prompt", "detailed checks on DevOps" in rendered)
    check("dispatcher appends needs-human instructions", "CONVERT TO NEEDS-HUMAN:" in rendered)


if __name__ == "__main__":
    run_tests()
    print(f"{PASS} passed, {FAIL} failed")
    sys.exit(1 if FAIL else 0)

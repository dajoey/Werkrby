"""Unit and offline acceptance tests for P4 producers, escalation helper, and sweeps.

Contract: docs/contracts/P4.md
Verifies:
- file_helm_thread: pure build_thread, daily ID stability, slug normalization, ack-only ask,
  log tail extraction, dry-run purity, fallback handling.
- Deterministic sweeps: kuma_down, stack_status, quota_floor pipeline purity (fetch/shape/decide/apply),
  hysteresis thresholds, auto-resolution, dry-run purity.
- Store kernel acceptance: producer payloads validate cleanly under helm.store gates.
"""
from __future__ import annotations

import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

import helm.store as store
from producers.sweeps.kuma_down import decide_kuma, shape_kuma_status
from producers.sweeps.quota_floor import decide_quota_floor, shape_quota_facts
from producers.sweeps.stack_status import decide_stack_status, shape_stack_status
from tools.file_helm_thread import build_thread, file_escalation, job_slug, read_tail

_passed = 0
_failed = 0


def check(desc: str, condition: bool, extra: str = "") -> None:
    global _passed, _failed
    if condition:
        _passed += 1
        print(f"  ok: {desc}")
    else:
        _failed += 1
        msg = f"  FAIL: {desc}"
        if extra:
            msg += f" -- {extra}"
        print(msg)


def test_file_helm_thread_builder():
    print("\n--- 1. file_helm_thread builder purity & contract ---")

    # 1.1 Slug normalization
    check("job_slug normalizes uppercase and punctuation",
          job_slug("Backup-Job #42_prod") == "backup-job-42-prod")
    check("job_slug deduplicates hyphens",
          job_slug("my---job---name") == "my-job-name")
    check("job_slug empty fallback",
          job_slug("   ") == "job")

    # 1.2 Log tail reader
    with tempfile.NamedTemporaryFile("w", delete=False) as tf:
        for i in range(100):
            tf.write(f"log line {i}\n")
        log_path = tf.name

    try:
        tail = read_tail(log_path, lines_count=40)
        lines = tail.splitlines()
        check("read_tail extracts exactly 40 lines", len(lines) == 40)
        check("read_tail ends with last line", lines[-1] == "log line 99")
        check("read_tail starts with line 60", lines[0] == "log line 60")
    finally:
        os.unlink(log_path)

    missing_tail = read_tail("/tmp/nonexistent-log-for-helm-test.log")
    check("read_tail gracefully handles missing file", "(log not found" in missing_tail)

    # 1.3 build_thread failure case
    now = datetime(2026, 9, 13, 14, 30, tzinfo=timezone.utc)
    t_fail = build_thread("Nightly Backup", 1, "/var/log/backup.log", now=now)
    check("stable failure thread_id format",
          t_fail["thread_id"] == "sched-nightly-backup-failure-20260913")
    check("title carries job and exit code",
          t_fail["title"] == "[Nightly Backup] run failed (exit 1)")
    check("ask kind is ack", t_fail["ask"]["kind"] == "ack")
    check("urgency is this-week", t_fail["urgency"] == "this-week")
    check("category is Scheduled Jobs", t_fail["category"] == "Scheduled Jobs")
    check("events contains log-tail event",
          len(t_fail["events"]) == 1 and t_fail["events"][0]["kind"] == "log-tail")

    # 1.4 build_thread review case (exit 0)
    t_rev = build_thread("audit-scan", 0, "/var/log/audit.log", now=now)
    check("stable review thread_id format",
          t_rev["thread_id"] == "sched-audit-scan-review-20260913")
    check("title carries review requested",
          t_rev["title"] == "[audit-scan] review requested")
    check("review ask kind is ack", t_rev["ask"]["kind"] == "ack")
    check("review urgency is whenever", t_rev["urgency"] == "whenever")

    # 1.5 Dry-run purity
    rc, info = file_escalation("test-dry", 2, "/dev/null", dry_run=True, now=now)
    check("file_escalation dry-run rc is 0", rc == 0)
    check("file_escalation dry-run action is dry_run", info.get("action") == "dry_run")
    check("file_escalation dry-run thread_id matches",
          info.get("thread_id") == "sched-test-dry-failure-20260913")


def test_store_validation_acceptance():
    print("\n--- 2. Store kernel acceptance of producer threads ---")
    with tempfile.TemporaryDirectory() as td:
        db_path = os.path.join(td, "triage.db")
        con = store.db(db_path)

        now = datetime(2026, 9, 13, 15, 0, tzinfo=timezone.utc)
        t_fail = build_thread("cron-sync", 1, "/dev/null", now=now)

        # Ingest into store
        res = store.put_threads(con, {"threads": [t_fail]})
        check("store accepts producer failure thread",
              res.get("created") == 1 and len(res.get("rejected", [])) == 0,
              str(res))

        cur = con.cursor()
        cur.execute("SELECT state FROM threads WHERE thread_id=?", ("sched-cron-sync-failure-20260913",))
        row = cur.fetchone()
        check("stored thread exists in DB", row is not None)
        check("stored thread has state needs-you", row and row[0] == "needs-you")

        # Re-running same day updates in place (idempotence)
        res_dup = store.put_threads(con, {"threads": [t_fail]})
        check("store updates same-day thread idempotently without creating duplicate",
              res_dup.get("updated") == 1 and res_dup.get("created") == 0,
              str(res_dup))


def test_kuma_down_sweep_pipeline():
    print("\n--- 3. kuma_down sweep pipeline & hysteresis ---")
    st = {"fails": 0, "alerted": False}
    now_dt = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)

    # Clean probe
    sh_up = shape_kuma_status(200, "")
    d_clean = decide_kuma(sh_up, st, alert_after=3, now_dt=now_dt)
    check("clean probe produces noop", d_clean["action"] == "noop")

    # 1 fail
    sh_down = shape_kuma_status(503, "Service Unavailable")
    d1 = decide_kuma(sh_down, st, alert_after=3, now_dt=now_dt)
    check("first failure streak does not alert (noop)", d1["action"] == "noop")
    check("streak count is 1", d1["new_state"]["fails"] == 1)

    # 2 fails
    d2 = decide_kuma(sh_down, d1["new_state"], alert_after=3, now_dt=now_dt)
    check("second failure streak does not alert (noop)", d2["action"] == "noop")
    check("streak count is 2", d2["new_state"]["fails"] == 2)

    # 3 fails -> raise
    d3 = decide_kuma(sh_down, d2["new_state"], alert_after=3, now_dt=now_dt)
    check("third failure streak raises thread", d3["action"] == "raise")
    check("sweep thread id formatted", d3["thread_id"] == "sweep-kuma-down-20260913")
    check("sweep ask is choice with options",
          d3["thread"]["ask"]["kind"] == "choice" and len(d3["thread"]["ask"]["options"]) >= 2)

    # 4 fails -> update existing
    d4 = decide_kuma(sh_down, d3["new_state"], alert_after=3, now_dt=now_dt)
    check("consequent failure updates existing alert", d4["action"] == "update")

    # Recovery -> resolve
    d_rec = decide_kuma(sh_up, d4["new_state"], alert_after=3, now_dt=now_dt)
    check("recovery resolves active alert", d_rec["action"] == "resolve")
    check("recovery clears fails counter", d_rec["new_state"]["fails"] == 0)


def test_stack_status_sweep_pipeline():
    print("\n--- 4. stack_status sweep pipeline ---")
    now_dt = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)

    # All healthy
    probes_ok = [
        {"name": "wiki", "url": "http://w", "ok": True, "status_code": 200, "error": ""},
        {"name": "gitea", "url": "http://g", "ok": True, "status_code": 200, "error": ""},
        {"name": "helm-gateway", "url": "http://h", "ok": True, "status_code": 200, "error": ""},
    ]
    sh_ok = shape_stack_status(probes_ok)
    check("shape reports all healthy", sh_ok["all_healthy"] is True)

    d_ok = decide_stack_status(sh_ok, {"fails": 0, "alerted": False}, threshold=2, now_dt=now_dt)
    check("clean stack produces noop", d_ok["action"] == "noop")

    # Failing service
    probes_fail = [
        {"name": "wiki", "url": "http://w", "ok": True, "status_code": 200, "error": ""},
        {"name": "gitea", "url": "http://g", "ok": False, "status_code": 502, "error": "Bad Gateway"},
        {"name": "helm-gateway", "url": "http://h", "ok": True, "status_code": 200, "error": ""},
    ]
    sh_fail = shape_stack_status(probes_fail)
    check("shape reports failed services", sh_fail["failed_services"] == ["gitea"])

    # 1 fail -> noop (hysteresis)
    d1 = decide_stack_status(sh_fail, {"fails": 0, "alerted": False}, threshold=2, now_dt=now_dt)
    check("first failure streak is noop", d1["action"] == "noop")

    # 2 fails -> raise
    d2 = decide_stack_status(sh_fail, d1["new_state"], threshold=2, now_dt=now_dt)
    check("second failure raises stack alert", d2["action"] == "raise")
    check("thread title names failed service", "gitea" in d2["thread"]["title"])
    check("thread urgency is immediate for core service", d2["thread"]["urgency"] == "immediate")

    # Recovery
    d_rec = decide_stack_status(sh_ok, d2["new_state"], threshold=2, now_dt=now_dt)
    check("stack recovery resolves thread", d_rec["action"] == "resolve")


def test_quota_floor_sweep_pipeline():
    print("\n--- 5. quota_floor sweep pipeline ---")
    st = {"tripped": False}

    # Clean
    sh_clean = shape_quota_facts(0, 10_000_000, {}, [], [])
    d_clean = decide_quota_floor(sh_clean, st)
    check("clean quota facts produces noop", d_clean["action"] == "noop")

    # T1: Paid tokens >= threshold
    sh_t1 = shape_quota_facts(50_000_001, 60_000_000, {"venice": 50_000_001}, [], [])
    d_t1 = decide_quota_floor(sh_t1, st)
    check("T1 paid tokens trips guard", d_t1["action"] == "raise")
    check("T1 summary includes reason", "T1" in d_t1["thread"]["summary"])

    # T2: Fallback lines in logs
    sh_t2 = shape_quota_facts(0, 10_000_000, {}, ["Model fallback via venice"], [])
    d_t2 = decide_quota_floor(sh_t2, st)
    check("T2 fallback lines trips guard", d_t2["action"] == "raise")
    check("T2 summary includes reason", "T2" in d_t2["thread"]["summary"])

    # T3: Dirty chains
    sh_t3 = shape_quota_facts(0, 10_000_000, {}, [], ["unsanctioned_profile:venice/bad-model"])
    d_t3 = decide_quota_floor(sh_t3, st)
    check("T3 dirty chains trips guard", d_t3["action"] == "raise")
    check("T3 summary includes reason", "T3" in d_t3["thread"]["summary"])

    # Recovery
    d_rec = decide_quota_floor(sh_clean, {"tripped": True})
    check("clean telemetry auto-resolves guard", d_rec["action"] == "resolve")


def main():
    test_file_helm_thread_builder()
    test_store_validation_acceptance()
    test_kuma_down_sweep_pipeline()
    test_stack_status_sweep_pipeline()
    test_quota_floor_sweep_pipeline()

    print(f"\n{_passed} passed, {_failed} failed")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())

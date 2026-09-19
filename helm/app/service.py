"""Helm standalone service: thin HTTP over the P1 store.

Contract: docs/contracts/P2.md. Every handler is parse -> one store call -> JSON.
The store (helm/store.py) stays free of HTTP. Compatibility target: the legacy
in-memory gateway that the existing Android app and web UI speak today.
"""
import hmac
import json
import os
import time

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, HTMLResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.routing import Route

import helm.store as store
import collections
import re
import threading
import traceback

TOKEN = os.environ.get("HELM_APP_TOKEN", "")
DB_PATH = os.environ.get("HELM_DB", "data/helm.db")
OPEN_PATHS = {"/api/meta", "/metrics"}


def _authorized(header: str) -> bool:
    """Constant-time `Authorization: Bearer <token>` check, header only.

    A token in a query string is written verbatim into access logs, proxy logs
    and browser history, so there is no `?token=` fallback. A malformed or
    non-ASCII header is unauthorized, never an exception.
    """
    try:
        provided = header.encode("utf-8")
        expected = ("Bearer " + TOKEN).encode("utf-8")
    except (AttributeError, UnicodeError):
        return False
    return hmac.compare_digest(provided, expected)


_metrics_lock = threading.Lock()
_request_counts = collections.defaultdict(int)
_request_duration_sum = collections.defaultdict(float)
_request_errors = collections.defaultdict(int)

# Metric labels are bounded by the route table. telemetry_gate is the outer
# middleware, so every request reaching the port is counted -- including the
# 401s and 404s of a client that never authenticated. Keying those by raw path
# would let anyone grow these dicts, and /metrics with them, without limit.
UNMATCHED_PATH = "/<unmatched>"
_PATH_PARAM_RE = re.compile(r"(\{[^{}]+\})")
_KNOWN_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH",
                            "DELETE", "OPTIONS", "TRACE", "CONNECT"})
_ROUTE_METRIC_KEYS = []


def _compile_route_metric_keys(templates):
    """One (matcher, label) pair per registered route path, in declaration order.

    Path parameters collapse to `:id`, which is the label the old id regex
    already produced for `t-` thread ids, so existing series do not churn.
    """
    keys = []
    seen = set()
    for template in templates:
        if template in seen:
            continue
        seen.add(template)
        pattern = "".join("[^/]+" if part.startswith("{") else re.escape(part)
                          for part in _PATH_PARAM_RE.split(template) if part)
        keys.append((re.compile("^" + pattern + "$"),
                     _PATH_PARAM_RE.sub(":id", template)))
    return keys


def _metric_path(path: str) -> str:
    for matcher, label in _ROUTE_METRIC_KEYS:
        if matcher.match(path):
            return label
    return UNMATCHED_PATH


def _record_http_metric(method: str, path: str, status: int, duration_s: float):
    norm_path = _metric_path(path)
    norm_method = method if method in _KNOWN_METHODS else "OTHER"
    with _metrics_lock:
        _request_counts[(norm_method, norm_path, status)] += 1
        _request_duration_sum[(norm_method, norm_path)] += duration_s
        if status >= 500:
            _request_errors[(norm_method, norm_path, status)] += 1


def _record_unhandled_error(trace_id: str, method: str, path: str, exc: Exception):
    try:
        con = _db()
        try:
            tb = traceback.format_exc()
            details = f"{type(exc).__name__}: {str(exc)}\n{tb}"
            store.record_operational_error(con, trace_id, "gateway", f"{method} {path}", details)
        finally:
            con.close()
    except Exception:
        pass


def _db():
    con = store.db(DB_PATH)
    con.execute("PRAGMA foreign_keys=ON")
    store.init_claims(con)
    return con


def _rej(exc):
    body = {"detail": exc.detail}
    if getattr(exc, "extra", None):
        body.update(exc.extra)
    return JSONResponse(body, status_code=exc.status)





# ---------------------------------------------------------------- meta/health


async def meta(request):
    return JSONResponse({
        "ok": True,
        "service": "helm",
        "version": "1.0.0-standalone",
        "time": time.time(),
        "auth": "bearer" if TOKEN else "disabled",
    })


async def health(request):
    con = _db()
    try:
        h = store.health(con)
    finally:
        con.close()
    return JSONResponse({"ok": True, "health": h})


async def status(request):
    con = _db()
    try:
        h = store.health(con)
        sup = store.list_suppressions(con)
    finally:
        con.close()
    return JSONResponse({
        "ok": True,
        "generated_at": time.time(),
        "health": h,
        "suppressions": len(sup),
        "db_path": os.path.abspath(DB_PATH),
        "db_age_s": max(0.0, time.time() - os.path.getmtime(DB_PATH)) if os.path.exists(DB_PATH) else None,
    })


# ------------------------------------------------------- email digest
# Pushed by the desktop exporter every 7 min (scheduled on the gateway host);
# read by the `email_digest` tool. Ported from the legacy in-memory gateway
# with file persistence so a container restart does not blank it until the
# next push. Auth: the existing auth_gate bearer covers these routes; nothing
# here loosens it.


def _email_digest_path():
    override = os.environ.get("HELM_EMAIL_DIGEST", "")
    if override:
        return override
    return os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "email-digest.json")


_email_digest_lock = threading.Lock()
_email_digest: dict = {}


def _email_digest_load():
    global _email_digest
    try:
        with open(_email_digest_path(), "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict) and isinstance(data.get("digest"), dict):
            _email_digest = {"received_at": float(data["received_at"]), "digest": data["digest"]}
    except Exception:
        pass


def _email_digest_save():
    tmp = _email_digest_path() + ".part"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(_email_digest, fh, ensure_ascii=False)
    os.replace(tmp, _email_digest_path())


_email_digest_load()


async def email_digest_push(request):
    global _email_digest
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"detail": "object body required"}, status_code=400)
    if not isinstance(payload, dict):
        return JSONResponse({"detail": "object body required"}, status_code=400)
    with _email_digest_lock:
        _email_digest = {"received_at": time.time(), "digest": payload}
        try:
            _email_digest_save()
        except Exception as exc:
            return JSONResponse({"detail": "persist failed: %s" % exc}, status_code=500)
    return JSONResponse({"ok": True, "count": payload.get("count", 0)})


async def email_digest_get(request):
    global _email_digest
    with _email_digest_lock:
        if not _email_digest:
            _email_digest_load()
        if not _email_digest:
            return JSONResponse({"detail": "no digest pushed yet"}, status_code=404)
        snapshot = {"received_at": _email_digest["received_at"], "digest": _email_digest["digest"]}
    return JSONResponse({
        "ts": time.time(),
        "age_s": int(time.time() - snapshot["received_at"]),
        **snapshot["digest"],
    })


# ------------------------------------------------------------------ threads


async def threads(request):
    include_done = request.query_params.get("include_done") in ("1", "true", "yes")
    include_snoozed = request.query_params.get("include_snoozed") in ("1", "true", "yes")
    con = _db()
    try:
        out = store.list_threads(con, include_done=include_done,
                                 include_snoozed=include_snoozed)
        woke = out["woke"]
    finally:
        con.close()
    return JSONResponse(out)


async def put_threads(request):
    payload = await request.json()
    if not isinstance(payload, dict):
        return JSONResponse({"detail": "object body required"}, status_code=400)
    con = _db()
    try:
        out = store.put_threads(con, payload)
        if out.get("accepted") == 0 and out.get("rejected_count", 0) > 0:
            return JSONResponse(out, status_code=400)
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse(out, status_code=200)


async def create_thread(request):
    """Hand-written card. Live route: /api/triage/threads/create (201).

    The store mints the id, skips mute rules, and sets state+ask together via
    intent so invariants cannot arrive half-formed.
    """
    body = await request.json()
    con = _db()
    try:
        out = store.create_thread(con, body)
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse(out, status_code=201)


def _thread_action(fn_name, **extra):
    async def handler(request):
        thread_id = request.path_params["thread_id"]
        body = {}
        if await request.body():
            try:
                body = await request.json()
            except json.JSONDecodeError:
                return JSONResponse({"detail": "object body required"}, status_code=400)
        con = _db()
        try:
            fn = getattr(store, fn_name)
            kwargs = dict(extra)
            for k in ("value", "note", "reason", "actor", "force", "wake_at_raw",
                      "wake_at", "hours", "blocked_on", "recheck_days"):
                if k in body:
                    kwargs[k] = body[k]
            if isinstance(body.get("attachments"), list):
                import base64 as _b64, hashlib as _hl
                norm = []
                for a in body["attachments"]:
                    a = dict(a)
                    raw = _b64.b64decode(a.get("content_base64") or "")
                    if raw:
                        a["size"] = len(raw)
                        a["sha256"] = _hl.sha256(raw).hexdigest()
                    clean, errs = store.normalize_attachment(a)
                    if errs:
                        return JSONResponse({"detail": "; ".join(errs)}, status_code=400)
                    norm.append(clean)
                kwargs["attachments"] = norm
            out = fn(con, thread_id, **kwargs)
        except store.StoreError as exc:
            return _rej(exc)
        finally:
            con.close()
        return JSONResponse(out, status_code=202)
    return handler


async def snooze_route(request):
    """Joey defers a card: hidden until wake_at, then back as needs-you."""
    thread_id = request.path_params["thread_id"]
    body = {}
    if await request.body():
        try:
            body = await request.json()
        except json.JSONDecodeError:
            return JSONResponse({"detail": "object body required"}, status_code=400)
    if not isinstance(body, dict):
        return JSONResponse({"detail": "object body required"}, status_code=400)
    con = _db()
    try:
        out = store.snooze(
            con, thread_id,
            wake_at_raw=body.get("wake_at_raw"),
            wake_at=body.get("wake_at"),
            hours=body.get("hours"),
            note=body.get("note", ""))
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse(out, status_code=202)


async def result(request):
    thread_id = request.path_params["thread_id"]
    body = await request.json()
    con = _db()
    try:
        out = store.post_result(con, thread_id, body)
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse(out, status_code=202)


async def append_note(request):
    """Safe note/result append (slice A, 2026-09-15): history-only write.

    Never flips state, never rewrites history; see store.append_note for
    the exact may-touch / never-touch contract.
    """
    thread_id = request.path_params["thread_id"]
    body = await request.json()
    if not isinstance(body, dict):
        return JSONResponse({"detail": "object body required"}, status_code=400)
    con = _db()
    try:
        out = store.append_note(
            con, thread_id,
            note=body.get("note", ""),
            actor=body.get("actor", ""),
            dedupe_key=body.get("dedupe_key", body.get("idempotency_key", "")))
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse(out, status_code=202)


# ------------------------------------------------------------- attachments

ATTACH_ROOT = os.environ.get("HELM_ATTACH_DIR", "data/attachments")


def _safe_segment(*segments) -> bool:
    """Every id that becomes a path segment must be one, at the join site too.

    store.IDENT_RE is enforced when a thread or attachment is created, but these
    ids arrive here from the URL, and Starlette's default converter is [^/]+,
    which matches '..'. attachment_get stats the joined path BEFORE it asks the
    store anything, so store-side validation alone leaves a file-existence
    oracle -- and a database written before that validation existed can still
    hold an id no producer could send today.
    """
    return all(store.IDENT_RE.match(seg or "") for seg in segments)


async def attach(request):
    thread_id = request.path_params["thread_id"]
    # Who is uploading. A browser behind Authentik is a person; a direct API
    # call with no human header is an agent. Default agent: mislabelling Joey
    # leaves his card open, mislabelling an agent answers his question for him.
    _human = (request.headers.get("X-Authentik-Username")
              or request.headers.get("X-Forwarded-User") or "").strip()
    _default_uploader = "joey" if _human else "agent"
    ctype = request.headers.get("Content-Type", "")
    if ctype.startswith("multipart/form-data"):
        form = await request.form()
        upload = form["file"]
        data = await upload.read()
        meta_in = {"filename": upload.filename, "mime": upload.content_type,
                   "caption": form.get("caption"),
                   "uploader": (form.get("uploader") or _default_uploader)}
    else:
        body = await request.json()
        data = None
        if body.get("content_base64"):
            import base64
            data = base64.b64decode(body["content_base64"])
        meta_in = {"filename": body.get("filename"), "mime": body.get("mime"),
                   "caption": body.get("caption"),
                   "uploader": (body.get("uploader") or _default_uploader)}
    if data is None:
        return JSONResponse({"detail": "multipart 'file' or JSON 'content_base64' required"}, status_code=400)
    import hashlib
    con = _db()
    try:
        clean, errs = store.normalize_attachment({**meta_in, "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest()})
        if errs:
            return JSONResponse({"detail": "; ".join(errs)}, status_code=400)
        rec = store.add_attachment(con, thread_id, clean)
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    if not _safe_segment(thread_id, rec["att_id"]):
        return JSONResponse({"detail": "no such thread"}, status_code=404)
    os.makedirs(os.path.join(ATTACH_ROOT, thread_id), exist_ok=True)
    path = os.path.join(ATTACH_ROOT, thread_id, rec["att_id"] + ".bin")
    with open(path, "wb") as f:
        f.write(data)
    return JSONResponse(rec, status_code=201)


async def attachment_get(request):
    thread_id = request.path_params["thread_id"]
    att_id = request.path_params["att_id"]
    if not _safe_segment(thread_id, att_id):
        return JSONResponse({"detail": "no such attachment"}, status_code=404)
    path = os.path.join(ATTACH_ROOT, thread_id, f"{att_id}.bin")
    if not os.path.isfile(path):
        return JSONResponse({"detail": "no such attachment"}, status_code=404)
    con = _db()
    try:
        metas = store.attachments_for(con, thread_id)
    finally:
        con.close()
    meta = next((m for m in metas if m["att_id"] == att_id), None)
    if meta is None:
        return JSONResponse({"detail": "no such attachment"}, status_code=404)
    return Response(open(path, "rb").read(), media_type=meta.get("mime") or "application/octet-stream")


async def attachments_list(request):
    thread_id = request.path_params["thread_id"]
    con = _db()
    try:
        metas = store.attachments_for(con, thread_id)
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse({"attachments": metas})


# ------------------------------------------------------- dispatch

# The dispatcher publishes its heartbeat here so the cross-host watchdog can
# read its age WITHOUT sharing a failure domain with the dispatcher host (contract P3 §6, PLAN rule 10). File-based, one blob:
# this is dispatcher plumbing, not store data, so it never touches the store.
DISPATCHER_HB_PATH = os.environ.get(
    "HELM_DISPATCHER_HB_FILE", "data/dispatcher/heartbeat-published.json")


async def dispatcher_heartbeat_get(request):
    if not os.path.isfile(DISPATCHER_HB_PATH):
        return JSONResponse({"detail": "no dispatcher heartbeat published yet"},
                            status_code=404)
    import json as _json
    try:
        with open(DISPATCHER_HB_PATH, encoding="utf-8") as fh:
            blob = _json.load(fh)
    except (OSError, ValueError):
        return JSONResponse({"detail": "heartbeat unreadable"}, status_code=500)
    return JSONResponse(blob)


async def dispatcher_heartbeat_put(request):
    body = await request.json()
    if not isinstance(body, dict) or not body.get("ts"):
        return JSONResponse({"detail": "heartbeat body must carry a numeric 'ts'"},
                            status_code=400)
    os.makedirs(os.path.dirname(os.path.abspath(DISPATCHER_HB_PATH)) or ".",
                exist_ok=True)
    tmp = DISPATCHER_HB_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(body, fh)
    os.replace(tmp, DISPATCHER_HB_PATH)
    return JSONResponse({"ok": True}, status_code=202)


async def pending_dispatch(request):
    con = _db()
    try:
        out = store.pending_dispatch(con)
    finally:
        con.close()
    return JSONResponse(out)


async def pending_working(request):
    con = _db()
    try:
        out = store.stale_dispatches(con)
    finally:
        con.close()
    return JSONResponse({"pending": out, "generated_at": time.time()})


async def dispatched(request):
    thread_id = request.path_params["thread_id"]
    body = await request.json()
    con = _db()
    try:
        out = store.mark_dispatched(
            con, thread_id, reply_ts=body.get("reply_ts"),
            ok=bool(body.get("ok", True)), detail=body.get("detail", ""),
            mode=body.get("mode", "reply"))
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse(out, status_code=202)


async def needs_joey_check(request):
    """The four-test as a route (contract: P3b §1.4): every producer calls
    this BEFORE filing. Read-only; 200 either way; the answer is data."""
    body = await request.json()
    t = body.get("thread")
    if not isinstance(t, dict):
        return JSONResponse({"detail": "'thread' object is required"}, status_code=400)
    con = _db()
    try:
        clean, errs = store.validate_thread(t)
        if errs:
            return JSONResponse({"detail": "; ".join(errs[:3])}, status_code=400)
        existing = con.execute(
            "SELECT * FROM threads WHERE thread_id=?", (clean["thread_id"],)).fetchone()
        nj_errors = store.needs_joey_gate_errors(con, clean, existing)
        needs_you, failed = not nj_errors, None
        if nj_errors:
            needs_you = False
            # "needs-you refused: T1-only-he-can: <fix>" -> the test name
            parts = nj_errors[0].split(":", 2)
            failed = parts[1].strip() if len(parts) > 2 else None
        return JSONResponse({
            "needs_you": needs_you,
            "failed_test": failed,
            "detail": nj_errors[0] if nj_errors else "all four tests pass",
        })
    finally:
        con.close()


async def gave_up_restore(request):
    body = await request.json() if await request.body() else {}
    con = _db()
    try:
        out = store.restore_gave_up(con)
    finally:
        con.close()
    return JSONResponse(out, status_code=202)


# ------------------------------------------------------------- lessons (P3c)

async def lessons(request):
    """The ledger read + the dispatcher's render view.

    GET /api/triage/lessons                      -> list (all, filters)
    GET /api/triage/lessons?render_for=<scope>   -> the prompt block's rows
    GET /api/triage/lessons?pending_gates=1      -> the honest gap (P3c §5)
    """
    import urllib.parse
    q = urllib.parse.parse_qs(request.url.query)
    con = _db()
    try:
        if q.get("pending_gates"):
            return JSONResponse({"pending_gates": store.lessons_pending_gates(con)})
        if q.get("render_for"):
            rows = store.lessons_for_prompt(con, q["render_for"][0])
            return JSONResponse({"count": len(rows), "lessons": rows})
        rows = store.lessons_list(
            con, scope=(q.get("scope") or [None])[0],
            status=(q.get("status") or [None])[0])
        return JSONResponse({"count": len(rows), "lessons": rows})
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()


async def lesson_get_route(request):
    con = _db()
    try:
        return JSONResponse(store.lesson_get(con, request.path_params["lesson_id"]))
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()


async def lesson_open_route(request):
    """The lesson door (contract: P3c §2): classify elsewhere (deterministic,
    learning/classify.py), file here. Producers POST the candidate + evidence."""
    body = await request.json()
    con = _db()
    try:
        out = store.open_lesson_thread(
            con,
            source_thread_id=body.get("source_thread_id") or "",
            event_ids=body.get("event_ids") or [],
            quote=body.get("quote") or "",
            rule_candidate=body.get("rule_candidate") or "",
            scope=body.get("scope") or "global",
            signals=body.get("signals") or [],
        )
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse(out, status_code=201)


async def lesson_from_thread_route(request):
    """The adoption application (contract: P3c §3/§4): the tick calls this
    after his tap; the store refuses it without one. Never auto-adopts."""
    body = await request.json() if await request.body() else {}
    con = _db()
    try:
        out = store.create_lesson_from_thread(
            con, request.path_params["thread_id"], actor="joey")
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse(out, status_code=202)


async def lesson_retire_route(request):
    body = await request.json()
    con = _db()
    try:
        out = store.retire_lesson(
            con, request.path_params["lesson_id"],
            reason=body.get("reason") or "",
            actor=body.get("actor") or "joey")
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse(out, status_code=202)


async def lesson_supersede_route(request):
    body = await request.json()
    con = _db()
    try:
        out = store.supersede_lesson(
            con, request.path_params["lesson_id"],
            rule_text=body.get("rule_text") or "",
            scope=body.get("scope"),
            actor=body.get("actor") or "joey")
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse(out, status_code=202)


async def lesson_gate_implemented_route(request):
    body = await request.json()
    con = _db()
    try:
        out = store.mark_gate_implemented(
            con, request.path_params["lesson_id"],
            commit_ref=body.get("commit") or "",
            actor=body.get("actor") or "orchestrator")
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse(out, status_code=202)


# ------------------------------------------------------- claims (the NEW surface)

CLAIM_LEASE_S = int(os.environ.get("HELM_CLAIM_LEASE_S", "1800"))


async def claim_create(request):
    """An executor claims a thread. Atomic; no spawn without a claim.

    Body: {executor_id, runtime, caps{timeout_s,max_budget_usd,max_turns,model}}
    Returns 201 {claim_id, lease_deadline} or 409 while a live claim exists.
    """
    thread_id = request.path_params["thread_id"]
    body = await request.json()
    executor_id = str(body.get("executor_id") or "").strip()
    runtime = str(body.get("runtime") or "").strip()
    caps = body.get("caps") if isinstance(body.get("caps"), dict) else {}
    if len(executor_id) < 3:
        return JSONResponse({"detail": "executor_id (>=3 chars) is required: who is claiming"}, status_code=400)
    if not runtime:
        return JSONResponse({"detail": "runtime is required (e.g. 'claude', 'hermes'): the store records it as data"}, status_code=400)
    con = _db()
    try:
        now = time.time()
        row = con.execute(
            "SELECT claim_id, deadline, released_at, result_at FROM claims "
            "WHERE thread_id=? AND released_at IS NULL AND result_at IS NULL",
            (thread_id,)).fetchone()
        if row and row["deadline"] > now:
            return JSONResponse({
                "detail": f"thread already claimed by a live claim until the lease expires",
                "claim_id": row["claim_id"], "deadline": row["deadline"],
            }, status_code=409)
        if row:
            con.execute("UPDATE claims SET released_at=? WHERE claim_id=?",
                        (now, row["claim_id"]))
        claim_id = f"c-{int(now*1000):013d}-{store.slug(executor_id, 16)}"
        con.execute(
            "INSERT INTO claims (claim_id, thread_id, executor_id, runtime, caps, "
            "claimed_at, deadline) VALUES (?,?,?,?,?,?,?)",
            (claim_id, thread_id, executor_id, runtime,
             json.dumps({"timeout_s": caps.get("timeout_s"),
                         "max_budget_usd": caps.get("max_budget_usd"),
                         "max_turns": caps.get("max_turns"),
                         "model": caps.get("model")}),
             now, now + CLAIM_LEASE_S))
        con.commit()
    finally:
        con.close()
    return JSONResponse({"claim_id": claim_id, "lease_deadline": now + CLAIM_LEASE_S,
                         "executor_id": executor_id, "runtime": runtime}, status_code=201)


async def claim_result(request):
    """Executor reports on its claim. Goes THROUGH post_result so every gate applies."""
    thread_id = request.path_params["thread_id"]
    claim_id = request.path_params["claim_id"]
    body = await request.json()
    con = _db()
    try:
        row = con.execute(
            "SELECT deadline, released_at, result_at, executor_id, runtime, caps "
            "FROM claims WHERE claim_id=? AND thread_id=?", (claim_id, thread_id)).fetchone()
        if row is None:
            return JSONResponse({"detail": "no such claim for this thread"}, status_code=404)
        if row["result_at"] is not None:
            return JSONResponse({"detail": "claim already reported; a re-report needs a new claim"}, status_code=409)
        now = time.time()
        expired = row["deadline"] < now
        # The accounting fields ride INTO the result event as data (issue #2).
        accounting = {
            "executor_id": row["executor_id"], "runtime": row["runtime"],
            "model": body.get("model"), "models_billed": body.get("models_billed"),
            "cost_usd": body.get("cost_usd"), "cost_basis": body.get("cost_basis", "unknown"),
            "duration_s": body.get("duration_s"), "num_turns": body.get("num_turns"),
            "capped": bool(body.get("capped", False)),
            "timed_out": bool(body.get("timed_out", False)),
            "permission_denials": body.get("permission_denials"),
            "claim_expired": expired,
        }
        out = store.post_result(con, thread_id, dict(body, executor=accounting))
        con.execute("UPDATE claims SET result_at=?, reported=? WHERE claim_id=?",
                    (now, json.dumps(accounting), claim_id))
        con.commit()
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse({"ok": True, "claim_id": claim_id, "expired": expired,
                         "result": out}, status_code=202)


async def claim_release(request):
    """Release a claim without a result (run failed / gave up / crash cleanup).

    Idempotent; a released claim can never 409 a fresh claim, so a failed run
    retried on a later tick claims anew instead of colliding with its own
    dead claim until the lease expires.
    """
    thread_id = request.path_params["thread_id"]
    claim_id = request.path_params["claim_id"]
    body = await request.json() if await request.body() else {}
    con = _db()
    try:
        row = con.execute(
            "SELECT released_at, result_at FROM claims WHERE claim_id=? AND thread_id=?",
            (claim_id, thread_id)).fetchone()
        if row is None:
            return JSONResponse({"detail": "no such claim for this thread"}, status_code=404)
        if row["result_at"] is not None:
            return JSONResponse({"detail": "claim already reported; results are final"},
                                status_code=409)
        if row["released_at"] is None:
            con.execute("UPDATE claims SET released_at=? WHERE claim_id=?",
                        (time.time(), claim_id))
            con.commit()
        return JSONResponse({"ok": True, "claim_id": claim_id,
                             "note": str(body.get("note") or "")[:200]}, status_code=202)
    finally:
        con.close()


async def claims_list(request):
    state = request.query_params.get("state", "live")
    con = _db()
    try:
        now = time.time()
        rows = con.execute(
            "SELECT claim_id, thread_id, executor_id, runtime, claimed_at, deadline, "
            "released_at, result_at FROM claims ORDER BY claimed_at DESC LIMIT 200").fetchall()
        out = []
        for r in rows:
            live = r["released_at"] is None and r["result_at"] is None and r["deadline"] > now
            item = dict(r)
            item["live"] = live
            item["expired"] = r["released_at"] is None and r["result_at"] is None and r["deadline"] <= now
            if state == "live" and not live:
                continue
            if state == "expired" and not item["expired"]:
                continue
            out.append(item)
    finally:
        con.close()
    return JSONResponse({"claims": out, "generated_at": now})


# ------------------------------------------------------------------ misc


async def suppressions(request):
    con = _db()
    try:
        out = store.list_suppressions(con)
    finally:
        con.close()
    return JSONResponse({"suppressions": out})


async def resort(request):
    body = await request.json() if await request.body() else {}
    name = str(body.get("category") or "").strip()
    if not name:
        return JSONResponse({"detail": "category is required"}, status_code=400)
    con = _db()
    try:
        store._touch_category(con, name, time.time())
        con.commit()
    finally:
        con.close()
    return JSONResponse({"ok": True, "category": name}, status_code=202)


# ------------------------------------------------------------------ settings & labels (P6)


async def get_settings_route(request):
    con = _db()
    try:
        st = store.get_settings(con)
        lbls = store.list_labels(con, include_disabled=True)
    finally:
        con.close()
    return JSONResponse({"settings": st, "labels": lbls})


async def post_settings_route(request):
    body = await request.json()
    if not isinstance(body, dict):
        return JSONResponse({"detail": "object body required"}, status_code=400)
    con = _db()
    try:
        st = store.put_settings(con, body)
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse({"ok": True, "settings": st}, status_code=200)


async def list_labels_route(request):
    include_disabled = request.query_params.get("include_disabled") in ("1", "true", "yes")
    con = _db()
    try:
        lbls = store.list_labels(con, include_disabled=include_disabled)
    finally:
        con.close()
    return JSONResponse({"labels": lbls})


async def put_label_route(request):
    body = await request.json()
    if not isinstance(body, dict):
        return JSONResponse({"detail": "object body required"}, status_code=400)
    con = _db()
    try:
        lbl = store.put_label(con, body)
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse({"ok": True, "label": lbl}, status_code=200)


async def delete_label_route(request):
    name = request.path_params["name"]
    con = _db()
    try:
        res = store.delete_label(con, name)
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse(res, status_code=200)


async def thread_label_route(request):
    thread_id = request.path_params["thread_id"]
    body = await request.json()
    if not isinstance(body, dict):
        return JSONResponse({"detail": "object body required"}, status_code=400)
    label = body.get("label", "")
    con = _db()
    try:
        res = store.set_thread_label(con, thread_id, label)
    except store.StoreError as exc:
        return _rej(exc)
    finally:
        con.close()
    return JSONResponse(res, status_code=202)


async def settings_ui_page(request):
    con = _db()
    try:
        st = store.get_settings(con)
        lbls = store.list_labels(con, include_disabled=True)
    finally:
        con.close()

    labels_rows = ""
    for l in lbls:
        status_tag = f'<span class="tag {"enabled" if l["enabled"] else "disabled"}">{"enabled" if l["enabled"] else "disabled"}</span>'
        needs_human_tag = '<span class="tag enabled">yes</span>' if l["convert_to_needs_human"] else '<span class="tag">no</span>'
        labels_rows += f"""<tr>
          <td><strong>{l["name"]}</strong></td>
          <td>{l["description"]}</td>
          <td><code>{l["executor_runtime"]}</code></td>
          <td>{needs_human_tag}</td>
          <td>{status_tag}</td>
          <td><pre style="max-height: 80px; overflow-y: auto; margin:0; font-size: 11px;">{l["prompt_template"]}</pre></td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Helm Intake & Label Settings</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; max-width: 960px; margin: 30px auto; padding: 0 20px; line-height: 1.5; color: #1f2937; background: #f9fafb; }}
    h1, h2 {{ color: #111827; }}
    .intro-banner {{ background: #eff6ff; border-left: 4px solid #2563eb; padding: 16px 20px; margin-bottom: 24px; border-radius: 4px; }}
    .intro-banner p {{ margin: 6px 0; font-size: 15px; color: #1e3a8a; }}
    .card {{ background: #ffffff; border: 1px solid #e5e7eb; border-radius: 6px; padding: 20px; margin-bottom: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 13px; }}
    th, td {{ text-align: left; padding: 10px 12px; border-bottom: 1px solid #e5e7eb; vertical-align: top; }}
    th {{ background: #f3f4f6; font-weight: 600; }}
    .tag {{ display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: 500; background: #e5e7eb; color: #374151; }}
    .tag.enabled {{ background: #dcfce7; color: #166534; }}
    .tag.disabled {{ background: #fee2e2; color: #991b1b; }}
    code {{ background: #f3f4f6; padding: 2px 4px; border-radius: 3px; font-family: monospace; }}
  </style>
</head>
<body>
  <h1>Helm Intake & Label Settings</h1>
  
  <div class="intro-banner">
    <p><strong>Intake cards enter via the API or app. Each card is tagged with a label.</strong></p>
    <p><strong>A label defines what instructions the autonomous worker receives and which model runtime executes it.</strong></p>
    <p><strong>Cards labeled 'needs-human' are converted into structured decision cards for Joey with multiple-choice options and no context assumed.</strong></p>
  </div>

  <div class="card">
    <h2>Global Intake Settings</h2>
    <table>
      <thead>
        <tr><th>Setting Key</th><th>Current Value</th></tr>
      </thead>
      <tbody>
        <tr><td><code>default_intake_label</code></td><td>{st.get("default_intake_label", "")}</td></tr>
        <tr><td><code>llm_conversion_enabled</code></td><td>{st.get("llm_conversion_enabled", "")}</td></tr>
        <tr><td><code>needs_human_prompt</code></td><td>{st.get("needs_human_prompt", "")}</td></tr>
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>Configured Labels ({len(lbls)})</h2>
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Description</th>
          <th>Runtime</th>
          <th>Needs Human</th>
          <th>Status</th>
          <th>Prompt Brief Template</th>
        </tr>
      </thead>
      <tbody>
        {labels_rows}
      </tbody>
    </table>
  </div>
</body>
</html>"""
    return HTMLResponse(html)


def build_app():
    routes = [
        Route("/api/meta", meta),
        Route("/api/health", health),
        Route("/api/status", status),
        Route("/api/triage/threads", threads, methods=["GET"]),
        Route("/api/triage/threads", put_threads, methods=["PUT"]),
        Route("/api/triage/threads/create", create_thread, methods=["POST"]),
        Route("/api/triage/threads/{thread_id}/reply",
              _thread_action("reply"), methods=["POST"]),
        Route("/api/triage/threads/{thread_id}/close",
              _thread_action("close_thread"), methods=["POST"]),
        Route("/api/triage/threads/{thread_id}/reopen",
              _thread_action("reopen_thread"), methods=["POST"]),
        Route("/api/triage/threads/{thread_id}/mute",
              _thread_action("mute"), methods=["POST"]),
        Route("/api/triage/threads/{thread_id}/snooze",
              snooze_route, methods=["POST"]),
        Route("/api/triage/threads/{thread_id}/result", result, methods=["POST"]),
        Route("/api/triage/threads/{thread_id}/note", append_note, methods=["POST"]),
        Route("/api/triage/threads/{thread_id}/attach", attach, methods=["POST"]),
        Route("/api/triage/threads/{thread_id}/attachments", attachments_list),
        Route("/api/triage/threads/{thread_id}/attachments/{att_id}", attachment_get),
        Route("/api/triage/threads/{thread_id}/claims", claim_create, methods=["POST"]),
        Route("/api/triage/threads/{thread_id}/claims/{claim_id}/result", claim_result, methods=["POST"]),
        Route("/api/triage/threads/{thread_id}/claims/{claim_id}/release", claim_release, methods=["POST"]),
        Route("/api/triage/claims", claims_list),
        Route("/api/triage/pending-dispatch", pending_dispatch),
        Route("/api/triage/dispatcher-heartbeat", dispatcher_heartbeat_get),
        Route("/api/triage/dispatcher-heartbeat", dispatcher_heartbeat_put,
              methods=["POST"]),
        Route("/api/triage/pending-working", pending_working),
        Route("/api/triage/threads/{thread_id}/dispatched", dispatched, methods=["POST"]),
        Route("/api/triage/needs-joey-check", needs_joey_check, methods=["POST"]),
        Route("/api/triage/gave-up/restore", gave_up_restore, methods=["POST"]),
        Route("/api/triage/lessons", lessons, methods=["GET"]),
        Route("/api/triage/lessons", lesson_open_route, methods=["POST"]),
        Route("/api/triage/lessons/{lesson_id}", lesson_get_route, methods=["GET"]),
        Route("/api/triage/lessons/{lesson_id}/retire", lesson_retire_route, methods=["POST"]),
        Route("/api/triage/lessons/{lesson_id}/supersede", lesson_supersede_route, methods=["POST"]),
        Route("/api/triage/lessons/{lesson_id}/gate-implemented", lesson_gate_implemented_route, methods=["POST"]),
        Route("/api/triage/lessons/from-thread/{thread_id}", lesson_from_thread_route, methods=["POST"]),
        Route("/api/triage/suppressions", suppressions),
        Route("/api/triage/resort", resort, methods=["POST"]),
        Route("/settings", settings_ui_page),
        Route("/api/triage/settings", get_settings_route, methods=["GET"]),
        Route("/api/triage/settings", post_settings_route, methods=["POST"]),
        Route("/api/triage/labels", list_labels_route, methods=["GET"]),
        Route("/api/triage/labels", put_label_route, methods=["POST"]),
        Route("/api/triage/labels/{name}", delete_label_route, methods=["DELETE"]),
        Route("/api/triage/threads/{thread_id}/label", thread_label_route, methods=["POST"]),
        Route("/api/email/digest", email_digest_get, methods=["GET"]),
        Route("/api/email/digest", email_digest_push, methods=["POST"]),
        Route("/metrics", metrics_route, methods=["GET"]),
    ]

    global _ROUTE_METRIC_KEYS
    _ROUTE_METRIC_KEYS = _compile_route_metric_keys([r.path for r in routes])

    async def telemetry_gate(request, call_next):
        trace_id = request.headers.get("x-trace-id") or f"tr-req-{int(time.time()*1000)}-{os.urandom(4).hex()}"
        start_ts = time.time()
        try:
            response = await call_next(request)
            dur = time.time() - start_ts
            response.headers["X-Trace-ID"] = trace_id
            response.headers["X-Response-Time"] = f"{(dur*1000):.2f}ms"
            _record_http_metric(request.method, request.url.path, response.status_code, dur)
            return response
        except Exception as exc:
            dur = time.time() - start_ts
            _record_http_metric(request.method, request.url.path, 500, dur)
            _record_unhandled_error(trace_id, request.method, request.url.path, exc)
            return JSONResponse({
                "detail": "Internal server error",
                "trace_id": trace_id,
                "error": str(exc),
            }, status_code=500, headers={"X-Trace-ID": trace_id})

    async def auth_gate(request, call_next):
        if request.url.path in OPEN_PATHS:
            return await call_next(request)
        if not TOKEN:
            return JSONResponse({"detail": "server has no HELM_APP_TOKEN configured; refusing open"}, status_code=500)
        if not _authorized(request.headers.get("Authorization", "")):
            return JSONResponse({"detail": "unauthorized"}, status_code=401)
        return await call_next(request)

    from starlette.middleware import Middleware
    app = Starlette(routes=routes, middleware=[
        Middleware(BaseHTTPMiddleware, dispatch=telemetry_gate),
        Middleware(BaseHTTPMiddleware, dispatch=auth_gate),
    ])
    return app


async def metrics_route(request):
    lines = []
    lines.append("# HELP helm_http_requests_total Total number of HTTP requests processed")
    lines.append("# TYPE helm_http_requests_total counter")
    with _metrics_lock:
        for (m, p, s), count in _request_counts.items():
            lines.append(f'helm_http_requests_total{{method="{m}",path="{p}",status="{s}"}} {count}')

    lines.append("# HELP helm_http_request_duration_seconds_sum Total request latency in seconds")
    lines.append("# TYPE helm_http_request_duration_seconds_sum counter")
    with _metrics_lock:
        for (m, p), dur in _request_duration_sum.items():
            lines.append(f'helm_http_request_duration_seconds_sum{{method="{m}",path="{p}"}} {dur:.4f}')

    lines.append("# HELP helm_threads_total Total number of triage threads by state")
    lines.append("# TYPE helm_threads_total gauge")
    try:
        con = _db()
        try:
            rows = con.execute("SELECT state, COUNT(*) as cnt FROM threads WHERE tombstoned=0 GROUP BY state").fetchall()
            for r in rows:
                lines.append(f'helm_threads_total{{state="{r["state"]}"}} {r["cnt"]}')
        finally:
            con.close()
    except Exception:
        pass

    content = "\n".join(lines) + "\n"
    return Response(content, media_type="text/plain; version=0.0.4; charset=utf-8")


app = build_app()

"""OS-level jail for executor runs — OUTSIDE the CLI (spike §4 + §6.2).

Measured on this host 2026-09-12 (all probes live, $HOME/.jesc-* checks):

  1. `systemd-run --scope` CANNOT apply sandbox settings — user mode or root.
     Both reject ProtectSystem=strict with "Unknown assignment". Enforcement
     needs a transient SERVICE. The contract's "--scope" wording cannot be
     honoured as written on this systemd; the service form is the same jail
     with the semantics the contract demands (hardened exec env,
     ReadWritePaths scoping, uid separation). Deviation recorded in
     P3.md §open-questions.
  2. ProtectSystem=strict alone did NOT protect /home on this host: a write to
     $HOME/.jesc-A-* LANDED. ProtectHome=read-only is REQUIRED, not optional.
     The contract named ProtectSystem only; measured, and added.
  3. IPAddressDeny=any kills all networking including loopback (curl rc=28) —
     executors get NO network at all. The harness posts results; the executor
     never talks to anything.

Fallback (no systemd / no root): setpriv --reuid / runuser. That mode has NO
filesystem write-scope: a process with our uid can still write $HOME. It is
LOUD — mode is "fallback-degraded", every spawn under it logs a warning saying
exactly what protection is missing, and it NEVER spawns untrusted work unless
`allow_degraded_jail` (env HELM_JAIL_ALLOW_DEGRADED=1, default OFF) explicitly
opts in. Fail closed, never silently degraded.

UID model: service mode runs as a dedicated `helm-exec` user when it exists
(--uid/--gid) so the executor is not the operator's uid; otherwise as the
invoking user with namespace protections only. The run_dir is chmod 0777
pre-spawn so either uid can write it.

The escape test (spike §4) is re-fired IN the jail by tests and by --selftest:
a write outside the run dir must be REFUSED. If any write lands outside, P3
does not ship — and at runtime the dispatcher refuses to spawn when the jail
self-check fails (fail closed).
"""
from __future__ import annotations

import os
import pwd
import shutil
import subprocess

from .common import log

HELM_EXEC_USER = "helm-exec"


def _q(s: str) -> str:
    """Single-quote a path for sh -c. Paths never contain quotes here."""
    return "'" + str(s).replace("'", "'\\''") + "'"

# systemd unit name prefix for transient units (never installed, transient)
_UNIT_PREFIX = "helm-exec-run"

# Seconds to wait for systemd-run to return the process's own exit code.
# --wait makes systemd-run block until the command exits; anything slower is a
# systemd-level failure and surfaces as a timeout of its own.
_SYSTEMD_WAIT_S = 30


def detect_mode() -> str:
    """Pick the jail mode for this host: 'service' | 'fallback-degraded'.

    Service mode needs systemd PID1 + passwordless sudo (transient units are a
    root operation; user-mode scopes cannot sandbox — measured). Anything else
    degrades LOUDLY to the setpriv/runuser fallback.
    """
    if not os.path.isdir("/run/systemd/system"):
        return "fallback-degraded"
    if shutil.which("systemd-run") is None:
        return "fallback-degraded"
    if not _sudo_available():
        return "fallback-degraded"
    return "service"


def _sudo_available() -> bool:
    try:
        p = subprocess.run(["sudo", "-n", "true"], capture_output=True, timeout=10)
        return p.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def exec_uid_gid() -> tuple[int | None, int | None]:
    """(uid, gid) of the dedicated helm-exec user, or (None, None).

    HELM_JAIL_EXEC_UID/GID override for hermetic test runs that keep the
    jailed process on the invoking user (stub executors that must be able to
    run python3 from a venv, e.g.). Production leaves it unset and uses the
    dedicated helm-exec uid.
    """
    forced_uid = os.environ.get("HELM_JAIL_EXEC_UID")
    if forced_uid is not None:
        try:
            return int(forced_uid), int(os.environ.get("HELM_JAIL_EXEC_GID", forced_uid))
        except ValueError:
            pass
    try:
        pw = pwd.getpwnam(HELM_EXEC_USER)
        return pw.pw_uid, pw.pw_gid
    except KeyError:
        return None, None


PROVIDER_HOSTS = (
    "api.anthropic.com",
    "api.z.ai",
    "api.venice.ai",
)


def provider_allowlist() -> str:
    """Resolve the provider hosts to IPs at jail-build time. Resolved IPs, not
    wildcard domains: the allowlist stays narrow even if DNS changes under us
    mid-run, and no general egress is ever granted. DNS servers themselves are
    passed through so name resolution works inside the jail."""
    import socket
    entries = ["127.0.0.1/8"]
    # the system resolvers, so getaddrinfo works inside the jail
    try:
        with open("/etc/resolv.conf") as fh:
            for line in fh:
                parts = line.split()
                if len(parts) >= 2 and parts[0] in ("nameserver",):
                    entries.append(parts[1])
    except OSError:
        pass
    for host in os.environ.get("HELM_JAIL_EXTRA_HOSTS", "").split(","):
        host = host.strip()
        if host:
            try:
                entries.append(socket.gethostbyname(host))
            except OSError:
                pass
    hosts_env = os.environ.get("HELM_JAIL_PROVIDER_HOSTS",
                               ",".join(PROVIDER_HOSTS))
    for h in (x.strip() for x in hosts_env.split(",")):
        if h:
            try:
                entries.append(socket.gethostbyname(h))
            except OSError:
                pass
    # dedupe, preserve order
    seen, out = set(), []
    for e in entries:
        if e not in seen:
            seen.add(e)
            out.append(e)
    return " ".join(out)


def _service_properties(run_dir: str) -> list:
    """systemd property args for the jail. ReadWritePaths is the ONLY writable
    path; everything else is ro (ProtectSystem) or hidden (PrivateTmp)."""
    props = [
        "-p", "ProtectSystem=strict",
        "-p", "ProtectHome=read-only",
        "-p", "ReadWritePaths=%s" % run_dir,
        "-p", "PrivateTmp=true",
        # Network: deny the open internet, allow exactly what an executor
        # legitimately needs — DNS resolution and its inference provider.
        # IPAddressDeny=any alone (the original contract) blocks the
        # provider too: the executor can never reach its model (live-gate
        # finding 2026-09-12). Allow= list wins over Deny=any in systemd,
        # and the allowlist is resolved IPs, not wildcards: no general
        # egress, no LAN, no sideways movement — only the provider
        # endpoints the configured runtimes call.
        "-p", "IPAddressDeny=any",
        "-p", "IPAddressAllow=%s" % provider_allowlist(),
        "-p", "MemoryDenyWriteExecute=no",   # models run node/python runtimes
        "-p", "NoNewPrivileges=yes",
        "-p", "RestrictSUIDSGID=yes",
    ]
    uid, gid = exec_uid_gid()
    if uid is not None:
        props += ["--uid=%d" % uid, "--gid=%d" % gid]
    return props


def build_service_argv(cmd_argv: list, run_dir: str, unit_name: str,
                       env_file: str | None = None) -> list:
    """The transient-service invocation (data; tests inspect, never run)."""
    argv = ["sudo", "systemd-run", "--quiet", "--collect", "--wait",
            "--unit=%s" % unit_name]
    if env_file:
        # EnvironmentFile: env vars reach the service without touching argv,
        # ps output, or the two systemd settings the service itself could leak
        # into. The file lives in run_dir (in-scope, 0600, retention-swept).
        argv += ["-p", "EnvironmentFile=%s" % env_file]
    argv += _service_properties(run_dir)
    # cwd = run dir: relative paths land in scope, and PrivateTmp=true hides
    # the real /tmp (a run dir under /tmp would be invisible to the service).
    argv += ["-p", "WorkingDirectory=%s" % run_dir, "--service-type=exec"]
    # ExecStart REQUIRES an absolute path; resolve argv[0] here. A relative
    # argv[0] makes systemd-run fail instantly with rc 1 — which a naive
    # escape probe then reads as "refused" (a vacuous pass, the unfired-guard
    # trap). Resolving it here is what keeps the probe honest.
    exe = cmd_argv[0]
    if not os.path.isabs(exe):
        resolved = shutil.which(exe)
        if resolved is None:
            raise FileNotFoundError(
                "executor binary %r not found on PATH; an unresolvable "
                "executor must fail loudly, not spawn a failing unit" % exe)
        exe = resolved
    argv += [exe] + list(cmd_argv[1:])
    return argv


def build_fallback_argv(cmd_argv: list, run_dir: str) -> list:
    """setpriv flavor: uid separation only, NO filesystem scope (loud)."""
    uid, gid = exec_uid_gid()
    if uid is not None:
        return ["setpriv", "--reuid", str(uid), "--regid", str(gid),
                "--clear-groups", cmd_argv[0]] + list(cmd_argv[1:])
    return ["runuser", "-u", os.environ.get("HELM_FALLBACK_USER", "nobody"),
            "--"] + list(cmd_argv[0:1]) + list(cmd_argv[1:])


# Env vars that cross into the jailed service. `sudo systemd-run` strips the
# caller's environment (env_reset), so anything the executor needs must ride
# the EnvironmentFile instead. HELM_STUB_* is test plumbing; the credential
# vars are production (fetched at spawn time, never in argv, never logged).
#
# Provider credentials: the executor's inference key reaches it as an env var
# because the jailed hermes cannot run the fleet's network-bound secrets
# helper (IPAddressDeny=any allowlists only DNS + provider hosts; the helper
# cannot reach the vault, times out, and hermes falls through its fallback
# chain to a dead provider — live-gate finding 2026-09-13). The dispatcher
# therefore fetches the provider key BEFORE spawn and passes it through the
# EnvironmentFile. The var NAMES come from HELM_PROVIDER_ENV_VARS (comma
# list) so no runtime is hardwired here; values are never logged.
#
# P7 (contract: docs/contracts/P7.md §1): HELM_PROVIDER_ENV_VARS is ADDITIVE.
# The production run script overrode it with the zai-only list and silently
# dropped the anthropic names — every SWITCH-to-claude then died unauthenticated
# in the jail (live-gate defect A, 2026-09-13). The fallback runtime's
# credential names are NOT overridable: an override may add names, never disarm
# the fallback (LESSONS 23). Verified against the installed claude 2.1.x
# binary: ANTHROPIC_AUTH_TOKEN -> Bearer header, ANTHROPIC_API_KEY -> API-key
# auth, CLAUDE_CODE_OAUTH_TOKEN accepted; "ANTHROPIC_TOKEN" is NOT read by the
# CLI (it rides anyway so an adapter can read the fleet's own storage name).
ANTHROPIC_AUTH_VARS = ("ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY",
                       "ANTHROPIC_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN")
PROVIDER_ENV_VARS = tuple(dict.fromkeys(
    list(ANTHROPIC_AUTH_VARS) + [
        v.strip() for v in os.environ.get(
            "HELM_PROVIDER_ENV_VARS",
            "ZAI_API_KEY,Z_AI_API_KEY").split(",")
        if v.strip()]))
PASSTHROUGH_ENV = (
    "HELM_RUN_DIR", "HELM_STUB_MODE", "HELM_STUB_CAP_BUDGET",
    # P3b §2 testing: the stub's per-runtime mode (claude fails its last
    # attempt while the switched hermes pass succeeds) rides through the
    # jail like the other stub knobs.
    "HELM_STUB_MODE_HERMES",
    # HOME: the hermes runtime reads its provider config (config.yaml) under
    # $HOME/.hermes. ProtectHome=read-only still blocks every WRITE; a runtime
    # that cannot read its own config cannot run at all (live-gate finding
    # 2026-09-12: "No inference provider configured").
    "HOME",
) + PROVIDER_ENV_VARS


def _merged_env(env: dict | None) -> dict:
    out = {}
    for k in PASSTHROUGH_ENV:
        v = os.environ.get(k)
        if v is not None:
            out[k] = v
    out.update(env or {})
    return out


class Jail:
    """One jail context for one run_dir."""

    def __init__(self, run_dir: str, mode: str | None = None,
                 allow_degraded: bool | None = None):
        self.run_dir = os.path.abspath(run_dir)
        self.mode = mode or detect_mode()
        if allow_degraded is None:
            allow_degraded = os.environ.get("HELM_JAIL_ALLOW_DEGRADED") == "1"
        self.allow_degraded = allow_degraded
        self.degraded_reason = ""
        self.unit_seq = 0

    # -- spawn ----------------------------------------------------------

    def prepare(self) -> dict:
        """Create + chmod the run dir so either uid can write it. Returns env
        file info (path written by caller via write_env_file)."""
        os.makedirs(self.run_dir, exist_ok=True)
        os.chmod(self.run_dir, 0o777)
        return {"run_dir": self.run_dir, "mode": self.mode}

    def write_env_file(self, env: dict) -> str:
        """Write the executor env file into run_dir (0600). Secrets land here,
        never in argv or ps. The jail's ReadWritePaths covers run_dir, so the
        service can read it."""
        path = os.path.join(self.run_dir, ".env")
        lines = []
        for k, v in sorted(env.items()):
            if v is None:
                continue
            sv = str(v).replace("\\", "\\\\").replace("\n", "\\n")
            lines.append("%s=%s" % (k, sv))
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        os.chmod(path, 0o600)
        return path

    def spawn(self, cmd_argv: list, env: dict | None = None,
              timeout_s: int = 300, stdin_text: str = "",
              env_file: str | None = None) -> subprocess.Popen:
        """Spawn cmd_argv under the jail. Returns a Popen the harness waits on.

        timeout(1) wraps the whole thing — the wall-clock cap is OURS, held by
        the runner, not the runtime's.
        """
        if self.mode == "fallback-degraded" and not self.allow_degraded:
            raise PermissionError(
                "jail is DEGRADED (no systemd service mode): refusing to spawn "
                "untrusted work without filesystem scope. Set "
                "HELM_JAIL_ALLOW_DEGRADED=1 only for trusted/stub executors.")
        os.makedirs(self.run_dir, exist_ok=True)
        os.chmod(self.run_dir, 0o777)
        self.unit_seq += 1
        unit = "%s-%d-%d" % (_UNIT_PREFIX, os.getpid(), self.unit_seq)
        env = _merged_env(env)
        env_f = env_file or (self.write_env_file(env))
        if self.mode == "service":
            argv = build_service_argv(cmd_argv, self.run_dir, unit, env_f)
        else:
            argv = build_fallback_argv(cmd_argv, self.run_dir)
            # env via process env for the fallback (no systemd EnvironmentFile)
        timeout_argv = ["timeout", "--kill-after=10s", "%ds" % int(timeout_s)] + argv
        penv = dict(os.environ)
        if self.mode != "service" and env:
            penv.update(env)
        proc = subprocess.Popen(
            timeout_argv, stdin=subprocess.PIPE if stdin_text else subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=self.run_dir,
            env=penv, text=True, encoding="utf-8", errors="replace")
        if stdin_text and proc.stdin is not None:
            try:
                proc.stdin.write(stdin_text)
                proc.stdin.close()
            except (BrokenPipeError, OSError):
                pass
        return proc

    # -- the escape test --------------------------------------------------

    def escape_probe(self, outside_paths: list | None = None,
                     timeout_s: int = 30) -> dict:
        """Re-fire the spike's escape test IN this jail: write outside run_dir.

        TWO-SIDED, never vacuous. A probe that only checks "nothing appeared
        outside" passes even when the jail never ran the command at all
        (measured: a relative argv[0] made systemd-run exit 1 instantly, and
        the naive probe read that as "refused"). So the probe first writes
        INSIDE the run dir and requires that write to LAND — proof the jailed
        command actually executed — and only then are the outside writes
        required to be refused. ok = inside landed AND every outside refused.
        """
        home = os.path.expanduser("~")
        targets = outside_paths or [
            os.path.join(home, ".helm-jail-escape-probe"),
            "/tmp/helm-jail-escape-probe-%d" % os.getpid(),
        ]
        inside = os.path.join(self.run_dir, "probe-inside.txt")
        try:
            if os.path.exists(inside):
                os.unlink(inside)
        except OSError:
            pass
        attempts = []
        # control: the jailed command must be able to write IN scope
        try:
            proc = self.spawn(["sh", "-c", "echo ran > %s" % _q(inside)],
                              env={}, timeout_s=timeout_s)
            proc.communicate(timeout=timeout_s + 20)
        except PermissionError:
            return {"ok": False, "mode": self.mode, "attempts": [],
                    "reason": "degraded jail refused to spawn the control write"}
        inside_ok = os.path.exists(inside)
        if not inside_ok:
            return {"ok": False, "mode": self.mode,
                    "attempts": [{"path": inside, "refused": False,
                                  "note": "CONTROL FAILED: the jailed command "
                                          "could not write inside its own run "
                                          "dir, so the jail is broken (or never "
                                          "ran); outside-writes prove nothing"}]}
        for i, path in enumerate(targets):
            token = "esc-%d-%d" % (os.getpid(), i)
            probe_argv = ["sh", "-c", "echo %s > %s 2>/dev/null; echo rc=$?" % (token, _q(path))]
            try:
                proc = self.spawn(probe_argv, env={}, timeout_s=timeout_s)
                out, err = proc.communicate(timeout=timeout_s + 20)
                rc = proc.returncode
            except PermissionError:
                attempts.append({"path": path, "refused": True, "rc": None,
                                 "note": "degraded jail refused to spawn"})
                continue
            refused = not os.path.exists(path)
            attempts.append({"path": path, "refused": refused, "rc": rc,
                             "stdout_tail": (out or "")[:200],
                             "stderr_tail": (err or "")[:200]})
            if os.path.exists(path):
                try:
                    os.unlink(path)  # clean up our own probe artifact
                except OSError:
                    pass
        return {"ok": all(a["refused"] for a in attempts), "mode": self.mode,
                "attempts": attempts}

    def selfcheck(self) -> dict:
        """Full jail self-check: escape probe (service mode only; degraded
        mode is documented-unable). Used by tick before spawning anything."""
        if self.mode == "fallback-degraded":
            return {"ok": False, "mode": self.mode,
                    "reason": "degraded jail has no filesystem scope; refusing "
                              "untrusted spawns (allow_degraded opt-in only)"}
        res = self.escape_probe()
        res["selfcheck"] = True
        return res


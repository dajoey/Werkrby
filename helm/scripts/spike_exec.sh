#!/usr/bin/env bash
# P2b spike: spawn ONE capped, scoped, headless executor run and report it once.
#
#   scripts/spike_exec.sh <prompt_file> <run_dir> [KEY=VALUE ...]
#
# Caps (overridable as KEY=VALUE args; all enforced HERE, never by the prompt):
#   RUNTIME=claude       claude | hermes  -- Helm is executor-agnostic (issue #2)
#   TIMEOUT_S=300        wall clock, enforced by timeout(1)
#   MAX_TURNS=8          model turns  (claude 2.1.246: NO FLAG -- see below)
#   MAX_BUDGET_USD=1.50  spend ceiling (claude only; hermes has no such flag)
#   RECEIVER=http://127.0.0.1:18723/result
#   MODEL=sonnet  EFFORT=low
#
# Measured on this install 2026-09-12, and the reason the two branches differ:
#   claude 2.1.246 has --max-budget-usd but NO --max-turns (the flag the work
#     dispatcher's poll.py assumes is gone). Turn count is observed after the
#     fact from the JSON envelope, not bounded before it.
#   hermes (native one-shot) has NEITHER a turn cap nor a budget cap, and no
#     per-invocation profile flag. Its only hard cap is timeout(1) here.
# Both gaps are findings, recorded in docs/SPIKE-P2b.md; neither is patched over
# by asking the prompt nicely (LESSONS 35: caps belong to the runner).
#
# Design notes that matter more than the code:
#
#  * cwd IS the run dir -- and MEASURED 2026-09-12, cwd ALONE SCOPES NOTHING.
#    Both runtimes, handed an absolute path by the prompt, wrote outside their
#    run dir on the first try (runs/claude-violation, runs/hermes-violation).
#    cwd is where relative paths land, not a wall. What actually held, measured:
#      hermes: HERMES_WRITE_SAFE_ROOT="$RUN_DIR" -> "Write denied: ... is
#              outside HERMES_WRITE_SAFE_ROOT" and no file on disk.
#      claude: NOTHING TESTED YET HOLDS. --allowedTools + cwd + dontAsk let the
#              write through with permission_denials: []. Unsolved; see the doc.
#    So the default below is set by the RUNNER, never asked of the prompt.
#  * Bash is DENIED. An executor holding Bash has the whole filesystem whatever
#    else is configured, so a write-scope claim alongside Bash would be theatre.
#  * Agent/Task are denied BY NAME: boundaries.py records a denied run reaching
#    for exactly the Agent tool as its way around a denial.
#  * The HARNESS posts the result, not the model. The executor gets no network
#    tool at all, so "exactly one result per run" is a property of this script
#    instead of a thing the model has to cooperate with.
#  * No secrets are passed, written or logged. The Anthropic subscription token
#    is inherited from the environment (sk-ant-oat01-*, from `claude
#    setup-token`); never echoed, never written to the run dir, never committed.
set -uo pipefail

PROMPT_FILE="${1:?usage: spike_exec.sh <prompt_file> <run_dir> [KEY=VAL ...]}"
RUN_DIR="${2:?usage: spike_exec.sh <prompt_file> <run_dir> [KEY=VAL ...]}"
shift 2

RUNTIME=claude
TIMEOUT_S=300
MAX_TURNS=8
MAX_BUDGET_USD=1.50
RECEIVER="http://127.0.0.1:18723/result"
HEALTH="http://127.0.0.1:18723/health"
MODEL="sonnet"
EFFORT="low"
for kv in "$@"; do
  case "$kv" in
    *=*) export "${kv%%=*}"="${kv#*=}" ;;
    *) echo "spike_exec: bad arg '$kv' (want KEY=VALUE)" >&2; exit 2 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"   # absolute: we cd away later
RUN_ID="$(basename "$RUN_DIR")"
mkdir -p "$RUN_DIR"
RUN_DIR="$(cd "$RUN_DIR" && pwd)"        # absolute: the escape test needs real paths
cp "$PROMPT_FILE" "$RUN_DIR/prompt.txt"

# Refuse to run on a pay-go credential. A wrong credential authenticates fine
# and bills somewhere else (poll.py says exactly this) -- assert, never assume.
# `claude setup-token` subscription tokens are sk-ant-oat01-*; pay-go is sk-ant-api*.
if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
  echo "spike_exec: ANTHROPIC_API_KEY is set -- that is pay-go. Refusing (PLAN rule 4)." >&2
  exit 3
fi
case "${ANTHROPIC_AUTH_TOKEN:-}" in
  sk-ant-api*)
    echo "spike_exec: ANTHROPIC_AUTH_TOKEN looks like a pay-go API key. Refusing." >&2
    exit 3 ;;
esac

# Prove the receiver is up BEFORE spending anything, so a missing POST afterwards
# can only mean the POST failed -- never "the receiver was never listening".
if ! python3 -c "import sys,urllib.request
try:
    urllib.request.urlopen('$HEALTH', timeout=5).read()
except Exception as e:
    sys.stderr.write('spike_exec: receiver health check failed: %r\n' % (e,))
    sys.exit(1)"; then
  exit 4
fi

ALLOWED="Read,Glob,Grep,Write,Edit,TodoWrite"
DENIED="Bash,BashOutput,KillShell,Agent,Task,WebFetch,WebSearch,NotebookEdit,SlashCommand"
HERMES_TOOLSETS="${HERMES_TOOLSETS:-file,todo}"   # file ops only; no terminal, no web

# The one write-scope mechanism measured to actually refuse (runs/hermes-5-
# violation-saferoot): hermes' file tools reject any write resolving outside
# these roots. Default it to the run dir so scoping is on unless a caller says
# otherwise. hermes' own agent/file_safety.py calls this "defense-in-depth, NOT
# a security boundary: the terminal tool runs as the same OS user" -- which is
# exactly why the terminal toolset is not in HERMES_TOOLSETS above.
export HERMES_WRITE_SAFE_ROOT="${HERMES_WRITE_SAFE_ROOT:-$RUN_DIR}"

# cd INTO the run dir. This is where the run's OWN files land -- it is NOT the
# write-scope boundary (measured: both runtimes wrote outside it on request).
# The scope is HERMES_WRITE_SAFE_ROOT above for hermes, and nothing yet for claude.
cd "$RUN_DIR" || { echo "spike_exec: cannot cd into $RUN_DIR" >&2; exit 6; }

START="$(date +%s.%N)"
case "$RUNTIME" in
  claude)
    # --bare is the work-Helm pattern: no hooks, no plugins, no CLAUDE.md
    # auto-discovery, no keychain -- auth strictly from the injected env token.
    # That is what makes a run COLD in a way a second run cannot quietly undo.
    # NB: no --max-turns on 2.1.246; the turn bound here is budget + wall clock.
    # NB2: this invocation is NOT write-scoped. --permission-mode dontAsk
    # auto-approves a Write to an absolute path outside cwd (measured: it wrote
    # /tmp and reported permission_denials: []). Until a claude-side deny rule
    # is measured refusing, treat this branch as "trusted prompt only".
    timeout --kill-after=10s "${TIMEOUT_S}s" \
      claude -p --bare \
        --output-format json \
        --model "$MODEL" \
        --effort "$EFFORT" \
        --max-budget-usd "$MAX_BUDGET_USD" \
        --allowedTools "$ALLOWED" \
        --disallowedTools "$DENIED" \
        --permission-mode dontAsk \
        --no-session-persistence \
        --strict-mcp-config \
        < "$RUN_DIR/prompt.txt" \
        > "$RUN_DIR/stdout.json" 2> "$RUN_DIR/stderr.txt"
    RC=$?
    ;;
  hermes)
    # -z takes the prompt as an ARGUMENT, not on stdin, and prints only the final
    # text; --usage-file is the only machine-readable accounting it offers.
    # -t restricts the toolsets for THIS invocation (no terminal/code_execution:
    # a shell tool would make any write-scope claim theatre). --ignore-rules
    # keeps AGENTS.md/memory/skills out, which is this runtime's nearest
    # equivalent of --bare. There is no per-invocation profile flag: the sticky
    # default profile is used, and changing it is global state we refuse to touch.
    #
    # --in is NOT redundant with the cd above. Measured 2026-09-12: without it,
    # hermes resolved the agent's relative paths against $HOME, not the process
    # cwd -- runs/hermes-violation/ESCAPED-home-result.json is the file it left
    # in the operator's home directory. Process cwd alone does not scope this runtime.
    timeout --kill-after=10s "${TIMEOUT_S}s" \
      hermes -z "$(cat "$RUN_DIR/prompt.txt")" \
        --in "$RUN_DIR" \
        --usage-file "$RUN_DIR/usage.json" \
        --reasoning "$EFFORT" \
        -t "$HERMES_TOOLSETS" \
        --ignore-rules \
        > "$RUN_DIR/stdout.txt" 2> "$RUN_DIR/stderr.txt"
    RC=$?
    ;;
  *) echo "spike_exec: unknown RUNTIME '$RUNTIME' (want claude|hermes)" >&2; exit 2 ;;
esac
END="$(date +%s.%N)"

# 124 is timeout(1)'s "I killed it". Named so a wall-clock stop is never read as
# the executor having failed on its own.
TIMED_OUT=false
[ "$RC" -eq 124 ] && TIMED_OUT=true

DURATION="$(python3 -c "print(round($END-$START,2))")"

RUN_ID="$RUN_ID" RUN_DIR="$RUN_DIR" RC="$RC" DURATION="$DURATION" \
TIMED_OUT="$TIMED_OUT" RECEIVER="$RECEIVER" TIMEOUT_S="$TIMEOUT_S" \
MAX_TURNS="$MAX_TURNS" MAX_BUDGET_USD="$MAX_BUDGET_USD" MODEL="$MODEL" \
RUNTIME="$RUNTIME" \
  python3 "$SCRIPT_DIR/spike_report.py"

exit "$RC"

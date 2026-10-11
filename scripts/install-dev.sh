#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

usage() {
  cat <<'EOF'
Usage: scripts/install-dev.sh [--bin-dir DIRECTORY]

Build Werkrby for development and install it as werkrby in a user bin
directory, with spyjo and spynel as compatibility symlinks to it.
The default is $WERKRBY_DEV_BIN_DIR when set, then $SPYNEL_DEV_BIN_DIR,
otherwise $HOME/.local/bin.
EOF
}

if [ -n "${WERKRBY_DEV_BIN_DIR:-}" ]; then
  bin_dir=$WERKRBY_DEV_BIN_DIR
elif [ -n "${SPYNEL_DEV_BIN_DIR:-}" ]; then
  bin_dir=$SPYNEL_DEV_BIN_DIR
elif [ -n "${HOME:-}" ]; then
  bin_dir="$HOME/.local/bin"
else
  echo "HOME is unset; pass --bin-dir or set WERKRBY_DEV_BIN_DIR" >&2
  exit 2
fi
case ${1:-} in
  "") ;;
  --bin-dir)
    if [ "$#" -ne 2 ] || [ -z "$2" ]; then
      usage >&2
      exit 2
    fi
    bin_dir=$2
    ;;
  --bin-dir=*)
    if [ "$#" -ne 1 ]; then
      usage >&2
      exit 2
    fi
    bin_dir=${1#--bin-dir=}
    ;;
  -h|--help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

case $bin_dir in
  /*) ;;
  *)
    echo "install directory must be absolute: $bin_dir" >&2
    exit 2
    ;;
esac

mkdir -p "$bin_dir"
bin_dir=$(CDPATH= cd -- "$bin_dir" && pwd)
target="$bin_dir/werkrby"
for candidate in "$target" "$bin_dir/spyjo" "$bin_dir/spynel"; do
  if [ -e "$candidate" ] && [ ! -f "$candidate" ] && [ ! -L "$candidate" ]; then
    echo "refusing to replace non-file target: $candidate" >&2
    exit 1
  fi
done

built_binary=$("$script_dir/dev.sh" build)
if [ ! -x "$built_binary" ]; then
  echo "development build did not produce an executable: $built_binary" >&2
  exit 1
fi
staged=$(mktemp "$bin_dir/.werkrby.dev.XXXXXX")
cleanup() {
  if [ -e "$staged" ]; then
    unlink "$staged"
  fi
}
trap cleanup EXIT HUP INT TERM
cp "$built_binary" "$staged"
chmod 0755 "$staged"
mv -f "$staged" "$target"
for alias_name in spyjo spynel; do
  ln -sf "$target" "$bin_dir/$alias_name"
done
trap - EXIT HUP INT TERM

# Replacing the executable under a running instance leaves its /proc/<pid>/exe
# marked (deleted), which wedges election in a silent acquire/release loop.
# Restart any active instance so it re-executes from the new binary.
# Best-effort: a failed or unavailable restart never fails the install.
restarted_units=""
if command -v systemctl >/dev/null 2>&1; then
  for unit in $(systemctl --user list-units --type=service --state=running 'spynel-*' --no-legend --no-pager 2>/dev/null | awk '{print $1}' || true); do
    case "$unit" in
      spynel-*.service)
        if systemctl --user restart "$unit" 2>/dev/null; then
          restarted_units="$restarted_units $unit"
        fi
        ;;
    esac
  done
fi
if [ -n "$restarted_units" ]; then
  echo "Restarted running instance(s):$restarted_units"
fi

path_contains() {
  previous_ifs=$IFS
  IFS=:
  for path_entry in ${PATH:-}; do
    if [ "$path_entry" = "$1" ]; then
      IFS=$previous_ifs
      return 0
    fi
  done
  IFS=$previous_ifs
  return 1
}

echo "Installed development build: $target (and linked $bin_dir/spyjo and $bin_dir/spynel to it)"
if ! path_contains "$bin_dir"; then
  cat <<EOF

$bin_dir is not currently on PATH. Add this line to your shell profile:

  export PATH="$bin_dir:\$PATH"

Then open a new terminal. For the current terminal, run that export command once.
EOF
  exit 0
fi

resolved=$(command -v werkrby || true)
if [ "$resolved" != "$target" ]; then
  cat <<EOF

Warning: PATH currently resolves werkrby to ${resolved:-another location} before $target.
Move $bin_dir earlier in PATH to run this development build by name.
EOF
  exit 0
fi

echo "Ready: run 'werkrby' from this or any other terminal."

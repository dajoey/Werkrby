#!/bin/bash
# Worker bootstrap: source this. Fetches GITEA_TOKEN from Infisical at runtime
# into the environment + a 0700 askpass helper; nothing secret is written to
# the repo, argv, or logs. Requires an Infisical machine-identity env file
# defining INFISICAL_URL, INFISICAL_CLIENT_ID, INFISICAL_CLIENT_SECRET and
# INFISICAL_PROJECT_ID; point INFISICAL_IDENTITY_FILE at it.
set -e
IDENT="${INFISICAL_IDENTITY_FILE:?set INFISICAL_IDENTITY_FILE to the Infisical machine-identity env file}"
set -a; . "$IDENT"; set +a
TOK=$(curl -s -X POST "$INFISICAL_URL/api/v1/auth/universal-auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"clientId\":\"$INFISICAL_CLIENT_ID\",\"clientSecret\":\"$INFISICAL_CLIENT_SECRET\"}" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["accessToken"])')
# Vault coordinates come from the caller: override these for a different secret
# name, environment, or (URL-encoded) folder than the defaults below.
SECRET_NAME="${GITEA_TOKEN_SECRET:-GITEA_TOKEN}"
SECRET_ENV="${INFISICAL_ENV:-prod}"
SECRET_PATH="${INFISICAL_SECRET_PATH:?set INFISICAL_SECRET_PATH to the URL-encoded vault folder holding the token}"
export GITEA_TOKEN=$(curl -s "$INFISICAL_URL/api/v3/secrets/raw/$SECRET_NAME?workspaceId=$INFISICAL_PROJECT_ID&environment=$SECRET_ENV&secretPath=$SECRET_PATH" \
  -H "Authorization: Bearer $TOK" | python3 -c 'import sys,json;print(json.load(sys.stdin)["secret"]["secretValue"])')
[ -n "$GITEA_TOKEN" ] || { echo "bootstrap: empty $SECRET_NAME from $SECRET_ENV$SECRET_PATH" >&2; return 1 2>/dev/null || exit 1; }
unset TOK
AP=$(mktemp /dev/shm/askpass.XXXXXX)
cat > "$AP" <<"ASKPASS"
#!/bin/sh
case "$1" in *sername*) printf "%s" "${GIT_USERNAME:-git}";; *) printf "%s" "$GITEA_TOKEN";; esac
ASKPASS
chmod 700 "$AP"
export GIT_ASKPASS="$AP" GIT_TERMINAL_PROMPT=0
trap 'rm -f "$AP"' EXIT

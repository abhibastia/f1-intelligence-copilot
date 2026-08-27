#!/usr/bin/env bash
#
# Shared preflight for every script in this directory. Source it, don't run it.
#
#     source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
#
# It exists for one reason: these scripts used to default to PROFILE=abhi, which
# meant a fork ran against a workspace its author had never heard of, or failed
# with an auth error that named a stranger's profile. There is no default here
# and there will not be one — the profile is named explicitly or the script
# stops and shows you what is available.

set -euo pipefail

bold() { printf '\n\033[1m▸ %s\033[0m\n' "$*"; }
ok()   { printf '  \033[32m✓\033[0m %s\n' "$*"; }
warn() { printf '  \033[33m!\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*"; }

# Config, all overridable from the environment. `.env.example` documents them.
CATALOG="${F1_CATALOG:-f1}"
VOLUME_SCHEMA="${F1_VOLUME_SCHEMA:-raw}"
VOLUME_NAME="${F1_VOLUME_NAME:-landing}"
VOLUME_PATH="/Volumes/${CATALOG}/${VOLUME_SCHEMA}/${VOLUME_NAME}"
LOCAL_LANDING="${F1_LOCAL_LANDING:-./landing}"
TARGET="${F1_TARGET:-dev}"

MIN_CLI_VERSION="0.292.0"

require_cli() {
  if ! command -v databricks >/dev/null 2>&1; then
    fail "the Databricks CLI is not installed"
    cat <<'EOF'

  Install it, then re-run:

      brew tap databricks/tap && brew install databricks     # macOS
      curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

EOF
    exit 1
  fi

  local version
  version=$(databricks --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
  # Sort-based comparison: the lowest of (installed, minimum) must be the
  # minimum for the installed version to be new enough.
  if [[ "$(printf '%s\n%s\n' "$MIN_CLI_VERSION" "$version" | sort -V | head -1)" != "$MIN_CLI_VERSION" ]]; then
    fail "CLI ${version} is older than the required ${MIN_CLI_VERSION}"
    echo "      Bundles and the aitools commands used here need the newer CLI."
    exit 1
  fi
  ok "CLI ${version}"
}

# Resolve the profile from --profile, DATABRICKS_PROFILE, or
# DATABRICKS_CONFIG_PROFILE. Never guesses.
resolve_profile() {
  PROFILE="${DATABRICKS_PROFILE:-${DATABRICKS_CONFIG_PROFILE:-}}"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --profile) PROFILE="${2:-}"; shift 2 ;;
      --profile=*) PROFILE="${1#*=}"; shift ;;
      *) shift ;;
    esac
  done

  if [[ -z "$PROFILE" ]]; then
    fail "no Databricks profile given"
    cat <<EOF

  Name one explicitly — this script will not pick for you:

      $0 --profile <name>
      export DATABRICKS_PROFILE=<name>      # for the whole session

  Profiles configured on this machine:

EOF
    databricks auth profiles 2>/dev/null | sed 's/^/      /' || echo "      (none — run: databricks auth login --host <url> --profile <name>)"
    echo
    exit 1
  fi
  export PROFILE
}

require_auth() {
  if ! databricks current-user me --profile "$PROFILE" >/dev/null 2>&1; then
    fail "profile '${PROFILE}' cannot reach a workspace"
    echo "      Re-authenticate:  databricks auth login --profile ${PROFILE}"
    exit 1
  fi
  local who host
  who=$(databricks current-user me --profile "$PROFILE" -o json | python3 -c 'import sys,json;print(json.load(sys.stdin).get("userName","?"))')
  host=$(databricks auth env --profile "$PROFILE" 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin)["env"].get("DATABRICKS_HOST","?"))' 2>/dev/null || echo "?")
  ok "authenticated as ${who} (${host})"
}

# Everything above, in the order every script needs it.
preflight() {
  require_cli
  resolve_profile "$@"
  require_auth
}

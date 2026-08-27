#!/usr/bin/env bash
#
# One command from a fresh clone to a working platform.
#
#     ./scripts/bootstrap.sh --profile <name>
#
# Steps, in the only order that works:
#
#   1. preflight      CLI present and new enough, profile named, auth valid
#   2. local tests    pure-Python; catches a broken checkout before anything costs money
#   3. catalog        catalog, four schemas, landing Volume
#   4. raw data       upload ./landing if present, otherwise backfill from the API first
#   5. deploy         validate and deploy the bundle
#   6. run            unit tests → ingest → pipeline → validation  (asks first)
#
# Step 6 is the only one that consumes Databricks compute, and on Free Edition
# compute is a daily allowance rather than a bill. It prompts before starting,
# and `--yes` answers the prompt for CI. Steps 1-5 are free and idempotent, so
# re-running after a failure costs nothing.
#
# Flags:
#   --profile <name>   Databricks CLI profile (required; no default, ever)
#   --yes              don't prompt before spending compute
#   --skip-backfill    fail rather than pull from the API if ./landing is empty
#   --target <name>    bundle target (default: dev)

source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

RUN_YES=0
SKIP_BACKFILL=0
ARGS=("$@")
for i in "${!ARGS[@]}"; do
  case "${ARGS[$i]}" in
    --yes)           RUN_YES=1 ;;
    --skip-backfill) SKIP_BACKFILL=1 ;;
    --target)        TARGET="${ARGS[$((i+1))]:-dev}" ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# ─────────────────────────── 1. preflight ────────────────────────────
bold "Preflight"
preflight "$@"
command -v python3 >/dev/null || { fail "python3 not found"; exit 1; }
ok "python3 $(python3 -V 2>&1 | awk '{print $2}')"

# ─────────────────────────── 2. local tests ──────────────────────────
bold "Local tests"
if python3 -c 'import pytest' 2>/dev/null; then
  if python3 -m pytest; then
    ok "test suite passed"
  else
    fail "tests failed — fix these before spending compute"
    exit 1
  fi
else
  warn "pytest not installed; skipping. Install with:"
  echo "      python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt"
fi
python3 scripts/check_expectations.py

# ─────────────────────────── 3. catalog ──────────────────────────────
bold "Catalog, schemas, volume"
./scripts/create_catalog.sh --profile "$PROFILE"

# ─────────────────────────── 4. raw data ─────────────────────────────
bold "Raw data"
LOCAL_FILES=$(find "$LOCAL_LANDING" -name '*.json' 2>/dev/null | wc -l | tr -d ' ')
VOLUME_DIRS=$(databricks fs ls "dbfs:${VOLUME_PATH}" --profile "$PROFILE" 2>/dev/null | wc -l | tr -d ' ')

if [[ "$LOCAL_FILES" -eq 0 ]]; then
  if [[ "$SKIP_BACKFILL" -eq 1 ]]; then
    fail "${LOCAL_LANDING} is empty and --skip-backfill was passed"
    exit 1
  fi
  warn "no local landing data — backfilling from the API"
  echo "      This takes roughly 40 minutes: the laps endpoint is ~11 pages per"
  echo "      race and Jolpica's sustained limit is 450 requests/hour. It uses"
  echo "      no Databricks compute, and re-running skips closed rounds."
  python3 src/ingestion/ingest.py --mode backfill --root "$LOCAL_LANDING"
  LOCAL_FILES=$(find "$LOCAL_LANDING" -name '*.json' | wc -l | tr -d ' ')
fi
ok "${LOCAL_FILES} local payload(s)"

# Always upload. The Files API costs no compute, and the writer's own file
# naming makes a re-upload a no-op for anything already there.
./scripts/upload_landing.sh --profile "$PROFILE" "$LOCAL_LANDING"

# ─────────────────────────── 5. deploy ───────────────────────────────
bold "Deploy"
databricks bundle validate --strict -t "$TARGET" --profile "$PROFILE"
databricks bundle deploy -t "$TARGET" --profile "$PROFILE"
ok "bundle deployed to target '${TARGET}'"

# ─────────────────────────── 6. run ──────────────────────────────────
bold "Run"
if [[ "$RUN_YES" -ne 1 ]]; then
  cat <<EOF
  The next step runs f1_end_to_end: unit tests → ingest → pipeline → validation.
  This is the only step that consumes your Databricks compute allowance.
EOF
  read -r -p "  Run it now? [y/N] " answer
  [[ "$answer" =~ ^[Yy] ]] || { warn "skipped"; echo; databricks bundle summary -t "$TARGET" --profile "$PROFILE"; exit 0; }
fi

databricks bundle run f1_end_to_end -t "$TARGET" --profile "$PROFILE"

bold "Done"
databricks bundle summary -t "$TARGET" --profile "$PROFILE"

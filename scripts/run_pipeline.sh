#!/usr/bin/env bash
#
# Starts a pipeline update and polls THAT update to a terminal state.
#
# Polling the update rather than the pipeline matters: the top-level pipeline
# state flips back to RUNNING on retry, so a loop watching the pipeline can spin
# past a real FAILED forever.
#
# On failure it prints error.exceptions[0].message — the top-level message only
# ever says "Update X is FAILED", which tells you nothing.
#
# Usage:
#     ./scripts/run_pipeline.sh <pipeline_id> [--full-refresh] --profile <name>

source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
preflight "$@"

PIPELINE_ID="${1:?usage: run_pipeline.sh <pipeline_id> [--full-refresh] --profile <name>}"
shift || true

REFRESH_FLAG=()
if [[ "${1:-}" == "--full-refresh" ]]; then
  echo "⚠️  Full refresh reprocesses streaming sources from scratch and destroys"
  echo "    streaming state. Ctrl-C now if that was not intended."
  sleep 5
  REFRESH_FLAG=(--full-refresh)
fi

UPDATE_ID=$(databricks pipelines start-update "$PIPELINE_ID" "${REFRESH_FLAG[@]}" \
  --profile "$PROFILE" -o json \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["update_id"])')

echo "update ${UPDATE_ID} started"

while :; do
  STATE=$(databricks pipelines get-update "$PIPELINE_ID" "$UPDATE_ID" --profile "$PROFILE" -o json \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["update"]["state"])')
  printf '%s  state=%s\n' "$(date +%H:%M:%S)" "$STATE"
  case "$STATE" in
    COMPLETED) echo "✅ update completed"; exit 0 ;;
    FAILED|CANCELED) break ;;
  esac
  sleep 30
done

echo
echo "❌ update ${STATE} — real errors below:"
databricks pipelines list-pipeline-events "$PIPELINE_ID" --profile "$PROFILE" -o json \
  | python3 -c '
import sys, json
events = json.load(sys.stdin)
errors = [e for e in events if e.get("level") == "ERROR"]
for e in errors[:5]:
    exceptions = e.get("error", {}).get("exceptions", [])
    body = exceptions[0]["message"] if exceptions else "(no exception body)"
    print("─" * 70)
    print("event   :", e.get("event_type"))
    print("summary :", (e.get("message") or "")[:200])
    print("cause   :", body[:1200])
if not errors:
    print("no ERROR-level events found")
'
exit 1

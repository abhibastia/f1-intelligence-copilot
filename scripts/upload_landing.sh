#!/usr/bin/env bash
#
# Uploads locally-backfilled raw JSON into the UC Volume.
#
# This is the one deployment step the bundle does not own. DABs manage the
# pipeline, job and dashboard; bulk data upload is not a bundle concern, and the
# catalog/schemas/volume are created by create_catalog.sh because Free Edition
# refuses catalog creation over the API.
#
# The backfill runs locally rather than as a job on purpose: Free Edition has a
# daily compute quota, and the Files API upload consumes none of it.
#
# Usage:
#     python3 src/ingestion/ingest.py --mode backfill --root ./landing
#     ./scripts/upload_landing.sh --profile <name> [local_dir]

source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
preflight "$@"

# The source directory is the first bare argument. `--profile <name>` is
# consumed by preflight, so its value must not be mistaken for a path.
LOCAL_DIR="$LOCAL_LANDING"
skip_next=0
for arg in "$@"; do
  if [[ $skip_next -eq 1 ]]; then skip_next=0; continue; fi
  case "$arg" in
    --profile) skip_next=1 ;;
    --*)       ;;
    *)         LOCAL_DIR="$arg" ;;
  esac
done

if [[ ! -d "$LOCAL_DIR" ]]; then
  echo "ERROR: '${LOCAL_DIR}' not found. Run the backfill first:" >&2
  echo "  python3 src/ingestion/ingest.py --mode backfill --root ${LOCAL_DIR}" >&2
  exit 1
fi

LOCAL_COUNT=$(find "$LOCAL_DIR" -name '*.json' | wc -l | tr -d ' ')
echo "▸ Uploading ${LOCAL_COUNT} files from ${LOCAL_DIR} → ${VOLUME_PATH}"

# The dbfs: prefix is required even for UC Volume paths.
databricks fs cp -r --overwrite "$LOCAL_DIR" "dbfs:${VOLUME_PATH}" --profile "$PROFILE"

echo "▸ Endpoint directories now in the volume:"
databricks fs ls "dbfs:${VOLUME_PATH}" --profile "$PROFILE" | sed 's/^/    /'

cat <<EOF

Next:
  databricks bundle deploy -t ${TARGET} --profile ${PROFILE}
  databricks bundle run f1_end_to_end -t ${TARGET} --profile ${PROFILE}
EOF

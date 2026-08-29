#!/usr/bin/env bash
# Land harvested text and weather into the Unity Catalog Volume, beside the
# Jolpica endpoints the ingestion job writes.
#
# WHY THIS EXISTS
# Race reports and weather originally went straight from the harvester into
# Lakebase, skipping the lakehouse entirely - so structured data had Volume
# landing, Bronze/Silver/Gold, expectations and lineage, while unstructured data
# had none of it. That asymmetry was historical, not designed.
#
# This puts the raw text in the Volume where the rest of the raw data lives, so
# the lakehouse holds every source in its original form and a pipeline stage can
# consume it later. It uses the Files API - no compute.
#
# Usage:
#     ./scripts/land_text_to_volume.sh --profile <name>

source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
preflight "$@"

VOL="dbfs:${VOLUME_PATH}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Landing text sources into $VOL"
for pair in "race_reports:wikipedia" "race_weather:weather" "races:race_spine"; do
  src="${pair%%:*}"; dest="${pair##*:}"
  file="$ROOT/data/${src}.json"
  [ -f "$file" ] || { echo "  skip $src (not harvested)"; continue; }
  databricks fs mkdir "$VOL/$dest" --profile "$PROFILE" 2>/dev/null || true
  databricks fs cp "$file" "$VOL/$dest/${src}.json" --overwrite --profile "$PROFILE"
  echo "  $src -> $VOL/$dest/${src}.json ($(du -h "$file" | cut -f1))"
done

# Pit stops are per-race files; land them under their own endpoint prefix so the
# partitioning matches the Jolpica endpoints already in the Volume.
if [ -d "$ROOT/data/pitstops" ]; then
  databricks fs mkdir "$VOL/pitstops_detail" --profile "$PROFILE" 2>/dev/null || true
  databricks fs cp -r "$ROOT/data/pitstops" "$VOL/pitstops_detail" --overwrite --profile "$PROFILE"
  echo "  pitstops -> $VOL/pitstops_detail/ ($(ls "$ROOT/data/pitstops" | wc -l | tr -d ' ') files)"
fi

echo
databricks fs ls "$VOL" --profile "$PROFILE"

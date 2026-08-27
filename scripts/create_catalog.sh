#!/usr/bin/env bash
#
# Creates the catalog, the four schemas, and the landing Volume.
#
# Idempotent — every step tolerates objects that already exist, so re-running
# after a partial failure or a quota reset is safe.
#
# On Databricks Free Edition the catalog is the awkward part. Three creation
# paths exist and they fail differently, so the script tries all of them in
# increasing order of cost before falling back to manual instructions:
#
#   1. CLI, plain            — no compute. Fails: metastore has no storage root.
#   2. CLI + storage root    — no compute. Fails: "use the UI with Default Storage".
#   3. SQL CREATE CATALOG    — needs a warehouse, so it needs daily quota.
#
# Path 3 is the one worth retrying after a quota reset: it has never actually
# been rejected, only blocked by an unavailable warehouse.
#
# Schemas and Volumes have no such restriction — plain UC API calls, no compute.
#
# Usage:
#     ./scripts/create_catalog.sh --profile <name>

source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
preflight "$@"

SCHEMAS=(raw bronze silver gold)

catalog_exists() {
  databricks catalogs get "$CATALOG" --profile "$PROFILE" >/dev/null 2>&1
}

# ─────────────────────────────── catalog ────────────────────────────────
bold "Catalog: ${CATALOG}"

if catalog_exists; then
  ok "already exists"
else
  # Path 1 — plain CLI create.
  if databricks catalogs create "$CATALOG" \
       --comment "Formula 1 Race Intelligence capstone" \
       --profile "$PROFILE" >/dev/null 2>&1; then
    ok "created via CLI"
  else
    warn "CLI create refused (expected on Free Edition) — trying with storage root"

    # Path 2 — reuse the metastore's default storage root, read from any
    # existing catalog. Managed catalogs on Default Storage all share one.
    STORAGE_ROOT=$(
      databricks catalogs list --profile "$PROFILE" -o json 2>/dev/null \
      | python3 -c '
import sys, json
for c in json.load(sys.stdin):
    if c.get("catalog_type") == "MANAGED_CATALOG" and c.get("storage_root"):
        print(c["storage_root"]); break
' || true)

    if [[ -n "$STORAGE_ROOT" ]] && databricks catalogs create "$CATALOG" \
         --comment "Formula 1 Race Intelligence capstone" \
         --storage-root "$STORAGE_ROOT" \
         --profile "$PROFILE" >/dev/null 2>&1; then
      ok "created via CLI with storage root"
    else
      warn "storage-root create refused — trying SQL (needs warehouse quota)"

      # Path 3 — SQL. Blocked by quota rather than by policy, so this is the
      # one to retry tomorrow.
      SQL_OUT=$(databricks experimental aitools tools query \
        "CREATE CATALOG IF NOT EXISTS ${CATALOG} COMMENT 'Formula 1 Race Intelligence capstone'" \
        --profile "$PROFILE" 2>&1 || true)

      if catalog_exists; then
        ok "created via SQL"
      else
        fail "all three paths failed"
        echo "      reason: ${SQL_OUT}" | head -2
        cat <<EOF

  ────────────────────────────────────────────────────────────────
  Create the catalog by hand, then re-run this script:

      Databricks UI → Catalog → Create catalog
          Name         : ${CATALOG}
          Storage type : Default storage

  If the failure above mentions a daily limit, the SQL path is not
  refused — only rate-limited. Re-running tomorrow may succeed
  without touching the UI at all.
  ────────────────────────────────────────────────────────────────
EOF
        exit 1
      fi
    fi
  fi
fi

# ─────────────────────────────── schemas ────────────────────────────────
# Plain UC API calls — no compute, so these work regardless of quota.
bold "Schemas"
for schema in "${SCHEMAS[@]}"; do
  if databricks schemas get "${CATALOG}.${schema}" --profile "$PROFILE" >/dev/null 2>&1; then
    ok "${CATALOG}.${schema} already exists"
  elif databricks schemas create "$schema" "$CATALOG" --profile "$PROFILE" >/dev/null 2>&1; then
    ok "${CATALOG}.${schema} created"
  else
    fail "${CATALOG}.${schema} could not be created"
    databricks schemas create "$schema" "$CATALOG" --profile "$PROFILE" 2>&1 | head -3
    exit 1
  fi
done

# ──────────────────────────────── volume ────────────────────────────────
bold "Volume"
VOLUME_FQN="${CATALOG}.${VOLUME_SCHEMA}.${VOLUME_NAME}"
if databricks volumes read "$VOLUME_FQN" --profile "$PROFILE" >/dev/null 2>&1; then
  ok "${VOLUME_FQN} already exists"
elif databricks volumes create "$CATALOG" "$VOLUME_SCHEMA" "$VOLUME_NAME" MANAGED \
       --profile "$PROFILE" >/dev/null 2>&1; then
  ok "${VOLUME_FQN} created"
else
  fail "${VOLUME_FQN} could not be created"
  databricks volumes create "$CATALOG" "$VOLUME_SCHEMA" "$VOLUME_NAME" MANAGED \
    --profile "$PROFILE" 2>&1 | head -3
  exit 1
fi

# ─────────────────────────────── verify ─────────────────────────────────
bold "Verifying"
databricks schemas list "$CATALOG" --profile "$PROFILE" 2>/dev/null | head -10
echo
ok "landing path: /Volumes/${CATALOG}/${VOLUME_SCHEMA}/${VOLUME_NAME}"

cat <<EOF

Next:
  ./scripts/upload_landing.sh
EOF

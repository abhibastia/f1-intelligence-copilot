#!/usr/bin/env bash
# Copy the shared f1lake package into an app directory.
#
# A Databricks App deploys from a SINGLE source path, so a sibling package is
# not importable at runtime. Rather than maintaining a second copy by hand -
# which is exactly how two modules drift until one silently breaks - f1lake/ is
# the single source of truth and gets copied in at build time.
set -euo pipefail
APP_DIR="${1:?usage: build_app.sh <app_dir>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

rm -rf "$ROOT/$APP_DIR/f1lake"
mkdir -p "$ROOT/$APP_DIR/f1lake"
# Only the runtime modules. load.py and seed_gold.py are local-only ingestion
# tooling and would drag pandas-scale dependencies into the app image.
for m in __init__.py schema.py embedder.py; do
  cp "$ROOT/f1lake/$m" "$ROOT/$APP_DIR/f1lake/$m"
done

# f1_broker is shared too: the MCP server exposes these functions over the
# protocol, and the frontend's in-process agent calls the same ones. One copy
# means a bug is a bug in both paths rather than a divergence between them.
if [ "$APP_DIR" != "mcp_server" ]; then
  cp "$ROOT/mcp_server/f1_broker.py" "$ROOT/$APP_DIR/f1_broker.py"
fi
echo "copied shared modules -> $APP_DIR ($(ls "$ROOT/$APP_DIR/f1lake" | tr '\n' ' '))"

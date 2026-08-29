#!/usr/bin/env bash
# Render the mermaid diagrams in README.md and docs/*.md to PNG.
#
# Mermaid renders on GitHub, but not in a plain editor - so a reader who opens
# README.md as a plain file sees a code block where the architecture should
# be. These PNGs are that fallback.
#
# Generated rather than screenshotted: the source stays the markdown, so the
# images cannot drift from it, and there is no browser chrome or scaling loss.
#
#   npm install -g @mermaid-js/mermaid-cli
#   ./scripts/render_diagrams.sh [output_dir]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:-$ROOT/docs/diagrams}"
mkdir -p "$OUT"
command -v mmdc >/dev/null || { echo "mmdc not found: npm i -g @mermaid-js/mermaid-cli" >&2; exit 1; }

render() {  # <source.md> <name> <width>
  local src="$1" name="$2" width="$3" tmp
  tmp="$(mktemp -t "$name").mmd"
  python3 - "$ROOT/$src" "$tmp" <<'PY'
import pathlib, re, sys
m = re.search(r"```mermaid\n(.*?)```", pathlib.Path(sys.argv[1]).read_text(), re.S)
if not m:
    raise SystemExit(f"no mermaid block in {sys.argv[1]}")
pathlib.Path(sys.argv[2]).write_text(m.group(1))
PY
  # White background: a transparent PNG turns black wherever this gets pasted.
  mmdc -i "$tmp" -o "$OUT/$name.png" -b white -w "$width" >/dev/null 2>&1
  echo "  $OUT/$name.png"
}

render README.md              architecture-system  2200
render docs/architecture.md   architecture-pipeline 1600
render docs/copilot_design.md architecture-copilot  1600

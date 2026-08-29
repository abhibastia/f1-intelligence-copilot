# Screenshots

Captured manually — the two Databricks Apps here (`app/`, `mcp_server/`) and
the AI/BI dashboard all sit behind Databricks OAuth on Free Edition, with no
public URL, so there's no way to automate this (no headless browser carries a
Databricks identity). Referenced from the gallery near the top of the root
`README.md`.

| File | Where | What it shows |
|---|---|---|
| `app-finding.png` | `f1-intelligence-ui`, nav **01 The finding** | The wet-race-vs-wet-day narrative, with the Copilot chat rail answering a real question (2024 São Paulo GP) — RAG grounded in race-report prose. |
| `app-season-explorer.png` | nav **03 Season explorer** | The 2026 season table with weather populated for every completed round, including round 12 (Dutch GP) — the round whose ERA5 lag this project's own runbook debugging fixed. |
| `app-saved-items.png` | nav **06 What it saved** | Watchlist, predictions and race notes — everything the agent's write tools have persisted to Lakebase during chat. |
| `dashboard-championship.png` | AI/BI dashboard, **Championship Swing** page | One of the dashboard's 7 decision pages, read straight off Gold. |
| `dashboard-activity.png` | AI/BI dashboard, **Assistant Activity** page | Tool-call counts, writes, errors, per-tool breakdown — fed by the Change Data Feed loop from `agent_tool_calls`. The one view a static dashboard couldn't produce on its own. |

To refresh any of these: same app, same nav item, same ~1440px browser
width, overwrite the file at the same path — no other edits needed, the
README already points at these exact filenames.

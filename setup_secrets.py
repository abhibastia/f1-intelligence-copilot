"""One-time setup: create the Databricks secret scope this project reads from.

    python3 setup_secrets.py [--profile <profile>]

READ THIS BEFORE RUNNING IT
---------------------------
**Nothing in this repository requires a credential today.** Jolpica-F1 and the
Open-Meteo ERA5 archive are both keyless, and every other call goes through the
Databricks CLI's own auth. You can build, deploy and run the whole platform
without ever running this script.

It exists for the two cases where that stops being true:

  * Jolpica issues API keys to lift the unauthenticated rate limits (0.5 req/s
    sustained, 450/hour — see `src/ingestion/config.py` for why those numbers
    are what they are). A key turns a 40-minute laps backfill into a short one.
  * Open-Meteo's commercial tier requires a key. The free archive endpoint this
    project uses does not.

Storing a key here does NOT wire it into the ingestion client — no code path
reads these secrets yet, deliberately, because guessing an auth scheme that has
not been published produces a client that fails in a confusing way. When you
have a real key and its documented header, read it in `jolpica_client.py` with:

    from databricks.sdk import WorkspaceClient
    key = WorkspaceClient().dbutils.secrets.get(scope="f1", key="jolpica-api-key")

Both prompts are skippable — press Enter to leave a secret unset. Re-running is
safe: the scope and every ACL are created only if missing, and re-entering a
value overwrites it.
"""

import argparse
import getpass
import sys

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import DatabricksError
    from databricks.sdk.service import workspace
except ImportError:
    sys.exit(
        "databricks-sdk is not installed. Run:\n"
        "    pip install -r requirements-dev.txt"
    )

SCOPE = "f1"

# key -> what it is for. Both optional; see the module docstring.
SECRETS = {
    "jolpica-api-key": "Jolpica-F1 API key (lifts the 450 req/hour ceiling)",
    "open-meteo-api-key": "Open-Meteo commercial key (the free archive needs none)",
}


def ensure_scope(w: WorkspaceClient) -> None:
    """Create the scope, tolerating one that is already there."""
    existing = {s.name for s in w.secrets.list_scopes()}
    if SCOPE in existing:
        print(f"  scope '{SCOPE}' already exists")
        return
    w.secrets.create_scope(scope=SCOPE)
    print(f"  scope '{SCOPE}' created")


def put_secret(w: WorkspaceClient, key: str, description: str) -> bool:
    """Prompt for one secret. Empty input leaves it unset."""
    value = getpass.getpass(f"  {key} — {description}\n    (Enter to skip): ")
    if not value.strip():
        print("    skipped")
        return False
    w.secrets.put_secret(scope=SCOPE, key=key, string_value=value)
    print("    stored")
    return True


def ensure_acl(w: WorkspaceClient) -> None:
    """Let workspace users read the scope.

    On a single-user Free Edition workspace this changes nothing; on a shared
    workspace it is the difference between a job that runs and a job that
    cannot see its own configuration.
    """
    try:
        w.secrets.put_acl(
            scope=SCOPE,
            principal="users",
            permission=workspace.AclPermission.READ,
        )
        print("  read ACL granted to 'users'")
    except DatabricksError as exc:
        # Free Edition workspaces may not expose the `users` group.
        print(f"  read ACL not applied ({exc}) — fine on a single-user workspace")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        help="Databricks CLI profile. Omit to use DATABRICKS_CONFIG_PROFILE "
             "or the DEFAULT profile.",
    )
    args = parser.parse_args()

    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    print(f"▸ Workspace: {w.config.host}")

    ensure_scope(w)

    stored = [key for key, desc in SECRETS.items() if put_secret(w, key, desc)]

    if stored:
        ensure_acl(w)

    print()
    if stored:
        print(f"Stored: {', '.join(stored)}")
        print(f"Read one back with:  databricks secrets get-secret {SCOPE} <key>")
    else:
        print("No secrets stored — which is the expected outcome today.")
        print("Every data source this project uses is keyless.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

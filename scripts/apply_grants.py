"""Apply the Unity Catalog access model. Declarative, idempotent, no compute.

    python3 scripts/apply_grants.py --profile <name>              # apply
    python3 scripts/apply_grants.py --profile <name> --dry-run    # show the diff
    python3 scripts/apply_grants.py --profile <name> --show       # read back

WHY THIS EXISTS
---------------
The README opens by calling this a governed platform. Until this script, that
was a claim with nothing behind it: every object was owner-only by default, and
the only grant in the metastore was the BROWSE that Unity Catalog attaches to a
new catalog on its own. A governance story nobody can query is a slide, not a
control.

PRINCIPALS UNITY CATALOG WILL ACTUALLY ACCEPT
--------------------------------------------
Account-level groups and individual users. **Not workspace-local groups**: this
workspace lists `admins` and `users` under `databricks groups list`, and UC
rejects both with "Could not find principal with name admins". On Free Edition
that leaves `account users` — every user in the account — and named emails.

So the engineer tier is a parameter rather than a hardcoded group. Point it at
an account-level group in a real workspace; leave it unset here and Silver,
Bronze and the landing Volume stay owner-only, which is the correct outcome for
a single-user workspace and still demonstrates the layering.

THE MODEL
---------
Read access narrows as the data gets rawer, which is the whole argument for a
medallion layout:

    account users   Gold only         the marts are the product
    --engineers     + Silver, Bronze  debugging needs the layers below
    owner           everything        the pipeline's run-as identity

An analyst can query `f1.gold.driver_performance` and cannot see the Bronze
payload it came from, or the raw JSON in the landing Volume. That is the
demonstrable half of "governed by Unity Catalog" — not that permissions exist,
but that they differ by layer and you can prove which.

Note that USE CATALOG and USE SCHEMA are traversal, not reading: without them a
SELECT fails even when the table grant is present. Both are granted at the level
above, which is why `account users` gets USE CATALOG on `f1` but no schema
access outside Gold.

WHY NOT THE BUNDLE
------------------
DABs can express grants, but only on securables it owns, and this bundle
deliberately owns neither the catalog nor the schemas — Free Edition refuses
catalog creation over the API, and a `bundle destroy` that dropped the schemas
would take the data with it. Grants follow the objects they sit on, so they live
with `create_catalog.sh` on the scripts side of that boundary.

This uses the Unity Catalog permissions API through the CLI, so it costs no
compute: no warehouse starts, and it works with the daily quota exhausted.
"""

import argparse
import json
import subprocess
import sys

# Privileges are named the way Unity Catalog stores them: USE_CATALOG, not
# "USE CATALOG". The API accepts either and returns the underscored form, so
# comparing against the spaced form finds no match, reports every securable as
# needing a change, and re-applies on every run for ever.
CONSUMERS = "account users"


def access_model(
    catalog: str, engineers: str | None = None
) -> list[tuple[str, str, dict[str, list[str]]]]:
    """securable_type, full name, {principal: privileges}.

    Each entry is the complete intended state for that securable. The script
    diffs against what is there, so re-running changes nothing.
    """
    model: list[tuple[str, str, dict[str, list[str]]]] = [
        # Traversal only. Seeing that the catalog exists is not reading
        # anything in it.
        ("catalog", catalog, {CONSUMERS: ["USE_CATALOG"]}),

        # The marts are the product: the one layer a consumer reads, and the
        # only one the dashboard needs.
        ("schema", f"{catalog}.gold", {CONSUMERS: ["USE_SCHEMA", "SELECT"]}),

        # The metric view is the semantic layer over those marts. The schema
        # grant already covers it; naming it explicitly is what makes the
        # governed metric definitions visible in the access model rather than
        # inherited by accident.
        ("table", f"{catalog}.gold.driver_metrics", {CONSUMERS: ["SELECT"]}),
    ]

    if not engineers:
        return model

    model[0][2][engineers] = ["USE_CATALOG"]
    model[1][2][engineers] = ["USE_SCHEMA", "SELECT"]
    model += [
        # Deliberately not the consumer tier. Silver holds quarantine views and
        # pre-join facts; reading them without the marts' as-of joins is how
        # someone quietly reports a driver's points against the wrong team.
        ("schema", f"{catalog}.silver", {engineers: ["USE_SCHEMA", "SELECT"]}),

        # Debugging only. Bronze is raw payloads with no types.
        ("schema", f"{catalog}.bronze", {engineers: ["USE_SCHEMA", "SELECT"]}),

        ("schema", f"{catalog}.raw", {engineers: ["USE_SCHEMA"]}),

        # READ_VOLUME, never WRITE_VOLUME: the landing zone has exactly one
        # writer, the ingestion job. A second writer breaks the idempotency
        # contract that lets Silver deduplicate on the newest _ingest_ts.
        ("volume", f"{catalog}.raw.landing", {engineers: ["READ_VOLUME"]}),
    ]
    return model


def cli(args: list[str], profile: str) -> dict:
    result = subprocess.run(
        ["databricks", *args, "--profile", profile, "-o", "json"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return json.loads(result.stdout or "{}")


def current(securable: str, name: str, profile: str) -> dict[str, set[str]]:
    got = cli(["grants", "get", securable, name], profile)
    return {
        a["principal"]: set(a.get("privileges", []))
        for a in got.get("privilege_assignments", [])
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, help="Databricks CLI profile")
    parser.add_argument("--catalog", default="f1")
    parser.add_argument(
        "--engineers",
        help="Account-level group or user email granted Silver, Bronze and the "
             "landing Volume. Workspace-local groups are rejected by Unity "
             "Catalog. Omit on a single-user workspace: those layers then stay "
             "owner-only, which is the right answer there.",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="print what would change and exit")
    parser.add_argument("--show", action="store_true",
                        help="print effective grants and exit")
    args = parser.parse_args()

    model = access_model(args.catalog, args.engineers)

    if args.show:
        for securable, name, _ in model:
            print(f"\n{securable} {name}")
            for principal, privileges in sorted(current(securable, name, args.profile).items()):
                print(f"  {principal:20s} {', '.join(sorted(privileges))}")
        return 0

    changed = 0
    for securable, name, intended in model:
        try:
            existing = current(securable, name, args.profile)
        except RuntimeError as exc:
            print(f"  SKIP  {securable} {name}: {exc}")
            continue

        changes = []
        for principal, privileges in intended.items():
            missing = sorted(set(privileges) - existing.get(principal, set()))
            if missing:
                changes.append({"principal": principal, "add": missing})

        if not changes:
            print(f"  ok    {securable} {name}")
            continue

        summary = "; ".join(f"{c['principal']} += {', '.join(c['add'])}" for c in changes)
        if args.dry_run:
            print(f"  WOULD {securable} {name}: {summary}")
            changed += 1
            continue

        cli(["grants", "update", securable, name,
             "--json", json.dumps({"changes": changes})], args.profile)
        print(f"  SET   {securable} {name}: {summary}")
        changed += 1

    print(f"\n{changed} securable(s) {'would change' if args.dry_run else 'changed'}")
    print("Re-running is safe: only missing privileges are added.")
    return 0


if __name__ == "__main__":
    _status = main()
    if _status:
        sys.exit(_status)

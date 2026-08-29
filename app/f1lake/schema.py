"""
Lakebase connection helper and the authoritative schema.

Lakebase is Databricks-managed Postgres. It is the OPERATIONAL half of this
project: the agent reads from it and, crucially, writes to it. The medallion
Gold marts in Delta remain the analytical half. Two stores, one shared key -
every table here carries (season, round), the same key `f1.silver.dim_race`
uses, so a semantic hit in a race report pivots straight into that race's
results.

WHY THE DDL LIVES HERE
----------------------
`databricks psql` connects as the workspace identity, while this code and the
deployed apps connect as the native Postgres role inside the secret URL.
Postgres grants table ownership to whoever ran CREATE TABLE. Tables created via
psql would be readable by the app but never alterable or indexable by it, which
surfaces much later as a 42501 on CREATE INDEX. Creating them here means the
role that reads and writes also owns.

WHY pg8000, NOT psycopg2
-------------------------
psycopg2-binary bundles its own compiled OpenSSL/libpq. On Databricks
serverless job compute the kernel already has other native extensions loaded
(pandas, pyarrow, grpc) before this module ever imports - `import psycopg2`
there SIGABRTs immediately, deterministically, before a single query runs. It
worked locally and in the Databricks Apps runtime, which is why it went
undetected until jobs/run_harvest.py and jobs/run_seed_gold.py were actually
run as jobs. pg8000 is pure Python - no compiled extension, so no ABI/symbol
collision is possible - and DBAPI-compatible enough (`%s` paramstyle, the same
exception hierarchy shape) that every call site above this module is
unchanged; only this module's connection and cursor handling differ.
"""

import base64
import os
from contextlib import contextmanager
from urllib.parse import urlparse

from pg8000 import dbapi

_SCOPE = os.environ.get("LAKEBASE_SECRET_SCOPE", "database")
_KEY = os.environ.get("LAKEBASE_SECRET_KEY", "lakebase-url")

# all-MiniLM-L6-v2. Must match the embedding model and the VECTOR(n) column;
# pgvector rejects an insert whose dimensionality differs from the column.
EMBEDDING_DIM = int(os.environ.get("EMBEDDING_DIM", "384"))

DOCUMENTS = "f1_documents"
EMBEDDINGS = "f1_embeddings"
WEATHER = "f1_race_weather"
RACES = "f1_races"
WATCHLIST = "f1_watchlist"
PREDICTIONS = "f1_predictions"
NOTES = "f1_race_notes"
TOOL_CALLS = "agent_tool_calls"
PIT_STOPS = "f1_pit_stops"
STINTS = "f1_stints"

# Seeded from Gold marts that didn't exist in the standalone copilot project —
# see f1lake/seed_gold.py. Field-comparison strategy (stops vs. the rest of the
# field), race pace independent of finishing position, and the constructors'
# championship, none of which the per-stint STINTS/PIT_STOPS pair above can
# answer on its own.
RACE_STRATEGY_SUMMARY = "f1_race_strategy_summary"
LAP_PACE = "f1_lap_pace"
CONSTRUCTOR_STANDINGS = "f1_constructor_standings"

_cached_url: str | None = None


def lakebase_url() -> str:
    """Resolve the Postgres URL, preferring an explicit env var for local dev."""
    global _cached_url
    if _cached_url is not None:
        return _cached_url

    explicit = os.environ.get("LAKEBASE_URL")
    if explicit:
        _cached_url = explicit
        return _cached_url

    # Imported lazily so LAKEBASE_URL alone is enough to run with no Databricks
    # auth configured at all.
    from databricks.sdk import WorkspaceClient

    secret = WorkspaceClient().secrets.get_secret(scope=_SCOPE, key=_KEY)
    _cached_url = base64.b64decode(secret.value).decode("utf-8")
    return _cached_url


def safe_message(exc: Exception) -> str:
    """An exception message that is safe to show a user.

    The driver puts the connection target in its errors - `could not
    translate host name "ep-....cloud.databricks.com"`, and for an auth
    failure the role name too. Those strings travel: a tool failure becomes an
    "error" field in the tool result, which the agent repeats and the UI
    renders in the trace. A Lakebase outage would therefore print the database
    hostname into every visitor's browser.

    Errors we raise ourselves are written for users and pass through unchanged.
    Anything else is replaced; the detail is still logged server-side, where it
    belongs.
    """
    if isinstance(exc, (ValueError, LookupError)):
        return str(exc)
    if isinstance(exc, dbapi.Error):
        return "The database is unavailable."
    return "The request could not be completed."


def _dictify(cur, rows) -> list[dict]:
    """pg8000 cursors return plain tuples; every caller here expects dicts, the
    shape RealDictCursor gave under psycopg2."""
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in rows]


@contextmanager
def cursor(conn):
    """pg8000 cursors, unlike psycopg2's, don't implement the context manager
    protocol directly - this restores `with schema.cursor(conn) as cur:` for
    every call site outside this module without rewriting each one's body."""
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


@contextmanager
def connection():
    url = urlparse(lakebase_url())
    conn = dbapi.connect(
        host=url.hostname,
        port=url.port or 5432,
        database=url.path.lstrip("/"),
        user=url.username,
        password=url.password,
        ssl_context=True,
    )
    try:
        yield conn
    finally:
        conn.close()


def query(sql: str, params: tuple | dict | None = None) -> list[dict]:
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        rows = _dictify(cur, cur.fetchall())
        cur.close()
        return rows


def returning(sql: str, params: tuple | dict | None = None) -> list[dict]:
    """Run a writing statement with a RETURNING clause, and COMMIT it.

    Distinct from query() on purpose. An INSERT ... RETURNING run through
    query() looks entirely successful - the RETURNING clause hands back the new
    row, id and all - but query() never commits, so closing the connection rolls
    the write back. The caller sees a row it will never be able to read again.

    That is a silent data-loss bug: an agent would tell a user "saved" and the
    database would disagree. Writes go through this function; reads go through
    query(); the two are not interchangeable.
    """
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        rows = _dictify(cur, cur.fetchall())
        conn.commit()
        cur.close()
        return rows


def log_tool_call(
    tool_name: str,
    arguments: dict,
    outcome: str,
    is_write: bool = False,
    summary: str | None = None,
    duration_ms: int | None = None,
    session_id: str | None = None,
) -> None:
    """Record one agent tool call. Never raises.

    This is the source of the Change Data Feed loop: rows land here, CDF carries
    them into a Delta analytics table, and the app reads that back to show what
    the agent actually did.

    Telemetry must never break the thing it observes. If this insert fails the
    tool call has already succeeded, and an agent that cannot answer because a
    logging write failed is strictly worse than one with a gap in its logs.
    """
    import json as _json

    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""INSERT INTO {TOOL_CALLS}
                    (session_id, tool_name, is_write, arguments, outcome,
                     summary, duration_ms)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (
                    session_id,
                    tool_name,
                    is_write,
                    _json.dumps(arguments, default=str),
                    outcome,
                    summary,
                    duration_ms,
                ),
            )
            conn.commit()
            cur.close()
    except Exception:
        import logging as _logging

        _logging.getLogger("schema").warning(
            "tool-call logging failed; the tool itself was unaffected", exc_info=True
        )


def execute(sql: str, params: tuple | dict | None = None) -> int:
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        conn.commit()
        rowcount = cur.rowcount
        cur.close()
        return rowcount


def execute_values(
    cur, sql: str, argslist: list[tuple], template: str | None = None, page_size: int = 100
) -> None:
    """Batch-insert many rows in one statement each, psycopg2.extras.execute_values'
    call shape - `sql` contains a literal `VALUES %s` that gets expanded to
    `VALUES (%s,...),(%s,...),...` per page, with `template` overriding the
    per-row placeholder group (e.g. to add a `::vector` cast or a literal
    `now()`). Reimplemented here, rather than imported, because pg8000 (see the
    module docstring for why it replaced psycopg2) has no bulk-values helper -
    every call site elsewhere in f1lake was written against psycopg2.extras'
    signature and is unchanged by this swap.
    """
    if not argslist:
        return
    if "%s" not in sql:
        raise ValueError("execute_values() requires a literal VALUES %s in sql")

    for start in range(0, len(argslist), page_size):
        page = argslist[start : start + page_size]
        if template:
            group = template
        else:
            group = "(" + ",".join(["%s"] * len(page[0])) + ")"
        values_sql = ",".join([group] * len(page))
        flat_params = [v for row in page for v in row]
        cur.execute(sql.replace("%s", values_sql, 1), flat_params)


DDL = [
    "CREATE EXTENSION IF NOT EXISTS vector",
    # Name resolution depends on this. resolve_race and resolve_driver fold
    # accents so "Sao Paulo" matches "São Paulo" and "Perez" matches "Pérez" -
    # four call sites in f1_broker. It was installed by hand while fixing that
    # and never added here, so a database created from this DDL alone had
    # pgvector but no unaccent: the app would deploy, look healthy, and fail
    # every single tool call that resolves a name.
    "CREATE EXTENSION IF NOT EXISTS unaccent",
    # --- reference: the race spine, mirrored from Gold ------------------------
    f"""
    CREATE TABLE IF NOT EXISTS {RACES} (
        season          INT NOT NULL,
        round           INT NOT NULL,
        race_name       TEXT NOT NULL,
        race_date       DATE,
        circuit_id      TEXT,
        circuit_name    TEXT,
        circuit_country TEXT,
        circuit_lat     DOUBLE PRECISION,
        circuit_long    DOUBLE PRECISION,
        wikipedia_url   TEXT,
        PRIMARY KEY (season, round)
    )
    """,
    # --- unstructured: race reports ------------------------------------------
    f"""
    CREATE TABLE IF NOT EXISTS {DOCUMENTS} (
        id          TEXT PRIMARY KEY,
        season      INT NOT NULL,
        round       INT NOT NULL,
        race_name   TEXT NOT NULL,
        race_date   DATE,
        circuit_id  TEXT,
        section     TEXT NOT NULL,
        title       TEXT,
        url         TEXT,
        body        TEXT NOT NULL,
        source_type TEXT NOT NULL DEFAULT 'wikipedia_race_report',
        synced_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_{DOCUMENTS}_race ON {DOCUMENTS} (season, round)",
    f"""
    CREATE TABLE IF NOT EXISTS {EMBEDDINGS} (
        id          TEXT PRIMARY KEY,
        document_id TEXT NOT NULL REFERENCES {DOCUMENTS} (id) ON DELETE CASCADE,
        season      INT NOT NULL,
        round       INT NOT NULL,
        section     TEXT,
        chunk_index INT NOT NULL,
        chunk_text  TEXT NOT NULL,
        embedding   VECTOR({EMBEDDING_DIM}) NOT NULL,
        model_name  TEXT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (document_id, chunk_index)
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_{EMBEDDINGS}_race ON {EMBEDDINGS} (season, round)",
    # HNSW over IVFFlat: IVFFlat picks centroids at build time and needs
    # representative rows to already exist, which is wrong for a table that
    # starts empty. vector_cosine_ops must match the `<=>` operator used at
    # query time - an index built with a different opclass is silently ignored.
    f"CREATE INDEX IF NOT EXISTS idx_{EMBEDDINGS}_hnsw "
    f"ON {EMBEDDINGS} USING hnsw (embedding vector_cosine_ops)",
    # --- measured race-day weather -------------------------------------------
    f"""
    CREATE TABLE IF NOT EXISTS {WEATHER} (
        season           INT NOT NULL,
        round            INT NOT NULL,
        race_name        TEXT NOT NULL,
        race_date        DATE,
        circuit_id       TEXT,
        circuit_name     TEXT,
        conditions       TEXT,
        temp_max         DOUBLE PRECISION,
        temp_min         DOUBLE PRECISION,
        temp_mean        DOUBLE PRECISION,
        precipitation_mm DOUBLE PRECISION,
        rain_mm          DOUBLE PRECISION,
        wind_speed_max   DOUBLE PRECISION,
        wind_gusts_max   DOUBLE PRECISION,
        was_wet          BOOLEAN NOT NULL,
        wet_threshold_mm DOUBLE PRECISION NOT NULL,
        source           TEXT NOT NULL DEFAULT 'open-meteo-archive',
        synced_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (season, round)
    )
    """,
    # --- agent WRITE targets --------------------------------------------------
    f"""
    CREATE TABLE IF NOT EXISTS {WATCHLIST} (
        id          BIGSERIAL PRIMARY KEY,
        user_id     TEXT NOT NULL DEFAULT 'default',
        entity_type TEXT NOT NULL CHECK (entity_type IN ('driver','constructor','circuit')),
        entity_ref  TEXT NOT NULL,
        note        TEXT,
        added_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (user_id, entity_type, entity_ref)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {PREDICTIONS} (
        id          BIGSERIAL PRIMARY KEY,
        user_id     TEXT NOT NULL DEFAULT 'default',
        season      INT NOT NULL,
        round       INT NOT NULL,
        prediction  TEXT NOT NULL,
        confidence  TEXT CHECK (confidence IN ('low','medium','high')),
        rationale   TEXT,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_{PREDICTIONS}_race ON {PREDICTIONS} (season, round)",
    f"""
    CREATE TABLE IF NOT EXISTS {NOTES} (
        id         BIGSERIAL PRIMARY KEY,
        user_id    TEXT NOT NULL DEFAULT 'default',
        season     INT NOT NULL,
        round      INT NOT NULL,
        note       TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_{NOTES}_race ON {NOTES} (season, round)",
    # --- strategy: pit stops and reconstructed stints -------------------------
    f"""
    CREATE TABLE IF NOT EXISTS {PIT_STOPS} (
        season      INT NOT NULL,
        round       INT NOT NULL,
        driver_id   TEXT NOT NULL,
        stop_number INT NOT NULL,
        lap         INT NOT NULL,
        time_of_day TEXT,
        duration_s  DOUBLE PRECISION,
        PRIMARY KEY (season, round, driver_id, stop_number)
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_{PIT_STOPS}_race ON {PIT_STOPS} (season, round)",
    f"""
    CREATE TABLE IF NOT EXISTS {STINTS} (
        season       INT NOT NULL,
        round        INT NOT NULL,
        driver_id    TEXT NOT NULL,
        stint_number INT NOT NULL,
        start_lap    INT NOT NULL,
        end_lap      INT,
        laps         INT,
        entry_reason TEXT,
        exit_reason  TEXT,
        PRIMARY KEY (season, round, driver_id, stint_number)
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_{STINTS}_race ON {STINTS} (season, round)",
    # --- agent telemetry: the CDF source -------------------------------------
    # Every tool call the agent makes lands here. Change Data Feed carries these
    # rows into a Delta analytics table (see notebooks/cdf_agent_analytics.py),
    # which is what lets the app show what the agent actually did rather than
    # what it claimed to do.
    f"""
    CREATE TABLE IF NOT EXISTS {TOOL_CALLS} (
        id          BIGSERIAL PRIMARY KEY,
        called_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        session_id  TEXT,
        tool_name   TEXT NOT NULL,
        is_write    BOOLEAN NOT NULL DEFAULT false,
        arguments   JSONB NOT NULL,
        outcome     TEXT NOT NULL,
        summary     TEXT,
        duration_ms INT
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_{TOOL_CALLS}_called_at ON {TOOL_CALLS} (called_at DESC)",
]


def ensure_schema() -> None:
    """Create every table and index. Idempotent, safe to call on any write path.

    Each statement runs in its own transaction (autocommit). psycopg2 puts a
    connection into an aborted state after any error, so in one shared
    transaction a single recoverable failure - the 42501 that CREATE EXTENSION
    raises for a non-superuser when the extension already exists - would roll
    back every table created before it.
    """
    with connection() as conn:
        conn.autocommit = True
        cur = conn.cursor()
        for statement in DDL:
            try:
                cur.execute(statement)
            except dbapi.DatabaseError as exc:
                # 42501 = insufficient_privilege. pg8000 surfaces the SQLSTATE
                # as exc.args[0]["C"] (the wire protocol's ErrorResponse
                # fields), not as a distinct exception class the way
                # psycopg2.errors.InsufficientPrivilege does.
                if isinstance(exc.args[0], dict) and exc.args[0].get("C") == "42501":
                    # Only CREATE EXTENSION should reach here. Anything else
                    # genuinely failing surfaces later as a missing table.
                    continue
                raise
        cur.close()


def smoke_test() -> int:
    """Write one row, read it back, delete it. Returns 0 on success.

    The check that matters is the read-back, not the write. An
    `INSERT ... RETURNING` run on a connection that is never committed hands
    back a real row with a real id and then rolls the whole thing back when the
    connection closes - so a write can report complete success and leave nothing
    behind. That shipped here once. Anything claiming to verify the database is
    reachable has to close the connection and look again.

    Cleans up after itself, so it is safe to run against the live database.
    """
    import uuid as _uuid

    marker = f"smoke-{_uuid.uuid4().hex[:8]}"
    print(f"  connecting     … {'ok' if query('SELECT 1 AS ok')[0]['ok'] == 1 else 'FAILED'}")

    race = query(f"SELECT season, round FROM {RACES} ORDER BY season, round LIMIT 1")
    if not race:
        print("  FAILED: no races loaded — run the harvest and seed steps first")
        return 1
    season, rnd = race[0]["season"], race[0]["round"]

    rows = returning(
        f"INSERT INTO {NOTES} (user_id, season, round, note) VALUES (%s, %s, %s, %s) RETURNING id",
        ("smoke-test", season, rnd, marker),
    )
    new_id = rows[0]["id"]
    print(f"  wrote          … id {new_id}")

    # New connection on purpose: the point is that the row outlived the one
    # that created it.
    found = query(f"SELECT id FROM {NOTES} WHERE note = %s", (marker,))
    if not found:
        print("  FAILED: the write reported success but the row is not readable")
        return 1
    print(f"  read back      … id {found[0]['id']}")

    execute(f"DELETE FROM {NOTES} WHERE note = %s", (marker,))
    left = query(f"SELECT 1 FROM {NOTES} WHERE note = %s", (marker,))
    print(f"  cleaned up     … {'ok' if not left else 'FAILED'}")
    return 0 if not left else 1


def _main() -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m f1lake.schema",
        description="Create the Lakebase tables, or check the write path works.",
    )
    parser.add_argument(
        "--ensure", action="store_true", help="create every table and index (idempotent)"
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="write one row, read it back on a new connection, delete it",
    )
    args = parser.parse_args()

    if not (args.ensure or args.smoke):
        parser.print_help()
        return 2

    if args.ensure:
        ensure_schema()
        counts = query("""
            SELECT (SELECT count(*) FROM f1_races)      AS races,
                   (SELECT count(*) FROM f1_documents)  AS documents,
                   (SELECT count(*) FROM f1_embeddings) AS embeddings""")[0]
        print(
            f"  schema ensured … {counts['races']} races, "
            f"{counts['documents']} documents, {counts['embeddings']} embeddings"
        )
    if args.smoke:
        return smoke_test()
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())

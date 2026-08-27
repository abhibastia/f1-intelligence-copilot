"""
Load harvested data into Lakebase: races and race reports (chunked and embedded).

Runs locally. Costs no Databricks compute.

    python -m f1lake.load --data data

Idempotent throughout. Every table upserts on its natural key, so a re-run after
a partial harvest updates in place rather than duplicating - which matters,
because the Wikipedia harvest is expected to be resumed.

Race-day weather is NOT loaded here. It is seeded straight from the governed
`f1.gold.race_conditions` mart by `f1lake.seed_gold` — see that module's
`seed_weather()`. This module used to also load a locally-harvested
`race_weather.json`, a second independent fetch of the same Open-Meteo
measurement the Spark pipeline already computes and quality-checks; merging the
two projects removed that duplication.
"""

import argparse
import hashlib
import json
import logging
import os

from f1lake import embedder, schema
from f1lake.schema import execute_values

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("load")

# Race-report sections run from a two-line summary to several thousand
# characters. 900/150 keeps a chunk inside the model's 256-token window with
# enough overlap that a sentence straddling a boundary stays retrievable from
# both sides.
CHUNK_SIZE = 900
CHUNK_OVERLAP = 150


def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """Split text into overlapping windows."""
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= size:
        return [text]

    step = size - overlap
    chunks = []
    for start in range(0, len(text), step):
        piece = text[start:start + size].strip()
        if piece:
            chunks.append(piece)
        if start + size >= len(text):
            break
    return chunks


def document_id(season: int, rnd: int, section: str) -> str:
    """Deterministic id so a re-load updates the same row instead of adding one."""
    raw = f"{season}|{rnd}|{section}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:24]


def disambiguate(sections: list[dict]) -> list[tuple[str, str]]:
    """Make section names unique within one race, preserving reading order.

    A Wikipedia article can repeat a heading at different nesting levels - the
    2025 Las Vegas Grand Prix has "Race" twice. Since the document id is derived
    from (season, round, section), a repeat produced two rows with the same id
    in one batch, which Postgres rejects outright:

        ON CONFLICT DO UPDATE command cannot affect row a second time

    Suffixing the repeat rather than dropping it keeps both bodies - they are
    different text - and keeps the label meaningful, so a retrieval hit can
    still cite "from the Race section" instead of an opaque ordinal.
    """
    seen: dict[str, int] = {}
    out = []
    for s in sections:
        name = s["section"]
        seen[name] = seen.get(name, 0) + 1
        label = name if seen[name] == 1 else f"{name} ({seen[name]})"
        out.append((label, s["text"]))
    return out


def load_races(path: str) -> int:
    races = json.load(open(path))
    rows = [
        (r["season"], r["round"], r["race_name"], r["race_date"] or None,
         r["circuit_id"], r["circuit_name"], r["circuit_country"],
         r["circuit_lat"], r["circuit_long"], r["wikipedia_url"])
        for r in races
    ]
    sql = f"""
        INSERT INTO {schema.RACES}
            (season, round, race_name, race_date, circuit_id, circuit_name,
             circuit_country, circuit_lat, circuit_long, wikipedia_url)
        VALUES %s
        ON CONFLICT (season, round) DO UPDATE SET
            race_name=EXCLUDED.race_name, race_date=EXCLUDED.race_date,
            circuit_id=EXCLUDED.circuit_id, circuit_name=EXCLUDED.circuit_name,
            circuit_country=EXCLUDED.circuit_country,
            circuit_lat=EXCLUDED.circuit_lat, circuit_long=EXCLUDED.circuit_long,
            wikipedia_url=EXCLUDED.wikipedia_url
    """
    with schema.connection() as conn:
        with schema.cursor(conn) as cur:
            execute_values(cur, sql, rows, page_size=100)
            conn.commit()
    return len(rows)


def harvested_races() -> set[tuple[int, int]]:
    """(season, round) pairs that already have at least one document.

    The durable equivalent of harvest/run.py's local "already have" check
    against race_reports.json - used by jobs/run_harvest.py, where progress
    has to be checked against Lakebase because ephemeral job compute has no
    local file that survives a retry.
    """
    return {(r["season"], r["round"]) for r in
            schema.query(f"SELECT DISTINCT season, round FROM {schema.DOCUMENTS}")}


def load_documents(reports: list[dict]) -> tuple[int, int]:
    """Load race-report sections as documents. Returns (reports, sections).

    Takes reports already in memory rather than a file path, so one race can
    be written immediately after it's fetched (see harvest.wikipedia.
    fetch_and_store) instead of accumulating in memory until a batch write at
    the end - which is fine on a laptop but loses everything on ephemeral job
    compute if the process is killed before that final write.
    """
    rows = []
    for report in reports:
        for label, body in disambiguate(report["sections"]):
            rows.append((
                document_id(report["season"], report["round"], label),
                report["season"], report["round"], report["race_name"],
                report["race_date"] or None, report["circuit_id"],
                label, report["title"], report["url"], body,
            ))
    sql = f"""
        INSERT INTO {schema.DOCUMENTS}
            (id, season, round, race_name, race_date, circuit_id,
             section, title, url, body)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            body=EXCLUDED.body, title=EXCLUDED.title, url=EXCLUDED.url,
            synced_at=now()
    """
    with schema.connection() as conn:
        with schema.cursor(conn) as cur:
            execute_values(cur, sql, rows, page_size=100)
            conn.commit()
    return len(reports), len(rows)


def load_embeddings(batch_size: int = 64, rebuild: bool = False) -> int:
    """Chunk and embed documents that have no embeddings yet.

    NOT EXISTS rather than LEFT JOIN ... IS NULL: a document has many chunks, so
    the join form would need a DISTINCT to avoid returning it once per chunk.
    """
    if rebuild:
        docs = schema.query(
            f"SELECT id, season, round, section, body FROM {schema.DOCUMENTS} ORDER BY id"
        )
    else:
        docs = schema.query(f"""
            SELECT d.id, d.season, d.round, d.section, d.body
            FROM {schema.DOCUMENTS} d
            WHERE NOT EXISTS (
                SELECT 1 FROM {schema.EMBEDDINGS} e WHERE e.document_id = d.id
            )
            ORDER BY d.id
        """)

    if not docs:
        logger.info("  no documents need embedding")
        return 0

    pending = []
    for doc in docs:
        for i, piece in enumerate(chunk_text(doc["body"])):
            pending.append((f"{doc['id']}_{i}", doc["id"], doc["season"],
                            doc["round"], doc["section"], i, piece))
    logger.info("  %d documents -> %d chunks", len(docs), len(pending))

    sql = f"""
        INSERT INTO {schema.EMBEDDINGS}
            (id, document_id, season, round, section, chunk_index,
             chunk_text, embedding, model_name, created_at)
        VALUES %s
        ON CONFLICT (document_id, chunk_index) DO UPDATE SET
            chunk_text=EXCLUDED.chunk_text, embedding=EXCLUDED.embedding,
            model_name=EXCLUDED.model_name, created_at=now()
    """
    # The %s::vector cast lives in the row template. Binding the float list
    # directly would store a double precision[] and force a follow-up
    # UPDATE ... ::vector that is easy to forget - and whose omission makes
    # search silently return nothing rather than erroring.
    template = "(%s, %s, %s, %s, %s, %s, %s, %s::vector, %s, now())"

    written = 0
    with schema.connection() as conn:
        with schema.cursor(conn) as cur:
            for start in range(0, len(pending), batch_size):
                batch = pending[start:start + batch_size]
                vectors = embedder.embed_texts([b[6] for b in batch])
                payload = [
                    (cid, did, season, rnd, section, idx, text,
                     embedder.to_pgvector(vec), embedder.MODEL_NAME)
                    for (cid, did, season, rnd, section, idx, text), vec
                    in zip(batch, vectors)
                ]
                execute_values(cur, sql, payload, template=template, page_size=100)
                conn.commit()
                written += len(payload)
                logger.info("    embedded %d/%d", written, len(pending))
    return written


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data", default="data")
    p.add_argument("--rebuild-embeddings", action="store_true")
    args = p.parse_args()

    logger.info("Ensuring schema ...")
    schema.ensure_schema()

    logger.info("Loading races ...")
    logger.info("  %d races", load_races(os.path.join(args.data, "races.json")))

    logger.info("Loading race reports ...")
    with open(os.path.join(args.data, "race_reports.json")) as fh:
        reports, sections = load_documents(json.load(fh))
    logger.info("  %d reports -> %d sections", reports, sections)

    logger.info("Embedding ...")
    load_embeddings(rebuild=args.rebuild_embeddings)

    totals = schema.query(f"""
        SELECT (SELECT count(*) FROM {schema.RACES})      AS races,
               (SELECT count(*) FROM {schema.DOCUMENTS})  AS documents,
               (SELECT count(*) FROM {schema.EMBEDDINGS}) AS embeddings,
               (SELECT count(*) FROM {schema.WEATHER})    AS weather
    """)[0]
    logger.info("\nDone. races=%(races)d documents=%(documents)d "
                "embeddings=%(embeddings)d weather=%(weather)d", totals)


if __name__ == "__main__":
    main()

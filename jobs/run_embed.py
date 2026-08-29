"""Databricks task: chunk and embed race-report sections into Lakebase pgvector.

This was assumed impossible on Free Edition serverless, on the basis of a
day-2 notebook that was memory-killed loading sentence-transformers. That
conclusion was never retested for a small model: both all-MiniLM-L6-v2 and its
successor BAAI/bge-small-en-v1.5 are well under 150 MB and load fine. Probed
before wiring it up, and the probe succeeded.

Incremental by default - only documents with no embeddings are processed - so a
re-run after new race reports is cheap.
"""

import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(message)s")
_here = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
sys.path.insert(0, os.path.dirname(_here))

os.environ.setdefault("HF_HOME", "/tmp/.cache/hf")
os.environ.setdefault("SENTENCE_TRANSFORMERS_HOME", "/tmp/.cache/hf")

from f1lake import load as L
from f1lake import schema

schema.ensure_schema()
written = L.load_embeddings()
totals = schema.query("""
    SELECT (SELECT count(*) FROM f1_documents)  AS documents,
           (SELECT count(*) FROM f1_embeddings) AS embeddings""")[0]
logging.getLogger("embed-job").info(
    "embedded %d new chunk(s). documents=%d embeddings=%d",
    written,
    totals["documents"],
    totals["embeddings"],
)

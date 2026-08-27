"""
Embedding model — loaded once per process, lazily.

Runs on the laptop, not in Databricks. That is not a shortcut: a Free Edition
serverless notebook is memory-killed while loading sentence-transformers/torch,
dying with "The Python process exited unexpectedly" before any embedding work
starts. Embedding locally and writing vectors to Lakebase over the network costs
zero Databricks compute, which is the scarcest resource in this build.

Lazy loading matters for the deployed MCP server: the model takes ~20s to load
on a cold container, and doing that at import time would stall startup past the
Databricks Apps health-check window and get the app killed before it serves a
request.
"""

import logging
import os
import threading

logger = logging.getLogger("embedder")

MODEL_NAME = os.environ.get("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
EMBEDDING_DIM = int(os.environ.get("EMBEDDING_DIM", "384"))
_CACHE_DIR = os.environ.get("SENTENCE_TRANSFORMERS_HOME", "/tmp/.cache/huggingface")

_model = None
_lock = threading.Lock()


def get_model():
    """Return the process-wide model, loading it on first call.

    Guarded by a lock because the MCP server handles requests concurrently and
    two simultaneous first-requests would otherwise both download and load.
    """
    global _model
    if _model is not None:
        return _model
    with _lock:
        if _model is None:
            from sentence_transformers import SentenceTransformer

            os.makedirs(_CACHE_DIR, exist_ok=True)
            logger.info("Loading embedding model %s", MODEL_NAME)
            _model = SentenceTransformer(MODEL_NAME, cache_folder=_CACHE_DIR)
            logger.info("Embedding model loaded")
    return _model


def embed_texts(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    vectors = get_model().encode(texts, show_progress_bar=False)
    return [[float(x) for x in v] for v in vectors]


def embed_query(text: str) -> list[float]:
    return embed_texts([text])[0]


def to_pgvector(vector: list[float]) -> str:
    """Render a vector in pgvector's text input format: '[0.1,0.2,...]'.

    Bound as an ordinary string and cast with `%s::vector` in SQL. This is the
    documented way to bind a vector without pgvector's psycopg2 adapter, and it
    avoids the float8[] round-trip that would otherwise need a follow-up
    `UPDATE ... SET embedding = embedding::vector` - a step that is easy to
    forget and whose omission makes search silently return nothing.
    """
    return "[" + ",".join(repr(float(x)) for x in vector) + "]"

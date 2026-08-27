"""
Wikipedia race reports — the unstructured-data source.

Every Grand Prix has an article whose "Report" section is genuine narrative
prose: how the race unfolded, incidents, strategy, controversy. That is the text
worth embedding. Results tables are not - the structured pipeline already models
those far better than free text ever could.

WHY THIS SOURCE
---------------
`f1.silver.dim_race` already carries `wikipedia_url` for all 71 races, so the
corpus is pre-addressed by the pipeline: no search, no title construction, no
disambiguation between "2024 Brazilian Grand Prix" and "Brazilian Grand Prix".
The URL is a fact from the data source, not a guess.

The MediaWiki API is free, needs no key, and returns plain text directly with
`prop=extracts&explaintext`, so there is no HTML to strip.
"""

import logging
import os
import random
import re
import time
from urllib.parse import unquote, urlparse

import requests

logger = logging.getLogger("wikipedia")

API_URL = "https://en.wikipedia.org/w/api.php"

# Wikipedia's User-Agent policy requires a descriptive agent with a contact
# route; anonymous-looking clients are rate-limited aggressively. A first run
# with a generic agent and a 0.2s delay was throttled with HTTP 429 after
# exactly ten requests, so both were wrong. Not a secret - override with
# WIKI_USER_AGENT to point at your own contact.
USER_AGENT = os.environ.get(
    "WIKI_USER_AGENT",
    "f1-intelligence-copilot/1.0 (set WIKI_USER_AGENT to your own contact route)",
)

TIMEOUT = 30
COURTESY_DELAY = 1.0   # 1 req/s sustained
MAX_RETRIES = 5
BACKOFF_BASE = 2.0

# Sections that are navigation or bookkeeping rather than narrative. Embedding
# these adds noise to every retrieval without adding meaning.
SKIP_SECTIONS = {
    "references", "external links", "see also", "notes", "further reading",
    "bibliography", "sources",
}

# Standings tables render as prose-free noise in explaintext output - long runs
# of names and numbers. They are already modelled in Gold; keeping them here
# would let a semantic search return a wall of digits.
SKIP_PREFIXES = (
    "championship standings",
    "drivers' championship standings",
    "constructors' championship standings",
)


class WikipediaError(RuntimeError):
    """The article could not be fetched or contained no usable prose."""


def title_from_url(url: str) -> str:
    """Extract the article title from a canonical Wikipedia URL."""
    path = urlparse(url).path
    if not path.startswith("/wiki/"):
        raise WikipediaError(f"Not a Wikipedia article URL: {url!r}")
    return unquote(path[len("/wiki/"):]).replace("_", " ")


def fetch_extract(title: str, session: requests.Session | None = None) -> str:
    """Fetch an article's plain-text extract, retrying on throttling.

    A 429 is a request to slow down, not a failure - treating it as fatal threw
    away 49 of 59 articles on the first run. Honour Retry-After when Wikipedia
    sends it, otherwise back off exponentially with jitter so retries from a
    resumed run do not re-synchronise into another burst.
    """
    session = session or requests.Session()
    params = {
        "action": "query", "format": "json", "prop": "extracts",
        "explaintext": 1, "redirects": 1, "titles": title,
    }

    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(
                API_URL, params=params,
                headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT,
            )
            if response.status_code in (429, 503):
                retry_after = response.headers.get("Retry-After")
                wait = (
                    float(retry_after) if retry_after and retry_after.isdigit()
                    else BACKOFF_BASE ** attempt + random.uniform(0, 1)
                )
                logger.warning("  throttled (%s), waiting %.1fs [attempt %d/%d]",
                               response.status_code, wait, attempt + 1, MAX_RETRIES)
                time.sleep(wait)
                last_error = f"HTTP {response.status_code}"
                continue
            response.raise_for_status()
            payload = response.json()
            break
        except requests.RequestException as exc:
            last_error = str(exc)
            time.sleep(BACKOFF_BASE ** attempt + random.uniform(0, 1))
        except ValueError:
            raise WikipediaError(f"Wikipedia returned non-JSON for {title!r}")
    else:
        raise WikipediaError(
            f"Wikipedia request failed for {title!r} after {MAX_RETRIES} attempts: {last_error}"
        )

    pages = payload.get("query", {}).get("pages", {})
    if not pages:
        raise WikipediaError(f"No pages returned for {title!r}")

    page = next(iter(pages.values()))
    if "missing" in page:
        raise WikipediaError(f"Article does not exist: {title!r}")

    extract = (page.get("extract") or "").strip()
    if not extract:
        raise WikipediaError(f"Article has no extract: {title!r}")
    return extract


def split_sections(extract: str) -> list[tuple[str, str]]:
    """Split a plain-text extract into (section_name, body) pairs.

    MediaWiki renders headings as `== Heading ==` / `=== Sub ===` in explaintext
    output. Splitting on them lets a retrieval hit cite "from the Race report
    section" rather than pointing at an undifferentiated wall of text, and lets
    the noisy sections be dropped by name.
    """
    parts = re.split(r"\n=+ *([^=\n]+?) *=+ *\n", "\n" + extract)

    sections: list[tuple[str, str]] = []
    lead = parts[0].strip()
    if lead:
        sections.append(("Summary", lead))

    for i in range(1, len(parts) - 1, 2):
        name = parts[i].strip()
        body = parts[i + 1].strip()
        if not body:
            continue
        lowered = name.lower()
        if lowered in SKIP_SECTIONS or lowered.startswith(SKIP_PREFIXES):
            continue
        sections.append((name, body))

    return sections


def fetch_race_report(race, session: requests.Session | None = None) -> dict:
    """Fetch and section one race's Wikipedia article.

    Returns a dict carrying the (season, round) key so every downstream chunk
    joins to the Gold marts for the same race.
    """
    title = title_from_url(race.wikipedia_url)
    extract = fetch_extract(title, session=session)
    sections = split_sections(extract)
    if not sections:
        raise WikipediaError(f"No usable sections in {title!r}")

    return {
        "season": race.season,
        "round": race.round,
        "race_name": race.race_name,
        "race_date": race.race_date,
        "circuit_id": race.circuit_id,
        "title": title,
        "url": race.wikipedia_url,
        "sections": [{"section": name, "text": body} for name, body in sections],
        "total_chars": sum(len(body) for _, body in sections),
    }


def fetch_many(races, sleep: float = COURTESY_DELAY) -> tuple[list[dict], list[dict]]:
    """Fetch reports for many races. Returns (reports, failures).

    Sequential and rate-limited by choice: 71 articles is trivial volume, and a
    burst of parallel requests against a free community API to save twenty
    seconds is a bad trade.
    """
    session = requests.Session()
    reports, failures = [], []
    for i, race in enumerate(races, 1):
        try:
            report = fetch_race_report(race, session=session)
            reports.append(report)
            logger.info(
                "[%d/%d] %s %s — %d sections, %d chars",
                i, len(races), race.season, race.race_name,
                len(report["sections"]), report["total_chars"],
            )
        except WikipediaError as exc:
            failures.append({"season": race.season, "round": race.round,
                             "race_name": race.race_name, "error": str(exc)})
            logger.warning("[%d/%d] %s %s — FAILED: %s",
                           i, len(races), race.season, race.race_name, exc)
        time.sleep(sleep)
    return reports, failures

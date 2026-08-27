"""HTTP client for the Jolpica-F1 API.

Handles the three things that make this API awkward: pagination, rate limits,
and a community-maintained backend that occasionally 5xxs. Everything else in
the project assumes it can call `fetch_all` and get a complete payload or an
exception — no partial results, no silent truncation.
"""

from __future__ import annotations

import logging
import time
import urllib.error
import urllib.request
import json
from typing import Any

from collections import deque

from config import (
    BACKOFF_BASE_SECONDS,
    BASE_URL,
    MAX_RETRIES,
    PAGE_SIZE,
    REQUEST_TIMEOUT_SECONDS,
    REQUESTS_PER_HOUR,
    REQUESTS_PER_SECOND,
)

log = logging.getLogger(__name__)

HOUR_SECONDS = 3600.0


class JolpicaError(RuntimeError):
    """Raised when a request cannot be completed after all retries."""


class RateBudget:
    """Enforces both of Jolpica's limits: burst and sustained.

    The burst limit is a minimum gap between requests. The sustained limit needs
    a sliding window — a counter reset on the hour would allow 500 requests at
    59 minutes and another 500 at 61, which is 1,000 in two minutes.

    Only the burst half existed before. That was adequate while a full backfill
    was ~200 calls; `laps` is ~850 across three seasons, so a run that respects
    only the per-second gap would breach the hourly ceiling in the first five
    minutes and start collecting 429s for the rest of the harvest.

    `clock` and `sleeper` are injectable so the waiting behaviour can be tested
    without a test that actually waits an hour.
    """

    def __init__(self, per_second: float, per_hour: int,
                 clock=time.monotonic, sleeper=time.sleep) -> None:
        self._min_interval = 1.0 / per_second
        self._per_hour = per_hour
        self._clock = clock
        self._sleep = sleeper
        self._window: deque[float] = deque()
        # None, not 0.0: "no request yet" has to be distinguishable from "a
        # request at time zero", or the first call waits for a gap it never
        # needed. Production hid this because time.monotonic() starts large.
        self._last_request_at: float | None = None

    def _drop_expired(self, now: float) -> None:
        while self._window and now - self._window[0] >= HOUR_SECONDS:
            self._window.popleft()

    def acquire(self) -> None:
        """Block until another request is allowed under both limits."""
        now = self._clock()
        self._drop_expired(now)

        # Sustained: wait for the oldest request in the window to age out.
        if len(self._window) >= self._per_hour:
            wait = HOUR_SECONDS - (now - self._window[0])
            if wait > 0:
                log.warning(
                    "hourly budget of %s reached — pausing %.0fs. This is the "
                    "sustained limit, not an error.", self._per_hour, wait)
                self._sleep(wait)
                now = self._clock()
                self._drop_expired(now)

        # Burst: keep a minimum gap between consecutive requests.
        if self._last_request_at is not None:
            gap = now - self._last_request_at
            if gap < self._min_interval:
                self._sleep(self._min_interval - gap)
                now = self._clock()

        self._last_request_at = now
        self._window.append(now)

    @property
    def used_this_hour(self) -> int:
        self._drop_expired(self._clock())
        return len(self._window)


_budget = RateBudget(REQUESTS_PER_SECOND, REQUESTS_PER_HOUR)


def _throttle() -> None:
    """Space requests to stay inside both published limits."""
    _budget.acquire()


def _get(url: str) -> dict[str, Any]:
    """Single GET with exponential back-off on 429 and 5xx."""
    for attempt in range(MAX_RETRIES):
        _throttle()
        try:
            request = urllib.request.Request(
                url, headers={"User-Agent": "f1-capstone-pipeline/1.0"}
            )
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            # 4xx other than 429 are our fault — a bad URL won't fix itself.
            if exc.code != 429 and exc.code < 500:
                raise JolpicaError(f"{exc.code} for {url}") from exc
            wait = BACKOFF_BASE_SECONDS ** (attempt + 1)
            log.warning("HTTP %s for %s — retrying in %.0fs", exc.code, url, wait)
            time.sleep(wait)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            wait = BACKOFF_BASE_SECONDS ** (attempt + 1)
            log.warning("%s for %s — retrying in %.0fs", type(exc).__name__, url, wait)
            time.sleep(wait)

    raise JolpicaError(f"exhausted {MAX_RETRIES} retries for {url}")


def fetch_all(path: str) -> dict[str, Any]:
    """Fetch every page of an endpoint and return one merged MRData payload.

    Jolpica paginates with limit/offset and reports `total` in every response.
    We walk pages until we have `total` records, then splice the accumulated
    records back into the first response's envelope so the result is shaped
    exactly like an unpaginated response.
    """
    first = _get(f"{BASE_URL}/{path}/?format=json&limit={PAGE_SIZE}&offset=0")
    mrdata = first["MRData"]
    total = int(mrdata.get("total", 0))

    table_key = _find_table_key(mrdata)
    if table_key is None:
        return first

    array_key = _find_array_key(mrdata[table_key])
    if array_key is None:
        return first

    records = mrdata[table_key][array_key]

    # Nested endpoints (results, qualifying) report `total` as the number of
    # *inner* records while the outer array holds one race. Walking pages by the
    # outer length would loop forever, so page on a running inner count instead.
    counted = _count_records(records, array_key)
    offset = PAGE_SIZE

    while counted < total:
        page = _get(f"{BASE_URL}/{path}/?format=json&limit={PAGE_SIZE}&offset={offset}")
        page_records = page["MRData"][table_key][array_key]
        if not page_records:
            log.warning("empty page at offset %s for %s — stopping early", offset, path)
            break
        records.extend(page_records)
        counted += _count_records(page_records, array_key)
        offset += PAGE_SIZE

    mrdata[table_key][array_key] = records
    return first


def _find_table_key(mrdata: dict[str, Any]) -> str | None:
    return next((k for k in mrdata if k.endswith("Table")), None)


def _find_array_key(table: dict[str, Any]) -> str | None:
    return next((k for k, v in table.items() if isinstance(v, list)), None)


def _count_records(records: list[dict[str, Any]], array_key: str) -> int:
    """Count the records `total` actually refers to.

    For flat endpoints (Drivers, Constructors, Races) that is the array length.
    For nested ones each element is a race or standings list wrapping the real
    records, so we sum the inner arrays.
    """
    if array_key not in ("Races", "StandingsLists"):
        return len(records)

    inner_keys = (
        "Results",
        "SprintResults",
        "QualifyingResults",
        "DriverStandings",
        "ConstructorStandings",
        # PitStops and Laps report `total` the same way: the outer array holds one
        # race, `total` counts the inner records. Omitting them made the loop
        # count 1 against a total of 30, fetch a second page, get nothing back,
        # and log "empty page — stopping early" once per race. Correct output,
        # one wasted request every time.
        "PitStops",
    )
    total = 0
    for record in records:
        # Laps nest one level deeper than anything else: a Races element holds
        # Laps, and each lap holds Timings — one per driver. `total` counts the
        # timings, so 5 laps on a page is 100 records, not 5. Counting the laps
        # would compare 5 against a total of 1,008 and page until the API ran
        # out of data.
        if "Laps" in record:
            total += sum(len(lap.get("Timings", [])) for lap in record["Laps"])
            continue
        inner = [record[k] for k in inner_keys if k in record]
        # A Races element with no inner array is a schedule entry — count it as one.
        total += sum(len(i) for i in inner) if inner else 1
    return total

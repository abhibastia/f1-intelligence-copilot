"""The idempotency contract.

These are the tests that protect a long harvest. `laps` is ~850 paginated
requests against a 500/hour ceiling, so a `should_write` that wrongly returns
True re-fetches data already on disk and spends an hour of rate limit that
cannot be recovered the same day.
"""
import datetime as dt
import os

import landing_writer as lw


class TestPartitionDir:
    def test_round_level_endpoint_gets_a_round_segment(self, tmp_path):
        path = lw.partition_dir(str(tmp_path), "results", 2024, 16)
        assert path.endswith(os.path.join("results", "season=2024", "round=16"))

    def test_season_level_endpoint_omits_it(self, tmp_path):
        path = lw.partition_dir(str(tmp_path), "races", 2024, None)
        assert path.endswith(os.path.join("races", "season=2024"))
        assert "round=" not in path


class TestIsClosed:
    def test_a_race_last_month_is_closed(self):
        assert lw.is_closed(dt.date(2026, 7, 1), today=dt.date(2026, 8, 1))

    def test_a_race_yesterday_is_still_open(self):
        """Results are amended after the flag — penalties, appeals, DSQs."""
        assert not lw.is_closed(dt.date(2026, 7, 31), today=dt.date(2026, 8, 1))

    def test_the_boundary_is_exclusive(self):
        today = dt.date(2026, 8, 1)
        cutoff = today - dt.timedelta(days=lw.CLOSED_AFTER_DAYS)
        assert not lw.is_closed(cutoff, today=today)
        assert lw.is_closed(cutoff - dt.timedelta(days=1), today=today)

    def test_an_unknown_date_is_treated_as_open(self):
        """Safer to re-pull than to skip a round we cannot date."""
        assert not lw.is_closed(None)


class TestShouldWrite:
    def _land(self, root, endpoint, season, round_no):
        directory = lw.partition_dir(str(root), endpoint, season, round_no)
        os.makedirs(directory, exist_ok=True)
        with open(os.path.join(directory, "x.json"), "w") as fh:
            fh.write("{}")

    def test_writes_when_the_partition_is_empty(self, tmp_path):
        assert lw.should_write(str(tmp_path), "results", 2024, 1, dt.date(2024, 3, 2))

    def test_skips_a_closed_round_that_already_landed(self, tmp_path):
        """The whole point: a re-run must not re-fetch a finished race."""
        self._land(tmp_path, "results", 2024, 1)
        assert not lw.should_write(
            str(tmp_path), "results", 2024, 1, dt.date(2024, 3, 2))

    def test_repulls_an_open_round_that_already_landed(self, tmp_path):
        self._land(tmp_path, "results", 2026, 12)
        assert lw.should_write(
            str(tmp_path), "results", 2026, 12, dt.date.today())

    def test_a_directory_with_no_json_counts_as_empty(self, tmp_path):
        directory = lw.partition_dir(str(tmp_path), "results", 2024, 1)
        os.makedirs(directory)
        with open(os.path.join(directory, "_committed"), "w") as fh:
            fh.write("")
        assert lw.should_write(
            str(tmp_path), "results", 2024, 1, dt.date(2024, 3, 2))


class TestWritePayload:
    def test_envelope_carries_the_provenance_silver_needs(self, tmp_path):
        """Bare MRData would leave no timestamp to order snapshots by, and the
        open-round dedupe depends on exactly that."""
        import json

        path = lw.write_payload(
            str(tmp_path), "results", 2026, 12,
            {"MRData": {"total": "1"}},
            "https://api.jolpi.ca/ergast/f1/2026/12/results/")
        landed = json.loads(open(path).read())
        assert set(landed) == {
            "_ingest_ts", "_source_url", "_season", "_round", "_endpoint", "payload"}
        assert landed["_endpoint"] == "results"
        assert landed["_round"] == "12"
        assert landed["payload"]["MRData"]["total"] == "1"

    def test_one_line_so_auto_loader_can_read_without_multiline(self, tmp_path):
        path = lw.write_payload(
            str(tmp_path), "races", 2026, None, {"MRData": {}}, "https://x")
        assert len(open(path).read().splitlines()) == 1

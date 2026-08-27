"""The race calendar drives everything downstream.

It decides which rounds exist, whether each is closed, and — once weather is
added — supplies the coordinates the archive is queried at. A malformed entry
must be skipped, not fatal: one bad race should not abort a season's harvest.
"""
import ast
import datetime as dt
import pathlib

import config
import ingest


def payload(races):
    return {"MRData": {"RaceTable": {"Races": races}}}


class TestParseRaceCalendar:
    def test_maps_round_to_date(self):
        cal = ingest.parse_race_calendar(payload([
            {"round": "1", "date": "2024-03-02"},
            {"round": "2", "date": "2024-03-09"},
        ]))
        assert cal == {1: dt.date(2024, 3, 2), 2: dt.date(2024, 3, 9)}

    def test_skips_a_malformed_entry_without_losing_the_rest(self):
        cal = ingest.parse_race_calendar(payload([
            {"round": "1", "date": "2024-03-02"},
            {"round": "2", "date": "not-a-date"},
            {"round": "3"},
            {"round": "4", "date": "2024-03-23"},
        ]))
        assert set(cal) == {1, 4}, "one bad race must not abort the season"

    def test_an_empty_season_is_empty_not_an_error(self):
        assert ingest.parse_race_calendar(payload([])) == {}


class TestSeasonSelection:
    """Seasons are derived from the date, never hardcoded.

    The bug this replaces: LIVE_SEASON = 2026. On 1 January the weekly job
    keeps succeeding and ingests nothing, forever, because a job with nothing
    to do looks exactly like a job doing nothing.
    """

    def test_live_season_is_the_calendar_year(self):
        assert config.live_season(dt.date(2027, 7, 1)) == 2027
        assert config.live_season(dt.date(2031, 1, 1)) == 2031

    def test_backfill_covers_every_season_from_the_floor(self):
        assert config.backfill_seasons(dt.date(2026, 6, 1)) == [2024, 2025, 2026]
        assert config.backfill_seasons(dt.date(2027, 1, 2)) == [2024, 2025, 2026, 2027]

    def test_incremental_keeps_the_previous_season_open(self):
        # A December race stays open into January; dropping the previous season
        # on 1 January would freeze its final round permanently.
        assert config.incremental_seasons(dt.date(2027, 1, 2)) == [2026, 2027]
        assert config.incremental_seasons(dt.date(2026, 8, 14)) == [2025, 2026]

    def test_incremental_does_not_reach_below_the_floor(self):
        assert config.incremental_seasons(dt.date(2024, 5, 1)) == [2024]

    def test_no_season_is_a_module_constant(self):
        """Checked against the AST, not the text.

        The comment in config.py names the old constants to explain why they
        are gone, so a substring search matches the very documentation that
        records the fix.
        """
        source = (
            pathlib.Path(__file__).resolve().parents[1]
            / "src" / "ingestion" / "config.py"
        ).read_text()
        assigned = {
            target.id
            for node in ast.parse(source).body
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name)
        }
        assert "LIVE_SEASON" not in assigned
        assert "BACKFILL_SEASONS" not in assigned
        assert "FIRST_SEASON" in assigned

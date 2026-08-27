"""Measured weather, and the lag that makes it dangerous.

ERA5 publishes on a ~5 day lag and answers 200 with a block of nulls for a date
it has not assimilated. The failure mode this file exists to prevent is landing
that as 0.0 mm: a race with no observation becoming a race with no rain, stated
confidently, on a dashboard.
"""
import datetime as dt

import pytest

import ingest
import weather
from config import ARCHIVE_LAG_DAYS


class TestArchiveLag:
    def test_an_old_race_is_available(self):
        assert weather.is_available(dt.date(2024, 3, 2), today=dt.date(2026, 8, 1))

    def test_a_race_inside_the_lag_is_not(self):
        today = dt.date(2026, 8, 1)
        assert not weather.is_available(today - dt.timedelta(days=1), today=today)

    def test_the_boundary_is_inclusive(self):
        today = dt.date(2026, 8, 1)
        edge = today - dt.timedelta(days=ARCHIVE_LAG_DAYS)
        assert weather.is_available(edge, today=today)
        assert not weather.is_available(edge + dt.timedelta(days=1), today=today)

    def test_a_future_race_is_not_available(self):
        today = dt.date(2026, 8, 1)
        assert not weather.is_available(today + dt.timedelta(days=30), today=today)


class TestObservationDetection:
    def test_real_measurement_is_an_observation(self):
        assert weather.has_observation(
            {"daily": {"precipitation_sum": [19.1], "temperature_2m_max": [24.0]}})

    def test_zero_rainfall_is_still_an_observation(self):
        """A dry race is data. Only a null is missing data."""
        assert weather.has_observation({"daily": {"precipitation_sum": [0.0]}})

    def test_a_null_reading_is_not_an_observation(self):
        """The whole point — this must not become 0.0 mm downstream."""
        assert not weather.has_observation({"daily": {"precipitation_sum": [None]}})

    def test_an_empty_daily_block_is_not(self):
        assert not weather.has_observation({"daily": {"precipitation_sum": []}})

    def test_a_payload_with_no_daily_block_is_not(self):
        assert not weather.has_observation({"error": True, "reason": "out of range"})


class TestUrl:
    def test_requests_a_single_day_at_the_circuit(self):
        url = weather.build_url(-23.7036, -46.6997, dt.date(2024, 11, 3))
        assert "latitude=-23.7036" in url
        assert "longitude=-46.6997" in url
        assert "start_date=2024-11-03" in url and "end_date=2024-11-03" in url

    def test_asks_for_utc_so_the_race_date_means_one_thing(self):
        """Circuits span many timezones; a local-time day boundary would put
        some races' rain on the wrong date."""
        assert "timezone=UTC" in weather.build_url(0.0, 0.0, dt.date(2024, 1, 1))

    def test_requests_precipitation(self):
        assert "precipitation_sum" in weather.build_url(0.0, 0.0, dt.date(2024, 1, 1))


class TestCoordinateParsing:
    def _payload(self, races):
        return {"MRData": {"RaceTable": {"Races": races}}}

    def test_reads_coordinates_from_the_races_payload(self):
        coords = ingest.parse_race_coordinates(self._payload([
            {"round": "1", "Circuit": {"Location": {"lat": "26.0325", "long": "50.5106"}}},
        ]))
        assert coords == {1: (26.0325, 50.5106)}

    def test_skips_a_race_with_no_location_without_losing_the_rest(self):
        coords = ingest.parse_race_coordinates(self._payload([
            {"round": "1", "Circuit": {"Location": {"lat": "26.0", "long": "50.5"}}},
            {"round": "2", "Circuit": {}},
            {"round": "3"},
            {"round": "4", "Circuit": {"Location": {"lat": "n/a", "long": "50.5"}}},
            {"round": "5", "Circuit": {"Location": {"lat": "1.0", "long": "2.0"}}},
        ]))
        assert set(coords) == {1, 5}, "one bad circuit must not abort the season"

    def test_negative_coordinates_survive(self):
        """São Paulo is south and west of zero — a sign error would put the
        weather query in the wrong hemisphere."""
        coords = ingest.parse_race_coordinates(self._payload([
            {"round": "21", "Circuit": {"Location": {"lat": "-23.7036", "long": "-46.6997"}}},
        ]))
        assert coords[21] == (-23.7036, -46.6997)


class TestIngestSkips:
    """The orchestration around the fetch, with no network."""

    def _summary(self):
        return ingest.RunSummary()

    def test_skips_a_race_inside_the_lag_without_calling_the_api(self, tmp_path, monkeypatch):
        called = []
        monkeypatch.setattr(weather, "fetch_race_weather",
                            lambda *a: called.append(a))
        summary = self._summary()
        ingest.ingest_race_weather(
            2026, 12, dt.date.today(), {12: (1.0, 2.0)}, str(tmp_path), summary)
        assert called == [], "must not spend a request on unpublished data"
        assert summary.partitions_skipped == 1
        assert summary.files_written == 0

    def test_reports_a_missing_circuit_rather_than_guessing(self, tmp_path):
        summary = self._summary()
        ingest.ingest_race_weather(
            2024, 5, dt.date(2024, 5, 5), {}, str(tmp_path), summary)
        assert summary.failures and "no coordinates" in summary.failures[0]
        assert summary.files_written == 0

    def test_lands_a_file_when_the_observation_exists(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            weather, "fetch_race_weather",
            lambda lat, lon, d: {"daily": {"precipitation_sum": [19.1]}})
        summary = self._summary()
        ingest.ingest_race_weather(
            2024, 16, dt.date(2024, 9, 1), {16: (45.6156, 9.2811)},
            str(tmp_path), summary)
        assert summary.files_written == 1
        landed = (tmp_path / "weather" / "season=2024" / "round=16")
        assert list(landed.glob("*.json")), "expected one landed file"

    def test_a_second_run_skips_a_closed_round(self, tmp_path, monkeypatch):
        """Idempotency applies to weather too — an observation for a finished
        race never changes."""
        monkeypatch.setattr(
            weather, "fetch_race_weather",
            lambda lat, lon, d: {"daily": {"precipitation_sum": [0.0]}})
        for _ in range(2):
            summary = self._summary()
            ingest.ingest_race_weather(
                2024, 16, dt.date(2024, 9, 1), {16: (45.6, 9.2)},
                str(tmp_path), summary)
        assert summary.files_written == 0 and summary.partitions_skipped == 1

    def test_a_fetch_failure_is_recorded_not_raised(self, tmp_path, monkeypatch):
        def boom(*_a):
            raise weather.WeatherError("archive returned no observation")
        monkeypatch.setattr(weather, "fetch_race_weather", boom)
        summary = self._summary()
        ingest.ingest_race_weather(
            2024, 16, dt.date(2024, 9, 1), {16: (45.6, 9.2)}, str(tmp_path), summary)
        assert summary.failures and "weather 2024 r16" in summary.failures[0]
        assert summary.files_written == 0

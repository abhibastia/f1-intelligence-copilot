"""Pagination and record counting.

Jolpica reports `total` as the number of *inner* records for nested endpoints
while the outer array holds one race. Counting the outer length would mean the
loop never reaches `total` and pages forever — the exact failure mode that
turns a 12-page lap fetch into an unbounded one.
"""
import jolpica_client as jc


class TestCountRecords:
    def test_flat_endpoint_counts_the_array(self):
        drivers = [{"driverId": "verstappen"}, {"driverId": "norris"}]
        assert jc._count_records(drivers, "Drivers") == 2

    def test_nested_results_counts_inner_records_not_races(self):
        """One race carrying 20 results is 20 toward `total`, not 1."""
        races = [{"round": "1", "Results": [{}] * 20}]
        assert jc._count_records(races, "Races") == 20

    def test_a_schedule_entry_with_no_inner_array_counts_as_one(self):
        """The races endpoint returns bare schedule entries."""
        races = [{"round": "1"}, {"round": "2"}]
        assert jc._count_records(races, "Races") == 2

    def test_standings_lists_count_their_inner_standings(self):
        lists = [{"round": "1", "DriverStandings": [{}] * 22}]
        assert jc._count_records(lists, "StandingsLists") == 22

    def test_pit_stops_count_as_inner_records(self):
        """`total` for pitstops is the number of stops, while the outer array
        holds one race. Counting the race would make the loop fetch a second,
        empty page for every round."""
        races = [{"round": "16", "PitStops": [{}] * 30}]
        assert jc._count_records(races, "Races") == 30

    def test_laps_count_their_timings_not_the_laps(self):
        """Laps nest twice: a race holds laps, a lap holds one timing per
        driver. `total` counts timings, so 5 laps of 20 drivers is 100 records.
        Counting the laps would compare 5 against 1,008 and page forever."""
        races = [{"round": "16", "Laps": [{"Timings": [{}] * 20} for _ in range(5)]}]
        assert jc._count_records(races, "Races") == 100

    def test_a_lap_with_no_timings_contributes_nothing(self):
        races = [{"round": "16", "Laps": [{"number": "1"}]}]
        assert jc._count_records(races, "Races") == 0

    def test_eleven_pages_of_laps_need_eleven_requests(self, monkeypatch):
        """The real shape: 1,008 timings at 100 per page."""
        pages = []
        for i in range(11):
            n = 5 if i < 10 else 1                      # last page is short
            pages.append({"MRData": {"total": "1008", "RaceTable": {"Races": [
                {"Laps": [{"Timings": [{}] * 20} for _ in range(n)]}]}}})
        calls = []

        def fake_get(url):
            calls.append(url)
            return pages[min(len(calls) - 1, len(pages) - 1)]

        monkeypatch.setattr(jc, "_get", fake_get)
        jc.fetch_all("2024/16/laps")
        assert len(calls) == 11, f"expected 11 requests, made {len(calls)}"

    def test_a_single_page_of_pit_stops_needs_one_request(self, monkeypatch):
        """The regression this guards: 30 stops in one page must not trigger a
        second fetch."""
        calls = []

        def fake_get(url):
            calls.append(url)
            return {"MRData": {"total": "30",
                               "RaceTable": {"Races": [{"PitStops": [{}] * 30}]}}}

        monkeypatch.setattr(jc, "_get", fake_get)
        jc.fetch_all("2024/16/pitstops")
        assert len(calls) == 1, "counting the race instead of the stops costs a request per round"

    def test_mixed_pages_sum(self):
        races = [{"Results": [{}] * 20}, {"Results": [{}] * 18}]
        assert jc._count_records(races, "Races") == 38


class TestKeyDiscovery:
    def test_finds_the_table_key_whatever_it_is_called(self):
        assert jc._find_table_key({"xmlns": "", "RaceTable": {}}) == "RaceTable"
        assert jc._find_table_key({"StandingsTable": {}}) == "StandingsTable"

    def test_returns_none_when_absent(self):
        assert jc._find_table_key({"xmlns": "", "total": "0"}) is None

    def test_finds_the_first_list_in_the_table(self):
        assert jc._find_array_key({"season": "2024", "Races": []}) == "Races"

    def test_array_key_none_when_no_list(self):
        assert jc._find_array_key({"season": "2024"}) is None


class TestFetchAllPaging:
    """`fetch_all` is exercised with a stub transport — no network, no waiting."""

    def _install(self, monkeypatch, pages):
        calls = []

        def fake_get(url):
            calls.append(url)
            return pages[len(calls) - 1]

        monkeypatch.setattr(jc, "_get", fake_get)
        return calls

    def test_stops_once_total_is_reached(self, monkeypatch):
        page = lambda n, total: {
            "MRData": {"total": str(total), "RaceTable": {"Races": [{"Results": [{}] * n}]}}
        }
        calls = self._install(monkeypatch, [page(100, 150), page(50, 150)])
        result = jc.fetch_all("2024/1/laps")
        assert len(calls) == 2, "should stop as soon as the running count reaches total"
        assert len(result["MRData"]["RaceTable"]["Races"]) == 2

    def test_single_page_makes_one_request(self, monkeypatch):
        calls = self._install(monkeypatch, [
            {"MRData": {"total": "20", "RaceTable": {"Races": [{"Results": [{}] * 20}]}}}])
        jc.fetch_all("2024/1/results")
        assert len(calls) == 1

    def test_an_empty_page_stops_the_loop(self, monkeypatch):
        """A `total` that overstates reality must not page forever."""
        calls = self._install(monkeypatch, [
            {"MRData": {"total": "9999", "RaceTable": {"Races": [{"Results": [{}] * 100}]}}},
            {"MRData": {"total": "9999", "RaceTable": {"Races": []}}},
        ])
        jc.fetch_all("2024/1/laps")
        assert len(calls) == 2, "should give up rather than loop on an empty page"

    def test_payload_without_a_table_returns_unchanged(self, monkeypatch):
        self._install(monkeypatch, [{"MRData": {"total": "0"}}])
        assert jc.fetch_all("x")["MRData"]["total"] == "0"

"""Name resolution against live data.

Every case here was a real failure. None would have been caught by unit-testing
the resolver with the strings a developer types - they only appeared when a
language model chose the arguments, saying "Monza" and "Sao Paulo" where the
data says "Italian Grand Prix" and "São Paulo".
"""
import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def broker(lakebase):
    import f1_broker
    return f1_broker


class TestRaceResolution:
    @pytest.mark.parametrize("query,expected_round", [
        ("16", 16),                    # round number
        ("Italian", 16),               # race name
        ("Monza", 16),                 # CIRCUIT name - the agent's first guess
        ("Silverstone", 12),
        ("Interlagos", 21),
    ])
    def test_resolves_by_round_name_or_circuit(self, broker, query, expected_round):
        assert broker.resolve_race(2024, query)["round"] == expected_round

    @pytest.mark.parametrize("query", ["Sao Paulo", "São Paulo"])
    def test_accents_fold_both_ways(self, broker, query):
        """The data says "São Paulo"; nobody types the accent. Before unaccent,
        the agent told the user the race did not exist."""
        assert broker.resolve_race(2024, query)["round"] == 21

    def test_unknown_race_raises_rather_than_guessing(self, broker):
        with pytest.raises(broker.UnknownRaceError):
            broker.resolve_race(2024, "Atlantis")

    def test_resolution_is_reported_back(self, broker):
        """A wrong match must be visible in the answer, not silent."""
        assert "São Paulo" in broker.resolve_race(2024, "Sao Paulo")["race_name"]


class TestDriverResolution:
    @pytest.mark.parametrize("query,expected", [
        ("Verstappen", "Max Verstappen"),
        ("max_verstappen", "Max Verstappen"),
        ("VER", "Max Verstappen"),
        ("Perez", "Sergio Pérez"),          # accent folding
        ("Hulkenberg", "Nico Hülkenberg"),  # umlaut folding
    ])
    def test_resolves_by_name_id_code_or_unaccented(self, broker, query, expected):
        assert broker.resolve_driver(query, 2024)["driver_name"] == expected

    def test_unknown_driver_raises(self, broker):
        with pytest.raises(broker.UnknownDriverError):
            broker.resolve_driver("Nobody At All", 2024)

    def test_empty_input_raises(self, broker):
        with pytest.raises(broker.UnknownDriverError):
            broker.resolve_driver("", 2024)


class TestRetrievalFloor:
    def test_search_never_returns_a_single_passage(self, broker):
        """A model asking for top_k=1 gets a coin flip. It once drew a passage
        from the wrong race and hedged on a question it had previously answered
        correctly, so the floor is enforced in the broker rather than left to
        the prompt."""
        result = broker.search_reports("wet race safety car", top_k=1)
        assert len(result["results"]) >= 3

    def test_search_is_capped(self, broker):
        assert len(broker.search_reports("rain", top_k=999)["results"]) <= broker.MAX_RESULTS

    def test_empty_query_rejected(self, broker):
        with pytest.raises(ValueError):
            broker.search_reports("   ")

    def test_results_carry_weather_for_cross_checking(self, broker):
        """Each hit must carry its race's measured weather, or narrative and
        measurement cannot be checked against each other."""
        for hit in broker.search_reports("rain", top_k=3)["results"]:
            assert "was_wet" in hit and "precipitation_mm" in hit


class TestWeatherHonesty:
    def test_missing_observation_is_not_reported_as_dry(self, broker):
        """"No data" and "no rain" are different claims. A future race has no
        observation, and saying it was fair weather would be a lie."""
        result = broker.race_weather(2026, 23)   # Abu Dhabi, not yet run
        assert result.get("weather_available") is False
        assert "no data" in result.get("note", "").lower()

    def test_known_wet_race_is_flagged(self, broker):
        result = broker.race_weather(2024, "Sao Paulo")
        assert result["was_wet"] is True
        assert result["precipitation_mm"] >= result["wet_threshold_mm"]


class TestStrategy:
    def test_stints_exceed_stops_by_one(self, broker):
        """A driver with two stops ran three stints. Off-by-one here would
        misreport every strategy."""
        result = broker.race_strategy(2024, "Suzuka")
        for driver in result["by_driver"]:
            if driver["stops"] is not None and driver["stints"] is not None:
                assert driver["stints"] == driver["stops"] + 1

    def test_spread_reflects_real_disagreement(self, broker):
        result = broker.strategy_spread(2024, limit=3)
        assert result["races"], "no strategy data loaded"
        for race in result["races"]:
            assert race["max_stints"] >= race["min_stints"]


class TestSeasonSchedule:
    """The schedule tool exists so relative references - "the next race" - can be
    turned into a round number. Without it the model passed the phrase through as
    a race name and answered from nothing."""

    def test_rounds_are_ordered_and_complete(self, broker):
        result = broker.season_schedule(2024)
        rounds = [r["round"] for r in result["schedule"]]
        assert rounds == sorted(rounds), "schedule must be in race order"
        assert rounds == list(range(1, len(rounds) + 1)), "no gaps in the season"

    def test_next_race_after_sao_paulo_is_las_vegas(self, broker):
        """The exact lookup the agent failed before this tool existed."""
        schedule = broker.season_schedule(2024)["schedule"]
        sao_paulo = next(r for r in schedule if "Paulo" in r["race_name"])
        following = next(r for r in schedule if r["round"] == sao_paulo["round"] + 1)
        assert "Las Vegas" in following["race_name"]
        assert following["winner"] == "George Russell"

    def test_unraced_rounds_have_no_winner(self, broker):
        """A scheduled round is in the spine long before anyone drives it, so a
        null winner is the only thing separating the two."""
        result = broker.season_schedule(2026)
        assert result["completed"] < result["rounds"], "2026 should be part-run"
        for race in result["schedule"]:
            if race["winner"] is None:
                assert not broker._has_results(2026, race["round"])

    def test_strategy_for_an_unraced_round_says_so(self, broker):
        """Returning empty fields sent the model searching the report corpus for
        a result that cannot exist, and it answered "not specified in the search
        results" - a retrieval failure, not the truth."""
        result = broker.race_strategy(2026, "Italian Grand Prix")
        assert result.get("status") == "not_yet_raced"
        assert "has not been raced yet" in result["message"]


class TestSessionTimeline:
    """The per-tool aggregate says get_race_weather ran 16 times averaging
    2.2 s. True, and impossible to picture. The session view shows one
    conversation's calls in order, which is where the reasoning path is."""

    def test_steps_stay_in_call_order(self, broker):
        import sys, os
        sys.path.insert(0, os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app"))
        import ui_data
        for session in ui_data.recent_sessions(5):
            assert session["steps"], "a session with no calls should not be listed"
            assert session["calls"] == len(session["steps"]), \
                "the header count must match the steps actually shown"

    def test_test_telemetry_is_excluded(self, broker):
        """pytest logs a `test_tool` call of its own. It is not something the
        agent did, and showing it in the app would misreport the agent's work."""
        import sys, os
        sys.path.insert(0, os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app"))
        import ui_data
        names = {s["tool_name"] for ses in ui_data.recent_sessions(20)
                 for s in ses["steps"]}
        assert not any(n.startswith("test_") for n in names)


class TestRenamedRaces:
    """Races get renamed. Interlagos is the São Paulo Grand Prix in 2024 and the
    Brazilian Grand Prix in 2026; Catalunya is Spanish then Barcelona. Asking
    about Brazil in 2024 failed while the same question about 2026 worked, which
    reads as missing data rather than a naming change."""

    def test_a_later_name_resolves_an_earlier_season(self, broker):
        race = broker.resolve_race(2024, "Brazilian Grand Prix")
        assert race["race_name"] == "São Paulo Grand Prix"
        assert race["round"] == 21

    def test_an_earlier_name_still_resolves_its_own_season(self, broker):
        assert broker.resolve_race(2026, "Brazilian Grand Prix")["round"] == 20

    def test_the_other_renamed_circuit(self, broker):
        assert broker.resolve_race(2026, "Barcelona")["race_name"] == "Barcelona Grand Prix"

    def test_a_round_number_never_reaches_the_alias_lookup(self, broker):
        """`needle` is only bound on the name path; an unguarded fallback would
        raise NameError for every numeric round."""
        assert broker.resolve_race(2024, 16)["race_name"] == "Italian Grand Prix"

    def test_a_genuinely_unknown_race_still_fails(self, broker):
        with pytest.raises(broker.UnknownRaceError):
            broker.resolve_race(2024, "Atlantis")

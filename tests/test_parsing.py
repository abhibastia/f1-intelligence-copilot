"""Parsing and chunking - the pure logic, and the bugs that hid in it."""
import pytest

from f1lake.load import chunk_text, disambiguate, document_id
from f1lake.load_strategy import parse_duration
from harvest.wikipedia import split_sections, title_from_url, WikipediaError

# The WMO-code / wet-threshold parsing this file used to test here lived in
# harvest/weather.py, a second independent Open-Meteo fetch. Merging with
# formula1-capstone-project removed it: weather is now seeded from the
# governed f1.gold.race_conditions mart (src/pipeline/02b_silver_weather.py
# computes the same WET_THRESHOLD_MM = 1.0 and WMO-code CONDITIONS mapping in
# Spark, covered by that pipeline's own tests) rather than fetched twice.


class TestPitStopDuration:
    """Durations arrive in two formats and one was silently dropped.

    Jolpica reports most stops as plain seconds but long ones as M:SS.mmm.
    Treating the second form as unparseable discarded 83 stops - which were
    precisely the interesting ones, because a 65-second stop is a race-defining
    failure rather than noise.
    """

    def test_plain_seconds(self):
        assert parse_duration("23.456") == pytest.approx(23.456)

    def test_minutes_seconds_format(self):
        assert parse_duration("1:05.820") == pytest.approx(65.820)

    def test_long_stoppage(self):
        # Monaco 2024: the race was suspended and the field changed tyres.
        assert parse_duration("12:57.770") == pytest.approx(777.77)

    @pytest.mark.parametrize("value", [None, "", "not-a-number", "1:2:3:4"])
    def test_unparseable_returns_none_rather_than_raising(self, value):
        assert parse_duration(value) is None


class TestChunking:
    def test_short_text_is_one_chunk(self):
        assert chunk_text("a short section") == ["a short section"]

    def test_empty_text_yields_nothing(self):
        assert chunk_text("") == []
        assert chunk_text("   ") == []

    def test_long_text_splits_with_overlap(self):
        chunks = chunk_text("word " * 500, size=900, overlap=150)
        assert len(chunks) > 1
        # Overlap must actually overlap, or a sentence on a boundary is lost
        # from both sides.
        assert chunks[0][-100:] in chunks[1] or chunks[1][:100] in chunks[0]

    def test_every_chunk_within_size(self):
        for chunk in chunk_text("x" * 5000, size=900, overlap=150):
            assert len(chunk) <= 900


class TestSectionDisambiguation:
    """The 2025 Las Vegas article repeats the heading "Race".

    Since a document id derives from (season, round, section), the repeat
    produced two rows with the same id in one batch, which Postgres rejects:
    "ON CONFLICT DO UPDATE command cannot affect row a second time".
    """

    def test_unique_names_are_untouched(self):
        out = disambiguate([{"section": "Race", "text": "a"},
                            {"section": "Qualifying", "text": "b"}])
        assert [name for name, _ in out] == ["Race", "Qualifying"]

    def test_repeated_name_is_suffixed_not_dropped(self):
        out = disambiguate([{"section": "Race", "text": "first"},
                            {"section": "Race", "text": "second"}])
        assert [name for name, _ in out] == ["Race", "Race (2)"]
        # Both bodies survive - they are different text.
        assert [body for _, body in out] == ["first", "second"]

    def test_disambiguated_ids_are_unique(self):
        out = disambiguate([{"section": "Race", "text": "a"},
                            {"section": "Race", "text": "b"}])
        ids = {document_id(2025, 22, name) for name, _ in out}
        assert len(ids) == 2

    def test_document_id_is_deterministic(self):
        assert document_id(2024, 21, "Race report") == document_id(2024, 21, "Race report")


class TestWikipediaParsing:
    def test_title_from_url(self):
        assert title_from_url(
            "https://en.wikipedia.org/wiki/2024_S%C3%A3o_Paulo_Grand_Prix"
        ) == "2024 São Paulo Grand Prix"

    def test_non_article_url_rejected(self):
        with pytest.raises(WikipediaError):
            title_from_url("https://example.com/not-wikipedia")

    def test_sections_are_split_on_headings(self):
        sections = split_sections("Lead text.\n\n== Race report ==\nIt rained.\n")
        names = [n for n, _ in sections]
        assert "Summary" in names and "Race report" in names

    def test_noise_sections_are_dropped(self):
        sections = split_sections(
            "Lead.\n\n== Race report ==\nBody.\n\n== References ==\n[1] cite\n")
        assert "References" not in [n for n, _ in sections]

    def test_standings_tables_are_dropped(self):
        sections = split_sections(
            "Lead.\n\n== Championship standings after the race ==\n1 VER 400\n")
        assert not any("standings" in n.lower() for n, _ in sections)


class TestErrorMessagesAreSafe:
    """Tool errors travel: an exception message becomes an "error" field in the
    tool result, which the agent repeats and the UI renders in the trace. A
    psycopg2 failure names the host and role it could not reach."""

    def test_database_errors_do_not_leak_the_connection_target(self):
        import psycopg2
        from f1lake import schema
        try:
            psycopg2.connect(
                "postgresql://someuser:hunter2@secret-host.example.com:5432/db",
                connect_timeout=1)
            raise AssertionError("expected the connection to fail")
        except psycopg2.Error as exc:
            safe = schema.safe_message(exc)
        assert "secret-host" not in safe
        assert "someuser" not in safe and "hunter2" not in safe
        assert safe == "The database is unavailable."

    def test_messages_written_for_users_pass_through(self):
        """UnknownRaceError and friends subclass ValueError and are already
        phrased for a reader - replacing them would make the agent less able to
        explain itself, not more secure."""
        from f1lake import schema
        assert schema.safe_message(
            ValueError('No race matched "Foo" in 2024.')) == 'No race matched "Foo" in 2024.'

    def test_unexpected_types_are_replaced(self):
        from f1lake import schema
        assert schema.safe_message(RuntimeError("/etc/secrets/token: permission denied")) \
            == "The request could not be completed."

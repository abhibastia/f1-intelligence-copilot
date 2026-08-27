"""Both of Jolpica's limits.

The burst limit was always enforced. The sustained one was not, and it is the
one that binds on a large backfill: `laps` is ~850 paginated requests against a
500/hour ceiling, so a run respecting only the per-second gap breaches the hour
in the first five minutes and spends the rest of the harvest collecting 429s.

The clock and sleeper are injected so these assert on *how long it would wait*
rather than actually waiting.
"""
import jolpica_client as jc


class FakeClock:
    """A clock the test advances, plus a sleeper that advances it."""

    def __init__(self):
        self.now = 0.0
        self.slept = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds

    @property
    def long_waits(self):
        """Sleeps that are the hourly pause rather than burst spacing.

        Burst spacing is sub-second by construction; the sustained pause is
        minutes. Filtering keeps these tests asserting on one limit at a time.
        """
        return [s for s in self.slept if s > 1.0]


def budget(per_second=2.0, per_hour=450):
    clock = FakeClock()
    return jc.RateBudget(per_second, per_hour, clock=clock, sleeper=clock.sleep), clock


class TestBurstLimit:
    def test_consecutive_requests_are_spaced(self):
        b, clock = budget(per_second=2.0)
        b.acquire()
        b.acquire()
        assert clock.slept == [0.5], "second request should wait 1/2 s"

    def test_no_wait_when_enough_time_has_passed(self):
        b, clock = budget(per_second=2.0)
        b.acquire()
        clock.now += 10
        b.acquire()
        assert clock.slept == []


class TestSustainedLimit:
    def test_stops_at_the_hourly_ceiling(self):
        """The bug this exists for: 5 requests allowed, the 6th must wait."""
        b, clock = budget(per_second=1000.0, per_hour=5)
        for _ in range(5):
            b.acquire()
        assert clock.long_waits == [], "the first 5 are within budget"

        b.acquire()
        assert len(clock.long_waits) == 1
        assert clock.long_waits[0] > 3500, "should wait out the rest of the hour"

    def test_the_window_slides_rather_than_resetting(self):
        """A counter reset on the hour would allow 450 requests at minute 59 and
        another 450 at minute 61. The window has to age individual requests out."""
        b, clock = budget(per_second=1000.0, per_hour=3)
        for _ in range(3):
            b.acquire()
        clock.now += jc.HOUR_SECONDS + 1        # every request ages out
        b.acquire()
        assert clock.long_waits == [], "expired requests should not count against the budget"

    def test_partially_expired_window_frees_exactly_one_slot(self):
        b, clock = budget(per_second=1000.0, per_hour=2)
        b.acquire()
        clock.now += 100
        b.acquire()
        clock.now += jc.HOUR_SECONDS - 100 + 1  # only the first ages out
        b.acquire()
        assert clock.long_waits == []
        b.acquire()
        assert len(clock.long_waits) == 1, "budget is full again, so this one waits"

    def test_used_this_hour_reports_the_live_window(self):
        b, clock = budget(per_second=1000.0, per_hour=10)
        for _ in range(4):
            b.acquire()
        assert b.used_this_hour == 4
        clock.now += jc.HOUR_SECONDS + 1
        assert b.used_this_hour == 0


class TestRealisticHarvest:
    def test_850_lap_requests_are_paced_across_hours(self):
        """Three seasons of laps at 12 pages a round. The point is that it does
        not fire them all immediately — not that it is fast."""
        b, clock = budget(per_second=2.0, per_hour=450)
        for _ in range(850):
            b.acquire()
        assert clock.now > jc.HOUR_SECONDS, (
            "850 requests must span more than an hour under a 450/hour ceiling")
        assert b.used_this_hour <= 450

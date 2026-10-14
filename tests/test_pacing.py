import unittest

from api.pacing import Pacer
from api.steam_api import STORE_REQUEST_INTERVAL_SECONDS
from jobs.base import RunContext


class FakeTime:
    """A clock that only moves when the code sleeps or work is simulated."""

    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(round(seconds, 3))
        self.now += seconds


class PacerTests(unittest.TestCase):
    def test_the_first_request_does_not_wait(self):
        fake = FakeTime()
        Pacer(2.1, fake.sleep, fake.clock).wait()

        self.assertEqual(fake.sleeps, [])

    def test_waits_only_for_the_rest_of_the_interval(self):
        fake = FakeTime()
        pacer = Pacer(2.1, fake.sleep, fake.clock)

        pacer.wait()
        fake.now += 0.5  # the request, parsing and loading took 0.5 s
        pacer.wait()

        self.assertEqual(fake.sleeps, [1.6])

    def test_no_wait_when_the_interval_already_passed(self):
        fake = FakeTime()
        pacer = Pacer(1.1, fake.sleep, fake.clock)

        pacer.wait()
        fake.now += 2.4  # e.g. a Store request and its pause in between
        pacer.wait()

        self.assertEqual(fake.sleeps, [])

    def test_request_starts_are_never_closer_than_the_interval(self):
        fake = FakeTime()
        pacer = Pacer(2.1, fake.sleep, fake.clock)
        starts = []

        for work in (0.0, 0.3, 3.0, 0.1, 2.0):
            pacer.wait()
            starts.append(fake.now)
            fake.now += work

        gaps = [later - earlier for earlier, later in zip(starts, starts[1:])]
        self.assertTrue(all(gap >= 2.1 - 1e-9 for gap in gaps))


class RunPacingTests(unittest.TestCase):
    def test_a_run_shares_one_pacer_per_api_across_steps(self):
        sleeps = []
        ctx = RunContext(
            connection=None, cursor=None, run_id=1, session=None,
            store=None, sleep_fn=sleeps.append
        )

        ctx.pace("store")     # last Store details request
        ctx.pace("steamspy")  # a different API: no wait
        ctx.pace("store")     # first price batch: waits for the rest

        self.assertEqual(len(sleeps), 1)
        self.assertGreater(sleeps[0], STORE_REQUEST_INTERVAL_SECONDS - 0.1)
        self.assertLessEqual(sleeps[0], STORE_REQUEST_INTERVAL_SECONDS)


if __name__ == "__main__":
    unittest.main()

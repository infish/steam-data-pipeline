import unittest
from datetime import datetime

from jobs.base import describe_timing
from jobs.timing import StepTimer


NOW = datetime(2026, 10, 14, 20, 0)


class FakeClock:
    def __init__(self):
        self.seconds = 0.0

    def __call__(self):
        return self.seconds


def timer_with_clock():
    clock = FakeClock()
    timer = StepTimer(lambda: NOW, clock=clock)
    return timer, clock


class StepTimerTests(unittest.TestCase):
    def test_nested_steps_record_their_parent_and_order(self):
        timer, clock = timer_with_clock()

        with timer.step("job"):
            with timer.step("fetch"):
                clock.seconds += 1.5

            with timer.step("load") as step:
                clock.seconds += 0.25
                step["rows"] = 40

        records = {record["step"]: record for record in timer.records}
        self.assertEqual(
            [record["step"] for record in timer.records],
            ["job", "fetch", "load"]
        )
        self.assertIsNone(records["job"]["parent_step"])
        self.assertEqual(records["fetch"]["parent_step"], "job")
        self.assertEqual(records["fetch"]["duration_ms"], 1500)
        self.assertEqual(records["load"]["rows"], 40)
        self.assertEqual(records["job"]["duration_ms"], 1750)
        self.assertEqual(records["job"]["position"], 0)

    def test_rate_limit_pauses_count_as_waiting(self):
        timer, clock = timer_with_clock()

        def fake_sleep(seconds):
            clock.seconds += seconds

        sleep = timer.wrap_sleep(fake_sleep)

        with timer.step("store_details") as step:
            for _ in range(3):
                sleep(2.1)
                clock.seconds += 0.4

        self.assertEqual(step["duration_ms"], 7500)
        self.assertEqual(step["wait_ms"], 6300)

    def test_requests_are_the_responses_recorded_during_the_step(self):
        timer, _ = timer_with_clock()
        recorded = {"stored": 5}
        timer.requests_fn = lambda: sum(recorded.values())

        with timer.step("prices") as step:
            recorded["stored"] += 3
            recorded["duplicate"] = 2

        self.assertEqual(step["requests"], 5)

    def test_a_failing_step_and_its_parents_are_marked_failed(self):
        timer, _ = timer_with_clock()

        with self.assertRaises(ValueError):
            with timer.step("job"):
                with timer.step("load_games"):
                    pass

                with timer.step("load_company_links"):
                    raise ValueError("error 1869")

        statuses = {
            record["step"]: record["status"] for record in timer.records
        }
        self.assertEqual(statuses, {
            "job": "failed",
            "load_games": "ok",
            "load_company_links": "failed",
        })
        self.assertEqual(len(timer.finished_records()), 3)

    def test_run_log_summarises_the_phases(self):
        timer, clock = timer_with_clock()

        with timer.step("setup"):
            clock.seconds += 0.1

        with timer.step("job"):
            with timer.step("fetch"):
                clock.seconds += 2

        self.assertEqual(
            describe_timing(timer),
            "took 2.1 s: setup 0.1 s, job 2.0 s"
        )


if __name__ == "__main__":
    unittest.main()

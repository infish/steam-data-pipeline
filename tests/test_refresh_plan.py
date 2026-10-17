import unittest

from jobs.refresh import per_run, plan_slices


class RefreshPlanTests(unittest.TestCase):
    def test_slices_cover_the_catalog_within_the_refresh_days(self):
        # 5,045 games: details every 7 days, reviews daily, 24 runs a day.
        self.assertEqual(plan_slices(5045, 7, 1, 300), (31, 211))
        self.assertGreaterEqual(31 * 24 * 7, 5045)
        self.assertGreaterEqual(211 * 24, 5045)

    def test_zero_days_switches_a_slice_off(self):
        self.assertEqual(per_run(5045, 0), 0)
        self.assertEqual(plan_slices(5045, 0, 1, 300), (0, 211))

    def test_overrides_replace_the_derived_counts(self):
        self.assertEqual(plan_slices(4, 7, 1, 300, 120, 0), (120, 0))

    def test_the_request_ceiling_shrinks_both_in_proportion(self):
        details, reviews = plan_slices(5045, 7, 1, 100)

        self.assertEqual(details + reviews, 100)
        self.assertGreaterEqual(details, 1)
        self.assertLess(details, reviews)

    def test_an_empty_catalog_asks_for_nothing(self):
        self.assertEqual(plan_slices(0, 7, 1, 300), (0, 0))


if __name__ == "__main__":
    unittest.main()

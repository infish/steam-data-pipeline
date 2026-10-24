import json
import os
import unittest
from datetime import datetime
from unittest.mock import patch

from jobs.base import utc_now

from .base import IntegrationTestCase


def at(day):
    moment = datetime(2026, 10, day, 18, 0)
    return lambda: moment


def priced(final, initial=None, discount=0, currency="USD"):
    return {
        "success": True,
        "data": {
            "price_overview": {
                "currency": currency,
                "initial": final if initial is None else initial,
                "final": final,
                "discount_percent": discount,
                "initial_formatted": "",
                "final_formatted": "",
            }
        },
    }


NO_PRICE = {"success": True, "data": []}
UNAVAILABLE = {"success": False}


class PriceHistoryIntegrationTests(IntegrationTestCase):
    def versions(self, appid, country="us"):
        return self.query(
            "SELECT * FROM game_price_history"
            " WHERE appid = %s AND country_code = %s ORDER BY price_id",
            (appid, country)
        )

    def check(self, day, prices=None):
        overrides = {
            f"store_price:{appid}": state
            for appid, state in (prices or {}).items()
        }
        return self.run_job("catalog_daily", overrides, now_fn=at(day))

    def test_first_check_opens_one_version_per_game(self):
        run_id = self.check(1)

        states = {
            row["appid"]: row
            for row in self.query("SELECT * FROM game_price_history")
        }
        self.assertEqual(sorted(states), [570, 620, 730, 427520])
        self.assertTrue(all(row["is_current"] for row in states.values()))
        self.assertTrue(all(
            row["opened_by_run_id"] == run_id for row in states.values()
        ))

        # From the recorded Phase 10 responses.
        portal = states[620]
        self.assertEqual(
            (portal["price_state"], portal["currency"],
             portal["initial_price"], portal["final_price"],
             portal["discount_percent"]),
            ("priced", "USD", 999, 199, 80)
        )
        self.assertEqual(states[730]["price_state"], "no_price")
        self.assertIsNone(states[730]["currency"])

    def test_an_unchanged_price_writes_nothing(self):
        self.check(1)
        before = self.query("SELECT * FROM game_price_history")

        self.check(2)

        self.assertEqual(self.query("SELECT * FROM game_price_history"), before)

    def test_a_price_change_closes_the_old_version_and_opens_a_new_one(self):
        self.check(1)
        second_run = self.check(2, {427520: priced(1750, 3500, 50)})

        old, new = self.versions(427520)
        self.assertFalse(old["is_current"])
        self.assertEqual(old["valid_to"], at(2)())
        self.assertEqual(old["closed_by_run_id"], second_run)
        self.assertEqual(old["final_price"], 3500)

        self.assertTrue(new["is_current"])
        self.assertIsNone(new["valid_to"])
        self.assertEqual(new["valid_from"], at(2)())
        self.assertEqual(
            (new["initial_price"], new["final_price"],
             new["discount_percent"]),
            (3500, 1750, 50)
        )

    def test_free_to_paid(self):
        self.check(1)
        self.check(2, {730: priced(1499)})

        free, paid = self.versions(730)
        self.assertEqual(free["price_state"], "no_price")
        self.assertEqual(free["valid_to"], at(2)())
        self.assertEqual(paid["price_state"], "priced")
        self.assertEqual(paid["final_price"], 1499)
        self.assertTrue(paid["is_current"])

    def test_removed_from_sale(self):
        self.check(1)
        self.check(2, {620: UNAVAILABLE})

        on_sale, removed = self.versions(620)
        self.assertEqual(on_sale["price_state"], "priced")
        self.assertFalse(on_sale["is_current"])
        self.assertEqual(removed["price_state"], "unavailable")
        self.assertIsNone(removed["currency"])
        self.assertTrue(removed["is_current"])

        # Back on sale later: a third version.
        self.check(3, {620: priced(999)})
        self.assertEqual(
            [row["price_state"] for row in self.versions(620)],
            ["priced", "unavailable", "priced"]
        )

    def test_summary_derives_lowest_price_sales_and_on_sale(self):
        self.check(1, {427520: priced(3500)})
        self.check(2, {427520: priced(1750, 3500, 50)})
        self.check(3, {427520: priced(3500)})
        self.check(4, {427520: priced(2625, 3500, 25)})
        self.check(5, {427520: priced(3500)})

        (summary,) = self.query(
            "SELECT * FROM game_price_summary WHERE appid = 427520"
        )
        self.assertEqual(summary["price_state"], "priced")
        self.assertEqual(summary["current_final_price"], 3500)
        self.assertEqual(summary["on_sale"], 0)
        self.assertEqual(summary["lowest_price"], 1750)
        self.assertEqual(summary["lowest_price_currency"], "USD")
        self.assertEqual(summary["discount_events"], 2)
        # The last sale ended when the day-5 check found the full price.
        self.assertEqual(
            summary["days_since_last_discount"],
            (utc_now().date() - at(5)().date()).days
        )

        (portal,) = self.query(
            "SELECT * FROM game_price_summary WHERE appid = 620"
        )
        self.assertEqual(portal["on_sale"], 1)
        self.assertEqual(portal["days_since_last_discount"], 0)

    def test_each_store_country_has_its_own_history(self):
        self.check(1)

        with patch.dict(os.environ, {"STEAM_STORE_COUNTRY": "gb"}):
            self.check(2, {427520: priced(2500, currency="GBP")})

        self.assertEqual(len(self.versions(427520, "us")), 1)
        (uk,) = self.versions(427520, "gb")
        self.assertEqual((uk["currency"], uk["final_price"]), ("GBP", 2500))

        (request,) = self.query(
            "SELECT request_params FROM raw_api_responses"
            " WHERE source = 'store_prices' AND JSON_EXTRACT("
            " request_params, '$.cc') = 'gb'"
        )
        self.assertEqual(
            json.loads(request["request_params"])["filters"],
            "price_overview"
        )

    def test_a_failed_batch_leaves_prices_unchanged_with_a_warning(self):
        self.check(1)
        before = self.query("SELECT * FROM game_price_history")

        with self.assertLogs(level="WARNING"):
            run_id = self.run_job(
                "catalog_daily",
                {"store_prices:570,620,730,427520": (503, {"error": "busy"})},
                now_fn=at(2)
            )

        self.assertEqual(self.query("SELECT * FROM game_price_history"), before)
        self.assertIn(
            "Store price check failed for 1 batch(es) (4 games",
            self.run_row(run_id)["warning_message"]
        )

    def test_a_null_or_partial_answer_writes_no_price_history(self):
        # The Store sometimes answers a whole batch with null; before
        # v1.3.0 every game in it went "unavailable" (and back next day).
        self.check(1)
        before = self.query("SELECT * FROM game_price_history")
        batch = "store_prices:570,620,730,427520"

        with self.assertLogs(level="WARNING"):
            null_run = self.run_job("catalog_daily", {batch: None},
                                    now_fn=at(2))
            partial_run = self.run_job(
                "catalog_daily",
                {batch: {"730": {"success": True, "data": []}}},
                now_fn=at(3)
            )

        self.assertEqual(
            self.query("SELECT * FROM game_price_history"), before
        )
        self.assertIn("Store price check failed for 1 batch(es)",
                      self.run_row(null_run)["warning_message"])
        self.assertIn("left out 3 game(s), prices unchanged",
                      self.run_row(partial_run)["warning_message"])


if __name__ == "__main__":
    unittest.main()

import os
import unittest
from unittest.mock import patch

from config import get_max_rejected_percent
from quality.data_quality import validate_games_data
from transform.data_transformer import transform_games_data


def game(appid, **overrides):
    row = {
        "appid": appid,
        "name": f"Game {appid}",
        "owners": "10,000 .. 20,000",
        "positive_reviews": "90",
        "negative_reviews": "10",
        "average_playtime_forever_minutes": "120",
        "average_playtime_2weeks_minutes": "30",
        "median_playtime_forever_minutes": "60",
        "median_playtime_2weeks_minutes": "15",
        "ccu": "500",
        "price_cents": "1999",
        "initial_price_cents": "2999",
        "discount_percent": "33"
    }
    row.update(overrides)
    return row


def games(count, **bad_rows):
    """count good rows with app IDs 1..count, then the given bad rows."""
    rows = [game(appid) for appid in range(1, count + 1)]
    rows += [game(int(appid), **fields) for appid, fields in bad_rows.items()]
    return transform_games_data(rows)


class ValidateGamesDataTests(unittest.TestCase):
    def test_null_owners_is_rejected_not_a_crash(self):
        df = games(99, **{"500": {"owners": None}})

        valid, rejected = validate_games_data(df)

        self.assertEqual(len(valid), 99)
        self.assertNotIn(500, set(valid["appid"]))
        self.assertEqual(len(rejected), 1)
        appid, reasons = rejected[0]
        self.assertEqual(appid, 500)
        self.assertIn("owners is missing", reasons[0])

    def test_malformed_owners_are_rejected(self):
        df = games(
            97,
            **{
                "501": {"owners": "lots"},
                "502": {"owners": 1000},
                "503": {"owners": "10 .. "}
            }
        )

        valid, rejected = validate_games_data(df)

        self.assertEqual(len(valid), 97)
        self.assertEqual([appid for appid, _ in rejected], [501, 502, 503])

    def test_empty_name_is_rejected(self):
        df = games(
            98,
            **{"600": {"name": "   "}, "601": {"name": None}}
        )

        valid, rejected = validate_games_data(df)

        self.assertEqual(len(valid), 98)
        self.assertEqual(
            rejected,
            [
                (600, ["name is missing or empty"]),
                (601, ["name is missing or empty"])
            ]
        )

    def test_row_with_several_problems_lists_every_reason(self):
        df = games(99, **{"700": {"name": "", "ccu": "-1"}})

        _, rejected = validate_games_data(df)

        self.assertEqual(
            rejected,
            [(700, ["name is missing or empty", "ccu is negative"])]
        )

    def test_rejections_over_the_threshold_raise(self):
        df = games(
            94,
            **{str(appid): {"owners": None} for appid in range(900, 906)}
        )

        with self.assertRaisesRegex(ValueError, r"6 of 100 rows \(6\.0%\)"):
            validate_games_data(df, max_rejected_percent=5)

    def test_rejections_under_the_threshold_return_the_valid_subset(self):
        df = games(
            95,
            **{str(appid): {"owners": None} for appid in range(900, 905)}
        )

        valid, rejected = validate_games_data(df, max_rejected_percent=5)

        self.assertEqual(list(valid["appid"]), list(range(1, 96)))
        self.assertEqual(len(rejected), 5)

    def test_structural_problems_still_raise(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            validate_games_data(transform_games_data([game(1), game(1)]))

        with self.assertRaisesRegex(ValueError, "Missing columns: ccu"):
            validate_games_data(games(3).drop(columns=["ccu"]))

    def test_all_rows_rejected_raises_even_with_no_limit(self):
        with self.assertRaisesRegex(ValueError, "No rows passed"):
            validate_games_data(
                transform_games_data([game(1, name="")]),
                max_rejected_percent=100
            )


class MaxRejectedPercentConfigTests(unittest.TestCase):
    def test_defaults_to_five(self):
        with patch.dict(os.environ, {"PIPELINE_MAX_REJECTED_PERCENT": ""}):
            self.assertEqual(get_max_rejected_percent(), 5.0)

    def test_rejects_invalid_values(self):
        for value in ("abc", "-1", "101"):
            with (
                self.subTest(value=value),
                patch.dict(
                    os.environ,
                    {"PIPELINE_MAX_REJECTED_PERCENT": value}
                ),
                self.assertRaises(ValueError)
            ):
                get_max_rejected_percent()


if __name__ == "__main__":
    unittest.main()

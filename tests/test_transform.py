import unittest

import pandas as pd

from transform.data_transformer import transform_games_data


class TransformGamesDataTests(unittest.TestCase):
    def test_calculates_owner_and_review_metrics(self):
        games = [{
            "appid": 1,
            "name": "Test Game",
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
        }]

        result = transform_games_data(games).iloc[0]

        self.assertEqual(result["owners_low"], 10_000)
        self.assertEqual(result["owners_high"], 20_000)
        self.assertEqual(result["estimated_owners"], 15_000)
        self.assertEqual(result["total_reviews"], 100)
        self.assertEqual(result["review_score_percent"], 90.0)

    def test_zero_reviews_produces_missing_score(self):
        games = [{
            "appid": 2,
            "name": "Unreviewed Game",
            "owners": "0 .. 20,000",
            "positive_reviews": "0",
            "negative_reviews": "0",
            "average_playtime_forever_minutes": "0",
            "average_playtime_2weeks_minutes": "0",
            "median_playtime_forever_minutes": "0",
            "median_playtime_2weeks_minutes": "0",
            "ccu": "0",
            "price_cents": "0",
            "initial_price_cents": "0",
            "discount_percent": "0"
        }]

        result = transform_games_data(games).iloc[0]

        self.assertTrue(pd.isna(result["review_score_percent"]))


if __name__ == "__main__":
    unittest.main()
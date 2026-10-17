"""Deterministic synthetic API responses for scale testing (ADR 6).

SyntheticSession answers every route the pipeline uses with generated
payloads shaped like the recorded ones in tests/fixtures/sources/, for any
number of games. The same app ID always gets the same answer, so runs are
repeatable. It never touches the network.
"""

import random
from datetime import date, timedelta

from ingest.replay import FixtureSession


GAMES_PER_PAGE = 1000
GENRES = [
    (1, "Action"), (2, "Strategy"), (3, "RPG"), (4, "Casual"),
    (9, "Racing"), (18, "Sports"), (23, "Indie"), (25, "Adventure"),
    (28, "Simulation"), (29, "Massively Multiplayer"), (37, "Free To Play"),
]
CATEGORIES = [
    (1, "Multi-player"), (2, "Single-player"), (9, "Co-op"),
    (22, "Steam Achievements"), (29, "Steam Trading Cards"),
    (30, "Steam Workshop"), (35, "In-App Purchases"), (62, "Family Sharing"),
]
LANGUAGES = ["English", "French", "German", "Spanish - Spain", "Japanese",
             "Russian", "Simplified Chinese", "Czech", "Polish", "Korean"]
TAGS = [f"Tag {number:03d}" for number in range(300)]
STUDIOS = [f"Studio {number:04d}" for number in range(1500)]


def appid_for(page, index):
    """App IDs spread like real ones, unique across pages."""
    return 10 + (page * GAMES_PER_PAGE + index) * 10


def rng_for(appid, salt=""):
    return random.Random(f"{appid}:{salt}")


class SyntheticSession(FixtureSession):
    """A FixtureSession that generates every answer.

    pages: SteamSpy pages that exist (more return an empty page).
    chart_extra: games in Valve's charts that are not on any page.
    """

    def __init__(self, pages=5, chart_extra=30, overrides=None):
        super().__init__(overrides)
        self.pages = pages
        self.chart_extra = chart_extra

    def catalog(self):
        return [
            appid_for(page, index)
            for page in range(self.pages)
            for index in range(GAMES_PER_PAGE)
        ]

    def _fixture(self, route):
        name, _, key = route.partition(":")

        if name == "store_prices":
            return self._price_batch(key.split(","))

        builder = getattr(self, f"_{name}", None)
        return builder(key) if builder else None

    def _price_batch(self, appids):
        return {appid: self._price_entry(int(appid)) for appid in appids}

    # ---- SteamSpy ------------------------------------------------------

    def _steamspy_all(self, key):
        page = int(key or 0)

        if page >= self.pages:
            return {}

        return {
            str(appid): self._steamspy_row(appid)
            for appid in (
                appid_for(page, index) for index in range(GAMES_PER_PAGE)
            )
        }

    def _steamspy_row(self, appid):
        rng = rng_for(appid, "spy")
        low = rng.choice([20000, 50000, 100000, 200000, 500000, 1000000])
        price = rng.choice([0, 499, 999, 1999, 2999, 5999])
        return {
            "appid": appid,
            "name": f"Synthetic Game {appid}",
            "developer": rng.choice(STUDIOS),
            "publisher": rng.choice(STUDIOS),
            "score_rank": "",
            "positive": rng.randint(10, 500000),
            "negative": rng.randint(1, 50000),
            "userscore": 0,
            "owners": f"{low:,} .. {low * 2:,}",
            "average_forever": rng.randint(0, 20000),
            "average_2weeks": rng.randint(0, 2000),
            "median_forever": rng.randint(0, 10000),
            "median_2weeks": rng.randint(0, 1000),
            "price": str(price),
            "initialprice": str(price),
            "discount": "0",
            "ccu": rng.randint(0, 50000),
        }

    def _steamspy_appdetails(self, key):
        appid = int(key)
        rng = rng_for(appid, "tags")
        return {
            **self._steamspy_row(appid),
            "tags": {
                tag: rng.randint(10, 5000) for tag in rng.sample(TAGS, 20)
            },
        }

    # ---- Store -------------------------------------------------------

    def _store_appdetails(self, key):
        appid = int(key)
        rng = rng_for(appid, "store")
        released = date(2005, 1, 1) + timedelta(days=rng.randint(0, 7500))
        is_free = rng.random() < 0.1
        return {str(appid): {"success": True, "data": {
            "type": "game",
            "name": f"Synthetic Game {appid}",
            "developers": [rng.choice(STUDIOS)],
            "publishers": [rng.choice(STUDIOS)],
            "is_free": is_free,
            "required_age": rng.choice([0, 0, 0, 16, 18]),
            "release_date": {
                "coming_soon": False,
                "date": released.strftime("%-d %b, %Y"),
            },
            "platforms": {
                "windows": True,
                "mac": rng.random() < 0.3,
                "linux": rng.random() < 0.2,
            },
            "metacritic": {"score": rng.randint(40, 95)},
            "recommendations": {"total": rng.randint(10, 500000)},
            "genres": [
                {"id": str(genre_id), "description": name}
                for genre_id, name in rng.sample(GENRES, rng.randint(1, 3))
            ],
            "categories": [
                {"id": category_id, "description": name}
                for category_id, name in rng.sample(CATEGORIES, 4)
            ],
            "supported_languages": ", ".join(
                rng.sample(LANGUAGES, rng.randint(1, 6))
            ),
            "price_overview": None if is_free else self._price(appid),
        }}}

    def _price(self, appid):
        rng = rng_for(appid, "price")
        initial = rng.choice([499, 999, 1999, 2999, 5999])
        discount = rng.choice([0, 0, 0, 25, 50])
        return {
            "currency": "USD",
            "initial": initial,
            "final": initial * (100 - discount) // 100,
            "discount_percent": discount,
        }

    def _price_entry(self, appid):
        if rng_for(appid, "store").random() < 0.1:
            return {"success": True, "data": []}

        return {
            "success": True,
            "data": {"price_overview": self._price(appid)},
        }

    def _steam_review_summary(self, key):
        rng = rng_for(int(key), "reviews")
        positive = rng.randint(10, 500000)
        negative = rng.randint(1, max(2, positive // 4))
        return {"success": 1, "query_summary": {
            "num_reviews": 0,
            "review_score": 8,
            "review_score_desc": "Very Positive",
            "total_positive": positive,
            "total_negative": negative,
            "total_reviews": positive + negative,
        }}

    def _steam_review_histogram(self, key):
        rng = rng_for(int(key), "histogram")
        return {"success": 1, "results": {
            "rollup_type": "month",
            # One monthly bucket (2026-09-01) and one daily bucket.
            "rollups": [{
                "date": 1_788_220_800,
                "recommendations_up": rng.randint(100, 10000),
                "recommendations_down": rng.randint(10, 1000),
            }],
            "recent": [{
                "date": 1_790_812_800,
                "recommendations_up": rng.randint(1, 500),
                "recommendations_down": rng.randint(0, 50),
            }],
        }}

    def _steam_news(self, key):
        return {"appnews": {"appid": int(key), "newsitems": []}}

    # ---- Valve -------------------------------------------------------

    def _chart_appids(self):
        catalog = self.catalog()
        extra = [9_000_000 + number * 10 for number in range(self.chart_extra)]
        return (catalog[:100 - len(extra)] + extra)[:100]

    def _valve_top_concurrent(self, key):
        return {"response": {
            "last_update": 1_760_000_000,
            "ranks": [
                {"rank": rank, "appid": appid,
                 "concurrent_in_game": 1_000_000 // rank}
                for rank, appid in enumerate(self._chart_appids(), start=1)
            ],
        }}

    def _valve_most_played(self, key):
        return {"response": {
            "rollup_date": 1_760_000_000,
            "ranks": [
                {"rank": rank, "appid": appid, "last_week_rank": rank,
                 "peak_in_game": 2_000_000 // rank}
                for rank, appid in enumerate(self._chart_appids(), start=1)
            ],
        }}

    def _valve_current_players(self, key):
        return {"response": {
            "player_count": rng_for(int(key), "players").randint(0, 9999),
            "result": 1,
        }}

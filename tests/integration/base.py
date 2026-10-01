import os
import unittest
from datetime import datetime
from unittest.mock import patch

from config import get_mysql_config
from database.mysql_database import get_connection
import main


INTEGRATION_ENABLED = os.getenv("INTEGRATION_DB") == "1"

# Children before parents, so deletes respect the foreign keys.
PIPELINE_TABLES = [
    "steam_game_measurements",
    "game_metric_snapshots",
    "games",
    "pipeline_runs",
]

IGNORED_TABLES = {"flyway_schema_history"}


def steamspy_game(appid, name, owners, positive, negative):
    return {
        "appid": appid,
        "name": name,
        "developer": "Valve",
        "publisher": "Valve",
        "owners": owners,
        "positive": positive,
        "negative": negative,
        "average_forever": 1200,
        "average_2weeks": 300,
        "median_forever": 600,
        "median_2weeks": 150,
        "ccu": 1000,
        "price": "999",
        "initialprice": "1999",
        "discount": "50",
        "languages": "English, Czech",
        "genre": "Action",
    }


CANNED_TOP_GAMES = {
    "730": steamspy_game(
        730, "Counter-Strike 2", "50,000,000 .. 100,000,000", 9000, 1000
    ),
    "620": steamspy_game(620, "Portal 2", "10,000,000 .. 20,000,000", 990, 10),
    "570": steamspy_game(570, "Dota 2", "100,000,000 .. 200,000,000", 800, 200),
}

CHART_MEASURED_AT = datetime(2026, 10, 1, 18, 0, 0)

CANNED_MEASUREMENTS = [
    {
        "appid": 730,
        "current_players": 1_500_000,
        "player_rank": 1,
        "source_measured_at": CHART_MEASURED_AT,
        "positive_reviews": 7000,
        "negative_reviews": 1000,
        "total_reviews": 8000,
        "review_score_percent": 87.5,
    },
    {
        "appid": 620,
        "current_players": 4000,
        "player_rank": None,
        "source_measured_at": None,
        "positive_reviews": 990,
        "negative_reviews": 10,
        "total_reviews": 1000,
        "review_score_percent": 99.0,
    },
    {
        # Valve-measured but absent from the SteamSpy page: not loaded.
        "appid": 999999,
        "current_players": 50,
        "player_rank": 100,
        "source_measured_at": CHART_MEASURED_AT,
        "positive_reviews": None,
        "negative_reviews": None,
        "total_reviews": None,
        "review_score_percent": None,
    },
]


@unittest.skipUnless(INTEGRATION_ENABLED, "set INTEGRATION_DB=1 to run")
class IntegrationTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        database = get_mysql_config()["database"]

        if not database.endswith("_test"):
            raise unittest.SkipTest(
                f"refusing to run against {database!r}: "
                "MYSQL_DATABASE must end in '_test'"
            )

    def setUp(self):
        self.reset_tables()

    def query(self, sql, params=()):
        connection = get_connection()

        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(sql, params)
            return cursor.fetchall()
        finally:
            connection.close()

    def count(self, table):
        return self.query(f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]

    def run_lock_is_free(self):
        return self.query(
            "SELECT IS_FREE_LOCK(%s) AS free",
            (main.RUN_LOCK_NAME,)
        )[0]["free"] == 1

    def reset_tables(self):
        connection = get_connection()

        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_type = 'BASE TABLE'
                """
            )
            existing = {row[0] for row in cursor.fetchall()}
            unknown = existing - set(PIPELINE_TABLES) - IGNORED_TABLES

            if unknown:
                raise AssertionError(
                    "Add new tables to PIPELINE_TABLES in FK order: "
                    + ", ".join(sorted(unknown))
                )

            for table in PIPELINE_TABLES:
                cursor.execute(f"DELETE FROM {table}")

            connection.commit()
        finally:
            connection.close()

    def run_pipeline(self, top_games=None, measurements=None):
        if top_games is None:
            top_games = CANNED_TOP_GAMES

        if measurements is None:
            measurements = CANNED_MEASUREMENTS

        with (
            patch.object(main, "get_top_games", return_value=top_games),
            patch.object(
                main,
                "get_steam_measurements",
                return_value=measurements
            ),
        ):
            main.run_pipeline()

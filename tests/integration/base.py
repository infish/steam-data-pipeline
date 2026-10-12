import calendar
import os
import unittest
from datetime import datetime
from unittest.mock import patch

from config import get_mysql_config
from database.mysql_database import get_connection
from ingest.replay import FixtureSession
import main


INTEGRATION_ENABLED = os.getenv("INTEGRATION_DB") == "1"

# Children before parents, so deletes respect the foreign keys.
PIPELINE_TABLES = [
    "mart_player_trends",
    "mart_player_peak_hours",
    "mart_review_trends",
    "mart_price_status",
    "mart_genre_summary",
    "mart_tag_summary",
    "mart_release_year_summary",
    "mart_engagement",
    "mart_event_impact",
    "steam_game_measurements",
    "player_observations",
    "review_observations",
    "player_daily",
    "most_played_daily",
    "game_tags",
    "tags",
    "game_languages",
    "languages",
    "game_categories",
    "categories",
    "game_genres",
    "genres",
    "game_details",
    "game_developers",
    "game_publishers",
    "companies",
    "game_price_history",
    "review_rollups",
    "review_reconciliation",
    "game_events",
    "game_metric_snapshots",
    "games",
    "raw_api_responses",
    "data_quality_results",
    "pipeline_runs",
]

ALL_JOBS = ("catalog_daily", "players_hourly", "reviews_daily")

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


def review_summary(positive, negative, label="Very Positive"):
    return {
        "success": 1,
        "query_summary": {
            "total_positive": positive,
            "total_negative": negative,
            "total_reviews": positive + negative,
            "review_score": 8,
            "review_score_desc": label,
        },
    }


def current_players(count):
    return {"response": {"player_count": count, "result": 1}}


CANNED_TOP_GAMES = {
    "730": steamspy_game(
        730, "Counter-Strike 2", "50,000,000 .. 100,000,000", 9000, 1000
    ),
    "620": steamspy_game(620, "Portal 2", "10,000,000 .. 20,000,000", 990, 10),
    "570": steamspy_game(570, "Dota 2", "100,000,000 .. 200,000,000", 800, 200),
}

TRACKED_APPIDS = "730,427520,620"

CHART_MEASURED_AT = datetime(2026, 10, 1, 18, 0, 0)

# HTTP payloads served by FixtureSession, keyed by ingest.replay.route_for.
# 730 is on the page and in the top 100; 620 is on the page but outside the
# top 100; 427520 (Factorio) is tracked but off the page, so it is fetched
# through SteamSpy appdetails; 999999 is top-100 only and is skipped.
CANNED_RESPONSES = {
    "steamspy_all": CANNED_TOP_GAMES,
    "steamspy_appdetails:427520": {
        **steamspy_game(
            427520, "Factorio", "5,000,000 .. 10,000,000", 200, 5
        ),
        "tags": {},
    },
    "valve_top_concurrent": {
        "response": {
            "last_update": calendar.timegm(CHART_MEASURED_AT.timetuple()),
            "ranks": [
                {"rank": 1, "appid": 730, "concurrent_in_game": 1_500_000},
                {"rank": 100, "appid": 999999, "concurrent_in_game": 50},
            ],
        }
    },
    "valve_current_players:620": current_players(4000),
    "valve_current_players:427520": current_players(20000),
    "steam_review_summary:730": review_summary(7000, 1000),
    "steam_review_summary:620": review_summary(990, 10),
    "steam_review_summary:427520": review_summary(195, 5),
    # 730, 620 and 427520 use the recorded Store and SteamSpy fixtures.
    "store_appdetails:570": {
        "570": {
            "success": True,
            "data": {
                "type": "game",
                "name": "Dota 2",
                "is_free": True,
                "required_age": 0,
                "release_date": {"coming_soon": False, "date": "Jul 9, 2013"},
                "platforms": {"windows": True, "mac": True, "linux": True},
                "recommendations": {"total": 2_000_000},
                "genres": [
                    {"id": "1", "description": "Action"},
                    {"id": "37", "description": "Free To Play"},
                ],
                "categories": [{"id": 1, "description": "Multi-player"}],
                "supported_languages": (
                    "English<strong>*</strong>, Czech<br><strong>*</strong>"
                    "languages with full audio support"
                ),
            },
        }
    },
    "steamspy_appdetails:570": {
        **steamspy_game(570, "Dota 2", "100,000,000 .. 200,000,000", 800, 200),
        "tags": {"MOBA": 100, "Free to Play": 50},
    },
}


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

    def execute(self, sql, params=()):
        connection = get_connection()

        try:
            cursor = connection.cursor()
            cursor.execute(sql, params)
            connection.commit()
            return cursor.lastrowid
        finally:
            connection.close()

    def count(self, table):
        return self.query(f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]

    def run_lock_is_free(self, job_name="catalog_daily"):
        return self.query(
            "SELECT IS_FREE_LOCK(%s) AS free",
            (f"steam_pipeline:{job_name}",)
        )[0]["free"] == 1

    def run_row(self, run_id):
        (row,) = self.query(
            "SELECT * FROM pipeline_runs WHERE run_id = %s",
            (run_id,)
        )
        return row

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

            # Self-references must go before the rows they point at.
            cursor.execute(
                "UPDATE raw_api_responses SET is_duplicate_of = NULL"
            )

            for table in PIPELINE_TABLES:
                cursor.execute(f"DELETE FROM {table}")

            connection.commit()
        finally:
            connection.close()

    def session(self, overrides=None):
        return FixtureSession({**CANNED_RESPONSES, **(overrides or {})})

    def run_pipeline(
        self,
        overrides=None,
        now_fn=None,
        session=None,
        jobs=ALL_JOBS
    ):
        """Run jobs against canned HTTP responses; return {job: run_id}."""
        session = session or self.session(overrides)
        options = {
            "session_factory": lambda: session,
            "sleep_fn": lambda seconds: None,
        }

        if now_fn is not None:
            options["now_fn"] = now_fn

        with patch.dict(os.environ, {"STEAM_TRACKED_APPIDS": TRACKED_APPIDS}):
            return main.run_pipeline(list(jobs), **options)

    def run_job(self, job_name, overrides=None, now_fn=None, session=None):
        return self.run_pipeline(
            overrides,
            now_fn=now_fn,
            session=session,
            jobs=(job_name,)
        )[job_name]

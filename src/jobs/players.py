"""players_hourly: Valve current players and the daily peak leaderboard."""

import logging
from datetime import timedelta

from api.steam_api import (
    ENRICHMENT_ERRORS,
    get_most_played,
    get_player_measurements,
)
from config import get_player_hourly_retention_days
from jobs.base import Job
from jobs.loading import known_appids


def load_player_observations(cursor, observations, run_id, collected_at):
    cursor.executemany(
        """
        INSERT INTO player_observations (
            run_id, appid, collected_at, source_measured_at,
            current_players, player_rank, source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        [
            (
                run_id,
                row["appid"],
                collected_at,
                row["source_measured_at"],
                row["current_players"],
                row["player_rank"],
                row["source"],
            )
            for row in observations
        ]
    )
    return len(observations)


def rebuild_player_daily(cursor, run_id, updated_at):
    """Recompute player_daily for the (game, UTC day) pairs of this run."""
    cursor.execute(
        """
        INSERT INTO player_daily (
            appid, observation_date, peak_players, avg_players,
            min_players, observation_count, best_rank, updated_at
        )
        SELECT * FROM (
            SELECT
                obs.appid,
                DATE(COALESCE(obs.source_measured_at, obs.collected_at))
                    AS observation_date,
                MAX(obs.current_players) AS peak_players,
                AVG(obs.current_players) AS avg_players,
                MIN(obs.current_players) AS min_players,
                COUNT(*) AS observation_count,
                MIN(obs.player_rank) AS best_rank,
                %s AS updated_at
            FROM player_observations AS obs
            JOIN (
                SELECT DISTINCT
                    appid,
                    DATE(COALESCE(source_measured_at, collected_at)) AS day
                FROM player_observations
                WHERE run_id = %s
            ) AS affected
                ON affected.appid = obs.appid
               AND affected.day
                   = DATE(COALESCE(obs.source_measured_at, obs.collected_at))
            GROUP BY obs.appid, observation_date
        ) AS rollup
        ON DUPLICATE KEY UPDATE
            peak_players = rollup.peak_players,
            avg_players = rollup.avg_players,
            min_players = rollup.min_players,
            observation_count = rollup.observation_count,
            best_rank = rollup.best_rank,
            updated_at = rollup.updated_at
        """,
        (updated_at, run_id)
    )


def load_most_played(cursor, rollup_date, rows, run_id):
    cursor.executemany(
        """
        INSERT INTO most_played_daily (
            rollup_date, appid, player_rank, last_week_rank,
            peak_players, run_id
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        AS new
        ON DUPLICATE KEY UPDATE
            player_rank = new.player_rank,
            last_week_rank = new.last_week_rank,
            peak_players = new.peak_players,
            run_id = new.run_id
        """,
        [
            (
                rollup_date,
                row["appid"],
                row["player_rank"],
                row["last_week_rank"],
                row["peak_players"],
                run_id,
            )
            for row in rows
        ]
    )
    return len(rows)


def delete_expired_observations(cursor, retention_days, now):
    """Hourly rows age out; player_daily keeps their rollups forever."""
    cursor.execute(
        "DELETE FROM player_observations WHERE collected_at < %s",
        (now - timedelta(days=retention_days),)
    )
    return cursor.rowcount


def run_players(ctx):
    measurements, warnings = get_player_measurements(ctx.session, ctx.store)
    ctx.warn(*warnings)

    known = known_appids(ctx.cursor, [row["appid"] for row in measurements])
    loadable = [row for row in measurements if row["appid"] in known]
    skipped = [row for row in measurements if row["appid"] not in known]

    if skipped:
        logging.info(
            "%s top-100 games not in catalog page; skipped",
            sum(1 for row in skipped if row["source"] == "top100")
        )

    for row in skipped:
        if row["source"] == "direct":
            logging.info(
                "Tracked app %s has no games row yet (catalog_daily has not"
                " loaded it); skipped",
                row["appid"]
            )

    collected_at = ctx.load_time()
    loaded = load_player_observations(
        ctx.cursor,
        loadable,
        ctx.run_id,
        collected_at
    )
    rebuild_player_daily(ctx.cursor, ctx.run_id, collected_at)

    try:
        rollup_date, leaderboard = get_most_played(ctx.session, ctx.store)
        load_most_played(ctx.cursor, rollup_date, leaderboard, ctx.run_id)
    except ENRICHMENT_ERRORS as error:
        ctx.warn(f"Valve most-played leaderboard unavailable: {error}")

    ctx.counters.update(
        rows_extracted=len(measurements),
        rows_loaded=loaded,
        measurements_loaded=loaded
    )

    if not ctx.replay:
        retention_days = get_player_hourly_retention_days()
        deleted = delete_expired_observations(
            ctx.cursor,
            retention_days,
            ctx.now_fn()
        )

        if deleted:
            logging.info(
                "Deleted %s player observations older than %s days",
                deleted,
                retention_days
            )


def clear_players_run(cursor, run_id):
    cursor.execute(
        "DELETE FROM player_observations WHERE run_id = %s",
        (run_id,)
    )


PLAYERS_JOB = Job(
    name="players_hourly",
    body=run_players,
    clear_run=clear_players_run,
    load_time_sql=(
        "SELECT MIN(collected_at) FROM player_observations WHERE run_id = %s"
    )
)

-- mart_engagement
-- Grain: one row per game with player data.
-- Sources: player_daily (latest day), current_game_metrics (latest catalog
-- snapshot: SteamSpy owner estimate and playtime), games.
-- Refresh: rebuilt after every job.
-- ROUGH by design: owners are a SteamSpy range midpoint and playtimes are
-- SteamSpy figures from the latest snapshot only (never a time series).
-- rough_peak_to_owners_pct is the latest daily peak as a share of estimated
-- owners; rough_playtime_skew is average / median playtime, where values
-- well above 1 mean a minority of players plays far more than the rest.
DELETE FROM mart_engagement;

INSERT INTO mart_engagement (
    appid, name, peak_players_latest_day, latest_day, estimated_owners,
    rough_peak_to_owners_pct, average_playtime_minutes,
    median_playtime_minutes, rough_playtime_skew, snapshot_time
)
WITH latest_day AS (
    SELECT
        appid,
        observation_date,
        peak_players,
        ROW_NUMBER() OVER (
            PARTITION BY appid
            ORDER BY observation_date DESC
        ) AS position
    FROM player_daily
)
SELECT
    games.appid,
    games.name,
    latest_day.peak_players,
    latest_day.observation_date,
    snapshot.estimated_owners,
    ROUND(
        latest_day.peak_players / NULLIF(snapshot.estimated_owners, 0) * 100,
        4
    ),
    snapshot.average_playtime_forever_minutes,
    snapshot.median_playtime_forever_minutes,
    ROUND(
        snapshot.average_playtime_forever_minutes
        / NULLIF(snapshot.median_playtime_forever_minutes, 0),
        2
    ),
    snapshot.snapshot_time
FROM latest_day
JOIN games ON games.appid = latest_day.appid
LEFT JOIN current_game_metrics AS snapshot
    ON snapshot.appid = latest_day.appid
WHERE latest_day.position = 1;

-- mart_player_peak_hours
-- Grain: one row per game with hourly player data.
-- Sources: player_observations (hourly, kept for PLAYER_HOURLY_RETENTION_DAYS),
-- games.
-- Refresh: rebuilt after every job.
-- For each Prague-local day with at least 12 hourly observations, the hour
-- of the day's highest player count is taken; the typical peak hour is the
-- most frequent of those (earliest hour on ties). Measurement time is
-- Valve's source time when known, otherwise collection time. MySQL has no
-- time zone tables here, so Prague time is derived from the EU rule: UTC+2
-- from 01:00 UTC on the last Sunday of March to 01:00 UTC on the last Sunday
-- of October, UTC+1 otherwise.
DELETE FROM mart_player_peak_hours;

INSERT INTO mart_player_peak_hours (
    appid, name, typical_peak_hour_prague, days_with_that_peak_hour,
    days_counted
)
WITH measured AS (
    SELECT
        appid,
        current_players,
        COALESCE(source_measured_at, collected_at) AS measured_at
    FROM player_observations
),
local_time AS (
    SELECT
        appid,
        current_players,
        measured_at + INTERVAL IF(
            measured_at >= TIMESTAMP(
                DATE(CONCAT(YEAR(measured_at), '-03-31'))
                - INTERVAL (DAYOFWEEK(CONCAT(YEAR(measured_at), '-03-31')) - 1)
                  DAY,
                '01:00:00'
            )
            AND measured_at < TIMESTAMP(
                DATE(CONCAT(YEAR(measured_at), '-10-31'))
                - INTERVAL (DAYOFWEEK(CONCAT(YEAR(measured_at), '-10-31')) - 1)
                  DAY,
                '01:00:00'
            ),
            2,
            1
        ) HOUR AS prague_time
    FROM measured
),
ranked_hours AS (
    SELECT
        appid,
        DATE(prague_time) AS prague_day,
        HOUR(prague_time) AS prague_hour,
        COUNT(*) OVER (PARTITION BY appid, DATE(prague_time))
            AS observations_that_day,
        ROW_NUMBER() OVER (
            PARTITION BY appid, DATE(prague_time)
            ORDER BY current_players DESC, prague_time
        ) AS position_in_day
    FROM local_time
),
peak_hours AS (
    SELECT appid, prague_hour, COUNT(*) AS days
    FROM ranked_hours
    WHERE position_in_day = 1
      AND observations_that_day >= 12
    GROUP BY appid, prague_hour
),
typical AS (
    SELECT
        appid,
        prague_hour,
        days,
        SUM(days) OVER (PARTITION BY appid) AS days_counted,
        ROW_NUMBER() OVER (
            PARTITION BY appid
            ORDER BY days DESC, prague_hour
        ) AS position
    FROM peak_hours
)
SELECT typical.appid, games.name, typical.prague_hour, typical.days,
       typical.days_counted
FROM typical
JOIN games ON games.appid = typical.appid
WHERE typical.position = 1;

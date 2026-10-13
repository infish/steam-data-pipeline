-- mart_player_trends
-- Grain: one row per game per UTC day with player data.
-- Sources: player_daily (hourly player_observations rolled up per day), games.
-- Refresh: rebuilt after every job.
-- Day-over-day and week-over-week changes compare the daily peak with the
-- calendar day 1 or 7 days earlier, and are NULL when that day has no data.
-- A game is in the top 100 on a day when any observation that day came from
-- Valve's top-100 chart (best_rank is set). Entries and exits are flagged only
-- when the neighbouring day was collected, so gaps in collection are not
-- mistaken for movements. Volatility is the coefficient of variation of the
-- daily average over the last 7 days, in percent.
DELETE FROM mart_player_trends;

INSERT INTO mart_player_trends (
    appid, observation_date, name, peak_players, avg_players,
    avg_players_7d, peak_change_pct_dod, peak_change_pct_wow, best_rank,
    rank_change_dod, in_top100, days_in_top100, entered_top100,
    exited_top100, volatility_7d_pct
)
WITH daily AS (
    SELECT
        player_daily.appid,
        player_daily.observation_date,
        games.name,
        player_daily.peak_players,
        player_daily.avg_players,
        player_daily.best_rank,
        player_daily.best_rank IS NOT NULL AS in_top100
    FROM player_daily
    JOIN games ON games.appid = player_daily.appid
),
collected_days AS (
    SELECT DISTINCT observation_date FROM player_daily
),
windowed AS (
    SELECT
        daily.*,
        AVG(avg_players) OVER last_7_days AS avg_players_7d,
        STDDEV_POP(avg_players) OVER last_7_days AS stddev_7d,
        COUNT(*) OVER last_7_days AS days_in_window,
        SUM(in_top100) OVER (
            PARTITION BY appid
            ORDER BY observation_date
            ROWS UNBOUNDED PRECEDING
        ) AS days_in_top100
    FROM daily
    WINDOW last_7_days AS (
        PARTITION BY appid
        ORDER BY observation_date
        RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW
    )
)
SELECT
    today.appid,
    today.observation_date,
    today.name,
    today.peak_players,
    today.avg_players,
    ROUND(today.avg_players_7d, 2),
    ROUND(
        (CAST(today.peak_players AS SIGNED)
            - CAST(yesterday.peak_players AS SIGNED))
        / NULLIF(yesterday.peak_players, 0) * 100,
        2
    ),
    ROUND(
        (CAST(today.peak_players AS SIGNED)
            - CAST(last_week.peak_players AS SIGNED))
        / NULLIF(last_week.peak_players, 0) * 100,
        2
    ),
    today.best_rank,
    CAST(yesterday.best_rank AS SIGNED) - CAST(today.best_rank AS SIGNED),
    today.in_top100,
    today.days_in_top100,
    today.in_top100
        AND NOT COALESCE(yesterday.in_top100, FALSE)
        AND previous_day.observation_date IS NOT NULL,
    today.in_top100
        AND NOT COALESCE(tomorrow.in_top100, FALSE)
        AND next_day.observation_date IS NOT NULL,
    IF(
        today.days_in_window >= 2,
        ROUND(today.stddev_7d / NULLIF(today.avg_players_7d, 0) * 100, 2),
        NULL
    )
FROM windowed AS today
LEFT JOIN daily AS yesterday
    ON yesterday.appid = today.appid
   AND yesterday.observation_date = today.observation_date - INTERVAL 1 DAY
LEFT JOIN daily AS last_week
    ON last_week.appid = today.appid
   AND last_week.observation_date = today.observation_date - INTERVAL 7 DAY
LEFT JOIN daily AS tomorrow
    ON tomorrow.appid = today.appid
   AND tomorrow.observation_date = today.observation_date + INTERVAL 1 DAY
LEFT JOIN collected_days AS previous_day
    ON previous_day.observation_date = today.observation_date - INTERVAL 1 DAY
LEFT JOIN collected_days AS next_day
    ON next_day.observation_date = today.observation_date + INTERVAL 1 DAY;

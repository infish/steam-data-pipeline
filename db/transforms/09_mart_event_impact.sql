-- mart_event_impact
-- Grain: one row per developer patch-notes event.
-- Sources: game_events (is_patch_notes from Steam's patchnotes tag, developer
-- posts only), player_daily, games.
-- Refresh: rebuilt after every job.
-- Compares the average daily players in the 7 UTC days before the event day
-- with the 7 days after it; the event day itself is excluded. This is a
-- correlation, not proof that the update caused the change: sales, seasons
-- and other news overlap. Averages are NULL when a window has no player data.
DELETE FROM mart_event_impact;

INSERT INTO mart_event_impact (
    gid, appid, name, title, event_time, avg_players_7d_before, days_before,
    avg_players_7d_after, days_after, change_pct
)
SELECT
    gid, appid, name, title, event_time, avg_before, days_before,
    avg_after, days_after,
    ROUND((avg_after - avg_before) / NULLIF(avg_before, 0) * 100, 2)
FROM (
    SELECT
        events.gid,
        events.appid,
        games.name,
        events.title,
        events.event_time,
        ROUND(AVG(IF(
            daily.observation_date < DATE(events.event_time),
            daily.avg_players, NULL
        )), 2) AS avg_before,
        COUNT(IF(
            daily.observation_date < DATE(events.event_time), 1, NULL
        )) AS days_before,
        ROUND(AVG(IF(
            daily.observation_date > DATE(events.event_time),
            daily.avg_players, NULL
        )), 2) AS avg_after,
        COUNT(IF(
            daily.observation_date > DATE(events.event_time), 1, NULL
        )) AS days_after
    FROM game_events AS events
    JOIN games ON games.appid = events.appid
    LEFT JOIN player_daily AS daily
        ON daily.appid = events.appid
       AND daily.observation_date
           BETWEEN DATE(events.event_time) - INTERVAL 7 DAY
               AND DATE(events.event_time) + INTERVAL 7 DAY
    WHERE events.is_patch_notes
      AND events.is_developer_post
    GROUP BY events.gid, events.appid, games.name, events.title,
             events.event_time
) AS windows;

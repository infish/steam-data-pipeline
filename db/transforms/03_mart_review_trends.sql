-- mart_review_trends
-- Grain: one row per game per UTC day with a review observation.
-- Sources: review_observations (daily review summary), review_rollups (daily
-- histogram buckets), games.
-- Refresh: after every job, incrementally (since v1.3.0): the newest day of
-- observations and the day before (a build just after midnight), plus every
-- day of the games with
-- histogram rollups (the tracked apps), whose 30-day share can change for
-- earlier days. Older rows of other games cannot change: a new observation
-- always lands on the current day. Reprocessing (@mart_full_rebuild = 1)
-- and an empty mart rebuild everything. A full rebuild of the whole history
-- after each of about 50 runs a day grew without bound. Last, a renamed
-- game's older rows take its current name, as a full rebuild would give
-- them (v1.3.1).
-- new_reviews is the change in total reviews since the game's previous
-- observation (it can be negative when Steam reclassifies reviews). The
-- 30-day positive share comes from the histogram's daily buckets for the 30
-- days ending on the observation date; sentiment drift is that share minus
-- the lifetime share from the review summary. The two endpoints count
-- different review sets (docs/sources.md), so compare drift, not counts.
SET @review_trends_from = (
    SELECT IF(
        @mart_full_rebuild = 1
            OR NOT EXISTS (SELECT 1 FROM mart_review_trends),
        DATE('1000-01-01'),
        DATE(MAX(collected_at)) - INTERVAL 1 DAY
    )
    FROM review_observations
);

DELETE FROM mart_review_trends
WHERE observation_date >= @review_trends_from;

DELETE FROM mart_review_trends
WHERE appid IN (SELECT appid FROM review_rollups);

INSERT INTO mart_review_trends (
    appid, observation_date, name, total_reviews, new_reviews,
    positive_share_lifetime, positive_share_30d, reviews_30d,
    sentiment_drift_points, review_score_desc
)
WITH rebuilt AS (
    SELECT appid, collected_at, run_id, total_reviews,
           review_score_percent, review_score_desc
    FROM review_observations
    WHERE collected_at >= @review_trends_from
    UNION ALL
    SELECT appid, collected_at, run_id, total_reviews,
           review_score_percent, review_score_desc
    FROM review_observations
    WHERE collected_at < @review_trends_from
      AND appid IN (SELECT appid FROM review_rollups)
),
latest_per_day AS (
    SELECT
        appid,
        DATE(collected_at) AS observation_date,
        total_reviews,
        review_score_percent,
        review_score_desc,
        ROW_NUMBER() OVER (
            PARTITION BY appid, DATE(collected_at)
            ORDER BY collected_at DESC, run_id DESC
        ) AS position
    FROM rebuilt
),
with_previous AS (
    SELECT
        latest_per_day.*,
        LAG(total_reviews) OVER (
            PARTITION BY appid
            ORDER BY observation_date
        ) AS previous_in_window
    FROM latest_per_day
    WHERE position = 1
)
SELECT
    observed.appid,
    observed.observation_date,
    games.name,
    observed.total_reviews,
    CAST(observed.total_reviews AS SIGNED)
        - CAST(COALESCE(
            observed.previous_in_window,
            -- A game's first row in the window: its latest observation on
            -- an earlier day, through the (appid, collected_at) index.
            (
                SELECT earlier.total_reviews
                FROM review_observations AS earlier
                WHERE earlier.appid = observed.appid
                  AND earlier.collected_at < observed.observation_date
                ORDER BY earlier.collected_at DESC, earlier.run_id DESC
                LIMIT 1
            )
        ) AS SIGNED),
    observed.review_score_percent,
    last_30_days.positive_share,
    last_30_days.reviews,
    ROUND(last_30_days.positive_share - observed.review_score_percent, 2),
    observed.review_score_desc
FROM with_previous AS observed
JOIN games ON games.appid = observed.appid
LEFT JOIN LATERAL (
    SELECT
        ROUND(
            SUM(recommendations_up)
            / NULLIF(SUM(recommendations_up + recommendations_down), 0)
            * 100,
            2
        ) AS positive_share,
        SUM(recommendations_up + recommendations_down) AS reviews
    FROM review_rollups
    WHERE review_rollups.appid = observed.appid
      AND review_rollups.rollup_type = 'day'
      AND review_rollups.period_start
          BETWEEN observed.observation_date - INTERVAL 29 DAY
              AND observed.observation_date
) AS last_30_days ON TRUE;

-- Rows before the window share one name per game, so comparing each game's
-- newest earlier row with games.name finds every rename (one primary-key
-- lookup per game). GROUP BY makes MySQL materialize the derived table, so
-- it may read the table being updated.
UPDATE mart_review_trends AS mart
JOIN (
    SELECT games.appid, games.name
    FROM games
    JOIN mart_review_trends AS older
        ON older.appid = games.appid
       AND older.observation_date = (
           SELECT MAX(earlier.observation_date)
           FROM mart_review_trends AS earlier
           WHERE earlier.appid = games.appid
             AND earlier.observation_date < @review_trends_from
       )
    WHERE NOT (older.name <=> games.name)
    GROUP BY games.appid, games.name
) AS renamed ON renamed.appid = mart.appid
SET mart.name = renamed.name
WHERE mart.observation_date < @review_trends_from;

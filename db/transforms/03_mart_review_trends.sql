-- mart_review_trends
-- Grain: one row per tracked game per UTC day with a review observation.
-- Sources: review_observations (daily review summary), review_rollups (daily
-- histogram buckets), games.
-- Refresh: rebuilt after every job.
-- new_reviews is the change in total reviews since the game's previous
-- observation (it can be negative when Steam reclassifies reviews). The
-- 30-day positive share comes from the histogram's daily buckets for the 30
-- days ending on the observation date; sentiment drift is that share minus
-- the lifetime share from the review summary. The two endpoints count
-- different review sets (docs/sources.md), so compare drift, not counts.
DELETE FROM mart_review_trends;

INSERT INTO mart_review_trends (
    appid, observation_date, name, total_reviews, new_reviews,
    positive_share_lifetime, positive_share_30d, reviews_30d,
    sentiment_drift_points, review_score_desc
)
WITH latest_per_day AS (
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
    FROM review_observations
),
with_previous AS (
    SELECT
        latest_per_day.*,
        LAG(total_reviews) OVER (
            PARTITION BY appid
            ORDER BY observation_date
        ) AS previous_total
    FROM latest_per_day
    WHERE position = 1
)
SELECT
    observed.appid,
    observed.observation_date,
    games.name,
    observed.total_reviews,
    CAST(observed.total_reviews AS SIGNED)
        - CAST(observed.previous_total AS SIGNED),
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

-- The latest successful catalog run that loaded SteamSpy snapshots. Since
-- ADR 5 the SteamSpy page is fetched only every STEAMSPY_PAGE_INTERVAL_DAYS,
-- so the latest catalog run may have none.
CREATE OR REPLACE VIEW latest_successful_run AS
SELECT MAX(runs.run_id) AS run_id
FROM pipeline_runs AS runs
WHERE runs.status = 'SUCCESS'
  AND runs.job_name IN ('catalog_daily', 'daily_legacy')
  AND EXISTS (
      SELECT 1 FROM game_metric_snapshots AS snapshots
      WHERE snapshots.run_id = runs.run_id
  );


CREATE OR REPLACE VIEW current_game_metrics AS
SELECT
    games.appid,
    games.name,
    games.developer,
    games.publisher,
    games.languages,
    games.genre,
    snapshots.run_id,
    snapshots.snapshot_time,
    snapshots.owners,
    snapshots.owners_low,
    snapshots.owners_high,
    snapshots.estimated_owners,
    snapshots.positive_reviews,
    snapshots.negative_reviews,
    snapshots.total_reviews,
    snapshots.review_score_percent,
    snapshots.average_playtime_forever_minutes,
    snapshots.average_playtime_2weeks_minutes,
    snapshots.median_playtime_forever_minutes,
    snapshots.median_playtime_2weeks_minutes,
    snapshots.ccu,
    snapshots.price_cents,
    snapshots.initial_price_cents,
    snapshots.discount_percent
FROM game_metric_snapshots AS snapshots
JOIN latest_successful_run AS latest
    ON snapshots.run_id = latest.run_id
JOIN games
    ON snapshots.appid = games.appid;


CREATE OR REPLACE VIEW top_games_by_owners AS
SELECT
    appid,
    name,
    developer,
    publisher,
    estimated_owners,
    review_score_percent,
    ccu,
    price_cents
FROM current_game_metrics;


CREATE OR REPLACE VIEW top_games_by_review_score AS
SELECT
    appid,
    name,
    developer,
    publisher,
    total_reviews,
    review_score_percent,
    estimated_owners,
    ccu
FROM current_game_metrics
WHERE total_reviews >= 10000;


CREATE OR REPLACE VIEW most_active_games_by_ccu AS
SELECT
    appid,
    name,
    developer,
    publisher,
    ccu,
    estimated_owners,
    review_score_percent
FROM current_game_metrics
WHERE ccu IS NOT NULL;


CREATE OR REPLACE VIEW free_vs_paid_summary AS
SELECT
    CASE
        WHEN price_cents = 0 THEN 'Free'
        WHEN price_cents > 0 THEN 'Paid'
        ELSE 'Unknown'
    END AS price_type,
    COUNT(*) AS game_count,
    ROUND(AVG(estimated_owners), 0) AS avg_estimated_owners,
    ROUND(AVG(review_score_percent), 2) AS avg_review_score_percent,
    ROUND(AVG(ccu), 0) AS avg_ccu
FROM current_game_metrics
GROUP BY price_type;


CREATE OR REPLACE VIEW publisher_summary AS
SELECT
    publisher,
    COUNT(*) AS game_count,
    ROUND(AVG(estimated_owners), 0) AS avg_estimated_owners,
    SUM(estimated_owners) AS total_estimated_owners,
    ROUND(AVG(review_score_percent), 2) AS avg_review_score_percent,
    SUM(ccu) AS total_ccu
FROM current_game_metrics
WHERE publisher IS NOT NULL
  AND TRIM(publisher) <> ''
GROUP BY publisher;


CREATE OR REPLACE VIEW game_metric_trends AS
SELECT
    snapshots.run_id,
    snapshots.appid,
    snapshots.snapshot_time,
    games.name,
    games.developer,
    games.publisher,
    games.genre,
    snapshots.estimated_owners,
    snapshots.positive_reviews,
    snapshots.negative_reviews,
    snapshots.total_reviews,
    snapshots.review_score_percent,
    snapshots.average_playtime_2weeks_minutes,
    snapshots.median_playtime_2weeks_minutes,
    snapshots.ccu,
    snapshots.price_cents,
    snapshots.discount_percent
FROM game_metric_snapshots AS snapshots
JOIN pipeline_runs AS runs
    ON snapshots.run_id = runs.run_id
JOIN games
    ON snapshots.appid = games.appid
WHERE runs.status = 'SUCCESS';


CREATE OR REPLACE VIEW game_metric_changes AS
SELECT
    run_id,
    appid,
    snapshot_time,
    name,
    genre,
    estimated_owners,
    total_reviews,
    review_score_percent,
    ccu,
    price_cents,
    LAG(snapshot_time) OVER game_history AS previous_snapshot_time,
    CAST(estimated_owners AS SIGNED)
        - LAG(CAST(estimated_owners AS SIGNED)) OVER game_history
        AS estimated_owners_change,
    CAST(total_reviews AS SIGNED)
        - LAG(CAST(total_reviews AS SIGNED)) OVER game_history
        AS total_reviews_change,
    review_score_percent
        - LAG(review_score_percent) OVER game_history
        AS review_score_change,
    CAST(ccu AS SIGNED)
        - LAG(CAST(ccu AS SIGNED)) OVER game_history
        AS ccu_change,
    CAST(price_cents AS SIGNED)
        - LAG(CAST(price_cents AS SIGNED)) OVER game_history
        AS price_change_cents
FROM game_metric_trends
WINDOW game_history AS (
    PARTITION BY appid
    ORDER BY snapshot_time, run_id
);


CREATE OR REPLACE VIEW daily_pipeline_summary AS
SELECT
    DATE(finished_at) AS run_date,
    job_name,
    COUNT(*) AS successful_runs,
    SUM(rows_extracted) AS rows_extracted,
    SUM(rows_loaded) AS rows_loaded,
    SUM(measurements_loaded) AS measurements_loaded
FROM pipeline_runs
WHERE status = 'SUCCESS'
GROUP BY DATE(finished_at), job_name;

-- Current price per game and Store country, with values derived from the
-- whole price history. Amounts are compared only within one currency: the
-- currency of the latest priced version.
CREATE OR REPLACE VIEW game_price_summary AS
WITH versions AS (
    SELECT
        history.*,
        LAG(history.discount_percent) OVER (
            PARTITION BY history.appid, history.country_code
            ORDER BY history.valid_from, history.price_id
        ) AS previous_discount_percent
    FROM game_price_history AS history
),
latest_currency AS (
    SELECT appid, country_code, currency
    FROM (
        SELECT
            appid,
            country_code,
            currency,
            ROW_NUMBER() OVER (
                PARTITION BY appid, country_code
                ORDER BY valid_from DESC, price_id DESC
            ) AS recency
        FROM game_price_history
        WHERE price_state = 'priced'
    ) AS priced
    WHERE recency = 1
)
SELECT
    current_version.appid,
    current_version.country_code,
    current_version.price_state,
    current_version.currency,
    current_version.initial_price AS current_initial_price,
    current_version.final_price AS current_final_price,
    current_version.discount_percent AS current_discount_percent,
    current_version.valid_from AS current_since,
    COALESCE(current_version.discount_percent, 0) > 0 AS on_sale,
    latest_currency.currency AS lowest_price_currency,
    (
        SELECT MIN(versions.final_price)
        FROM versions
        WHERE versions.appid = current_version.appid
          AND versions.country_code = current_version.country_code
          AND versions.price_state = 'priced'
          AND versions.currency = latest_currency.currency
    ) AS lowest_price,
    (
        SELECT COUNT(*)
        FROM versions
        WHERE versions.appid = current_version.appid
          AND versions.country_code = current_version.country_code
          AND versions.discount_percent > 0
          AND COALESCE(versions.previous_discount_percent, 0) = 0
    ) AS discount_events,
    CASE
        WHEN COALESCE(current_version.discount_percent, 0) > 0 THEN 0
        ELSE DATEDIFF(
            UTC_TIMESTAMP(),
            (
                SELECT MAX(versions.valid_to)
                FROM versions
                WHERE versions.appid = current_version.appid
                  AND versions.country_code = current_version.country_code
                  AND versions.discount_percent > 0
            )
        )
    END AS days_since_last_discount
FROM game_price_history AS current_version
LEFT JOIN latest_currency
    ON latest_currency.appid = current_version.appid
   AND latest_currency.country_code = current_version.country_code
WHERE current_version.is_current;

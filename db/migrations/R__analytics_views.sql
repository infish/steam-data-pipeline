CREATE OR REPLACE VIEW latest_successful_run AS
SELECT MAX(run_id) AS run_id
FROM pipeline_runs
WHERE status = 'SUCCESS';


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
    COUNT(*) AS successful_runs,
    SUM(rows_extracted) AS rows_extracted,
    SUM(rows_loaded) AS rows_loaded
FROM pipeline_runs
WHERE status = 'SUCCESS'
GROUP BY DATE(finished_at);
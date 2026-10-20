-- mart_release_year_summary
-- Grain: one row per release year with at least one game.
-- Sources: game_details (Store release date, free flag, Metacritic),
-- review_observations (each game's latest Steam review summary).
-- Refresh: rebuilt after every job.
-- Only games whose Store details have a complete release date are counted.
-- avg_review_score is the average Steam positive share; games without a
-- review summary yet are left out of it (ADR 5).
DELETE FROM mart_release_year_summary;

INSERT INTO mart_release_year_summary (
    release_year, game_count, free_games, paid_games, avg_review_score,
    avg_metacritic
)
SELECT
    YEAR(details.release_date),
    COUNT(*),
    COUNT(CASE WHEN details.is_free = TRUE THEN 1 END),
    COUNT(CASE WHEN details.is_free = FALSE THEN 1 END),
    -- The latest Steam review summary of each game, through the
    -- (appid, collected_at) index (docs/optimizations.md, entry 6).
    ROUND(AVG((
        SELECT reviews.review_score_percent
        FROM review_observations AS reviews
        WHERE reviews.appid = details.appid
        ORDER BY reviews.collected_at DESC, reviews.observation_id DESC
        LIMIT 1
    )), 2),
    ROUND(AVG(details.metacritic_score), 2)
FROM game_details AS details
WHERE details.release_date IS NOT NULL
GROUP BY YEAR(details.release_date);

-- mart_release_year_summary
-- Grain: one row per release year with at least one game.
-- Sources: game_details (Store release date, free flag, Metacritic),
-- current_game_metrics (latest catalog snapshot).
-- Refresh: rebuilt after every job.
-- Only games whose Store details have a complete release date are counted.
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
    ROUND(AVG(snapshot.review_score_percent), 2),
    ROUND(AVG(details.metacritic_score), 2)
FROM game_details AS details
LEFT JOIN current_game_metrics AS snapshot
    ON snapshot.appid = details.appid
WHERE details.release_date IS NOT NULL
GROUP BY YEAR(details.release_date);

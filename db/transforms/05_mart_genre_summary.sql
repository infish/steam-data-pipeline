-- mart_genre_summary
-- Grain: one row per Store genre with at least one game.
-- Sources: game_genres, genres, game_details (Store), current_game_metrics
-- (latest catalog snapshot: SteamSpy review score), player_observations
-- (latest players run), games.
-- Refresh: rebuilt after every job.
-- avg_review_score is SteamSpy's positive share from the latest catalog
-- snapshot, suitable for broad comparison only. Top-100 figures use the
-- latest hourly players run. Release-year groups and platform counts come
-- from the Store details, which cover a game once it has been refreshed.
DELETE FROM mart_genre_summary;

INSERT INTO mart_genre_summary (
    genre_id, genre, game_count, avg_review_score, avg_metacritic,
    top100_games, top100_current_players, released_before_2015,
    released_2015_2019, released_2020_or_later, release_unknown,
    windows_games, mac_games, linux_games, free_games, paid_games
)
WITH latest_players AS (
    SELECT appid, current_players
    FROM player_observations
    WHERE source = 'top100'
      AND run_id = (SELECT MAX(run_id) FROM player_observations)
),
game_facts AS (
    SELECT
        games.appid,
        snapshot.review_score_percent,
        details.metacritic_score,
        YEAR(details.release_date) AS release_year,
        COALESCE(details.platform_windows, FALSE) AS on_windows,
        COALESCE(details.platform_mac, FALSE) AS on_mac,
        COALESCE(details.platform_linux, FALSE) AS on_linux,
        details.is_free,
        latest_players.current_players
    FROM games
    LEFT JOIN current_game_metrics AS snapshot
        ON snapshot.appid = games.appid
    LEFT JOIN game_details AS details
        ON details.appid = games.appid
    LEFT JOIN latest_players
        ON latest_players.appid = games.appid
)
SELECT
    genres.genre_id,
    genres.description,
    COUNT(*),
    ROUND(AVG(facts.review_score_percent), 2),
    ROUND(AVG(facts.metacritic_score), 2),
    COUNT(facts.current_players),
    COALESCE(SUM(facts.current_players), 0),
    COUNT(CASE WHEN facts.release_year < 2015 THEN 1 END),
    COUNT(CASE WHEN facts.release_year BETWEEN 2015 AND 2019 THEN 1 END),
    COUNT(CASE WHEN facts.release_year >= 2020 THEN 1 END),
    COUNT(CASE WHEN facts.release_year IS NULL THEN 1 END),
    COUNT(CASE WHEN facts.on_windows THEN 1 END),
    COUNT(CASE WHEN facts.on_mac THEN 1 END),
    COUNT(CASE WHEN facts.on_linux THEN 1 END),
    COUNT(CASE WHEN facts.is_free = TRUE THEN 1 END),
    COUNT(CASE WHEN facts.is_free = FALSE THEN 1 END)
FROM game_genres
JOIN genres ON genres.genre_id = game_genres.genre_id
JOIN game_facts AS facts ON facts.appid = game_genres.appid
GROUP BY genres.genre_id, genres.description;

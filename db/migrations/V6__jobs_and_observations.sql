-- Split collection into jobs and store measurements by grain.

-- Every run belongs to a job. Runs before the split are 'daily_legacy'.
ALTER TABLE pipeline_runs
    ADD COLUMN job_name VARCHAR(50) NOT NULL DEFAULT 'daily_legacy'
        AFTER run_id,
    ADD INDEX idx_pipeline_runs_job_status (job_name, status, finished_at);


-- One row per game per players_hourly run.
CREATE TABLE player_observations (
    observation_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_id BIGINT UNSIGNED NOT NULL,
    appid INT UNSIGNED NOT NULL,
    collected_at DATETIME(6) NOT NULL,
    source_measured_at DATETIME(6) NULL,
    current_players INT UNSIGNED NOT NULL,
    player_rank SMALLINT UNSIGNED NULL,
    source VARCHAR(10) NOT NULL,
    UNIQUE KEY uq_player_observations_run_app (run_id, appid),
    INDEX idx_player_observations_app_time (appid, collected_at),
    INDEX idx_player_observations_collected (collected_at),
    CONSTRAINT chk_player_observations_source
        CHECK (source IN ('top100', 'direct')),
    CONSTRAINT fk_player_observations_run
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id),
    CONSTRAINT fk_player_observations_game
        FOREIGN KEY (appid) REFERENCES games(appid)
);


-- One row per tracked game per reviews_daily run.
CREATE TABLE review_observations (
    observation_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_id BIGINT UNSIGNED NOT NULL,
    appid INT UNSIGNED NOT NULL,
    collected_at DATETIME(6) NOT NULL,
    positive_reviews INT UNSIGNED NOT NULL,
    negative_reviews INT UNSIGNED NOT NULL,
    total_reviews INT UNSIGNED NOT NULL,
    review_score_percent DECIMAL(5,2) NULL,
    review_score TINYINT UNSIGNED NULL,
    review_score_desc VARCHAR(50) NULL,
    UNIQUE KEY uq_review_observations_run_app (run_id, appid),
    INDEX idx_review_observations_app_time (appid, collected_at),
    CONSTRAINT chk_review_observations_totals
        CHECK (positive_reviews + negative_reviews = total_reviews),
    CONSTRAINT fk_review_observations_run
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id),
    CONSTRAINT fk_review_observations_game
        FOREIGN KEY (appid) REFERENCES games(appid)
);


-- Daily player rollup per game; the day is the UTC date of the measurement
-- time (Valve's source time when known, otherwise collection time).
CREATE TABLE player_daily (
    appid INT UNSIGNED NOT NULL,
    observation_date DATE NOT NULL,
    peak_players INT UNSIGNED NOT NULL,
    avg_players DECIMAL(12,2) NOT NULL,
    min_players INT UNSIGNED NOT NULL,
    observation_count SMALLINT UNSIGNED NOT NULL,
    best_rank SMALLINT UNSIGNED NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (appid, observation_date),
    INDEX idx_player_daily_date (observation_date),
    CONSTRAINT fk_player_daily_game
        FOREIGN KEY (appid) REFERENCES games(appid)
);


-- Valve's daily peak leaderboard (GetMostPlayedGames). It covers the top 100
-- by daily peak, many of which are not in the catalog, so appid has no
-- foreign key to games.
CREATE TABLE most_played_daily (
    rollup_date DATE NOT NULL,
    appid INT UNSIGNED NOT NULL,
    player_rank SMALLINT UNSIGNED NOT NULL,
    last_week_rank SMALLINT UNSIGNED NULL,
    peak_players INT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (rollup_date, appid),
    CONSTRAINT fk_most_played_daily_run
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);


-- Copy the combined history. steam_game_measurements stays, read-only.
INSERT INTO player_observations (
    run_id, appid, collected_at, source_measured_at,
    current_players, player_rank, source
)
SELECT
    run_id, appid, collected_at, source_measured_at,
    current_players, player_rank,
    IF(player_rank IS NULL, 'direct', 'top100')
FROM steam_game_measurements;

INSERT INTO review_observations (
    run_id, appid, collected_at, positive_reviews, negative_reviews,
    total_reviews, review_score_percent
)
SELECT
    run_id, appid, collected_at, positive_reviews, negative_reviews,
    total_reviews, review_score_percent
FROM steam_game_measurements
WHERE total_reviews IS NOT NULL;

INSERT INTO player_daily (
    appid, observation_date, peak_players, avg_players, min_players,
    observation_count, best_rank, updated_at
)
SELECT
    appid,
    DATE(COALESCE(source_measured_at, collected_at)),
    MAX(current_players),
    AVG(current_players),
    MIN(current_players),
    COUNT(*),
    MIN(player_rank),
    UTC_TIMESTAMP(6)
FROM player_observations
GROUP BY appid, DATE(COALESCE(source_measured_at, collected_at));

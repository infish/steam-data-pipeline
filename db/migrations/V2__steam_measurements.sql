CREATE TABLE steam_game_measurements (
    measurement_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_id BIGINT UNSIGNED NOT NULL,
    appid INT UNSIGNED NOT NULL,
    collected_at DATETIME(6) NOT NULL,
    source_measured_at DATETIME(6),
    current_players INT UNSIGNED NOT NULL,
    positive_reviews INT UNSIGNED NOT NULL,
    negative_reviews INT UNSIGNED NOT NULL,
    total_reviews INT UNSIGNED NOT NULL,
    review_score_percent DECIMAL(5,2),
    UNIQUE KEY uq_steam_measurements_run_app (run_id, appid),
    INDEX idx_steam_measurements_app_time (appid, collected_at),
    CONSTRAINT fk_steam_measurements_run
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id),
    CONSTRAINT fk_steam_measurements_game
        FOREIGN KEY (appid) REFERENCES games(appid),
    CONSTRAINT chk_steam_measurements_reviews
        CHECK (positive_reviews + negative_reviews = total_reviews)
);

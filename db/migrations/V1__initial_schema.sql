CREATE TABLE pipeline_runs (
    run_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    started_at DATETIME(6) NOT NULL,
    finished_at DATETIME(6),
    status VARCHAR(20) NOT NULL,
    rows_extracted INT UNSIGNED NOT NULL DEFAULT 0,
    rows_loaded INT UNSIGNED NOT NULL DEFAULT 0,
    error_message TEXT,
    CONSTRAINT chk_pipeline_runs_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    INDEX idx_pipeline_runs_status_finished (status, finished_at)
);

CREATE TABLE games (
    appid INT UNSIGNED PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    developer VARCHAR(500),
    publisher VARCHAR(500),
    languages TEXT,
    genre VARCHAR(500),
    first_seen_at DATETIME(6) NOT NULL,
    last_seen_at DATETIME(6) NOT NULL
);

CREATE TABLE game_metric_snapshots (
    run_id BIGINT UNSIGNED NOT NULL,
    appid INT UNSIGNED NOT NULL,
    snapshot_time DATETIME(6) NOT NULL,
    owners VARCHAR(100),
    owners_low BIGINT UNSIGNED,
    owners_high BIGINT UNSIGNED,
    estimated_owners BIGINT UNSIGNED,
    positive_reviews INT UNSIGNED,
    negative_reviews INT UNSIGNED,
    total_reviews INT UNSIGNED,
    review_score_percent DECIMAL(5,2),
    average_playtime_forever_minutes INT UNSIGNED,
    average_playtime_2weeks_minutes INT UNSIGNED,
    median_playtime_forever_minutes INT UNSIGNED,
    median_playtime_2weeks_minutes INT UNSIGNED,
    ccu INT UNSIGNED,
    price_cents INT UNSIGNED,
    initial_price_cents INT UNSIGNED,
    discount_percent TINYINT UNSIGNED,
    PRIMARY KEY (run_id, appid),
    INDEX idx_snapshots_appid_time (appid, snapshot_time),
    INDEX idx_snapshots_time (snapshot_time),
    CONSTRAINT fk_snapshots_run
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id),
    CONSTRAINT fk_snapshots_game
        FOREIGN KEY (appid) REFERENCES games(appid)
);
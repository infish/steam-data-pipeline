-- Review history from the Steam review histogram, its reconciliation with
-- the review summary, and game update events from Steam news.

-- Up/down review counts per period, with the source's own dates. The
-- histogram has monthly rollups for a game's whole life and daily buckets
-- for the last 30 days; it returned no weekly buckets (docs/sources.md).
-- The current month and day are partial until they end.
CREATE TABLE review_rollups (
    appid INT UNSIGNED NOT NULL,
    rollup_type VARCHAR(5) NOT NULL,
    period_start DATE NOT NULL,
    recommendations_up INT UNSIGNED NOT NULL,
    recommendations_down INT UNSIGNED NOT NULL,
    is_partial BOOLEAN NOT NULL,
    fetched_at DATETIME(6) NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (appid, rollup_type, period_start),
    CONSTRAINT chk_review_rollups_type
        CHECK (rollup_type IN ('month', 'day')),
    CONSTRAINT fk_review_rollups_game
        FOREIGN KEY (appid) REFERENCES games(appid),
    CONSTRAINT fk_review_rollups_run
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

-- One row per game per reviews run: the histogram compared with the review
-- summary. The two endpoints count different review sets, so only the
-- positive share is held to a tolerance; the count difference is recorded.
CREATE TABLE review_reconciliation (
    reconciliation_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_id BIGINT UNSIGNED NOT NULL,
    appid INT UNSIGNED NOT NULL,
    checked_at DATETIME(6) NOT NULL,
    histogram_total INT UNSIGNED NOT NULL,
    summary_total INT UNSIGNED NOT NULL,
    count_difference_percent DECIMAL(8,2) NULL,
    histogram_positive_share DECIMAL(5,2) NULL,
    summary_positive_share DECIMAL(5,2) NULL,
    share_difference_points DECIMAL(5,2) NULL,
    tolerance_points DECIMAL(5,2) NOT NULL,
    within_tolerance BOOLEAN NOT NULL,
    UNIQUE KEY uq_review_reconciliation_run_app (run_id, appid),
    CONSTRAINT fk_review_reconciliation_run
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id),
    CONSTRAINT fk_review_reconciliation_game
        FOREIGN KEY (appid) REFERENCES games(appid)
);

-- News and update posts, one row per Steam news gid. event_time is the
-- source's publication time (UTC). Article text is not stored.
CREATE TABLE game_events (
    gid VARCHAR(32) PRIMARY KEY,
    appid INT UNSIGNED NOT NULL,
    title VARCHAR(500) NOT NULL,
    url VARCHAR(1000) NULL,
    event_time DATETIME NOT NULL,
    feed_label VARCHAR(100) NULL,
    feed_name VARCHAR(100) NULL,
    feed_type TINYINT UNSIGNED NULL,
    is_patch_notes BOOLEAN NOT NULL,
    is_developer_post BOOLEAN NOT NULL,
    first_seen_at DATETIME(6) NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    INDEX idx_game_events_app_time (appid, event_time),
    CONSTRAINT fk_game_events_game
        FOREIGN KEY (appid) REFERENCES games(appid),
    CONSTRAINT fk_game_events_run
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

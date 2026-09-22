CREATE TABLE benchmark_database_marker (
    marker_key VARCHAR(64) PRIMARY KEY,
    marker_value VARCHAR(255) NOT NULL
);

INSERT INTO benchmark_database_marker (marker_key, marker_value)
VALUES ('purpose', 'steam-data-pipeline-isolated-benchmark');

CREATE TABLE benchmark_scenarios (
    scenario_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    scenario_key VARCHAR(100) NOT NULL UNIQUE,
    seed BIGINT NOT NULL,
    generator_version VARCHAR(50) NOT NULL,
    schema_fingerprint CHAR(64) NOT NULL,
    game_count INT UNSIGNED NOT NULL,
    requested_day_count INT UNSIGNED NOT NULL,
    start_date DATE NOT NULL,
    synthetic BOOLEAN NOT NULL DEFAULT TRUE,
    state VARCHAR(20) NOT NULL,
    actual_started_at DATETIME(6) NOT NULL,
    actual_finished_at DATETIME(6),
    heartbeat_at DATETIME(6),
    CONSTRAINT chk_benchmark_scenario_state
      CHECK (state IN ('RUNNING', 'SUCCESS', 'FAILED', 'PAUSED'))
);

CREATE TABLE benchmark_days (
    scenario_id BIGINT UNSIGNED NOT NULL,
    day_index INT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    last_committed_appid INT UNSIGNED,
    committed_row_count INT UNSIGNED NOT NULL DEFAULT 0,
    state VARCHAR(20) NOT NULL,
    completed_at DATETIME(6),
    PRIMARY KEY (scenario_id, day_index),
    UNIQUE KEY uq_benchmark_day_run (run_id),
    CONSTRAINT fk_benchmark_day_scenario FOREIGN KEY (scenario_id)
      REFERENCES benchmark_scenarios(scenario_id),
    CONSTRAINT fk_benchmark_day_run FOREIGN KEY (run_id)
      REFERENCES pipeline_runs(run_id),
    CONSTRAINT chk_benchmark_day_state CHECK (state IN ('RUNNING', 'SUCCESS'))
);

CREATE TABLE benchmark_batches (
    scenario_id BIGINT UNSIGNED NOT NULL,
    day_index INT UNSIGNED NOT NULL,
    first_appid INT UNSIGNED NOT NULL,
    last_appid INT UNSIGNED NOT NULL,
    expected_count INT UNSIGNED NOT NULL,
    checksum CHAR(64) NOT NULL,
    generation_ms DECIMAL(14,3) NOT NULL,
    insert_ms DECIMAL(14,3) NOT NULL,
    committed_at DATETIME(6) NOT NULL,
    PRIMARY KEY (scenario_id, day_index, first_appid),
    CONSTRAINT fk_benchmark_batch_day FOREIGN KEY (scenario_id, day_index)
      REFERENCES benchmark_days(scenario_id, day_index)
);

CREATE TABLE benchmark_results (
    result_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    scenario_id BIGINT UNSIGNED NOT NULL,
    benchmark_identity VARCHAR(100) NOT NULL,
    schema_fingerprint CHAR(64) NOT NULL,
    image_reference VARCHAR(255),
    config_json JSON NOT NULL,
    query_label VARCHAR(100) NOT NULL,
    iteration INT UNSIGNED NOT NULL,
    cache_label VARCHAR(30) NOT NULL,
    elapsed_ms DECIMAL(14,3) NOT NULL,
    result_row_count BIGINT UNSIGNED NOT NULL,
    result_digest CHAR(64) NOT NULL,
    recorded_at DATETIME(6) NOT NULL,
    CONSTRAINT fk_benchmark_result_scenario FOREIGN KEY (scenario_id)
      REFERENCES benchmark_scenarios(scenario_id),
    INDEX idx_benchmark_results_query (scenario_id, query_label, cache_label)
);

CREATE TABLE catalog_imports (
    import_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    import_key VARCHAR(100) NOT NULL UNIQUE,
    state VARCHAR(20) NOT NULL,
    requested_pages INT UNSIGNED NOT NULL,
    next_page INT UNSIGNED NOT NULL DEFAULT 0,
    raw_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
    unique_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
    duplicate_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
    rejected_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_attempt_at DATETIME(6),
    termination_reason VARCHAR(255),
    started_at DATETIME(6) NOT NULL,
    finished_at DATETIME(6),
    run_id BIGINT UNSIGNED NOT NULL,
    CONSTRAINT fk_catalog_import_run FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id),
    CONSTRAINT chk_catalog_import_state CHECK (state IN ('RUNNING', 'SUCCESS', 'FAILED'))
);

CREATE TABLE catalog_seen_appids (
    import_id BIGINT UNSIGNED NOT NULL,
    appid INT UNSIGNED NOT NULL,
    first_page INT UNSIGNED NOT NULL,
    PRIMARY KEY (import_id, appid),
    CONSTRAINT fk_catalog_seen_import FOREIGN KEY (import_id)
      REFERENCES catalog_imports(import_id)
);

CREATE TABLE catalog_pages (
    import_id BIGINT UNSIGNED NOT NULL,
    page_number INT UNSIGNED NOT NULL,
    source_url VARCHAR(500) NOT NULL,
    fetched_at DATETIME(6) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    raw_count INT UNSIGNED NOT NULL,
    accepted_count INT UNSIGNED NOT NULL,
    duplicate_count INT UNSIGNED NOT NULL,
    rejected_count INT UNSIGNED NOT NULL,
    PRIMARY KEY (import_id, page_number),
    CONSTRAINT fk_catalog_page_import FOREIGN KEY (import_id)
      REFERENCES catalog_imports(import_id)
);

CREATE TABLE catalog_rejections (
    rejection_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    import_id BIGINT UNSIGNED NOT NULL,
    page_number INT UNSIGNED NOT NULL,
    source_key VARCHAR(100),
    reason VARCHAR(255) NOT NULL,
    CONSTRAINT fk_catalog_rejection_import FOREIGN KEY (import_id)
      REFERENCES catalog_imports(import_id)
);

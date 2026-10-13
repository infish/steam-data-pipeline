-- Raw landing layer: every API response is stored before it is parsed.
-- A response identical to the previous one for the same source and request
-- keeps only its metadata and points at the earlier row (is_duplicate_of).
-- Payload bodies are purged after RAW_RETENTION_DAYS; metadata is kept.
CREATE TABLE raw_api_responses (
    response_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_id BIGINT UNSIGNED NULL,
    source VARCHAR(50) NOT NULL,
    endpoint VARCHAR(255) NOT NULL,
    request_params JSON NOT NULL,
    request_params_hash CHAR(64) NOT NULL,
    http_status SMALLINT UNSIGNED NULL,
    fetched_at DATETIME(6) NOT NULL,
    payload MEDIUMBLOB NULL,
    payload_sha256 CHAR(64) NULL,
    payload_bytes INT UNSIGNED NULL,
    is_duplicate_of BIGINT UNSIGNED NULL,
    payload_purged_at DATETIME(6) NULL,
    error_message TEXT NULL,
    INDEX idx_raw_source_params_time (source, request_params_hash, fetched_at),
    INDEX idx_raw_run (run_id),
    INDEX idx_raw_fetched_at (fetched_at),
    INDEX idx_raw_duplicate_of (is_duplicate_of),
    CONSTRAINT fk_raw_run
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id),
    CONSTRAINT fk_raw_duplicate_of
        FOREIGN KEY (is_duplicate_of) REFERENCES raw_api_responses(response_id)
);

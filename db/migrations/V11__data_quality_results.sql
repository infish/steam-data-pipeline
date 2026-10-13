-- Data observability: every check result, from every run. A failing check
-- never deletes data; it adds a run warning that the dashboard shows.
CREATE TABLE data_quality_results (
    result_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    check_name VARCHAR(60) NOT NULL,
    target VARCHAR(150) NOT NULL,
    run_id BIGINT UNSIGNED NULL,
    status VARCHAR(4) NOT NULL,
    observed_value VARCHAR(100) NULL,
    threshold VARCHAR(150) NULL,
    details TEXT NULL,
    checked_at DATETIME(6) NOT NULL,
    INDEX idx_dq_check_target_time (check_name, target, checked_at),
    INDEX idx_dq_status_time (status, checked_at),
    INDEX idx_dq_run (run_id),
    CONSTRAINT chk_dq_status CHECK (status IN ('pass', 'warn', 'fail')),
    CONSTRAINT fk_dq_run
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

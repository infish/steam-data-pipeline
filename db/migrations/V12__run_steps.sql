-- How long each step of a pipeline run took, for finding slow or redundant
-- work. One row per step; steps nest through parent_step (the run's phases
-- setup, job, finish, marts and checks are top-level).
CREATE TABLE run_steps (
    step_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_id BIGINT UNSIGNED NOT NULL,
    position SMALLINT UNSIGNED NOT NULL,
    step VARCHAR(100) NOT NULL,
    parent_step VARCHAR(100) NULL,
    status VARCHAR(10) NOT NULL,
    started_at DATETIME(6) NOT NULL,
    duration_ms INT UNSIGNED NOT NULL,
    wait_ms INT UNSIGNED NOT NULL,
    requests INT UNSIGNED NOT NULL,
    rows_affected INT UNSIGNED NULL,
    UNIQUE KEY uq_run_steps_position (run_id, position),
    INDEX idx_run_steps_step_time (step, started_at),
    CONSTRAINT fk_run_steps_run
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

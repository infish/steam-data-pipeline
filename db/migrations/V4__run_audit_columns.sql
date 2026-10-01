ALTER TABLE pipeline_runs
    ADD COLUMN measurements_loaded INT UNSIGNED NOT NULL DEFAULT 0
        AFTER rows_loaded,
    ADD COLUMN rows_rejected INT UNSIGNED NOT NULL DEFAULT 0
        AFTER measurements_loaded,
    ADD COLUMN warning_message TEXT NULL
        AFTER error_message;

-- Measurements are inserted in the same transaction as their run, so the
-- per-run row count is the exact historical value.
UPDATE pipeline_runs AS runs
JOIN (
    SELECT run_id, COUNT(*) AS measurement_count
    FROM steam_game_measurements
    GROUP BY run_id
) AS counts
    ON counts.run_id = runs.run_id
SET runs.measurements_loaded = counts.measurement_count;

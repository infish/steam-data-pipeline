CREATE TABLE IF NOT EXISTS games (
    appid INT PRIMARY KEY,
    name VARCHAR(255),
    owners VARCHAR(100),
    owners_low BIGINT,
    owners_high BIGINT,
    estimated_owners BIGINT,
    ingestion_time DATETIME
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id INT AUTO_INCREMENT PRIMARY KEY,
    started_at DATETIME NOT NULL,
    finished_at DATETIME,
    status VARCHAR(20) NOT NULL,
    rows_loaded INT DEFAULT 0,
    error_message TEXT
);

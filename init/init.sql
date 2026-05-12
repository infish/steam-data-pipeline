CREATE TABLE IF NOT EXISTS games (
    appid INT PRIMARY KEY,
    name VARCHAR(255),
    developer VARCHAR(500),
    publisher VARCHAR(500),
    owners VARCHAR(100),
    owners_low BIGINT,
    owners_high BIGINT,
    estimated_owners BIGINT,
    positive_reviews INT,
    negative_reviews INT,
    total_reviews INT,
    review_score_percent DECIMAL(5,2),
    average_playtime_forever_minutes INT,
    average_playtime_2weeks_minutes INT,
    median_playtime_forever_minutes INT,
    median_playtime_2weeks_minutes INT,
    ccu INT,
    price_cents INT,
    initial_price_cents INT,
    discount_percent INT,
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

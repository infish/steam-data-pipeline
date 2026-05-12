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

CREATE OR REPLACE VIEW top_games_by_owners AS
SELECT
    appid,
    name,
    developer,
    publisher,
    owners,
    estimated_owners,
    review_score_percent,
    ccu,
    price_cents
FROM games
ORDER BY estimated_owners DESC;

CREATE OR REPLACE VIEW top_games_by_review_score AS
SELECT
    appid,
    name,
    developer,
    publisher,
    total_reviews,
    review_score_percent,
    estimated_owners,
    ccu
FROM games
WHERE total_reviews >= 10000
ORDER BY review_score_percent DESC;

CREATE OR REPLACE VIEW most_active_games_by_ccu AS
SELECT
    appid,
    name,
    developer,
    publisher,
    ccu,
    estimated_owners,
    review_score_percent
FROM games
WHERE ccu IS NOT NULL
ORDER BY ccu DESC;

CREATE OR REPLACE VIEW free_vs_paid_summary AS
SELECT
    CASE
        WHEN price_cents = 0 THEN 'Free'
        ELSE 'Paid'
    END AS price_type,
    COUNT(*) AS game_count,
    ROUND(AVG(estimated_owners), 0) AS avg_estimated_owners,
    ROUND(AVG(review_score_percent), 2) AS avg_review_score_percent,
    ROUND(AVG(ccu), 0) AS avg_ccu
FROM games
GROUP BY price_type;

CREATE OR REPLACE VIEW publisher_summary AS
SELECT
    publisher,
    COUNT(*) AS game_count,
    ROUND(AVG(estimated_owners), 0) AS avg_estimated_owners,
    SUM(estimated_owners) AS total_estimated_owners,
    ROUND(AVG(review_score_percent), 2) AS avg_review_score_percent,
    SUM(ccu) AS total_ccu
FROM games
WHERE publisher IS NOT NULL
GROUP BY publisher
ORDER BY total_estimated_owners DESC;

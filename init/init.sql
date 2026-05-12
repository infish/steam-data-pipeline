CREATE TABLE IF NOT EXISTS games (
    appid INT PRIMARY KEY,
    name VARCHAR(255),
    owners VARCHAR(100),
    owners_low BIGINT,
    owners_high BIGINT,
    estimated_owners BIGINT,
    ingestion_time DATETIME
);
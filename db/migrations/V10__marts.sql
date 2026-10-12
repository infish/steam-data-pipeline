-- Analytics marts: tables rebuilt from the normalized layer by the SQL files
-- in db/transforms/ after every job (see docs/marts.md). Grain, sources and
-- refresh are documented at the top of each transform file.

CREATE TABLE mart_player_trends (
    appid INT UNSIGNED NOT NULL,
    observation_date DATE NOT NULL,
    name VARCHAR(255) NOT NULL,
    peak_players INT UNSIGNED NOT NULL,
    avg_players DECIMAL(12,2) NOT NULL,
    avg_players_7d DECIMAL(12,2) NOT NULL,
    peak_change_pct_dod DECIMAL(16,2) NULL,
    peak_change_pct_wow DECIMAL(16,2) NULL,
    best_rank SMALLINT UNSIGNED NULL,
    rank_change_dod SMALLINT NULL,
    in_top100 BOOLEAN NOT NULL,
    days_in_top100 INT UNSIGNED NOT NULL,
    entered_top100 BOOLEAN NOT NULL,
    exited_top100 BOOLEAN NOT NULL,
    volatility_7d_pct DECIMAL(12,2) NULL,
    PRIMARY KEY (appid, observation_date)
);

CREATE TABLE mart_player_peak_hours (
    appid INT UNSIGNED PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    typical_peak_hour_prague TINYINT UNSIGNED NOT NULL,
    days_with_that_peak_hour INT UNSIGNED NOT NULL,
    days_counted INT UNSIGNED NOT NULL
);

CREATE TABLE mart_review_trends (
    appid INT UNSIGNED NOT NULL,
    observation_date DATE NOT NULL,
    name VARCHAR(255) NOT NULL,
    total_reviews INT UNSIGNED NOT NULL,
    new_reviews INT NULL,
    positive_share_lifetime DECIMAL(5,2) NULL,
    positive_share_30d DECIMAL(5,2) NULL,
    reviews_30d INT UNSIGNED NULL,
    sentiment_drift_points DECIMAL(6,2) NULL,
    review_score_desc VARCHAR(50) NULL,
    PRIMARY KEY (appid, observation_date)
);

CREATE TABLE mart_price_status (
    appid INT UNSIGNED NOT NULL,
    country_code CHAR(2) NOT NULL,
    name VARCHAR(255) NOT NULL,
    price_state VARCHAR(12) NOT NULL,
    currency CHAR(3) NULL,
    current_final_price INT UNSIGNED NULL,
    current_initial_price INT UNSIGNED NULL,
    discount_percent TINYINT UNSIGNED NULL,
    on_sale BOOLEAN NOT NULL,
    lowest_price INT UNSIGNED NULL,
    above_lowest_pct DECIMAL(16,2) NULL,
    discount_events INT UNSIGNED NOT NULL,
    days_since_last_discount INT NULL,
    current_since DATETIME(6) NOT NULL,
    PRIMARY KEY (appid, country_code)
);

CREATE TABLE mart_genre_summary (
    genre_id SMALLINT UNSIGNED PRIMARY KEY,
    genre VARCHAR(100) NOT NULL,
    game_count INT UNSIGNED NOT NULL,
    avg_review_score DECIMAL(5,2) NULL,
    avg_metacritic DECIMAL(5,2) NULL,
    top100_games INT UNSIGNED NOT NULL,
    top100_current_players BIGINT UNSIGNED NOT NULL,
    released_before_2015 INT UNSIGNED NOT NULL,
    released_2015_2019 INT UNSIGNED NOT NULL,
    released_2020_or_later INT UNSIGNED NOT NULL,
    release_unknown INT UNSIGNED NOT NULL,
    windows_games INT UNSIGNED NOT NULL,
    mac_games INT UNSIGNED NOT NULL,
    linux_games INT UNSIGNED NOT NULL,
    free_games INT UNSIGNED NOT NULL,
    paid_games INT UNSIGNED NOT NULL
);

CREATE TABLE mart_tag_summary (
    tag_id INT UNSIGNED PRIMARY KEY,
    tag VARCHAR(100) NOT NULL,
    game_count INT UNSIGNED NOT NULL,
    total_votes BIGINT UNSIGNED NOT NULL,
    avg_review_score DECIMAL(5,2) NULL,
    top100_games INT UNSIGNED NOT NULL,
    top100_current_players BIGINT UNSIGNED NOT NULL,
    released_before_2015 INT UNSIGNED NOT NULL,
    released_2015_2019 INT UNSIGNED NOT NULL,
    released_2020_or_later INT UNSIGNED NOT NULL,
    release_unknown INT UNSIGNED NOT NULL,
    windows_games INT UNSIGNED NOT NULL,
    mac_games INT UNSIGNED NOT NULL,
    linux_games INT UNSIGNED NOT NULL
);

CREATE TABLE mart_release_year_summary (
    release_year SMALLINT UNSIGNED PRIMARY KEY,
    game_count INT UNSIGNED NOT NULL,
    free_games INT UNSIGNED NOT NULL,
    paid_games INT UNSIGNED NOT NULL,
    avg_review_score DECIMAL(5,2) NULL,
    avg_metacritic DECIMAL(5,2) NULL
);

CREATE TABLE mart_engagement (
    appid INT UNSIGNED PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    peak_players_latest_day INT UNSIGNED NOT NULL,
    latest_day DATE NOT NULL,
    estimated_owners BIGINT UNSIGNED NULL,
    rough_peak_to_owners_pct DECIMAL(10,4) NULL,
    average_playtime_minutes INT UNSIGNED NULL,
    median_playtime_minutes INT UNSIGNED NULL,
    rough_playtime_skew DECIMAL(10,2) NULL,
    snapshot_time DATETIME(6) NULL
);

CREATE TABLE mart_event_impact (
    gid VARCHAR(32) PRIMARY KEY,
    appid INT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL,
    title VARCHAR(500) NOT NULL,
    event_time DATETIME NOT NULL,
    avg_players_7d_before DECIMAL(12,2) NULL,
    days_before INT UNSIGNED NOT NULL,
    avg_players_7d_after DECIMAL(12,2) NULL,
    days_after INT UNSIGNED NOT NULL,
    change_pct DECIMAL(16,2) NULL
);

-- Price history as a Type 2 slowly changing dimension: one row per price
-- version per game and Store country. A change closes the current version
-- (valid_to, is_current = FALSE) and opens a new one; no change writes
-- nothing. Amounts are in the currency's minor unit (cents for USD) and are
-- never mixed across currencies.
CREATE TABLE game_price_history (
    price_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    appid INT UNSIGNED NOT NULL,
    country_code CHAR(2) NOT NULL,
    -- priced: on sale with a price; no_price: sold without a price (for
    -- example free to play); unavailable: the Store returned no data.
    price_state VARCHAR(12) NOT NULL,
    currency CHAR(3) NULL,
    initial_price INT UNSIGNED NULL,
    final_price INT UNSIGNED NULL,
    discount_percent TINYINT UNSIGNED NULL,
    valid_from DATETIME(6) NOT NULL,
    valid_to DATETIME(6) NULL,
    is_current BOOLEAN NOT NULL,
    opened_by_run_id BIGINT UNSIGNED NOT NULL,
    closed_by_run_id BIGINT UNSIGNED NULL,
    -- At most one current version per game and country.
    current_key VARCHAR(20) AS (
        IF(is_current, CONCAT(appid, ':', country_code), NULL)
    ) STORED,
    UNIQUE KEY uq_price_current (current_key),
    INDEX idx_price_history_game (appid, country_code, valid_from),
    CONSTRAINT chk_price_state
        CHECK (price_state IN ('priced', 'no_price', 'unavailable')),
    CONSTRAINT chk_price_current_open
        CHECK (is_current = (valid_to IS NULL)),
    CONSTRAINT chk_price_priced_amounts
        CHECK (price_state <> 'priced'
               OR (currency IS NOT NULL AND final_price IS NOT NULL)),
    CONSTRAINT fk_price_history_game
        FOREIGN KEY (appid) REFERENCES games(appid),
    CONSTRAINT fk_price_history_opened_by
        FOREIGN KEY (opened_by_run_id) REFERENCES pipeline_runs(run_id),
    CONSTRAINT fk_price_history_closed_by
        FOREIGN KEY (closed_by_run_id) REFERENCES pipeline_runs(run_id)
);

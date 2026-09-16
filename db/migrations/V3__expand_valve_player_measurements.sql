ALTER TABLE steam_game_measurements
    ADD COLUMN player_rank SMALLINT UNSIGNED NULL AFTER current_players,
    MODIFY COLUMN positive_reviews INT UNSIGNED NULL,
    MODIFY COLUMN negative_reviews INT UNSIGNED NULL,
    MODIFY COLUMN total_reviews INT UNSIGNED NULL;

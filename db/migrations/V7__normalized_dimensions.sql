-- Normalized game dimensions. Lookup tables hold each value once; bridge
-- tables link games to them. games.developer/publisher keep the raw
-- SteamSpy strings the company links were parsed from.

-- 13a: companies from SteamSpy's developer and publisher strings.
CREATE TABLE companies (
    company_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    UNIQUE KEY uq_companies_name (name)
);

CREATE TABLE game_developers (
    appid INT UNSIGNED NOT NULL,
    company_id INT UNSIGNED NOT NULL,
    position TINYINT UNSIGNED NOT NULL,
    PRIMARY KEY (appid, company_id),
    INDEX idx_game_developers_company (company_id),
    CONSTRAINT fk_game_developers_game
        FOREIGN KEY (appid) REFERENCES games(appid),
    CONSTRAINT fk_game_developers_company
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

CREATE TABLE game_publishers (
    appid INT UNSIGNED NOT NULL,
    company_id INT UNSIGNED NOT NULL,
    position TINYINT UNSIGNED NOT NULL,
    PRIMARY KEY (appid, company_id),
    INDEX idx_game_publishers_company (company_id),
    CONSTRAINT fk_game_publishers_game
        FOREIGN KEY (appid) REFERENCES games(appid),
    CONSTRAINT fk_game_publishers_company
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
);


-- 13b: Steam Store details, refreshed for the stalest games each catalog
-- run. Columns are NULL when the Store did not return the value.
CREATE TABLE game_details (
    appid INT UNSIGNED PRIMARY KEY,
    store_available BOOLEAN NOT NULL,
    app_type VARCHAR(30) NULL,
    release_date DATE NULL,
    release_date_text VARCHAR(100) NULL,
    coming_soon BOOLEAN NULL,
    is_free BOOLEAN NULL,
    required_age TINYINT UNSIGNED NULL,
    metacritic_score TINYINT UNSIGNED NULL,
    platform_windows BOOLEAN NULL,
    platform_mac BOOLEAN NULL,
    platform_linux BOOLEAN NULL,
    recommendations_total INT UNSIGNED NULL,
    details_fetched_at DATETIME(6) NOT NULL,
    tags_fetched_at DATETIME(6) NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    INDEX idx_game_details_fetched (details_fetched_at),
    CONSTRAINT fk_game_details_game
        FOREIGN KEY (appid) REFERENCES games(appid),
    CONSTRAINT fk_game_details_run
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

-- Store genre and category IDs are Steam's own.
CREATE TABLE genres (
    genre_id SMALLINT UNSIGNED PRIMARY KEY,
    description VARCHAR(100) NOT NULL
);

CREATE TABLE game_genres (
    appid INT UNSIGNED NOT NULL,
    genre_id SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (appid, genre_id),
    INDEX idx_game_genres_genre (genre_id),
    CONSTRAINT fk_game_genres_game
        FOREIGN KEY (appid) REFERENCES games(appid),
    CONSTRAINT fk_game_genres_genre
        FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

CREATE TABLE categories (
    category_id SMALLINT UNSIGNED PRIMARY KEY,
    description VARCHAR(150) NOT NULL
);

CREATE TABLE game_categories (
    appid INT UNSIGNED NOT NULL,
    category_id SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (appid, category_id),
    INDEX idx_game_categories_category (category_id),
    CONSTRAINT fk_game_categories_game
        FOREIGN KEY (appid) REFERENCES games(appid),
    CONSTRAINT fk_game_categories_category
        FOREIGN KEY (category_id) REFERENCES categories(category_id)
);

CREATE TABLE languages (
    language_id SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    UNIQUE KEY uq_languages_name (name)
);

CREATE TABLE game_languages (
    appid INT UNSIGNED NOT NULL,
    language_id SMALLINT UNSIGNED NOT NULL,
    full_audio BOOLEAN NOT NULL,
    PRIMARY KEY (appid, language_id),
    INDEX idx_game_languages_language (language_id),
    CONSTRAINT fk_game_languages_game
        FOREIGN KEY (appid) REFERENCES games(appid),
    CONSTRAINT fk_game_languages_language
        FOREIGN KEY (language_id) REFERENCES languages(language_id)
);

-- SteamSpy user tags with their vote counts.
CREATE TABLE tags (
    tag_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    UNIQUE KEY uq_tags_name (name)
);

CREATE TABLE game_tags (
    appid INT UNSIGNED NOT NULL,
    tag_id INT UNSIGNED NOT NULL,
    votes INT UNSIGNED NOT NULL,
    PRIMARY KEY (appid, tag_id),
    INDEX idx_game_tags_tag (tag_id),
    CONSTRAINT fk_game_tags_game
        FOREIGN KEY (appid) REFERENCES games(appid),
    CONSTRAINT fk_game_tags_tag
        FOREIGN KEY (tag_id) REFERENCES tags(tag_id)
);

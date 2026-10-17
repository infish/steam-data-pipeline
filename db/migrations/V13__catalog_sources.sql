-- The game list no longer comes only from SteamSpy (ADR 5).
-- catalog_source: how a game entered the catalog: 'steamspy' (the SteamSpy
-- page or a tracked-app backfill), 'valve_chart' (Valve's top-100 chart or
-- most-played leaderboard) or 'tracked' (STEAM_TRACKED_APPIDS).
-- name_source: where games.name comes from: 'steamspy', 'store' (Store
-- details, preferred once known) or 'placeholder' ("App <appid>" until the
-- Store details arrive).
ALTER TABLE games
    ADD COLUMN catalog_source VARCHAR(20) NOT NULL DEFAULT 'steamspy',
    ADD COLUMN name_source VARCHAR(20) NOT NULL DEFAULT 'steamspy',
    ADD INDEX idx_games_catalog_source (catalog_source);

-- TRUE once the developer and publisher links come from the Store details;
-- SteamSpy strings then no longer replace them.
ALTER TABLE game_details
    ADD COLUMN companies_from_store BOOLEAN NOT NULL DEFAULT FALSE;

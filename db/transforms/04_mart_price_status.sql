-- mart_price_status
-- Grain: one row per game per Store country with a price history.
-- Sources: game_price_summary (view over game_price_history), games.
-- Refresh: rebuilt after every job.
-- above_lowest_pct is how far the current price is above the lowest price
-- recorded in the same currency (0 when it is at its lowest).
DELETE FROM mart_price_status;

INSERT INTO mart_price_status (
    appid, country_code, name, price_state, currency, current_final_price,
    current_initial_price, discount_percent, on_sale, lowest_price,
    above_lowest_pct, discount_events, days_since_last_discount,
    current_since
)
SELECT
    summary.appid,
    summary.country_code,
    games.name,
    summary.price_state,
    summary.currency,
    summary.current_final_price,
    summary.current_initial_price,
    summary.current_discount_percent,
    summary.on_sale,
    summary.lowest_price,
    IF(
        summary.price_state = 'priced'
            AND summary.currency = summary.lowest_price_currency,
        ROUND(
            (summary.current_final_price - summary.lowest_price)
            / NULLIF(summary.lowest_price, 0) * 100,
            2
        ),
        NULL
    ),
    summary.discount_events,
    summary.days_since_last_discount,
    summary.current_since
FROM game_price_summary AS summary
JOIN games ON games.appid = summary.appid;

# Analytics marts

MySQL has no materialized views, so the analytics layer is a set of mart
tables, each rebuilt by one SQL file in [`db/transforms/`](../db/transforms/).
After every successful job (and every reprocess), `build_marts` runs the files
in name order. Each file deletes and re-inserts one mart in its own
transaction: readers see either the old or the new contents, and a failing
transform keeps the previous contents and adds a run warning without
affecting the other marts or the job.

Every mart is then tested in SQL (`src/marts/tests.py`): key uniqueness,
non-null key columns, at least one row (a warning when empty, for example
before any news exists), and value ranges such as shares between 0 and 100.
A failed test adds a run warning, and every result is stored in
`data_quality_results` (see [`observability.md`](observability.md)).

All marts refresh **after every job**, so the hourly players job keeps them
at most an hour old. Each transform file starts with the same notes as below.

| Mart | Grain | Sources |
| --- | --- | --- |
| `mart_player_trends` | game × UTC day | `player_daily`, `games` |
| `mart_player_peak_hours` | game | `player_observations`, `games` |
| `mart_review_trends` | tracked game × UTC day | `review_observations`, `review_rollups`, `games` |
| `mart_price_status` | game × Store country | `game_price_summary` (over `game_price_history`), `games` |
| `mart_genre_summary` | Store genre | `game_genres`, `genres`, `game_details`, `current_game_metrics`, `player_observations`, `games` |
| `mart_tag_summary` | SteamSpy tag | `game_tags`, `tags`, `game_details`, `current_game_metrics`, `player_observations`, `games` |
| `mart_release_year_summary` | release year | `game_details`, `current_game_metrics` |
| `mart_engagement` | game | `player_daily`, `current_game_metrics`, `games` |
| `mart_event_impact` | developer patch-notes event | `game_events`, `player_daily`, `games` |

## Players

**`mart_player_trends`**: per game and UTC day, the daily peak and average
players, the 7-day rolling average (calendar days, `RANGE` frame),
day-over-day and week-over-week change of the peak (against the calendar day
1 or 7 days earlier; NULL when that day has no data), the best top-100 rank
and its change (positive = moved up), whether the game was in the top 100
that day, the running count of top-100 days, entries into and exits from the
top 100, and 7-day volatility (coefficient of variation of the daily average,
in percent). Entries and exits are flagged only when the neighbouring day was
collected, so gaps in collection are not mistaken for movement.

**`mart_player_peak_hours`**: the typical hour of the daily peak in Prague
time. For each Prague-local day with at least 12 hourly observations, the
hour with the highest player count is taken (Valve's source time when known,
otherwise collection time); the typical hour is the most frequent one,
earliest on ties. MySQL has no time-zone tables in this deployment, so Prague
time is derived from the EU rule: UTC+2 from 01:00 UTC on the last Sunday of
March until 01:00 UTC on the last Sunday of October, UTC+1 otherwise. Limited
to the hourly retention window (`PLAYER_HOURLY_RETENTION_DAYS`).

## Reviews

**`mart_review_trends`**: per tracked game and UTC day, total reviews, new
reviews since the previous observation (negative when Steam reclassifies
reviews), Steam's rating label, the lifetime positive share (review summary),
the positive share and review count of the 30 days ending that day (from the
review histogram's daily buckets), and **sentiment drift**: the 30-day share
minus the lifetime share. The two endpoints count different review sets
(`docs/sources.md`), so read drift, not count differences.

## Price

**`mart_price_status`**: per game and Store country, the current price state,
price and discount, whether it is on sale, the lowest recorded price in the
same currency, how far the current price is above it (`above_lowest_pct`, 0
at its lowest), the number of discount events and days since the last
discount ended.

## Catalog

**`mart_genre_summary`** and **`mart_tag_summary`**: per Store genre or
SteamSpy tag, the number of games, the average Steam review score (each game's
latest review summary; games without one yet are left out), the average Metacritic
score (genres), total tag votes (tags), how many of the games are in the
latest top-100 chart and their current players, release-year groups (before
2015, 2015–2019, 2020 or later, unknown) and Windows/Mac/Linux counts. Release
year and platforms come from Store details, which cover a game once it has
been refreshed.

**`mart_release_year_summary`**: per release year (games with a complete
Store release date), the number of games, free and paid counts, average
Steam review score and average Metacritic score.

Since v1.2.0 (ADR 5) these marts cover every game in `games`, including
those added from Valve's charts, and the review score comes from Steam
review summaries, which `reviews_daily` refreshes on a rotation. SteamSpy's
frozen review counts are no longer used.

## Engagement (rough)

**`mart_engagement`**: per game with player data, the latest daily peak, the
SteamSpy owner estimate (range midpoint) and average and median playtime from
the latest catalog snapshot, `rough_peak_to_owners_pct` (peak as a share of
estimated owners) and `rough_playtime_skew` (average ÷ median; well above 1
means a minority plays far more than the rest). These are rough by design:
owners are an estimate, and SteamSpy figures are used from the latest
snapshot only, never as a time series.

## Update impact

**`mart_event_impact`**: per developer patch-notes post (Steam's
`patchnotes` tag on the developer's own announcements), the average daily
players in the 7 UTC days before the event day and the 7 days after it (the
event day itself excluded), how many days had data in each window, and the
percentage change. This is a **correlation, not proof of cause**: sales,
seasons and other news overlap with updates.

# Data observability

After every successful job, the pipeline rebuilds the analytics marts, runs
the checks below, and stores every result in `data_quality_results`
(`check_name`, `target`, `run_id`, `status`, `observed_value`, `threshold`,
`details`, `checked_at`). Status is `pass`, `warn` or `fail`.

A `fail` **never deletes or blocks data**. It adds a line to the run's
`warning_message`, which the dashboard shows. A `warn` is stored and shown on
the pipeline-health page, but does not touch the run. The code is in
`src/quality/checks.py`; the thresholds are heuristics chosen for this
pipeline and can be tuned there.

## Freshness

The newest successful (`HTTP 200`) raw response per source must be younger
than 1.5 times the source's expected interval. A source that has never been
fetched successfully is a `warn` (normal right after a fresh install).

| Source | Expected interval | Fails after |
| --- | --- | --- |
| `valve_top_concurrent`, `valve_most_played` | 1 h | 1.5 h |
| `steamspy_all`, `store_appdetails`, `store_prices`, `steam_review_summary`, `steam_review_histogram`, `steam_news` | 24 h | 36 h |

`valve_current_players` and `steamspy_appdetails` are fetched only when
needed (tracked games outside the top 100, backfills, tag refreshes), so
their freshness is not checked.

## Volume

For `player_observations`, `review_observations`, `game_metric_snapshots`,
`raw_api_responses` and `data_quality_results`, the row count of the last
**complete** UTC day is compared with the median of the 7 days before it.
Outside ±30% is a `warn`; zero rows when the baseline is not zero is a
`fail`. Today's partial count is never used, because it would always look
low in the morning. With fewer than 3 earlier days of data there is no
baseline, and no result is stored.

## Staleness

- **SteamSpy fields unchanged** (`game_metric_snapshots`): the share of 12
  SteamSpy values per game (owner range, reviews, playtimes, `ccu`, prices)
  that did not change between the last two catalog runs. 95% or more is a
  `warn`, 99% or more a `fail`. During the September 2026 incident this was
  effectively 100%; this check makes that incident a permanent monitor.
- **Raw duplicate rate** (per source): the share of the last 24 hours'
  successful responses that repeated the previous body byte for byte
  (`raw_api_responses.is_duplicate_of`). Above 50% is a `warn` for
  `valve_top_concurrent`, `steamspy_all` and `steam_review_histogram`, where a
  repeat means a frozen feed. For other sources repeats are normal (prices
  and the daily leaderboard change rarely), so the rate is stored as
  information with status `pass`.

## Schema drift

The newest stored payload of each source must contain the fields that
[`sources.md`](sources.md) says the pipeline relies on (for example every
row's `concurrent_in_game` in the top-100 chart). Missing fields are a
`fail`, listed in `details`. A unit test checks that the recorded responses
in `tests/fixtures/sources/` satisfy the same rules.

## Table invariants

Every table added in Part 2 has at least one check; a unit test enforces this
for every `CREATE TABLE` in migrations V5 and later.

| Check | Target | Status on violation |
| --- | --- | --- |
| `consistent_daily_rollup`: min ≤ average ≤ peak, at least one observation | `player_daily` | fail |
| `recent_leaderboard_day`: newest leaderboard day at most 2 days old | `most_played_daily` | warn |
| `catalog_games_have_developers` / `_publishers` | `game_developers`, `game_publishers` | fail |
| `named_companies`, `described_genres`, `described_categories`, `named_languages`, `named_tags` | lookup tables | fail |
| `available_details_have_type` | `game_details` | fail |
| `genre_/category_/language_links_from_details`: links only for games with details | bridge tables | fail |
| `tag_links_have_fetch_time` | `game_tags` | fail |
| `one_current_price_version`, `ordered_price_versions` | `game_price_history` | fail |
| `single_partial_month` | `review_rollups` | fail |
| `latest_reconciliation_within_tolerance` | `review_reconciliation` | warn |
| `valid_events`: a title, and not more than a day in the future | `game_events` | fail |
| `no_stuck_runs`: no run `RUNNING` for more than 6 hours | `pipeline_runs` | fail |

## Mart tests

The Phase 16 mart tests (key uniqueness, non-null keys, non-empty, value
ranges; see [`marts.md`](marts.md)) are stored here too, with the same
`run_id`.

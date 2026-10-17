# Data observability

After every successful job, the pipeline rebuilds the analytics marts, runs
that job's checks below, and stores every result in `data_quality_results`
(`check_name`, `target`, `run_id`, `status`, `observed_value`, `threshold`,
`details`, `checked_at`). Status is `pass`, `warn` or `fail`.

A `fail` **never deletes or blocks data**. It adds a line to the run's
`warning_message`, which the dashboard shows. A `warn` is stored and shown on
the pipeline-health page, but does not touch the run. The code is in
`src/quality/checks.py`; the thresholds are heuristics chosen for this
pipeline and can be tuned there.

## Which job runs which check

Every check belongs to the job that produces its data, and runs only after
that job: catalog checks after `catalog_daily`, player checks after
`players_hourly`, review checks after `reviews_daily`. A few checks guard
the pipeline as a whole and run after every job: volume of
`raw_api_responses` and `data_quality_results`, and `no_stuck_runs`.

**Freshness** is the exception. It runs after every job as a watchdog, so a
job that stops running is noticed by the others. A stale source of another
job is stored, but it does not become a warning on the current run: the
hourly players run should not report "warnings" because the catalog is
stale.

So one problem is reported once per run of the job responsible, not by every
run. The dashboard counts a check as failing when its **latest** result is
`fail`, not by counting stored rows. The owner of each source is
`SOURCE_JOBS` in `src/quality/checks.py`, and the owner of each invariant is
listed in `INVARIANTS`.

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
  `warn`: SteamSpy is frozen, and `details` says since when ("frozen since
  at least 2026-09-01"), dated from the oldest result in the current run of
  frozen results. When the share drops below 95% after a frozen run, the
  result passes and `details` notes that SteamSpy is changing again.
  SteamSpy has been frozen since at least September 2026
  ([sources.md](sources.md#steamspy-catalog-page)). The pipeline cannot fix
  that, so since v1.2.0 the check no longer has a `fail` level, which used
  to add a warning to every catalog run. The dashboard labels SteamSpy
  figures as frozen while the latest result is not `pass`.
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

| Check | Target | Status on violation | Runs after |
| --- | --- | --- | --- |
| `consistent_daily_rollup`: min ≤ average ≤ peak, at least one observation | `player_daily` | fail | `players_hourly` |
| `recent_leaderboard_day`: newest leaderboard day at most 2 days old | `most_played_daily` | warn | `players_hourly` |
| `catalog_games_have_developers` / `_publishers` | `game_developers`, `game_publishers` | fail | `catalog_daily` |
| `named_companies`, `described_genres`, `described_categories`, `named_languages`, `named_tags` | lookup tables | fail | `catalog_daily` |
| `available_details_have_type` | `game_details` | fail | `catalog_daily` |
| `genre_/category_/language_links_from_details`: links only for games with details | bridge tables | fail | `catalog_daily` |
| `tag_links_have_fetch_time` | `game_tags` | fail | `catalog_daily` |
| `one_current_price_version`, `ordered_price_versions` | `game_price_history` | fail | `catalog_daily` |
| `single_partial_month` | `review_rollups` | fail | `reviews_daily` |
| `latest_reconciliation_within_tolerance` | `review_reconciliation` | warn | `reviews_daily` |
| `valid_events`: a title, and not more than a day in the future | `game_events` | fail | `reviews_daily` |
| `no_stuck_runs`: no run `RUNNING` for more than 6 hours | `pipeline_runs` | fail | every job |

## Run step timing

Every run stores how long each of its steps took in `run_steps`, to find slow
or redundant work. The phases are `setup` (lock, run row, connections),
`job`, `finish` (writing the run row), `marts` and `checks`. Inside `job` are
the job's own steps, for example:

| Job | Steps |
| --- | --- |
| `catalog_daily` | `fetch_catalog`, `backfill_tracked`, `validate`, `load_games`, `load_snapshots`, `load_company_links`, `store_details_and_tags`, `prices`, `raw_retention` |
| `players_hourly` | `fetch_players`, `load_observations`, `daily_rollup`, `most_played`, `hourly_retention` |
| `reviews_daily` | `fetch_summaries`, `load_summaries`, `histograms`, `news` |

Inside `marts` is one step per mart file plus `mart_tests`; inside `checks`
one step per check group (`freshness`, `volume`, `staleness`,
`schema_drift`, `invariants`).

For each step the timer stores:
- `duration_ms`, wall time from a monotonic clock;
- `wait_ms`, the part spent in deliberate pauses between API requests (the
  rate-limit intervals in [`request_budget.md`](request_budget.md));
- `requests`, the API responses recorded during the step;
- `rows_affected`, where the step reports it;
- `status`, `failed` for the steps an error passed through.

A failed run keeps its steps up to the failure. Storing the timings never
fails a run. The run log line also ends with the phase times, for example
`took 412.3 s: setup 0.1 s, job 408.9 s, finish 0.0 s, marts 2.1 s,
checks 1.2 s`.

The dashboard's **Steps** tab (Pipeline health) shows the latest run of each
job split into working and waiting time per step, run time by phase over the
last 14 days, and every step of the latest run.

Optimisations made from these timings, with their before and after numbers,
are logged in [`optimizations.md`](optimizations.md).

## Mart tests

The Phase 16 mart tests (key uniqueness, non-null keys, non-empty, value
ranges; see [`marts.md`](marts.md)) are stored here too, with the same
`run_id`.

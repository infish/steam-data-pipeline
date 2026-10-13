# Changelog

This file records user-visible changes to the Steam data pipeline.

## 2026-10-12

### Added

- Raw landing layer (`V5__raw_api_responses`): every API request is recorded
  in `raw_api_responses` before parsing, with compressed body, SHA-256, HTTP
  status, parameters and fetch time; parsers read the stored copy. A body
  identical to the previous one for the same request is stored once and later
  rows point to it (`is_duplicate_of`).
- `RAW_RETENTION_DAYS` (default 30): payload bodies older than this are
  removed at the end of each daily run; metadata is kept.
- `python src/reprocess.py --run-id N` rebuilds a run's loaded rows from its
  stored raw responses, idempotently.
- Collection is split into three jobs with their own runs, locks and
  warnings: `players_hourly` (every hour), `catalog_daily` and
  `reviews_daily` (`V6__jobs_and_observations`, `pipeline_runs.job_name`).
- `player_observations` and `review_observations` store measurements by
  grain; the existing `steam_game_measurements` rows were copied into them and
  the old table is kept read-only.
- `player_daily` rolls hourly observations up per game and UTC day (peak,
  average, minimum, count, best rank); `most_played_daily` stores Valve's
  daily-peak leaderboard.
- `PLAYER_HOURLY_RETENTION_DAYS` (default 90) deletes old hourly observations;
  daily rollups are kept.
- `JOB_PLAYERS_MINUTE`, `JOB_CATALOG_TIME` and `JOB_REVIEWS_TIME` configure
  the schedule; `PIPELINE_RUN_HOUR/MINUTE` still set the catalog time if
  `JOB_CATALOG_TIME` is not given.
- `docs/request_budget.md` lists requests per job and source.
- Normalized dimensions (`V7__normalized_dimensions`): `companies` with
  `game_developers`/`game_publishers` parsed from SteamSpy strings (keeping
  names such as "Valve, Inc." whole); `game_details` from the Steam Store
  (type, release date, platforms, Metacritic, free-to-play, required age,
  recommendations); `genres`, `categories` and `languages` with their bridge
  tables; SteamSpy `tags` with vote counts.
- Each catalog run refreshes Store details and tags for the
  `STORE_DETAILS_PER_RUN` (default 120) stalest games, new games first, paced
  within the Store and SteamSpy rate limits. `STEAM_STORE_COUNTRY` (default
  `us`) sets the Store country.
- Price history (`V8__price_history`): each catalog run checks all catalog
  prices in batches of 50 and keeps Type 2 versions in `game_price_history`
  (`valid_from`, `valid_to`, `is_current`, `price_state` of `priced`,
  `no_price` or `unavailable`); unchanged prices write nothing. The
  `game_price_summary` view derives current price, on-sale flag, lowest price,
  discount events and days since the last discount.
- Review history and events (`V9__review_history_and_events`):
  `reviews_daily` upserts Steam's review histogram into `review_rollups`
  (monthly for each game's life, daily for 30 days, partial periods marked),
  stores a share-based reconciliation with the review summary in
  `review_reconciliation`, and adds new news items to `game_events` with
  `is_patch_notes` from Steam's `patchnotes` tag.
- `python src/backfill.py --source review_histogram|news --appids …` loads
  the same data on demand as a `backfill` job; repeated runs add no
  duplicates.
- Analytics marts (`V10__marts`, `db/transforms/`): player trends, typical
  peak hour (Prague time), review trends with sentiment drift, price status,
  genre/tag/release-year summaries, rough engagement and patch-release
  impact. They are rebuilt after every job, one transaction per mart, and
  tested in SQL (key uniqueness, non-null keys, row count, value ranges);
  failures become run warnings. `docs/marts.md` documents each mart.
- Data observability (`V11__data_quality_results`): after every job,
  freshness per source, daily volume against the 7-day median, unchanged
  SteamSpy fields between catalog runs, raw duplicate rates, schema drift
  against `docs/sources.md`, table invariants and the mart tests are stored
  in `data_quality_results`; failures become run warnings and never delete
  data. `docs/observability.md` documents the checks.
- Multipage dashboard: Overview, Game detail, Market explorer, Pipeline
  health and SQL explorer, every chart captioned with its source and
  timestamp meaning; screenshots in `docs/screenshots/`.
- Dashboard design: the sidebar groups pages into Explore and Operate and
  shows each job's latest-run status (ok, warnings, late, failed) and age;
  pages open with bordered headline numbers and split detail into tabs; all
  charts share one style and a colour-vision-safe palette
  (`src/dashboard_style.py`, theme in `.streamlit/config.toml`). Run warnings
  are one compact notice with the details in an expander.
- `python src/reprocess.py --seed-fixtures` seeds an empty database from the
  recorded API responses.
- `docs/data_dictionary.md`, generated from `information_schema` and
  `docs/data_dictionary.json` by `scripts/generate_data_dictionary.py`; CI
  fails when it is out of date or a table or view has no description.
- `docs/lineage.md` (sources → raw → normalized → marts) and decision records
  in `docs/adr/`.
- README: layered architecture diagram and a "What this project
  demonstrates" section linking each practice to its files.

### Fixed

- The Game detail page warns again when the latest three hourly player
  counts or daily review totals are identical; the warning had been lost in
  the dashboard redesign.
- `docker-compose.yml` passes `PIPELINE_RUN_HOUR`/`PIPELINE_RUN_MINUTE` from
  an older `.env` through to the catalog schedule when `JOB_CATALOG_TIME` is
  not set.

### Changed

- The run lifecycle moved from `main.py` to `src/jobs/base.py`.
  `python src/main.py` runs all jobs once, or only the jobs named on the
  command line.
- `latest_successful_run` and `current_game_metrics` now refer to the latest
  catalog run; `daily_pipeline_summary` is grouped by job.
- The dashboard reads players and reviews from the new tables and shows the
  latest run and any warnings of each job.
- Genres and languages now come from the Steam Store into `game_genres` and
  `game_languages`; the empty `games.genre` and `games.languages` columns
  (see the 2026-10-01 known issue) are left as they are.
- The writer account is also granted `DELETE`, which retention and
  reprocessing need. It still cannot create, drop or alter tables.
- Upserting a game never moves `first_seen_at` later or `last_seen_at`
  earlier, and older data never overwrites newer names or companies.
- Integration tests feed recorded HTTP payloads through the whole path
  (HTTP, raw layer, parsing, loading) instead of patching API functions.

## 2026-10-01

### Added

- Integration tests in `tests/integration/` that run the pipeline against a
  real, migrated MySQL database with canned API payloads, covering a
  successful load and the rollback of a run that fails after the snapshot
  insert. They run only with `INTEGRATION_DB=1` and a `_test` database.
- A CI `integration-tests` job with a MySQL 8.0 service container and Flyway
  migrations.
- Migration `V4__run_audit_columns` adds `measurements_loaded`,
  `rows_rejected` and `warning_message` to `pipeline_runs`, and backfills
  `measurements_loaded` for historical runs.
- A MySQL named lock (`steam_pipeline_run`) so a second concurrent run exits
  cleanly without creating a run row.
- Leftover `RUNNING` rows from interrupted runs are marked `FAILED` as
  abandoned at the start of the next run.
- The dashboard shows a warning, with the details in an expander, when the
  latest run completed with warnings.
- `docs/sources.md` documents every current and candidate data source (URL,
  fields, timestamp meaning, freshness, rate limit, caveats, accepted or
  rejected), with recorded responses in `tests/fixtures/sources/`. Confirmed:
  Store price batching works only with `filters=price_overview`; the review
  histogram returns monthly and daily buckets (no weekly) whose counts differ
  from review totals; news items carry a `patchnotes` tag.
- `PIPELINE_MAX_REJECTED_PERCENT` (default `5`) caps the share of SteamSpy
  rows that may fail validation before the run fails.
- Tracked games missing from the SteamSpy page are fetched with SteamSpy
  `request=appdetails` (at most one request per second), so they get a
  `games` row and snapshot and their Valve measurements are stored instead of
  discarded. A failed lookup is a run warning. `rows_extracted` can now exceed
  1,000.
- A separate writer account (`WRITER_DB_USER`, `WRITER_DB_PASSWORD`) with
  `SELECT, INSERT, UPDATE` on the database. The scheduler and manual pipeline
  now connect as the writer; the schema owner (`PIPELINE_DB_*`) is used only
  by Flyway. Existing deployments must add the writer variables before
  upgrading; see "Upgrading an existing TrueNAS deployment" in the README.

### Changed

- Valve enrichment failures no longer fail the run. A failed top-100 chart
  falls back to direct player lookups for tracked apps, a failed player lookup
  skips that app's measurement, and a failed or inconsistent review lookup
  keeps the row with empty review fields. Each case is recorded in
  `pipeline_runs.warning_message`. SteamSpy failures still fail the run.
- SteamSpy rows with row-level problems (missing name, missing or malformed
  owners, negative values, review score out of range, totals mismatch) are
  rejected individually instead of failing the run. The count is stored in
  `pipeline_runs.rows_rejected` and summarized in `warning_message`.
  `rows_extracted` now counts every row received and `rows_loaded` only the
  valid ones.
- Aligned both GitHub workflows on `actions/checkout@v7` and removed the
  leftover `refactor/production-ready` branch trigger from the test workflow.
- Corrected the README project tree.
- Top-100 games outside the catalog page are logged as a single INFO count
  instead of a per-run warning listing every app ID.
- The `Publish Docker image` workflow runs the full test workflow (now
  reusable via `workflow_call`) first and publishes only if it passes.
- Images get a `vX.Y.Z` tag when a `vX.Y.Z` git tag is pushed, in addition to
  the commit SHA tag; `latest` now moves only for builds from `main`.
- `deploy/truenas-compose.yml` pins the image to `v1.0.0` instead of
  `latest`. The README documents the release procedure.
- The dashboard caches query results for 5 minutes; "Run example" stays
  uncached. Queries are read through a database cursor, which removes the
  pandas "only supports SQLAlchemy" warning.

### Fixed

- The Valve measurement chart no longer draws a line across runs that did
  not measure the selected game; those runs now leave a gap. Points fall back
  from source time to collection time to run finish time.
- Games with the same name are now distinct in the dashboard's game selector,
  which uses the app ID and shows `name (appid)`.
- The "latest three values are identical" warning ignores runs without a
  measurement.
- `db-bootstrap` now runs `ALTER USER` after `CREATE USER IF NOT EXISTS`, so a
  rotated dashboard password takes effect instead of locking the dashboard
  out.
- The MySQL healthcheck and bootstrap pass the root password through
  `MYSQL_PWD` instead of `-p` on the command line.
- `db-bootstrap` fails with a clear error if a value it writes into SQL
  contains characters other than letters, digits, `_` and `-`.
- The `mysql` service no longer receives unused dashboard credentials, so
  rotating the dashboard password does not restart the database.
- A null or malformed SteamSpy `owners` value no longer crashes the
  transform with a `TypeError`; it becomes a rejected row.
- A failure while recording a failed run (for example, a failed rollback) no
  longer replaces the original pipeline error; it is logged and the original
  error is re-raised.
- The scheduler now waits for the configured run time in absolute time, so
  the daily run no longer fires an hour late after the spring DST change or an
  hour early after the autumn change.

### Known issues

- `games.genre` and `games.languages` have always been empty for games loaded
  from the SteamSpy catalog page, because `request=all` rows do not include
  those fields. Only games backfilled through `request=appdetails` have them.

### Removed

- Deleted the unused `db/init/01_create_dashboard_user.sh`; the dashboard
  account is created by the `db-bootstrap` service.

## 2026-09-16

### Fixed

- Replaced misleading SteamSpy `ccu` trend charts with current-player
  observations from Valve's top-100 concurrent-player feed.
- Added direct Valve player lookups for configured tracked games outside the
  top 100 and retained focused Steam review measurements.
- Recorded Valve's source measurement timestamp where available, separately
  from pipeline collection time.
- Removed legacy SteamSpy snapshots from dashboard charts. They remain stored
  and queryable for audit purposes without being presented as fresh data.
- Removed SteamSpy `ccu` fields from the dashboard's regular analysis tables
  and SQL examples; the raw historical column remains available for audit.
- Removed stale SteamSpy review and price summaries from regular dashboard
  analyses and limited the in-dashboard SQL Explorer to current operational,
  catalog, and Valve measurement objects.
- Expanded current-player coverage to catalog games present in Valve's top-100
  response; 7 Days to Die now uses Valve data rather than the frozen SteamSpy
  value.
- Disabled the duplicate user systemd timer so the Compose scheduler is the
  sole daily scheduler.

### Changed

- The dashboard's activity ranking and headline player count now use Valve
  measurements.
- Review metrics appear only for games with collected Steam review summaries.
- Added migration `V3__expand_valve_player_measurements.sql` to support player
  rank and player-only observations.
- Documented the stale-data incident, timestamp semantics, source coverage, and
  single-scheduler requirement.

## 2026-09-14

### Fixed

- Added initial Valve current-player and review measurements for Counter-Strike,
  Factorio, and Portal 2 after confirming SteamSpy's bulk and per-app values
  were stale.
- Labeled existing SteamSpy metrics and collection timestamps accurately.
- Disabled pipeline collection on scheduler startup and added deadline
  rechecking to prevent early-wakeup duplicate runs.
- Added freshness warnings for three identical consecutive Valve observations.

### Added

- Added migration `V2__steam_measurements.sql` and the
  `steam_game_measurements` table without deleting or rewriting historical
  snapshots.

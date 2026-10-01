# Changelog

This file records user-visible changes to the Steam data pipeline.

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
- `PIPELINE_MAX_REJECTED_PERCENT` (default `5`) caps the share of SteamSpy
  rows that may fail validation before the run fails.
- Tracked games missing from the SteamSpy page are fetched with SteamSpy
  `request=appdetails` (at most one request per second), so they get a
  `games` row and snapshot and their Valve measurements are stored instead of
  discarded. A failed lookup is a run warning. `rows_extracted` can now exceed
  1,000.

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

### Fixed

- A null or malformed SteamSpy `owners` value no longer crashes the
  transform with a `TypeError`; it becomes a rejected row.
- A failure while recording a failed run (for example, a failed rollback) no
  longer replaces the original pipeline error; it is logged and the original
  error is re-raised.
- The scheduler now waits for the configured run time in absolute time, so
  the daily run no longer fires an hour late after the spring DST change or an
  hour early after the autumn change.

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

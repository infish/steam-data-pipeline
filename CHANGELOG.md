# Changelog

This file records user-visible changes to the Steam data pipeline.

## v1.4.2 — 2026-10-26

### Fixed

- A job slot that fell during a container restart (an app update) was
  skipped until the next one. On start, the scheduler now runs a slot missed
  in the last 15 minutes when that job has no successful run since.
- A job started while another job was loading its data waited 50 seconds
  for a database lock and failed. Tidying up abandoned runs now locks only
  those runs, and the mart rebuild no longer takes locks that block the
  other job's load.
- The sidebar run badges could lag minutes behind the dock: they now read
  the same 3-second check.
- On a phone, the job dock now sits at the top of the page and scrolls away
  with it, instead of staying fixed over the content.
- On a phone, a stray tap on a chart zoomed into it with no way back. Charts
  no longer zoom or pan; hover values still work.
- Replaced Streamlit's deprecated `use_container_width` with
  `width="stretch"`.

## v1.4.1 — 2026-10-25

### Changed

- The job dock shows only running jobs again (a bubble appears when a job
  starts and pops away after its finish), and hovering shows a card with
  the job's name, what it does, a progress bar, the time against its usual
  time and when it started, instead of a plain tooltip.

### Fixed

- The dock vanished when the sidebar was collapsed (phones, narrow
  windows): it was mounted in the sidebar, which then renders nothing. It is
  now mounted in the page.
- The tooltip flickered while hovered (it was rewritten on every frame).
- A run cut off by an app restart (still marked RUNNING) showed as running
  for ever: past its job's time limit plus 10 minutes it is now treated as
  interrupted (no bubble; "interrupted" in the sidebar).

## v1.4.0 — 2026-10-25

### Added

- **Motion on the dashboard**, from one small kit (`src/dashboard_motion.py`)
  where motion always means something (README, "Dashboard"):
  - A job dock at the top right of every page: one bubble per job, always
    shown. A running job's bubble fills with moving liquid and its glow
    breathes; at the end it ripples, shows a check (a cross if it failed)
    and drains back to idle. One small query every 3 seconds, shared by all
    visitors.
  - Sidebar dots that breathe while their job runs.
  - Liquid gauges for the Store-peak tile and each source's freshness.
  - Headline numbers that count up, chart bars that grow from their base,
    lines that draw from left to right and points that pop in.
  - Everything stays still for visitors who ask for reduced motion.

## v1.3.1 — 2026-10-24

Fixes from a review of v1.3.0, each checked against the code and fixed in
its own commit with a test that fails without it.

### Fixed

- **Reprocessing** a run in which a request succeeded on a retry dropped
  that request's data while reporting success: the replay used the stored
  retried attempt (a failed row) instead of the answer. Replays and the
  per-run game lists now skip retried-attempt rows. Do not reprocess runs
  from v1.3.0 with the old code; reprocessing them again with v1.3.1
  restores them while their raw payloads are kept (30 days).
- **`mart_review_trends`** gives a renamed game's older rows its current
  name, as a full rebuild would (the incremental build of v1.3.0 kept the
  old name in rows older than two days).
- **Request counts during an outage:** when every try of a request fails
  on a connection error or timeout, each attempt is recorded, not one.
- **reviews_daily's time limit** also stops its histogram and news
  requests.
- **The failure streak** counts a redirect or a non-JSON answer (a moved or
  switched-off API) as struggling.
- **Releases** publish one at a time, so two tags pushed close together
  cannot move `stable` backwards.
- **Dashboard:** when every database connection stays busy, visitors see
  "try again in a minute" instead of an error box.
- The worst case of one request is 135 s (v1.3.0 said about 90 s, leaving
  out the capped `Retry-After` waits), and one integration test no longer
  relies on a sleep.

## v1.3.0 — 2026-10-24

Fixes from an independent code review: all 13 findings were checked against
the code, confirmed and fixed, each in its own commit with a test that fails
without the fix. Adds migration V14 (indexes only).

### Fixed

- **Price history:** a price answer that leaves games out, or is not an
  object (the Store sometimes answers `null`), no longer marks those games
  "unavailable" and back the next day, which counted a sale twice. A
  missing game keeps its price with a warning; only `"success": false` is
  unavailable.
- **Store details:** a game the Store does not sell (delisted) no longer
  takes a Store-details slot every hour; it waits for its turn.
- **SteamSpy staleness check:** with weekly SteamSpy pages it never stored a
  result after 2026-10-17; it now compares the last two runs that loaded
  snapshots.
- **Review rotation:** a run that failed no longer holds its games back for
  a day; only attempts from successful runs count as a game's turn.
- **Reprocessing** an older refresh run no longer overwrites newer Store
  details, names and tags.
- **Review worker:** closes its HTTP session when its database connection
  fails; the stop logic and an error on the worker are now tested.

### Changed

- **Bounded failures:** a request is tried at most three times (20 s read
  timeout, waits of 5 s then 10 s, `Retry-After` capped at 30 s): about
  90 s worst case instead of 253 s. Retried attempts are recorded in the raw
  layer. Five struggling answers in a row stop a run's requests to that API.
  Each job has a time limit (refresh 20 min, catalog 25, reviews 15) after
  which it keeps what it has, so a slow API cannot hold up the hourly
  player counts (docs/request_budget.md, "When an API fails").
- **Scale:** V14 indexes the queries over ever-growing history (freshness,
  schema drift, the payload purge, the review rotation), and the purge only
  looks at bodies still stored. `mart_review_trends` is rebuilt
  incrementally (the last days plus the tracked apps); reprocessing rebuilds
  it in full.
- **Releases:** `stable` moves only for the highest plain `vX.Y.Z` tag, never
  for a pre-release or an older-line fix.
- **Public dashboard:** at most 10 database connections at once however many
  visitors; Explorer examples cached for a minute; bootstrap limits the
  dashboard account to 20 connections (applies when the TrueNAS YAML is next
  pasted).
- Docs that had drifted from the code (request budget, sources, README,
  marts).

## v1.2.4 — 2026-10-24

### Changed

- `refresh_hourly` fetches its review summaries on a second thread while
  the Store details and tags run, since the two now go to different servers
  with separate limits. Each keeps its own pacer, so neither sees a faster
  pace. Measured live on the same slice: 206.8 s to 143.7 s (−31%);
  docs/optimizations.md, entry 7, which also has the measured effect of
  quiet games (−41% to −53% review summaries a day).

### Fixed

- The volume check judges each finished day once instead of after every
  run, so one unusual day is one warning, not about 50. It no longer checks
  `raw_api_responses`, whose daily count follows the request schedule by
  design (quiet games, catch-up runs); the raw layer is checked per source
  by freshness, schema drift and the duplicate rate.
- A game whose review summary fails (Steam answers some removed games with
  an empty summary) waits for its next turn instead of heading the rotation
  and being asked, with a warning, every hour.
- Schema drift reads the newest run's answers (up to 20) and fails only when
  none is complete, so one odd answer no longer fails it; a format change
  hits every answer and still does.

## v1.2.3 — 2026-10-23

### Changed

- Releases also move a `stable` image tag, and the TrueNAS YAML follows it
  instead of a fixed version. After this one last YAML change, a new
  release shows up as an update in TrueNAS **Apps** and installs with one
  click. Pin a `vX.Y.Z` tag to hold or roll back a version. The code is the
  same as v1.2.2.

## v1.2.2 — 2026-10-23

### Fixed

- Review summaries come from the Web API's
  `IUserReviewsService/GetAppReviews` (no key). Valve switched off the
  Store's `appreviews` endpoint on 2026-10-22; it redirected to the Store
  home page, so from about 01:30 on 2026-10-23 every summary failed with a
  warning. Same totals (all purchases, all languages), same 1.2 s pacing,
  probed live up to 300 requests per 5 minutes. The rotation resumes with
  the games whose summary is oldest.
- A JSON API answer reached through a redirect is now recorded as a failed
  request instead of storing the page it redirected to as a success. The
  freshness check would have flagged the outage above within hours.
- The dashboard shows viewers a generic message instead of a Python
  traceback when something fails (`client.showErrorDetails = "none"`), so a
  public dashboard does not reveal file paths or the database user.
- Dashboard request budget: review summaries are counted against their own
  limit (tested at 300 per 5 minutes, paced at 250) instead of the Store's
  150, which marked the Store as over its limit after v1.2.1. The planned
  daily volumes now match the 5,000-game catalog (docs/request_budget.md):
  Store 770, reviews 5,040, SteamSpy 173 a day.

## v1.2.1 — 2026-10-20

### Changed

- Dashboard: the Operate pages are renamed **Pipeline** (was Pipeline
  health) and **Explorer** (was SQL explorer). Their URLs are unchanged.
- Store price checks ask for 200 games per request instead of 50, tested live
  up to 500. The daily price check for 5,040 games goes from 101 requests and
  211 s to 26 requests and 53 s (docs/optimizations.md, entry 3).
- Review summaries (`appreviews`) have their own pacer at 1.2 s instead of
  sharing the Store's 2.1 s; a live probe ran 300 per 5 minutes without a
  refusal. `refresh_hourly` goes from about 503 s to 313 s (entry 4).
- Quiet games get their review summary less often: a game whose last two
  summaries had the same review total waits `QUIET_REVIEWS_REFRESH_DAYS`
  (default 3) instead of `REVIEWS_REFRESH_DAYS` (1); a changed total makes
  it daily again. `0` turns this off (entry 5).

### Fixed

- The genre, tag and release-year marts looked up each game's latest review
  summary through a join that grew with the review history, so every run's
  mart rebuild got slower (49 s for the tag mart after 48 simulated hours at
  5,000 games). They now read it through the `(appid, collected_at)` index:
  0.11 s, identical results (entry 6).

## v1.2.0 — 2026-10-17

### Added

- A 5,000-game catalog (ADR 6): `STEAMSPY_PAGES` (default 5) SteamSpy pages
  of 1,000 games, one page a minute, weekly, plus Valve's chart games and
  tracked apps.
- `refresh_hourly` (every hour at `JOB_REFRESH_MINUTE`, default :37) does
  the Store work in hourly slices. Every game gets its Store details and
  tags every `DETAILS_REFRESH_DAYS` (7) and its Steam review summary every
  `REVIEWS_REFRESH_DAYS` (1): a daily review series for every game. Slice
  sizes follow the catalog size within `REFRESH_MAX_REQUESTS` (300) Store
  requests per run. `STORE_DETAILS_PER_RUN` and `REVIEW_SUMMARIES_PER_RUN`
  are now optional fixed overrides, empty by default.
- `scripts/scale_test.py` and `src/ingest/synthetic.py`: push a synthetic
  catalog through every job with the API pauses off; results for 5,000 and
  20,000 games in docs/scale.md.

- The game list no longer comes only from frozen SteamSpy (ADR 5,
  `V13__catalog_sources`):
  - `players_hourly` adds every game in Valve's top-100 chart and
    most-played leaderboard to `games` (`catalog_source = 'valve_chart'`) and
    measures it, instead of skipping games missing from SteamSpy's list
    (about 30 of the top 100 every hour). `CATALOG_FROM_VALVE_CHARTS`
    (default true) switches this.
  - Tracked apps always get a games row (`catalog_source = 'tracked'`).
  - Store details supply each game's name and developer and publisher links
    (`games.name_source`, `game_details.companies_from_store`); a later
    SteamSpy page never overwrites them. Counter-Strike 2 is no longer
    "Counter-Strike: Global Offensive".
  - Steam review summaries for the other catalog games, oldest first, paced
    with the Store limit (by `refresh_hourly`, see above).
- Data check `new_games_have_store_details`: warns when a game added from the
  charts or tracked still has no Store details after 3 days.
- Store details go first to games added from the charts or tracked, then to
  other games without details, then to games whose name is not from the
  Store yet.
- Games that SteamSpy does not know (released after it froze) get "no tags"
  instead of a run warning, and are asked again after `TAG_REFRESH_DAYS`.

- `TAG_REFRESH_DAYS` (default 30): SteamSpy tags are fetched only for
  refreshed games whose tags are missing or older than this; 0 restores
  fetching them with every Store refresh.
- Dashboard: a "Frozen source" notice next to SteamSpy figures (Market
  explorer, the Game detail tags) while the staleness check sees SteamSpy
  frozen. It goes away by itself once SteamSpy's values change again.
- ADR 5 (proposed): replacing SteamSpy as the source of the game list, with
  what changes, what is lost, the request budget and open questions.

### Changed

- Raising `STEAMSPY_PAGES` (or moving `STEAMSPY_PAGE`) makes the SteamSpy
  pages due at the next catalog run, instead of waiting for
  `STEAMSPY_PAGE_INTERVAL_DAYS`.
- `catalog_daily` keeps the game list and the prices for every game; Store
  details and tags moved to `refresh_hourly`. A failed SteamSpy page is now
  a run warning instead of a failed catalog run. `reviews_daily` covers the
  tracked games again; the other games' summaries come from
  `refresh_hourly`. Store details and review-summary checks run after
  `refresh_hourly`, with a one-hour freshness interval.

- The SteamSpy page is fetched every `STEAMSPY_PAGE_INTERVAL_DAYS` (default
  7; 1 = every catalog run). Prices run every day for the whole catalog. `latest_successful_run` is now the latest catalog run with
  SteamSpy snapshots, so `current_game_metrics` keeps working between
  fetches. SteamSpy's freshness interval follows the setting, and the daily
  snapshot volume check was removed.
- The genre, tag and release-year marts take their review score from Steam
  review summaries instead of SteamSpy's frozen counts. The dashboard calls
  it "Steam review score", and its catalog counts cover the whole game list,
  with how many came from Valve's charts.
- SteamSpy is treated as a frozen reference. Daily snapshots showed its
  owner ranges, review counts, playtimes and `ccu` unchanged on every run
  since at least 2026-09-01, with the same 1,000 games and names from before
  2023 (docs/sources.md). The `steamspy_unchanged_fields` check now warns
  with "frozen since at least <date>" instead of failing every catalog run,
  and notes when SteamSpy starts changing again.
- docs/optimizations.md entry 2: about 33 SteamSpy tag requests a day instead
  of 120.

### Fixed

- The run-duration chart showed `daily_legacy` runs in the same orange as
  `catalog_daily`; older job names are now grey.

## v1.1.0 — 2026-10-14

### Added

- Run step timing (`V12__run_steps`): every run stores how long each step
  took in `run_steps`. Steps are the run's phases, the job's own steps, each
  mart and each check group. Per step it records the duration, the time
  spent waiting between API requests on purpose, the API requests made and
  rows written. A failed run keeps its steps up to the failure. The run log
  line ends with the phase times. See docs/observability.md.
- Dashboard: a **Steps** tab on Pipeline health shows where the latest run
  of each job spent its time (working versus waiting for rate limits), run
  time by phase over 14 days, and every step of the latest run.
- README: running the local stack without the scheduler
  (`docker-compose.override.yml`) when another deployment collects.

- TrueNAS shows a **Web UI** button for the dashboard (`x-portals` in
  `deploy/truenas-compose.yml`) on new installs; TrueNAS ignores it when
  added to an existing app.

### Changed

- Rate-limited requests are paced per API instead of preceded by a fixed
  pause: a request waits only until the API's interval (2.1 s Store, 1.1 s
  SteamSpy) has passed since the previous request to that API started. The
  limits are as strict as before. Measured on the live APIs,
  `catalog_daily` went from 468.7 s to 292.8 s (−37%) with the same 261
  requests; see docs/optimizations.md, a new log of optimisations and how
  each was measured.
- Data checks run after the job that produces their data, and only that
  job's runs get their failures as warnings. Before, every check ran after
  every job, so one stale catalog showed up as warnings on every hourly
  players run and as dozens of failed checks a day. Freshness still runs
  after every job as a watchdog, but a stale source of another job is only
  stored, not added to the run's warnings.
- The dashboard counts a data check as failing when its latest result is
  `fail` ("Checks failing" on Pipeline health and in the sidebar), instead of
  counting stored failures. The Data quality tab lists each check that is
  not passing now, with how often it had that status in the last 7 days.
- The sidebar status badges and "Jobs healthy" cover the current jobs only;
  `daily_legacy`, the name of runs from before the job split, stays in the
  run history.
- README upgrade steps: keep existing user and database names (older
  deployments use `steam_pipeline` as the schema owner), keep a rollback copy
  of the old YAML, and where to find the bootstrap and migrate logs.

## v1.0.1 — 2026-10-13

### Fixed

- `catalog_daily` no longer fails with MySQL error 1869 ("Auto-increment
  value in UPDATE conflicts with internally generated values") when one run
  contains company, tag or language names that MySQL's collation treats as
  equal, such as "Valve" and "VALVE". This stopped the first live catalog run
  before any rows were loaded, which also left the developer and publisher
  checks failing.

## v1.0.0 — 2026-10-13

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
  pages open with bordered headline numbers and split detail into tabs; a
  black and orange theme; all charts share one style and a
  colour-vision-safe palette
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

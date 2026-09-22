# Steam pipeline capacity demonstration — implementation plan

Prepared 2026-09-22 for a Sol agent using Medium reasoning. This document is a plan, not a deployment record. Companion execution prompt: `docs/sol-medium-handoff.md`.

## 1. Objective and chosen scope

Demonstrate measured ingestion, recovery, and SQL query performance at **10 million historical rows**, then **40 million rows if the measured resource budget permits**. Produce reproducible commands and a portfolio-quality report with raw timing results. Row count alone is not the success criterion.

Implement and deploy an isolated benchmark edition of this repository on the existing TrueNAS server via SSH. Use the existing MySQL schema and shared loading functions so the exercise measures the actual project's storage model. Generate deterministic, explicitly synthetic data for scale. Also implement a resumable real SteamSpy catalog importer and exercise it on a bounded sample; downloading the entire catalog is optional and not a prerequisite for the capacity claim.

The user asked for a plan in this conversation. Executing the companion prompt authorizes the described benchmark implementation and deployment. It does **not** authorize modifying the existing production app, production database, or its schedule. Production adoption is a later change after the benchmark is reviewed.

Deliverables:

1. Code, tests, isolated local Compose and TrueNAS Custom App templates.
2. A repeatable 100,000 / 1,000,000 / 10,000,000-row progression with optional 40,000,000-row extension.
3. Demonstrated crash recovery, idempotency, bounded memory, and exact row counts.
4. Measured SQL performance and before/after evidence for any optimization.
5. `docs/capacity-report.md`, sanitized machine-readable results, and `docs/benchmark-runbook.md`.

Do not introduce Kafka, Spark, Kubernetes, another database engine, partitioning, or a cloud service. Do not invent historical observations or pass generated measurements off as Valve/SteamSpy facts. Do not add unrelated dashboard cleanup or change the production tracking list in this task.

## 2. Verified starting point and caveats

Local repository: `/home/infish/Projects/steam-data-pipeline`.

- Git origin: `git@github.com:infish/steam-data-pipeline.git`.
- Branch at inspection: `main`; clean worktree before adding these documents.
- Inspected commit: `21a506da5fcf93c7ee8dda0f36408c4c4a4c64e4`.
- SSH: `truenas_admin@192.168.0.3`, existing Ed25519 authentication. There is no `~/.ssh/config` file. Use existing known-host verification.
- TrueNAS `/etc/version`: `25.10.4`.
- Production app: `steam-data-pipeline`; discovered Compose project: `ix-steam-data-pipeline`.
- Production MySQL storage: `/mnt/Apps/steam-data-pipeline/mysql`.
- Production dashboard: port `8501`.
- Expected running services: mysql, scheduler, dashboard; completed services: db-bootstrap, migration-assets, migrate.
- Latest direct database check in this conversation: 1,000 distinct games; latest run reported 1,000 rows loaded. Abiotic Factor was absent. Recheck if this information is needed later; this is not a permanent assertion.
- Live scheduler settings verified on September 22: `STEAMSPY_MODE=all`, `STEAMSPY_PAGE=0`, `PIPELINE_TIMEZONE=Europe/Prague`, hour 20, minute 0, **`PIPELINE_RUN_ON_STARTUP=true`**. The repository template/default says false. Preserve the live configuration; do not overwrite it from a template.
- No explicit `STEAM_TRACKED_APPIDS` override was present in the live scheduler's allowlisted environment. The local source has defaults; do not assume the running image exactly matches it.
- Live scheduler/dashboard image ID: `sha256:5a24a9279cdb66a46b002faaf36a0d9e7792e25a76d4af68af25da97b2011086`. This is a Docker image ID, **not** a registry manifest digest usable interchangeably in an image reference.
- At 11:10 CEST on September 22: 4 logical CPUs, 32,012 MiB total RAM, approximately 2,744 MiB available, no swap. `df` showed about 130 GiB available on the production dataset. This is shared ZFS capacity, not a dedicated benchmark allowance. Recheck pool free space and dataset quotas.
- `sudo -n docker ...` fails with a password requirement; direct Docker socket access was previously denied. The authenticated TrueNAS middleware API works for this account.

Current code facts:

- `src/api/steam_api.py:get_top_games()` fetches one page and rejects empty responses. Its existing HTTP adapter automatically retries; a new bulk importer must not let retries bypass the bulk rate limiter.
- `src/main.py` constructs a DataFrame and lists for one whole page, writes games/snapshots/Valve measurements, and commits the data as one transaction. A RUNNING row is committed beforehand.
- `games` is keyed by appid; snapshots are keyed by `(run_id, appid)` with indexes on `(appid, snapshot_time)` and `snapshot_time`.
- Migrations V1–V3 and the repeatable analytics migration exist. Read every migration before designing additions; do not edit applied versioned migrations.
- `current_game_metrics` selects snapshots from the latest successful run, not the latest observation per game. Partial refreshes and mixed run purposes therefore require care.
- `game_metric_changes` uses window functions. MySQL views are not stored precomputed results; merely putting an aggregation in a view does not make it cheap.
- The publish workflow pushes `latest` as well as the commit SHA. Do not trigger that workflow unchanged for a benchmark branch.

## 3. Environment and resource boundaries

Create a separate TrueNAS Custom App named **`steam-data-pipeline-bench`**, discovered and managed through TrueNAS middleware. Use a dedicated sibling dataset/directory root **`/mnt/Apps/steam-data-pipeline-bench`** with `mysql`, `artifacts`, and `cache` children. Verify each target does not belong to an existing unrelated app before creating it.

Use its own MySQL container and credentials, with no published MySQL port and no shared Docker network or mounts with production. Two databases within the benchmark MySQL instance provide separation:

- `steam_pipeline_bench`: synthetic games and historical snapshots; the benchmark worker can write only here.
- `steam_catalog_stage`: real API sample/import; the importer can write only here.

Both databases use copies of the same project migrations. Put benchmark metadata migrations under `benchmarks/db/migrations/` and apply them only to these isolated databases. Never run them against production. Existing production Flyway checksums remain unchanged.

Services: mysql, one-shot bootstrap/migration assets/Flyway migration, and an idle worker (`restart: no`) available for explicit console jobs. No daily benchmark scheduler. Optionally add a small benchmark-only Streamlit results dashboard, bound to `127.0.0.1:8502` and accessed with SSH forwarding. If 8502 is occupied, choose and document another unused loopback port. Generate a static report regardless of UI availability.

Initial caps, verified in actual Docker runtime configuration:

- MySQL: 1 CPU, 1 GiB memory, 256 MiB InnoDB buffer pool, maximum 20 connections.
- Worker: 1 CPU, 384 MiB memory, one writer, initial batch size 1,000 rows.
- Optional dashboard: 0.25 CPU, 256 MiB; keep it stopped during the ingestion benchmark if needed.
- Keep normal transactional durability and foreign-key/unique checks enabled. Do not improve benchmark numbers by disabling fsync or integrity checks. Preserve and document binlog settings and size.
- Bounded log rotation and no unbounded debug/general-query logging.

Before starting the benchmark app require at least **3 GiB available host memory** and projected post-run pool free space of at least **20%**. The observed 2.7 GiB did not meet this conservative starting threshold. Recheck once at execution time; do not stop other apps or change ZFS ARC settings to force a pass. If the gate still fails, finish implementation and local small-scale validation, and report remote capacity testing as blocked by resource headroom.

Set a maximum **30 GiB total benchmark dataset budget**, including data, indexes, caches, logs, and artifacts; enforce a verified ZFS quota on the newly created benchmark dataset if available through the supported API. Do not change the parent dataset. If no reliable quota/monitor can be established, do not launch a large remote run.

After the 1-million-row run, measure logical table/index bytes, dataset allocation, binlogs, RSS, and growth per committed row. Project the next stage conservatively, including at least 2x headroom for temporary files, redo/undo, binary logs and index work. The projected peak must fit below 24 GiB of the 30 GiB budget and preserve the pool free-space floor. These are budget thresholds, not predicted storage sizes.

During a run sample host memory, dataset/pool free space, container memory/CPU and production HTTP health every 30 seconds. End the benchmark worker cleanly after its current batch if memory stays below 1 GiB for two samples, resource limits are exceeded, or the storage floor would be crossed. Stop the benchmark MySQL container too if memory does not recover; leave its data intact. Treat new production HTTP failures or sustained latency above both 2x baseline and 1 second as a reason to pause the benchmark. Write the reason and resumable progress to the report.

Avoid synthetic ingestion from 19:45 through 20:30 Europe/Prague. Check the live pipeline is not still running before resuming after that window. No production stop/restart or artificial production failure tests.

## 4. Implement in this order

### Phase A — inventory, isolated checkout, and baseline

1. Read applicable AGENTS.md files and rediscover repository status. Preserve user changes. Create an isolated worktree/feature branch for the implementation; include these two planning documents if they have not been committed.
2. Capture a sanitized deployment inventory through SSH. Discover containers by app and service labels; IDs in conversation history are stale-able. Record live app settings using an explicit allowlist, image references/digests, mounts, versions, and port allocations.
3. Confirm a supported privileged access route before planning Docker CLI operations. Use `midclt call core.get_methods` to inspect current `app.create`/`app.update` and dataset APIs. Do not assume request schemas or edit `/mnt/.ix-apps` generated Compose files directly.
4. Run existing unit tests and a small isolated MySQL integration test locally. Record the baseline commit, Python/MySQL versions, and failures before changes.
5. Implement benchmark target guards: writer schema allowlist, benchmark-specific credentials, and a metadata marker created by the benchmark bootstrap. Refuse a synthetic command against `steam_pipeline`, an unmarked database, or a mismatched scenario. Verify the guards before inserting any rows. Do not give benchmark credentials access to production.

### Phase B — bounded shared loading and deterministic history

Expected files (adjust names only when there is a clear repository convention):

- `src/database/loaders.py`: shared row conversion and batch insert/upsert functions extracted from `main.py`.
- `src/main.py`: delegate to shared functions without changing the existing single-page production transaction behavior.
- `benchmarks/generate.py`: deterministic synthetic input and CLI.
- `benchmarks/load.py`: batched transaction/checkpoint orchestration.
- `benchmarks/db/migrations/`: benchmark-only metadata schema.
- `tests/test_batch_loading.py`, `tests/test_benchmark_generator.py`, and MySQL integration tests.

The data generator must be network-independent and bounded by batch size. Use seed **20260922** and fixed UTC start date **2025-01-01**. Generate **100,000 synthetic game entities** with names such as `Synthetic Game 000001`; numeric IDs are scoped only to the isolated database and must never be sent to external APIs. Synthetic publishers/genres should have repeatable skew, and fact values should vary by entity/date with long-tail and seasonal patterns. Respect schema bounds, NULL behavior, owner-range ordering, review arithmetic and valid percentage ranges. Do not duplicate one identical record millions of times.

Use a stable per-row hash/PRNG keyed by `(generator_version, seed, appid, day)` so restart, batch size and execution order do not change row values. Do not use Python's process-randomized `hash()` for reproducibility. A batch must contain at most 1,000 rows initially; do not build the entire historical DataFrame or tuple list in memory.

One scenario, `scale-v1`, advances through prefixes of the same history:

| Stage | Games | Days per game | Expected historical rows |
| --- | ---: | ---: | ---: |
| Pilot | 100,000 | 1 | 100,000 |
| Calibration | 100,000 | 10 | 1,000,000 |
| Required target | 100,000 | 100 | 10,000,000 |
| Conditional extension | 100,000 | 400 | 40,000,000 |

Use one `pipeline_runs` row per generated day, with a stable benchmark day-to-run mapping. Synthetic observation time comes from the fixed date; actual generation start/finish time is stored separately. The report must label the entire synthetic scenario, its seed and generator version. Never claim these are 100,000 real games or years of downloaded history.

Benchmark tables must at minimum track:

- `benchmark_scenarios`: unique scenario key, seed, generator/schema versions, game count, requested day count, synthetic flag, state, actual start/end timestamps.
- `benchmark_days`: unique `(scenario_id, day_index)` to `run_id` mapping, last committed appid/offset, committed row count, completion state.
- `benchmark_batches`: unique `(scenario_id, day_index, first_appid)` batch identity, expected range/count, checksum, timings and commit time.
- `benchmark_results`: benchmark identity, schema/image/config identifiers, query label, iteration, cache label, elapsed time and result digest/count.

Commit the facts and their checkpoint **in the same database transaction**. A lost commit acknowledgement must be resolved by rereading the committed checkpoint, not blindly incrementing counters or creating a new run. After restart reuse the same scenario/day/run IDs. Completed keys with matching checksum are skipped; conflicting configuration or payload checksums fail explicitly. Count committed logical rows, not MySQL upsert `rowcount` semantics.

Hold a database advisory lock for the target scenario on a dedicated live connection; a second writer must fail fast. If that connection is lost, stop writing rather than assuming lock ownership. Update a heartbeat for diagnosis. Handle SIGTERM between batches and roll back an uncommitted batch. Hard-kill recovery must work from the last committed checkpoint. On recovery explicitly report/reconcile abandoned RUNNING metadata.

Example required interface (implement before use):

```bash
PYTHONPATH=src:. python -m benchmarks.generate --scenario scale-v1 --games 100000 --days 1 --seed 20260922 --start-date 2025-01-01 --batch-size 1000 --resume
PYTHONPATH=src:. python -m benchmarks.generate --scenario scale-v1 --games 100000 --days 10 --seed 20260922 --start-date 2025-01-01 --batch-size 1000 --resume
PYTHONPATH=src:. python -m benchmarks.generate --scenario scale-v1 --games 100000 --days 100 --seed 20260922 --start-date 2025-01-01 --batch-size 1000 --resume
```

Increasing `days` for this exact scenario extends the data; changes to seed, game count, schema fingerprint or generator version require a different empty target/scenario. Do not truncate to resolve a conflict. The 400-day command follows only after the resource gate and measured estimate pass.

### Phase C — real catalog ingestion in staging

Add a separate manual CLI, for example `src/catalog_import.py`, using `steam_catalog_stage`. Keep the production scheduler's existing one-page path/defaults unchanged. Add a page-fetch function; retain the old entry point for compatibility.

Requirements:

- Sequential `request=all&page=N`, starting at 0; default bounded smoke import of **10 pages**. An explicit `--full` option may continue to a configurable safety cap of 200 pages. Reaching the cap means truncated/incomplete coverage, not a full success claim.
- Enforce at least **65 seconds between bulk request attempts**, including retries, across restarts and importer instances using persisted request-attempt time and a database lock. Honor longer `Retry-After` delays. Disable hidden HTTP-adapter retries for bulk requests and perform bounded retries through this limiter. Connection errors, invalid JSON, 429 and 5xx are failures/retries, never end-of-catalog signals.
- The official docs do not specify a robust end-of-catalog signal. A valid, successful empty collection can be confirmed with one rate-limited retry and treated as a candidate end; stop on a documented end signal if available. A short page is not proof of completion; query the next page. Unexpected types/error payloads fail validation. Record the termination reason and call coverage API-observed, never guaranteed all Steam games.
- Cache successful page payloads with page number, source URL, fetched-at time, hash and import ID. Cache contains public game data only, with a bounded disk budget. A cache write should be atomic. Reject mismatched/corrupt cache entries. A resumed import can reuse its captured pages without refetching.
- A single staging run spans the import. Commit each page's facts and checkpoint atomically, while the run stays RUNNING until all requested pages are done. Completed pages survive interruptions. Latest-successful views must not expose a partially completed run.
- Deduplicate app IDs within/across pages with a documented first-observation-wins policy for a given import. Count and report duplicates, invalid records, raw rows, unique accepted games and rejected rows separately. Ownership sorting can shift between requests; pagination is not a snapshot-consistent census.
- Quarantine invalid records with sanitized reasons in staging artifacts; no silent truncation of overlength strings. Mark the result with a quality-warning count; do not report full-quality success if rejected records exist.
- Do not fetch per-app Valve player/review data for every catalog entry. The 100,000-entity synthetic benchmark and catalog coverage do not imply 100,000 live player measurements.
- Add unit fixtures covering empty/short/duplicate pages, 999999 hidden-ID records, invalid data, 429/Retry-After, retry pacing, crash recovery and counters. A 10-page live smoke test takes about 10 minutes plus processing/retries; do not keep re-running it for tests.

Official API source, read on September 22: https://steamspy.com/api.php — 1,000 entries/page, owner-sorted, bulk polling at most once per 60 seconds; most other requests once per second. Website catalog totals are only rough context, not an API completeness contract. Run the sample outside the production schedule window; do not run it if it could race with another known bulk client on the same server/IP.

### Phase D — query benchmark and evidence-based optimization

Add `benchmarks/queries.sql` and `benchmarks/run_queries.py`. Run against the same synthetic schema at every stage with fixed seeds and selected app IDs spanning popular/rare entities. Validate query results against exact generation invariants or an independently computed small reference.

Required workload:

1. One game's 365-day-or-available history ordered by time.
2. Top 25 games by estimated owners in the latest completed day.
3. Publisher totals for the latest completed day.
4. Daily loaded-row totals over the scenario.
5. Day-over-day owner/review deltas for one game, with the entity/date restriction inside the window-function input and one preceding observation where needed.
6. Exact fact row count once per completed stage, separated from interactive latency measurements.

Capture SQL text/parameters, EXPLAIN and representative EXPLAIN ANALYZE, first-run latency, at least 30 warmed repetitions per query, median/p95, result row counts/digests, and failures/timeouts. Warm measurements must actually execute against MySQL; disable application-result caching. Do not call first-run measurements cold-cache results unless cache state was controlled. Do not flush host caches or restart production to manufacture cold-cache tests. Cap representative analytical statements at 30 seconds; count queries may use an explicitly reported longer bound.

Targets to assess, not fabricate: warmed p95 under 1 second for single-game history and latest top-25, under 2 seconds for latest publisher summaries at 10 million facts. Report actual measurements and misses. Investigate indexed range access and query structure before adding indexes. New benchmark indexes must have migration definitions, measured size/write cost, and before/after plans/timings. Do not change production indexes.

If aggregation remains too slow, add a benchmark-only summary table keyed by completed run/day and publisher. Populate it once when that run completes, with transactional publication and deterministic rebuild. Measure build/storage cost and compare with raw-query results. Do not describe an ordinary MySQL view as precomputed. Avoid recomputing window functions over every historical row for an interactive one-game chart.

A benchmark page, if implemented, must show `SYNTHETIC BENCHMARK DATA`, exact entities/facts, seed, observed throughput/RSS/bytes, and query results. It should read stored benchmark measurements, paginate detail rows, and avoid a full `COUNT(*)` or full fact-table load on every page refresh. It is a separate entry point (for example `benchmarks/dashboard.py`), not the live dashboard relabeled.

## 5. Packaging and SSH deployment procedure

Build/test on the workstation, not by installing development packages on the TrueNAS appliance. Add `deploy/benchmark-compose.yml` for local integration and `deploy/truenas-benchmark-compose.yml` for the separate Custom App. Reuse the immutable application image for worker/migration assets; pin MySQL/Flyway to tested versions/digests, not a moving tag alone.

Publish an explicitly benchmark-tagged image such as `ghcr.io/infish/steam-data-pipeline:benchmark-<commit>`, resolve and deploy its registry digest. Never move `latest`, push to main, or run the existing publish workflow unmodified. Add a dedicated manual benchmark workflow or make tag selection safe on non-main refs, then verify the effective tags before dispatch. Keep the existing registry/package visibility and credentials. If registry publishing is unavailable, save/load an image via SSH only with legitimately available Docker authorization; do not silently change registry visibility or install an alternative daemon.

Use the current TrueNAS API method schemas to create the benchmark app. Preserve existing app configuration; do not copy production secrets into a committed Compose file. Generate dedicated benchmark passwords in memory, pass them in structured inputs or protected files, and redact all output. Where a file is unavoidable, mode 0600, outside the repository, and remove only that newly created temporary file afterward. Avoid `set -x`, full `docker inspect`, printing `.Config.Env`, or CLI-expanded password arguments.

Normal SSH route:

```bash
ssh -o BatchMode=yes -o ConnectTimeout=10 truenas_admin@192.168.0.3
midclt call app.get_instance steam-data-pipeline
midclt call app.container_console_choices steam-data-pipeline-bench
```

Filter middleware output before presenting it. Re-read the currently installed schemas before issuing writes. Do not hardcode container IDs or assume the manually profiled `pipeline` service is running.

Established access fallback when `sudo -n docker` is unavailable:

- Use the supported TrueNAS middleware API over SSH to manage the separate benchmark Custom App.
- The installed Python package is `truenas_api_client`; `midclt` works for this account.
- Supported app container consoles use `/websocket/shell` with a short-lived single-use `auth.generate_token` and options `{app_name, container_id, command: '/bin/sh'}`. Keep tokens in memory and never print them. Discover running container choices first. For the same-host Unix-client to loopback-WebSocket transition, origin matching required adjustment in the earlier diagnostic; inspect the installed handler and current schema rather than copying old authentication assumptions.
- A tiny console client should handle text control events, binary terminal output, connection close, command completion/exit codes and a real job-status check. Do not assume a three-second silence means completion. Never log authentication frames. For lengthy jobs, launch them detached with logs and a DB checkpoint/status record, and verify completion via exit status plus database state; a lost SSH connection must not corrupt progress.
- Use short-lived console sessions for bounded commands. For raw Docker runtime health/restart/RSS information, use genuinely current authorized APIs; cached `config.v2.json` health fields can be stale. The previous audit saw `starting` in persisted metadata and cannot establish Docker's current health state from that file.
- If privilege/authentication is insufficient for a required operation, finish unaffected code/tests/artifacts and identify the exact blocked operation. Do not edit sudoers, use private privilege-bypass methods, or install long-lived access credentials.

Deployment order: preflight → dedicated storage/credentials → immutable images → benchmark MySQL healthy → bootstrap → migrations/validation → idle worker → pilot → calibration/report → required target → optional extension. Migrations and one-shot services must exit zero before loading. Capture before/after production health and verify its image, config, containers and persistence were not modified.

## 6. Tests, milestones and acceptance

Run the repository's existing tests with `PYTHONPATH=src python -m unittest discover -s tests -v`. Extend existing tests only where shared production code changes. Add MySQL integration tests in an isolated database; mocks alone cannot establish transaction/recovery behavior.

Required cases:

- Same seed yields the same logical records across process restarts and batch sizes; changed configuration is rejected on resume.
- Kill only the benchmark worker mid-batch and immediately after a committed checkpoint. Resume the same scenario to the exact expected count with no duplicate `(run_id, appid)` keys and matching checksums.
- Run the same completed command twice; second run changes neither counts nor values and does not create duplicate day/run mappings.
- Two writers contend; only one acquires the scenario lock.
- Inject a malformed synthetic/API record and simulated SQL/API failure in test fixtures; metadata/counters and transaction boundaries remain correct.
- Attempt the synthetic CLI against an unmarked DB/production DB name; it refuses before writing.
- Validate FK relationships, per-day expected counts, owner ranges, review totals, date ordering, field bounds and deterministic sample digests at each completed stage.
- Verify RUNNING/partial runs are excluded from current successful analytics; summaries become visible only for completed days/imports.
- Existing scheduler tests remain passing, including failure survival and startup-option behavior if touched. Do not test failure survival by breaking the deployed production pipeline.
- Worker RSS remains below its cap and approximately bounded as dataset size grows. No OOM/restart loops or silent discarded rows.

Save sanitized artifacts under `benchmark-results/<run-id>/`: manifest JSON, per-batch CSV/JSONL timings, resource samples, query repetitions/plans, correctness results and generated report. Keep large raw datasets/caches and secrets out of Git. Check in a compact reproducible report and small sanitized representative results.

`docs/capacity-report.md` must distinguish:

- Real catalog games fetched versus synthetic entities/facts generated.
- Observed unique counts versus estimated metadata row counts.
- Generation time, database load time, total elapsed time, and rows/second; include seed/catalog loading separately.
- Data, index, binlog and total dataset bytes; logical MySQL and physical ZFS measurements differ.
- Hardware, container caps, buffer pool, schema/index definitions, image digest/commit, date, and concurrent load.
- First-run versus warmed query timings; raw versus summarized queries and summary-build cost.
- Required 10-million result versus optional 40-million result. A projection is never a completed benchmark.
- Changes that improved results, failed targets, limitations and safe rerun/recovery commands.

Completion means: implementation/tests pass, the isolated app is deployed where resources permit, recovery/idempotency are demonstrated, the 10-million stage and timings are actually measured, and evidence/report/runbook are delivered. If a resource or access gate prevents remote execution, say exactly which deliverables are complete and which benchmark remains unexecuted. Do not claim capacity from an estimate.

After collecting results, stop only the benchmark worker/optional dashboard/MySQL through the benchmark app lifecycle to release resources; retain its dataset and logs for inspection/resume. Document the start/resume command. No automated deletion, no `down -v`, no database purge. A later user-directed cleanup can remove this explicitly named benchmark dataset.

## 7. Optional follow-up after the demonstrated capacity

After reviewing the report, decide separately whether to adopt paginated imports, tracked-game catalog lookup (including Abiotic Factor), incremental current-state tables, retention, or selected query optimizations in production. That rollout needs a fresh backup/restore rehearsal, verified live configuration export, new migration numbers where applicable, an immutable release image and a deployment/rollback window. Changing production schedules, refreshing all games daily, or mixing synthetic and real metrics is outside this implementation's scope.

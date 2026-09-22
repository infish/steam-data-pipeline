# Steam pipeline capacity report

## Verdict

The isolated TrueNAS benchmark **demonstrated the required 10-million-fact
target** on 2026-09-22 without changing the production app, database, storage,
schedule, or image. The benchmark app remains deployed and idle with all data
retained. This is a capacity result for deterministic synthetic facts, not a
production rollout recommendation and not a claim about 10 million live Steam
observations.

## Deployment and isolation

- Branch/commit: `feature/capacity-benchmark` at
  `af75ebe460d52c4fb9efbe4f449edca8b230cf16` for the deployed image.
- Immutable image:
  `ghcr.io/infish/steam-data-pipeline@sha256:c9428e3c002a026297e7dca5ccd97a5e3734540cd64249f0d58c737c6b6a4c2d`.
- Existing TrueNAS creation job `176577` finished `SUCCESS`; no duplicate app
  or dataset was created.
- App/dataset: `steam-data-pipeline-bench` and
  `Apps/steam-data-pipeline-bench`, with a 30 GiB quota and separate `mysql`,
  `artifacts`, and `cache` children.
- Migrations succeeded in both `steam_pipeline_bench` and
  `steam_catalog_stage` (5/5 Flyway rows each). Both databases carried the
  benchmark marker. The benchmark writer was denied access to `steam_pipeline`
  with MySQL error 1044.
- Artifact and cache mounts were writable. MySQL had no published port and the
  benchmark app shared no database or storage mount with production.
- Effective limits were verified from cgroups: worker 384 MiB / 1 CPU; MySQL
  1 GiB / 1 CPU. MySQL used a 256 MiB buffer pool, 20 connections maximum,
  binary logging, `sync_binlog=1`, and `innodb_flush_log_at_trx_commit=1`.

## Staged synthetic results

All rows below are explicitly synthetic, generated with seed `20260922`, a
fixed 2025-01-01 start date, 100,000 games, and batches of 1,000 rows. Later
stages extended the same `scale-v1` scenario.

| Stage | Exact facts | Days | Elapsed | Dataset allocation | Worker peak | MySQL peak | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Pilot | 100,000 | 1 | 12 s | 40.6 MiB observed after pilot | 22.4 MiB | 421 MiB | PASS |
| Calibration | 1,000,000 | 10 | 117 s | 203.1 MiB | 22.2 MiB | 640 MiB | PASS |
| Required target | 10,000,000 | 100 | 1,436 s | 1.61 GiB | 22.5 MiB | 644 MiB | PASS |

The 1-million gate projected the 10-million allocation at roughly 2 GiB; with
the required 2x temporary/index/redo headroom it remained far below the 24 GiB
ceiling. At 10 million, `game_metric_snapshots` occupied 1,330,642,944 logical
data bytes and 633,307,136 logical index bytes. The full benchmark dataset had
28.39 GiB still available. A separate binlog-only byte count was not captured;
the dataset allocation includes MySQL files and is the enforced physical
quota measurement.

The completed 1-million command was rerun as a no-op: it exited successfully in
2 seconds with facts, distinct keys, batches, and days unchanged. A separate
recovery scenario was interrupted before its completion marker and then resumed
with the identical command. It finished at exactly 200,000 facts, 200,000
distinct `(run_id, appid)` keys, 200 batches, and two successful days.

The optional 40-million extension was not run. It is not needed for the required
10-million acceptance target and would be a separate retained-data decision.

## Query results at 10 million facts

Each warmed workload used 30 real MySQL executions; the exact count used one
explicitly reported longer execution.

| Query | Median | p95 | Rows | Target | Result |
| --- | ---: | ---: | ---: | --- | --- |
| One-game history | 1.295 ms | 2.599 ms | 100 | p95 < 1 s | PASS |
| Latest top 25 | 66.480 ms | 84.147 ms | 25 | p95 < 1 s | PASS |
| Latest publisher totals | 259.505 ms | 282.375 ms | 50 | p95 < 2 s | PASS |
| Daily loaded rows | 0.763 ms | 0.992 ms | 100 | informational | PASS |
| One-game deltas | 2.091 ms | 5.854 ms | 100 | informational | PASS |
| Exact fact count | 6,896.034 ms | n/a | 1 | longer bound reported | PASS |

Plans used the existing app/time index for one-game history and the primary
run/app key for latest-run queries. Latest top-25 used a temporary/filesort over
100,000 current-run rows but still met the latency target. No production index
was changed.

## Live catalog smoke test

The isolated `steam_catalog_stage` database completed a bounded, rate-limited
10-page SteamSpy smoke import in 587 seconds. It observed 10,000 raw records:
9,956 unique games, 44 duplicates, and zero rejected records. Each page had
1,000 raw records. The result is API-observed bounded coverage only; it is not a
complete Steam census and is kept separate from synthetic benchmark facts.

## Production and host health

Production remained `RUNNING` throughout. Its scheduler, dashboard, and MySQL
container IDs were unchanged from the initial observation, and their image
references remained `ghcr.io/infish/steam-data-pipeline:latest`,
`ghcr.io/infish/steam-data-pipeline:latest`, and `mysql:8.0`. The production
dashboard returned HTTP 200 during all samples. Sampled latency stayed far
below the pause threshold (generally 1-4 ms, never approaching 1 second).

Host available memory fluctuated, as expected on the shared ZFS host. The 10m
run started with 4,518 MiB available, briefly reached about 2,625 MiB, and
finished with 3,084 MiB available; it never approached the active-run stop rule
of less than 1 GiB for two samples. No other app was stopped and no host tuning
was changed. `/mnt/Apps` reported about 130.5 GiB available before the 10m run.

Sanitized machine-readable evidence is checked in at
`docs/evidence/remote-20260922-summary.json`. Local validation evidence remains
at `docs/evidence/local-100k-summary.json`.

## Scope boundary

No production migration, rollout, schedule change, `latest` publication, or
retained-data deletion was performed. Review this report before any separate
production adoption decision.

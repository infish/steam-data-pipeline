# 1. MySQL and an in-process scheduler

Status: accepted (2026-10)

## Context

The pipeline runs on a home TrueNAS server as a single Custom App: one Docker
Compose definition, no cluster, a few hundred megabytes of RAM to spare. It
collects modest volumes (about 1,000 catalog rows and 24 small player runs a
day) and must keep running unattended for months.

## Decision

- Store everything in **MySQL 8**, migrated by **Flyway**. MySQL runs well in
  one container, its window functions, CTEs and `LATERAL` joins cover the
  marts, and the dashboard can read it with a separate read-only account.
- Schedule jobs with a small **in-process scheduler** (`src/scheduler.py`): a
  table of jobs, each with its own next-run time, waits computed in absolute
  time so DST changes do not shift runs, and per-job MySQL named locks so a
  manual run never overlaps a scheduled one of the same job.

## Alternatives considered

- PostgreSQL: equally suitable; MySQL was already in place with history.
  Revisit if dbt becomes the transform tool (its MySQL adapter is community
  maintained).
- Airflow, Dagster or Prefect: strong orchestration and lineage features, but
  each adds a web server, a metadata database and workers to a home server
  for three jobs.
- cron or systemd timers: no shared state, no run records, and an earlier
  incident showed how easily two schedulers end up running side by side.

## Consequences

- No time-zone tables in MySQL: Prague time in SQL uses the EU DST rule
  (`db/transforms/02_mart_player_peak_hours.sql`).
- No materialized views: marts are tables rebuilt by SQL files
  (`docs/marts.md`).
- Job history, locks and retries live in our own tables (`pipeline_runs`),
  which the dashboard shows directly.

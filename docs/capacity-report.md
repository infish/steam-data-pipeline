# Steam pipeline capacity report

## Verdict

Remote capacity is **not yet demonstrated**. On 2026-09-22 the first required
preflight found 3,044 MiB available memory on the TrueNAS host, below the
3,072 MiB starting threshold. A later final check found 3,538 MiB available,
so the memory gate had recovered and production remained healthy. The next
required step—pushing the private repository's feature branch and benchmark tag
to its GitHub origin to publish the isolated GHCR image—was rejected by the
execution environment pending explicit user confirmation for that external
private-repository mutation. No benchmark dataset/app was created, no image was
published, and no remote rows were loaded.

## Implementation status

- Isolated worktree branch: `feature/capacity-benchmark`, based on
  `21a506da5fcf93c7ee8dda0f36408c4c4a4c64e4`.
- Deterministic generator: seed 20260922, fixed history start 2025-01-01,
  generator `scale-v1.0`, stable BLAKE2-derived per-row values and batches of at
  most 1,000 rows.
- Resumable loader: marked-database/name guard, per-scenario advisory lock,
  persistent day/run mapping, atomic fact/batch/checkpoint commits, checksum
  conflict detection, exact per-day count and SIGTERM between-batch handling.
- Catalog importer: separate staging target, no hidden HTTP retries, persistent
  attempt timing, 65-second pacing, bounded retries/cache, duplicate accounting,
  validation quarantine and confirmed-empty-page handling.
- Query workload: fixed SQL, plans, first/warmed execution, 30 warmed samples,
  digest consistency, median/p95 and persisted machine-readable results.
- Packaging: local and TrueNAS templates plus a manual benchmark-only GHCR
  workflow that never publishes `latest`.

## Measured results

| Item | Result |
| --- | --- |
| Remote synthetic facts | 0 (resource gate blocked) |
| Required 10 million | Unexecuted, not claimed |
| Conditional 40 million | Unexecuted, not projected or claimed |
| Real catalog games | 0 (live smoke import not run) |
| Query latency | Not measured against target scale |
| Recovery at target scale | Not demonstrated remotely |
| Host CPU / RAM | 4 logical CPUs / 32,012 MiB total |
| Host available RAM at gate | 3,044 MiB; minimum is 3,072 MiB |
| Host available RAM at final recheck | 3,538 MiB; memory gate then passed |
| Host swap | 0 |
| `/mnt/Apps` available | 131 GiB reported |

Local isolated validation (not a TrueNAS or 10-million-row capacity claim):

- The final suite passed 26/26 unit and marked-MySQL integration tests.
- A clean 100,000-row / 100-batch pilot completed in 6.95 seconds including
  Compose exec overhead (14,388 rows/s); summed database insertion time was
  3.087 seconds.
- A controlled hard kill left exactly 69,000 rows and 69 atomic checkpoints.
  Resume completed at exactly 100,000 rows / 100 batches with 100,000 distinct
  `(run_id, appid)` keys.
- Re-running the completed 2,000-row integration scenario changed neither row
  count nor distinct key count. A concurrent writer failed the advisory lock.
- On the scenario-scoped local 100,000-row query workload, warmed p95 was
  0.678 ms for one-game history, 23.578 ms for latest top-25, 100.252 ms for
  latest publisher totals, 0.406 ms for daily row totals, and 0.689 ms for
  one-game deltas. Exact count took 10.898 ms. These small-scale timings do not
  predict the required 10-million-row result.

Sanitized machine-readable evidence is checked in at
`docs/evidence/local-100k-summary.json`.

Synthetic entity counts and real API catalog counts are intentionally separate.
No generated measurement is presented as a Valve or SteamSpy observation.

## Remaining acceptance work

After explicit approval to push `feature/capacity-benchmark` and its
`benchmark-*` tag to the configured private GitHub origin, publish the immutable
benchmark image, establish the supported 30 GiB-quota benchmark dataset/app
through current TrueNAS middleware schemas, and run the 100k → 1m → 10m
sequence. Recheck memory immediately before creation because the observed
headroom fluctuated across the threshold. At 1m, apply
the measured storage/RSS projection gate before 10m. Capture container and host
samples every 30 seconds, recovery/no-op evidence, logical and physical bytes,
binlogs, plans, query samples, production health and exact counts. Run 40m only
if the measured 24 GiB peak and host-floor gates pass.

The exact implementation and resume commands are in
`docs/benchmark-runbook.md`. Production was left running and unchanged.

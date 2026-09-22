# Sol Medium execution prompt

Use Sol with Medium reasoning in a new task attached to `/home/infish/Projects/steam-data-pipeline`. Copy the prompt below. If the new task uses a worktree before these documents are committed, include copies of both planning files there.

---

Implement `docs/scaling-implementation-plan.md` and deliver the working capacity demonstration via SSH on my existing TrueNAS SCALE server. Read the entire plan and applicable AGENTS.md instructions first. This is an implementation request, not a request for another plan.

My objective is to demonstrate larger-dataset engineering capacity with measured performance, correctness, restart/resume behavior, and a reproducible report. Required target: 10 million explicitly synthetic historical rows across 100,000 synthetic game entities. Run the 40-million-row extension only if the plan's measured storage/memory/time gates permit it. Implement the bounded resumable SteamSpy catalog importer as specified, but do not require a complete external catalog download to complete the synthetic benchmark.

Repository: `/home/infish/Projects/steam-data-pipeline`; origin `git@github.com:infish/steam-data-pipeline.git`. SSH using existing credentials: `truenas_admin@192.168.0.3`. TrueNAS app currently in production: `steam-data-pipeline`. Production database storage: `/mnt/Apps/steam-data-pipeline/mysql`; dashboard port 8501. Discover current container IDs and settings; do not reuse IDs from old conversations.

Create an isolated Git worktree/feature branch and the separate TrueNAS Custom App `steam-data-pipeline-bench`, using `/mnt/Apps/steam-data-pipeline-bench` for its own data. You are authorized to implement/test code, publish a uniquely benchmark-tagged image to the existing GHCR package without changing its visibility or moving `latest`, create and configure the dedicated benchmark dataset/app/credentials, run and deliberately interrupt/restart only benchmark workers for recovery tests, and stop the benchmark app after preserving its results. Do not push to main or modify the production app/database/configuration/schedule/containers. Do not delete existing data or change privileges on the host. Synthetic credentials must have no production access.

Key findings to account for:

- Production scheduler really has `PIPELINE_RUN_ON_STARTUP=true`; the repository template says false. Preserve the deployed behavior.
- The current pipeline loads a whole run in one transaction, fetches one SteamSpy page, and filters Valve measurements to that catalog. Reuse/extract its loaders without silently changing the production transaction semantics.
- Synthetic history must remain in its own marked database and visibly labeled benchmark artifacts. The same schema/constraints/durability must be used to make the test credible.
- Rate-limit every SteamSpy bulk attempt including retries at 65 seconds or more; persist resumable progress and account for duplicates and incomplete catalog coverage.
- The latest observed host had 4 logical CPUs, 32 GiB RAM but only about 2.7 GiB available and no swap. Recheck the plan's minimum headroom before launching remote MySQL/large loads. Never stop other apps or change global host/ZFS settings to force capacity.
- `sudo -n docker` is not available to the SSH account. Use the supported authenticated TrueNAS middleware/app-console route described in the plan. Inspect current API schemas; do not edit generated `/mnt/.ix-apps` Compose files or bypass privilege checks.
- Persisted Docker metadata may contain stale health fields. Use live health checks and runtime state where available.
- Existing image publishing pushes `latest`; isolate benchmark publishing before dispatching it.

Work through the plan's phases in order. Test locally first, then use a 100k pilot and 1m calibration before 10m. Commit data and checkpoints atomically; prove exact counts after crash/resume and no-op reruns. Capture throughput, bounded RSS, logical/physical storage, raw query repetitions, plans, p50/p95, and correctness evidence. Meet query targets where practical and honestly report misses; do not lower integrity/durability to inflate numbers. No unapproved production writes or tuning.

Keep secrets in existing container environments or dedicated protected benchmark configuration; never print them or include them in committed files, process arguments, logs or reports. Use current app discovery and scoped database queries, not environment dumps.

Continue through implementation, tests, benchmark deployment, measured runs and documentation within the authorized scope. If a resource or access gate blocks a remote stage, complete everything else and identify the exact missing condition. Do not repeatedly ask for permission for work already authorized here, and do not claim a blocked benchmark passed.

Deliver `docs/capacity-report.md`, `docs/benchmark-runbook.md`, relevant tests, deploy templates, and sanitized machine-readable evidence. End with achieved versus unexecuted row targets, real versus synthetic counts, throughput/storage/query latency, recovery results, remaining limitations, commit/image references, and exact commands to resume the stopped benchmark environment. Leave production running and retain benchmark data for review.

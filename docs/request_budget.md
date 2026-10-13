# Request budget

How many requests each job makes, against each source's limit. The limits
and their basis are in [`sources.md`](sources.md#rate-limits). "Tracked" is
the number of app IDs in `STEAM_TRACKED_APPIDS` (default 3).

## Per job

| Job | Runs per day | Source | Requests per run | Requests per day |
| --- | --- | --- | --- | --- |
| `players_hourly` | 24 | Valve top concurrent players | 1 | 24 |
| | | Valve current players | tracked apps outside the top 100 (≤ 3) | ≤ 72 |
| | | Valve most played (daily peak) | 1 | 24 |
| `catalog_daily` | 1 | SteamSpy `request=all` | 1 | 1 |
| | | SteamSpy `request=appdetails` | tracked apps off the catalog page (≤ 3), plus tags for `STORE_DETAILS_PER_RUN` games (120) | ≤ 123 |
| | | Steam Store `api/appdetails` | `STORE_DETAILS_PER_RUN` (120) | 120 |
| | | Steam Store price batch | catalog games ÷ 50 (≈ 20) | ≈ 20 |
| `reviews_daily` | 1 | Steam review summary | tracked apps (3) | 3 |
| | | Steam review histogram | tracked apps (3) | 3 |
| | | Steam news | tracked apps (3) | 3 |
| `backfill` (manual) | on demand | review histogram or news | the given apps | — |

## Per source

| Source | Requests per day | Peak rate | Limit used for planning | Headroom |
| --- | --- | --- | --- | --- |
| Valve Web API (top concurrent, current players, most played, news) | ≤ 123 | ≤ 5 per hourly run | keep modest; no published keyless limit | n/a |
| SteamSpy `request=all` | 1 | 1 per day | 1 per 60 s | ample |
| SteamSpy other requests | ≤ 123 | 1 per 1.1 s (enforced pause) | 1 per s | pause is 10% above the limit |
| Steam Store (`api/appdetails`, price batches, `appreviews`, `appreviewhistogram`) | ≤ 146 | 1 per 2.1 s for details, prices and histograms | 150 per 5 min | the whole day's Store requests fit inside one 5-minute budget |

## Notes

- The catalog job alternates one Store request (after a 2.1 s pause) with
  one SteamSpy request (after a 1.1 s pause) per refreshed game, so 120 games
  take about 6.5 minutes and never exceed either limit. At 120 games per run,
  a 1,000-game catalog is fully refreshed about every 9 days.
  `STORE_DETAILS_PER_RUN` is capped at 140 so a run stays inside the Store
  budget.
- The most-played leaderboard changes once a day but is fetched every hour,
  because a missed day cannot be fetched later. Repeated identical responses
  cost only a metadata row in the raw landing layer.
- Jobs run one at a time in the scheduler, so their budgets do not overlap.
  A manual `docker compose run pipeline` can overlap the scheduler; the
  per-job locks keep the same job from running twice, but two different jobs
  may run at once. Their combined Store requests stay far below the budget.

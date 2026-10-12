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
| | | SteamSpy `request=appdetails` | tracked apps off the catalog page (≤ 3) | ≤ 3 |
| `reviews_daily` | 1 | Steam review summary | tracked apps (3) | 3 |

## Per source

| Source | Requests per day | Peak rate | Limit used for planning | Headroom |
| --- | --- | --- | --- | --- |
| Valve Web API (top concurrent, current players, most played) | ≤ 120 | ≤ 5 per hourly run | keep modest; no published keyless limit | n/a |
| SteamSpy `request=all` | 1 | 1 per day | 1 per 60 s | ample |
| SteamSpy other requests | ≤ 3 | 1 per 1.1 s (enforced pause) | 1 per s | pause is 10% above the limit |
| Steam Store (`appreviews`) | 3 | 3 per daily run | 150 per 5 min (shared Store budget) | 147 per 5 min |

## Notes

- The most-played leaderboard changes once a day but is fetched every hour,
  because a missed day cannot be fetched later. Repeated identical responses
  cost only a metadata row in the raw landing layer.
- Jobs run one at a time in the scheduler, so their budgets do not overlap.
  A manual `docker compose run pipeline` can overlap the scheduler; the
  per-job locks keep the same job from running twice, but two different jobs
  may run at once. Their combined Store requests stay far below the budget.

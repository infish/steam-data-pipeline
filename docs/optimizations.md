# Optimizations

Changes made to cut processing time, each found and checked with the run
step timings in `run_steps` (see [`observability.md`](observability.md#run-step-timing)
and the **Steps** tab on Pipeline health). Every entry records what was
slow, what changed, the measured effect, and why the change is safe.

Measure a change the same way: compare the job's steps before and after,
for example

```sql
SELECT run_id, step, duration_ms / 1000 AS seconds, wait_ms / 1000 AS waiting,
       requests
FROM run_steps
WHERE run_id IN (63, 66) AND (parent_step IS NULL OR parent_step = 'job')
ORDER BY run_id, position;
```

## 1. Pace each API instead of pausing before every request (v1.1.0)

**Found:** the first timed `catalog_daily` run (local stack, live APIs, run
63) took 468.7 s. `store_details_and_tags` took 419.1 s, and 384.0 s of that
was deliberate waiting. For each of the 120 refreshed games the job paused
2.1 s before the Store request and then another 1.1 s before the SteamSpy
request: 3.2 s of pauses per game on top of the requests themselves. The
SteamSpy pause was redundant, because the Store pause had already spaced the
SteamSpy requests further apart than SteamSpy needs. The fixed pauses also
ignored the time the previous request and its loading had already taken.
`prices` (48.7 s, 42.0 s waiting) and the review histograms used the same
pattern.

**Changed:** a pacer per rate-limited API and run (`src/api/pacing.py`,
`RunContext.pace("store")`, `RunContext.pace("steamspy")`). Before a request
it waits only until the API's interval has passed since the start of that
API's previous request. Time spent on responses, loading and other APIs'
requests counts towards the interval, so the SteamSpy wait inside the
details loop is usually zero, and the Store wait is 2.1 s minus whatever the
game's two requests and loading took. One Store pacer is shared by details,
price batches and histograms, and the SteamSpy page request counts towards
SteamSpy's interval. Replaced the fixed pauses in `dimensions.py`,
`prices.py`, `review_history.py` and the tracked-app SteamSpy lookups.

**Measured** (local stack, live APIs, same request count):

| Step | Before (run 63) | After (run 66) |
| --- | --- | --- |
| `catalog_daily` total | 468.7 s | 292.8 s (−37%) |
| `store_details_and_tags` | 419.1 s, 384.0 s waiting | 250.2 s, 216.5 s waiting |
| `prices` | 48.7 s, 42.0 s waiting | 42.0 s, 35.7 s waiting |
| Store requests | 140 | 140 |
| SteamSpy requests | 121 | 121 |

The remaining wait is the Store limit itself: 140 Store requests at one per
2.1 s is about 294 s.

**Why it is safe:** the guarantee is unchanged. Request starts to one API are
never closer than its interval (2.1 s Store, 1.1 s SteamSpy), which
`tests/test_pacing.py` checks. In run 66 the Store requests arrived on
average 2.10 s apart (2.00 s at least, measured at response time, which
jitters with latency), and at most 140 fell into any 5-minute window against
the limit of 150. A 2.1 s interval allows at most 143 in any 5 minutes,
whatever `STORE_DETAILS_PER_RUN` is set to.

## 2. Fetch SteamSpy tags only when they are due (v1.2.0)

**Found:** SteamSpy is frozen (see [`sources.md`](sources.md#steamspy-catalog-page)).
Its per-game `appdetails`, which the catalog job called for every game whose
Store details it refreshed, returned the same values as the frozen page for
240 of 240 games. Each game's Store details are refreshed about every 8–9
days (1,000 games, 120 a day), so its tags were fetched again every 8–9 days
with nothing new in them.

**Changed:** tags are fetched only for refreshed games whose tags are missing
or older than `TAG_REFRESH_DAYS` (default 30; 0 restores fetching them with
every refresh). Reprocessing a run fetches tags for the same games the run
did.

**Measured:** this saves requests, not time. In the details loop each SteamSpy
request already ran inside the Store's 2.1 s interval (entry 1), so skipping
it shortens the run by at most its response time, about 0.1–0.3 s per game.
Requests per day, once every game has tags:

| | Before | After |
| --- | --- | --- |
| SteamSpy `appdetails` for tags | 120 a day | about 33 a day (120 refreshes × 9 / 30) |

The first run with the change (local stack, run 67) still fetched 120 tags,
because its 120 games had none yet. `integration/test_dimensions.py` checks
that a second refresh within `TAG_REFRESH_DAYS` makes no tag requests.

**Why it is safe:** games without tags get them on their first refresh as
before, and every game's tags are refreshed at least every
`TAG_REFRESH_DAYS`, so SteamSpy updating again would reach every game's tags
within 30 days, and the staleness check would show it.

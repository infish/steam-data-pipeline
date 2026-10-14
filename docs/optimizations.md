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

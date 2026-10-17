# 4. SteamSpy for catalog data, not for time series

Status: accepted (2026-09, extended 2026-10)

## Context

SteamSpy provides a convenient 1,000-game catalog with owner estimates,
playtimes and review counts in one request. In September 2026 its values
were found to stay unchanged for long periods while the pipeline kept
storing them as daily snapshots, and the dashboard showed them as trends
(the stale-data incident in the README). SteamSpy gives no source timestamp,
so a successful response does not prove the data is fresh. Its catalog page
also lacks genres and languages (`docs/sources.md`).

## Decision

- Keep SteamSpy as the **catalog**: which games exist, names, companies, owner
  ranges, and user tags. Its snapshots are stored for audit and broad
  comparison.
- Take every **time series** from sources with real measurement times or
  source dates: Valve's player APIs (hourly, with `source_measured_at`), Steam
  review summaries and the review histogram (dated buckets), Store prices
  (Type 2 history) and Steam news (publication time).
- Take genres, languages, release dates and platforms from the Steam Store.
- Monitor SteamSpy staleness permanently: the share of its values unchanged
  between catalog runs is a data-quality check.

## Consequences

- Charts never show SteamSpy values over time; marts that use SteamSpy
  figures (genre averages, engagement) say so and use the latest snapshot
  only.
- A SteamSpy outage still fails the catalog job, because the catalog defines
  the games every other job relies on; Valve and Store failures only degrade
  runs.

## Update 2026-10-17

SteamSpy turned out to be frozen, not slow: owner ranges, review counts,
playtimes and `ccu` did not change from one run to the next between
2026-09-01 and 2026-10-14, and only prices moved (`docs/sources.md`). The
staleness check now reports this as a warning with a "frozen since" date
instead of failing every catalog run. Tags are refetched only after
`TAG_REFRESH_DAYS`, and the dashboard labels SteamSpy figures as frozen.
[ADR 5](0005-replace-steamspy-as-catalog-source.md) proposes replacing
SteamSpy as the source of the game list.

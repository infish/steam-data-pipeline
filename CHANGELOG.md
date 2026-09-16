# Changelog

This file records user-visible changes to the Steam data pipeline.

## 2026-09-16

### Fixed

- Replaced misleading SteamSpy `ccu` trend charts with current-player
  observations from Valve's top-100 concurrent-player feed.
- Added direct Valve player lookups for configured tracked games outside the
  top 100 and retained focused Steam review measurements.
- Recorded Valve's source measurement timestamp where available, separately
  from pipeline collection time.
- Removed legacy SteamSpy snapshots from dashboard charts. They remain stored
  and queryable for audit purposes without being presented as fresh data.
- Expanded current-player coverage to catalog games present in Valve's top-100
  response; 7 Days to Die now uses Valve data rather than the frozen SteamSpy
  value.
- Disabled the duplicate user systemd timer so the Compose scheduler is the
  sole daily scheduler.

### Changed

- The dashboard's activity ranking and headline player count now use Valve
  measurements.
- Review metrics appear only for games with collected Steam review summaries.
- Added migration `V3__expand_valve_player_measurements.sql` to support player
  rank and player-only observations.
- Documented the stale-data incident, timestamp semantics, source coverage, and
  single-scheduler requirement.

## 2026-09-14

### Fixed

- Added initial Valve current-player and review measurements for Counter-Strike,
  Factorio, and Portal 2 after confirming SteamSpy's bulk and per-app values
  were stale.
- Labeled existing SteamSpy metrics and collection timestamps accurately.
- Disabled pipeline collection on scheduler startup and added deadline
  rechecking to prevent early-wakeup duplicate runs.
- Added freshness warnings for three identical consecutive Valve observations.

### Added

- Added migration `V2__steam_measurements.sql` and the
  `steam_game_measurements` table without deleting or rewriting historical
  snapshots.

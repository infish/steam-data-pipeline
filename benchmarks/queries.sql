-- history_one_game
SELECT snapshot_time, estimated_owners, total_reviews, ccu
FROM game_metric_snapshots s
JOIN benchmark_days d ON d.run_id=s.run_id
WHERE d.scenario_id=%(scenario_id)s AND s.appid = %(appid)s
ORDER BY snapshot_time DESC
LIMIT 365;

-- latest_top_25
SELECT s.appid, g.name, s.estimated_owners
FROM game_metric_snapshots s
JOIN games g ON g.appid=s.appid
WHERE s.run_id=%(latest_run_id)s
ORDER BY s.estimated_owners DESC
LIMIT 25;

-- latest_publisher_totals
SELECT g.publisher, COUNT(*) game_count, SUM(s.estimated_owners) owners
FROM game_metric_snapshots s JOIN games g ON g.appid=s.appid
WHERE s.run_id=%(latest_run_id)s
GROUP BY g.publisher ORDER BY owners DESC;

-- daily_loaded_rows
SELECT d.day_index, d.committed_row_count
FROM benchmark_days d WHERE d.scenario_id=%(scenario_id)s
ORDER BY d.day_index;

-- one_game_deltas
WITH selected AS (
  SELECT snapshot_time, estimated_owners, total_reviews
  FROM game_metric_snapshots s JOIN benchmark_days d ON d.run_id=s.run_id
  WHERE d.scenario_id=%(scenario_id)s AND s.appid=%(appid)s
  ORDER BY snapshot_time DESC LIMIT 366
), deltas AS (
  SELECT snapshot_time, estimated_owners,
    estimated_owners-LAG(estimated_owners) OVER (ORDER BY snapshot_time) owner_delta,
    total_reviews-LAG(total_reviews) OVER (ORDER BY snapshot_time) review_delta
  FROM selected
)
SELECT * FROM deltas ORDER BY snapshot_time DESC LIMIT 365;

-- exact_fact_count
SELECT COUNT(*) FROM game_metric_snapshots s
JOIN benchmark_days d ON d.run_id=s.run_id
WHERE d.scenario_id=%(scenario_id)s;

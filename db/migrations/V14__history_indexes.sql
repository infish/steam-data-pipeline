-- Indexes for queries over tables that grow forever (raw metadata is kept
-- after its payload is purged; review observations have no retention), so
-- their cost stays flat as history grows (code review, v1.3.0).

-- Freshness and schema drift: the newest successful answer per source.
ALTER TABLE raw_api_responses
    ADD INDEX idx_raw_source_status_time (source, http_status, fetched_at);

-- Payload purge: only originals whose body is still stored are candidates
-- (duplicates never hold a body; purged originals have payload_purged_at).
ALTER TABLE raw_api_responses
    ADD INDEX idx_raw_unpurged_originals
        (is_duplicate_of, payload_purged_at, fetched_at);

-- Review rotation: the recent summaries of every game.
ALTER TABLE review_observations
    ADD INDEX idx_review_observations_collected (collected_at);

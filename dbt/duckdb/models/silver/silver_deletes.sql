-- =============================================================================
-- Model       : silver_deletes
-- Description : Deletions of the four ingested collections (post deleted, unlike,
--               un-repost, unfollow), one row per event, deduplicated on seq.
--               A delete carries no record: only the key of the deleted record,
--               which is often older than the 7-day retention (decision D18).
-- Source      : bronze.bronze_events (new DuckLake snapshots only, D16)
-- Output      : lake.silver.silver_deletes, split by day(event_time)
-- =============================================================================

WITH bronze AS (
    SELECT *
    FROM {{ bronze_new_rows() }}
    WHERE operation = 'delete'
),

deduplicated AS (
    {{ deduplicate_on_seq('bronze') }}
),

typed AS (
    SELECT
        seq,
        did,
        collection,
        rkey,
        event_time,
        processed_at,
        bronze_snapshot_id
    FROM deduplicated
)

SELECT * FROM typed

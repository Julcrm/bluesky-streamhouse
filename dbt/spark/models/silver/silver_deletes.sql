-- =============================================================================
-- Model       : silver_deletes
-- Description : Deletions of the four ingested collections (post deleted, unlike,
--               un-repost, unfollow), one row per event, deduplicated on seq.
--               A delete carries no record: only the key of the deleted record,
--               which is often older than the 7-day retention (decision D18).
-- Source      : bronze.bronze_events (new Iceberg append snapshots only)
-- Output      : lakekeeper.silver.silver_deletes, partitioned by days(event_time)
-- =============================================================================

WITH bronze AS (
    SELECT *
    FROM {{ bronze_new_rows() }} AS b
    WHERE operation = 'delete'
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
    FROM bronze
),

deduplicated AS (
    {{ deduplicate_on_seq('typed') }}
)

SELECT * FROM deduplicated

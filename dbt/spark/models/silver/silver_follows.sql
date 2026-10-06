-- =============================================================================
-- Model       : silver_follows
-- Description : Follow creations (app.bsky.graph.follow), one row per event,
--               deduplicated on seq. subject_did is the followed account.
--               Unfollows live in silver_deletes (D18).
-- Source      : bronze.bronze_events (new Iceberg append snapshots only)
-- Output      : lakekeeper.silver.silver_follows, partitioned by days(event_time)
-- =============================================================================

WITH bronze AS (
    SELECT *
    FROM {{ bronze_new_rows() }} AS b
    WHERE collection = 'app.bsky.graph.follow'
      AND operation = 'create'
),


typed AS (
    SELECT
        seq,
        did,
        rkey,
        cid,
        event_time,
        try_cast(get_json_object(record, '$.createdAt') AS TIMESTAMP)  AS created_at,
        get_json_object(record, '$.subject')                            AS subject_did,
        processed_at,
        bronze_snapshot_id
    FROM bronze
),

deduplicated AS (
    {{ deduplicate_on_seq('typed') }}
)

SELECT * FROM deduplicated

-- =============================================================================
-- Model       : silver_follows
-- Description : Follow creations (app.bsky.graph.follow), one row per event,
--               deduplicated on seq. subject_did is the followed account.
--               Unfollows live in silver_deletes (D18).
-- Source      : bronze.bronze_events (new DuckLake snapshots only, D16)
-- Output      : lake.silver.silver_follows, split by day(event_time)
-- =============================================================================

WITH bronze AS (
    SELECT *
    FROM {{ bronze_new_rows() }}
    WHERE collection = 'app.bsky.graph.follow'
      AND operation = 'create'
),

deduplicated AS (
    {{ deduplicate_on_seq('bronze') }}
),

typed AS (
    SELECT
        seq,
        did,
        rkey,
        cid,
        event_time,
        TRY_CAST(record::JSON->>'$.createdAt' AS TIMESTAMPTZ)   AS created_at,
        record::JSON->>'$.subject'                              AS subject_did,
        processed_at,
        bronze_snapshot_id
    FROM deduplicated
)

SELECT * FROM typed

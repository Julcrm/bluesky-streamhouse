-- =============================================================================
-- Model       : silver_reposts
-- Description : Repost creations (app.bsky.feed.repost), one row per event,
--               deduplicated on seq. The reposted record's author and key are split
--               out of its AT URI. Un-reposts live in silver_deletes (D18).
-- Source      : bronze.bronze_events (new Iceberg append snapshots only)
-- Output      : lakekeeper.silver.silver_reposts, partitioned by days(event_time)
-- =============================================================================

{{ subject_events('app.bsky.feed.repost') }}

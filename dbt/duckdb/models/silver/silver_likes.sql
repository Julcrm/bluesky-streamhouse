-- =============================================================================
-- Model       : silver_likes
-- Description : Like creations (app.bsky.feed.like), one row per event, deduplicated
--               on seq. The liked record's author and key are split out of its AT URI
--               (at://<did>/<collection>/<rkey>). Unlikes live in silver_deletes (D18).
-- Source      : bronze.bronze_events (new DuckLake snapshots only, D16)
-- Output      : transform.silver.silver_likes, split by day(event_time)
-- =============================================================================

{{ subject_events('app.bsky.feed.like') }}

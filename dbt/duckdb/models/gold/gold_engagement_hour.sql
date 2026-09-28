-- =============================================================================
-- Model       : gold_engagement_hour
-- Description : Engagement per UTC hour: likes, reposts, replies and quote posts
--               created, likes / reposts / posts deleted, and net likes / reposts
--               (created minus deleted in the hour; the deleted like is often older
--               than the hour, so a net value can be negative).
-- Source      : silver_posts, silver_likes, silver_reposts, silver_deletes
--               (hours touched since the last run, D19)
-- Output      : lake.gold.gold_engagement_hour, split by day(hour)
-- =============================================================================

{%- set silver_models = ['silver_posts', 'silver_likes', 'silver_reposts', 'silver_deletes'] %}
{%- set last_read, current = gold_snapshot_range() %}
{%- set hours = touched_hours(silver_models, last_read, current) %}

WITH posts AS (
    SELECT
        date_trunc('hour', event_time)                          AS hour,
        count(*) FILTER (WHERE reply_parent_uri IS NOT NULL)    AS replies,
        count(*) FILTER (WHERE embed_type IN (
            'app.bsky.embed.record', 'app.bsky.embed.recordWithMedia'
        ))                                                      AS quotes
    FROM {{ silver_for_hours('silver_posts', hours, current) }}
    WHERE operation = 'create'
    GROUP BY 1
),

likes AS (
    SELECT date_trunc('hour', event_time) AS hour, count(*) AS likes
    FROM {{ silver_for_hours('silver_likes', hours, current) }}
    GROUP BY 1
),

reposts AS (
    SELECT date_trunc('hour', event_time) AS hour, count(*) AS reposts
    FROM {{ silver_for_hours('silver_reposts', hours, current) }}
    GROUP BY 1
),

deletes AS (
    SELECT
        date_trunc('hour', event_time)                                      AS hour,
        count(*) FILTER (WHERE collection = 'app.bsky.feed.like')           AS likes_deleted,
        count(*) FILTER (WHERE collection = 'app.bsky.feed.repost')         AS reposts_deleted,
        count(*) FILTER (WHERE collection = 'app.bsky.feed.post')           AS posts_deleted
    FROM {{ silver_for_hours('silver_deletes', hours, current) }}
    GROUP BY 1
),

hours AS (
    SELECT hour FROM posts
    UNION SELECT hour FROM likes
    UNION SELECT hour FROM reposts
    UNION SELECT hour FROM deletes
),

engagement AS (
    SELECT
        h.hour,
        coalesce(l.likes, 0)                                        AS likes,
        coalesce(r.reposts, 0)                                      AS reposts,
        coalesce(p.replies, 0)                                      AS replies,
        coalesce(p.quotes, 0)                                       AS quotes,
        coalesce(d.likes_deleted, 0)                                AS likes_deleted,
        coalesce(d.reposts_deleted, 0)                              AS reposts_deleted,
        coalesce(d.posts_deleted, 0)                                AS posts_deleted,
        coalesce(l.likes, 0) - coalesce(d.likes_deleted, 0)         AS net_likes,
        coalesce(r.reposts, 0) - coalesce(d.reposts_deleted, 0)     AS net_reposts,
        {{ current }}::BIGINT                                       AS silver_snapshot_id
    FROM hours AS h
    LEFT JOIN posts AS p USING (hour)
    LEFT JOIN likes AS l USING (hour)
    LEFT JOIN reposts AS r USING (hour)
    LEFT JOIN deletes AS d USING (hour)
)

SELECT * FROM engagement

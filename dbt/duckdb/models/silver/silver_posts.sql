-- =============================================================================
-- Model       : silver_posts
-- Description : Post creations and edits (app.bsky.feed.post), one row per event,
--               deduplicated on seq. Text, languages, reply thread, embed type,
--               hashtags (facet tags + record tags, lowercase) and mentions are
--               typed out of the raw record. Deletions live in silver_deletes (D18).
-- Source      : bronze.bronze_events (new DuckLake snapshots only, D16)
-- Output      : transform.silver.silver_posts, split by day(event_time)
-- =============================================================================

WITH bronze AS (
    SELECT *
    FROM {{ bronze_new_rows() }}
    WHERE collection = 'app.bsky.feed.post'
      AND operation IN ('create', 'update')
),

deduplicated AS (
    {{ deduplicate_on_seq('bronze') }}
),

parsed AS (
    SELECT
        *,
        record::JSON                                                    AS r,
        coalesce(json_extract(record::JSON, '$.facets[*].features[*]'), []) AS features
    FROM deduplicated
),

typed AS (
    SELECT
        seq,
        did,
        rkey,
        cid,
        operation,
        event_time,
        -- Declared by the client (may be backdated): informative only, event_time rules (D19)
        TRY_CAST(r->>'$.createdAt' AS TIMESTAMPTZ)                      AS created_at,
        r->>'$.text'                                                    AS text,
        from_json(r->'$.langs', '["VARCHAR"]')                          AS langs,
        r->>'$.reply.parent.uri'                                        AS reply_parent_uri,
        r->>'$.reply.root.uri'                                          AS reply_root_uri,
        r->>'$.embed."$type"'                                           AS embed_type,
        list_distinct(list_concat(
            [lower(f->>'$.tag') FOR f IN features
                IF f->>'$."$type"' = 'app.bsky.richtext.facet#tag'],
            [lower(t) FOR t IN coalesce(json_extract_string(r, '$.tags[*]'), [])]
        ))                                                              AS hashtags,
        [f->>'$.did' FOR f IN features
            IF f->>'$."$type"' = 'app.bsky.richtext.facet#mention']     AS mention_dids,
        processed_at,
        bronze_snapshot_id
    FROM parsed
)

SELECT * FROM typed

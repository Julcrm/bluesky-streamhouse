-- =============================================================================
-- Model       : silver_posts
-- Description : Post creations and edits (app.bsky.feed.post), one row per event,
--               deduplicated on seq. Text, languages, reply thread, embed type,
--               hashtags (facet tags + record tags, lowercase) and mentions are
--               typed out of the raw record. Deletions live in silver_deletes (D18).
--               Same columns and semantics as dbt/duckdb (benchmark contract).
-- Source      : bronze.bronze_events (new Iceberg append snapshots only)
-- Output      : lakekeeper.silver.silver_posts, partitioned by days(event_time)
-- =============================================================================

{%- set batch = bronze_batch() %}

WITH bronze AS (
    SELECT *
    FROM {{ batch.rows }} AS b
    WHERE collection = 'app.bsky.feed.post'
      AND operation IN ('create', 'update')
),


parsed AS (
    SELECT
        *,
        -- Facet features as string maps ($type, tag, did, uri); a facet without
        -- features must not drop the others (flatten returns NULL on a NULL element)
        flatten(transform(
            coalesce(
                from_json(
                    get_json_object(record, '$.facets'),
                    'array<struct<features:array<map<string,string>>>>'
                ),
                CAST(array() AS array<struct<features:array<map<string,string>>>>)
            ),
            f -> coalesce(f.features, CAST(array() AS array<map<string,string>>))
        ))                                                              AS features
    FROM bronze
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
        try_cast(get_json_object(record, '$.createdAt') AS TIMESTAMP)  AS created_at,
        get_json_object(record, '$.text')                               AS text,
        from_json(get_json_object(record, '$.langs'), 'array<string>')  AS langs,
        get_json_object(record, '$.reply.parent.uri')                   AS reply_parent_uri,
        get_json_object(record, '$.reply.root.uri')                     AS reply_root_uri,
        get_json_object(record, '$.embed.$type')                        AS embed_type,
        -- NULL tags are dropped, as DuckDB's list_distinct does; lowercase with Unicode
        -- simple case mapping, as DuckDB (see macros/simple_lower.sql)
        filter(array_distinct(concat(
            transform(
                filter(features, f -> f['$type'] = 'app.bsky.richtext.facet#tag'),
                f -> {{ simple_lower("f['tag']") }}
            ),
            transform(
                coalesce(
                    from_json(get_json_object(record, '$.tags'), 'array<string>'),
                    CAST(array() AS array<string>)
                ),
                t -> {{ simple_lower('t') }}
            )
        )), h -> h IS NOT NULL)                                         AS hashtags,
        transform(
            filter(features, f -> f['$type'] = 'app.bsky.richtext.facet#mention'),
            f -> f['did']
        )                                                               AS mention_dids,
        processed_at,
        bronze_snapshot_id
    FROM parsed
),

deduplicated AS (
    {{ deduplicate_on_seq('typed', batch) }}
)

SELECT * FROM deduplicated

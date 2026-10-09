-- Likes and reposts share one shape: a `subject` strong ref (uri + cid) to a record.
-- AT URI layout: at://<did>/<collection>/<rkey>, so split_part indexes 3 and 5
{% macro subject_events(collection) %}
{%- set batch = bronze_batch() %}
WITH bronze AS (
    SELECT *
    FROM {{ batch.rows }} AS b
    WHERE collection = '{{ collection }}'
      AND operation = 'create'
),


typed AS (
    SELECT
        seq,
        did,
        rkey,
        cid,
        event_time,
        try_cast(get_json_object(record, '$.createdAt') AS TIMESTAMP)      AS created_at,
        get_json_object(record, '$.subject.uri')                            AS subject_uri,
        split_part(get_json_object(record, '$.subject.uri'), '/', 3)        AS subject_did,
        split_part(get_json_object(record, '$.subject.uri'), '/', 5)        AS subject_rkey,
        processed_at,
        bronze_snapshot_id
    FROM bronze
),

deduplicated AS (
    {{ deduplicate_on_seq('typed', batch) }}
)

SELECT * FROM deduplicated
{% endmacro %}

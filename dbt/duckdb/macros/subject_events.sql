-- Likes and reposts share one shape: a `subject` strong ref (uri + cid) to a record.
-- AT URI layout: at://<did>/<collection>/<rkey>, so split_part indexes 3 and 5
{% macro subject_events(collection) %}
WITH bronze AS (
    SELECT *
    FROM {{ bronze_new_rows() }}
    WHERE collection = '{{ collection }}'
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
        record::JSON->>'$.subject.uri'                          AS subject_uri,
        split_part(record::JSON->>'$.subject.uri', '/', 3)      AS subject_did,
        split_part(record::JSON->>'$.subject.uri', '/', 5)      AS subject_rkey,
        processed_at,
        bronze_snapshot_id
    FROM deduplicated
)

SELECT * FROM typed
{% endmacro %}

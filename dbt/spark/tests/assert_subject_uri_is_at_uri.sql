-- Assert that like and repost subjects are AT URIs (at://<did>/<collection>/<rkey>)
-- Otherwise subject_did / subject_rkey, split out of the URI, would be garbage.
-- A warning, not an error: records are written by clients and the relay does not check
-- them against the lexicon (a like of an https:// Mastodon URL reached prod on
-- 2026-09-30). Data quality is reported; it must not stop the pipeline.
-- Rows processed in the last var('test_window') (D25). Same test as dbt/duckdb.

{{ config(severity='warn', meta={'dagster': {'ref': {'name': 'silver_likes'}}}) }}

SELECT 'silver_likes' AS model, seq, subject_uri
FROM {{ ref('silver_likes') }}
WHERE processed_at >= now() - {{ var('test_window') }}
  AND NOT subject_uri RLIKE '^at://did:[a-z]+:[^/]+/[^/]+/[^/]+$'

UNION ALL

SELECT 'silver_reposts' AS model, seq, subject_uri
FROM {{ ref('silver_reposts') }}
WHERE processed_at >= now() - {{ var('test_window') }}
  AND NOT subject_uri RLIKE '^at://did:[a-z]+:[^/]+/[^/]+/[^/]+$'

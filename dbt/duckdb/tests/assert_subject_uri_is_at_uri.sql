-- Assert that like and repost subjects are AT URIs (at://<did>/<collection>/<rkey>)
-- Otherwise subject_did / subject_rkey, split out of the URI, would be garbage.
-- A warning, not an error: records are written by clients and the relay does not check
-- them against the lexicon (a like of an https:// Mastodon URL reached prod on
-- 2026-09-30). Data quality is reported; it must not stop the pipeline

{{ config(severity='warn') }}

SELECT 'silver_likes' AS model, seq, subject_uri
FROM {{ ref('silver_likes') }}
WHERE event_time >= now() - {{ var('recent_rows_window') }}
  AND NOT regexp_matches(subject_uri, '^at://did:[a-z]+:[^/]+/[^/]+/[^/]+$')

UNION ALL

SELECT 'silver_reposts' AS model, seq, subject_uri
FROM {{ ref('silver_reposts') }}
WHERE event_time >= now() - {{ var('recent_rows_window') }}
  AND NOT regexp_matches(subject_uri, '^at://did:[a-z]+:[^/]+/[^/]+/[^/]+$')

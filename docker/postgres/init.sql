-- =============================================================================
-- Postgres init — runs once on an empty volume.
-- The DuckLake catalog database is created from POSTGRES_DB;
-- this script adds the Dagster run/event storage database, the Iceberg catalog
-- database of branch A (Lakekeeper) and the neutral benchmark database
-- (branch_calendar, decision D28).
-- =============================================================================

CREATE DATABASE dagster;
CREATE DATABASE iceberg_catalog;
CREATE DATABASE bluesky_benchmark;

-- =============================================================================
-- Postgres init — runs once on an empty volume.
-- The DuckLake catalog database is created from POSTGRES_DB;
-- this script adds the Dagster run/event storage database and the Iceberg catalog
-- database of branch A (Lakekeeper).
-- =============================================================================

CREATE DATABASE dagster;
CREATE DATABASE iceberg_catalog;

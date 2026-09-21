-- Runs once, on first ClickHouse container start (empty data volume).
-- Only creates the database. Per-pipeline tables are created explicitly from a
-- declared schema at pipeline-creation time (Phase 2) — never inferred here.
CREATE DATABASE IF NOT EXISTS data_platform;

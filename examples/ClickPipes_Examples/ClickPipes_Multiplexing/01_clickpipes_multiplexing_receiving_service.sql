-- =============================================================================
-- ClickPipes Multiplexing (fan-in / consolidation)  ·  RECEIVER  ·  step 01
-- -----------------------------------------------------------------------------
-- RUN THIS ON: the single CONSOLIDATED ClickHouse Cloud service.
-- RUN ORDER  : run this file FIRST, then run 02_..._sender_service.sql on every
--              per-tenant (sender) service.
--
-- WHAT THIS SERVICE IS:
--   Many isolated per-tenant services each ingest raw data (with PII) via their
--   own ClickPipes. Each tenant service ANONYMIZES its data locally (drops PII)
--   and pushes only the anonymized copy here, over `remoteSecure`. As a result
--   this consolidated service *never* receives PII -- the isolation boundary is
--   enforced both by what the sender projects AND by the narrow grants below.
--
--   The consolidated table is a ReplacingMergeTree keyed on the original row id
--   plus a tenant_id, so rows from every tenant coexist and de-duplicate.
--
-- PLACEHOLDERS TO FILL: <STRONG_PASSWORD>
-- =============================================================================

-- 1) USERS & ROLES.
--    The user whose credentials each sender embeds in its `remoteSecure` call.
--    Create the identity and its (still empty) role up front, before any object
--    exists, so permissions are always in place before they are needed.
CREATE ROLE IF NOT EXISTS remote_ingest_role;
CREATE USER IF NOT EXISTS remote_writer
    IDENTIFIED BY '<STRONG_PASSWORD>';
GRANT remote_ingest_role TO remote_writer;

-- 2) DATABASE (single DB, receives every tenant's anonymized data).
CREATE DATABASE IF NOT EXISTS consolidated;

-- 3) PERMISSIONS -- deliberately scoped to ONLY the consolidated database.
--    This is the second half of the isolation guarantee: even though senders
--    authenticate as `remote_writer`, that identity can do nothing here except
--    INSERT anonymized rows into `consolidated`.
GRANT INSERT ON consolidated.* TO remote_ingest_role;

-- 4) TARGET TABLE.
--    - PII columns (email / full_name / phone) are ABSENT by construction: they
--      never leave the sender, so they cannot be stored here.
--    - Primary key = original id (customer_id) + tenant_id, so identical ids
--      from different tenants do not collide.
--    - ReplacingMergeTree(updated_at) de-duplicates on the sort key, keeping the
--      row with the greatest updated_at. On ClickHouse Cloud this is transparently
--      backed by the Shared (SharedReplacingMergeTree) engine.
CREATE TABLE IF NOT EXISTS consolidated.customers
(
    tenant_id   LowCardinality(String),          -- identifies the source tenant
    customer_id UInt64,                           -- original id from the tenant table
    country     LowCardinality(String),
    status      LowCardinality(String),
    created_at  DateTime,
    updated_at  DateTime                          -- version column for de-duplication
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, customer_id);                -- original id + tenant id as the key

-- 5) NETWORK ACCESS (do this in the Cloud console, not SQL):
--    Add the egress IP(s) of every sender service to THIS service's IP Access
--    List so the inbound remoteSecure connections are allowed.

-- -----------------------------------------------------------------------------
-- Verify (optional):
--   SELECT * FROM consolidated.customers FINAL ORDER BY tenant_id, customer_id;
--   SHOW GRANTS FOR remote_writer;   -- should show ONLY INSERT on consolidated.*
-- -----------------------------------------------------------------------------

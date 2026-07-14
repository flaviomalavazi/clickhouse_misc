-- =============================================================================
-- ClickPipes Demultiplexing (fan-out / separation)  ·  RECEIVER  ·  step 01
-- -----------------------------------------------------------------------------
-- RUN THIS ON: EACH per-tenant ClickHouse Cloud service (one run per tenant).
-- RUN ORDER  : run this file FIRST on every tenant service, then run
--              02_..._sender_service.sql on the single multiplexed service.
--
-- WHAT THIS SERVICE IS:
--   A tenant's own isolated service. The customer ingested ALL tenants into one
--   shared "multiplexed" service (cheaper / simpler ingestion) and now separates
--   each tenant out to its own service for isolation. The sender pushes only
--   this tenant's rows here, over `remoteSecure`.
--
--   Because the service is single-tenant, the tenant_id column is redundant and
--   is DROPPED by the sender before writing. This target table therefore carries
--   the original primary key MINUS the tenant_id.
--
--   The target database is named uniformly (`app`) on EVERY tenant service, so
--   the sender's per-tenant configuration differs only by host/credentials.
--
-- PLACEHOLDERS TO FILL: <STRONG_PASSWORD>
-- =============================================================================

-- 1) USERS & ROLES.
--    The user whose credentials the sender embeds in its `remoteSecure` call for
--    THIS tenant. Use a distinct, strong password per tenant service.
CREATE ROLE IF NOT EXISTS remote_ingest_role;
CREATE USER IF NOT EXISTS remote_writer
    IDENTIFIED BY '<STRONG_PASSWORD>';
GRANT remote_ingest_role TO remote_writer;

-- 2) DATABASE -- uniformly named target DB (identical name on every tenant service).
CREATE DATABASE IF NOT EXISTS app;

-- 3) PERMISSIONS -- scoped to ONLY this tenant's `app` database. Even though the
--    sender authenticates as `remote_writer`, that identity can do nothing here
--    beyond INSERT into `app`.
GRANT INSERT ON app.* TO remote_ingest_role;

-- 4) TARGET TABLE -- original schema WITHOUT the tenant_id column/key.
--    ReplacingMergeTree(updated_at) de-duplicates on the sort key (handy for
--    re-runs / backfills). On ClickHouse Cloud this is transparently backed by
--    the Shared (SharedReplacingMergeTree) engine.
CREATE TABLE IF NOT EXISTS app.orders
(
    order_id    UInt64,
    customer_id UInt64,
    amount      Decimal(18, 2),
    created_at  DateTime,
    updated_at  DateTime                          -- version column for de-duplication
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (customer_id, order_id);                 -- original PK, tenant_id removed

-- 5) NETWORK ACCESS (do this in the Cloud console, not SQL):
--    Add the egress IP(s) of the multiplexed (sender) service to THIS service's
--    IP Access List so the inbound remoteSecure connection is allowed.

-- -----------------------------------------------------------------------------
-- Verify (optional):
--   SELECT * FROM app.orders FINAL ORDER BY customer_id, order_id;
--   SHOW GRANTS FOR remote_writer;   -- should show ONLY INSERT on app.*
-- -----------------------------------------------------------------------------

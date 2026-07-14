-- =============================================================================
-- ClickPipes Multiplexing (fan-in / consolidation)  ·  SENDER  ·  step 02
-- -----------------------------------------------------------------------------
-- RUN THIS ON: EACH per-tenant ClickHouse Cloud service (one run per tenant,
--              changing <TENANT_ID> and the connection details each time).
-- RUN ORDER  : run 01_..._receiving_service.sql on the consolidated service
--              FIRST, then this file on every tenant service.
--
-- WHAT THIS SERVICE IS:
--   An isolated, single-tenant service. Its ClickPipe lands the tenant's RAW
--   data (including PII) into `app.customers`. This file:
--     1. builds a proxy table backed by remoteSecure() that points at the
--        consolidated service's table, and
--     2. attaches an incremental materialized view that -- on every ClickPipes
--        insert -- projects ONLY the anonymized columns (PII dropped) and writes
--        them through the proxy.
--   PII therefore never crosses the wire. No external orchestrator is needed:
--   the MV fires continuously as ClickPipes inserts arrive.
--
--   PATTERN: a materialized view is an INSERT trigger; it cannot target a remote
--   table directly, but it CAN re-route its output through a proxy table built
--   from remoteSecure():
--     CREATE TABLE outbox AS remoteSecure(...);            -- inserts forward remotely
--     CREATE MATERIALIZED VIEW mv TO outbox AS SELECT ...; -- insert trigger
--
--   ORDER MATTERS: permissions are granted BEFORE the MV is created, so the
--   identity running ClickPipes inserts can always materialize the view. Creating
--   the MV first could break ClickPipes inserts upstream.
--
--   ⚠ DEPENDENCY WARNING: this service's ingestion now depends on the REMOTE table.
--   The MV writes into the proxy on every ClickPipes insert, and the proxy forwards
--   that write to consolidated.customers on the receiver. If the remote table (or
--   the receiver service) is dropped/unreachable, the forwarded INSERT fails, which
--   fails the MV, which fails the ClickPipes insert into app.customers. ClickPipes
--   will report data as pulled from the source, but the push into ClickHouse will
--   NOT complete (rows do not land). Before dropping consolidated.customers, pause
--   the ClickPipe or drop this MV first.
--
-- PLACEHOLDERS TO FILL:
--   <RECEIVER_HOST>       native secure hostname of the consolidated service
--   <STRONG_PASSWORD>     password of `remote_writer` created on the receiver
--   <TENANT_ID>           this tenant's identifier, e.g. 'tenant_a'
--   <CLICKPIPES_USER>     the user/role your ClickPipe inserts as
-- =============================================================================

-- 0) REFERENCE ONLY -- the ClickPipe already creates/populates this landing table.
--    Shown so the column names used below are unambiguous. Do NOT re-create it
--    if the ClickPipe owns it.
--
--    CREATE TABLE app.customers
--    (
--        customer_id UInt64,
--        email       String,          -- PII  (never projected below)
--        full_name   String,          -- PII  (never projected below)
--        phone       String,          -- PII  (never projected below)
--        country     LowCardinality(String),
--        status      LowCardinality(String),
--        created_at  DateTime,
--        updated_at  DateTime
--    )
--    ENGINE = ReplacingMergeTree(updated_at)
--    ORDER BY customer_id;

-- 1) USERS & ROLES.
--    The push role that authorizes the remote write. Attach it to whoever your
--    ClickPipe inserts as (the MV materializes as part of that insert).
CREATE ROLE IF NOT EXISTS remote_push_role;
-- GRANT remote_push_role TO <CLICKPIPES_USER>;

-- 2) CONNECTION.
--    Connection to the consolidated (receiver) service, stored as a named
--    collection so the password never appears inline in queries or query_log.
--    Created before the grants and the proxy table that both reference it.
CREATE NAMED COLLECTION IF NOT EXISTS consolidated_conn AS
    host     = '<RECEIVER_HOST>',
    port     = 9440,                 -- ClickHouse Cloud native secure port
    user     = 'remote_writer',
    password = '<STRONG_PASSWORD>';

-- 3) PERMISSIONS -- granted up front, before the proxy table and MV exist.
--    The inserting identity needs: REMOTE (to use remoteSecure), access to the
--    named collection, and read/write on the app database.
GRANT REMOTE ON *.*                         TO remote_push_role;
GRANT NAMED COLLECTION ON consolidated_conn TO remote_push_role;
GRANT SELECT, INSERT ON app.*               TO remote_push_role;

-- 4) ROUTE TABLE (proxy): a local handle whose storage IS the remote consolidated
--    table. Any INSERT into it is forwarded to consolidated.customers over TLS.
--    Its structure is inferred from the remote table via remoteSecure().
CREATE TABLE IF NOT EXISTS app.customers_consolidated_outbox
    AS remoteSecure(consolidated_conn, database = 'consolidated', table = 'customers');

-- 5) MATERIALIZED VIEW -- the anonymization step.
--    Fires on every insert into app.customers. It:
--      - stamps this service's constant <TENANT_ID>,
--      - projects only non-PII columns (email / full_name / phone are omitted),
--      - writes the result to the proxy, which forwards it to the receiver.
CREATE MATERIALIZED VIEW IF NOT EXISTS app.mv_anonymize_customers
    TO app.customers_consolidated_outbox
AS
SELECT
    '<TENANT_ID>' AS tenant_id,
    customer_id,
    country,
    status,
    created_at,
    updated_at
FROM app.customers;                  -- email / full_name / phone deliberately NOT selected

-- 6) BACKFILL (optional, run ONCE) -- MVs only fire on NEW inserts, so rows that
--    existed in app.customers before step 5 are not sent automatically. This
--    mirrors the MV's SELECT exactly; the receiver's ReplacingMergeTree de-dupes
--    any overlap. Uncomment to run.
--
-- INSERT INTO app.customers_consolidated_outbox
-- SELECT '<TENANT_ID>', customer_id, country, status, created_at, updated_at
-- FROM app.customers;

-- -----------------------------------------------------------------------------
-- Verify (optional):
--   Insert a test row into app.customers, then on the CONSOLIDATED service:
--   SELECT * FROM consolidated.customers FINAL WHERE tenant_id = '<TENANT_ID>';
-- -----------------------------------------------------------------------------

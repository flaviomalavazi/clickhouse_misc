-- =============================================================================
-- ClickPipes Demultiplexing (fan-out / separation)  ·  SENDER  ·  step 02
-- -----------------------------------------------------------------------------
-- RUN THIS ON: the single MULTIPLEXED ClickHouse Cloud service (holds every
--              tenant's data). Repeat the per-tenant lines for each destination.
-- RUN ORDER  : run 01_..._receiving_service.sql on EVERY tenant service FIRST,
--              then this file on the multiplexed service.
--
-- WHAT THIS SERVICE IS:
--   One service ingested all tenants via a shared ClickPipe (cheaper / simpler).
--   This file routes each tenant's slice out to that tenant's own isolated
--   service. For each destination we build a proxy table backed by remoteSecure()
--   and an incremental MV that -- on every insert -- filters to one tenant and
--   DROPS the tenant_id column before forwarding. No external orchestrator: the
--   MVs fire continuously as ClickPipes inserts arrive.
--
--   PATTERN: a materialized view is an INSERT trigger; it cannot target a remote
--   table directly, but it CAN re-route its output through a proxy table built
--   from remoteSecure():
--     CREATE TABLE outbox AS remoteSecure(...);            -- inserts forward remotely
--     CREATE MATERIALIZED VIEW mv TO outbox AS SELECT ...; -- insert trigger
--
--   ORDER MATTERS: permissions are granted BEFORE the MVs are created, so the
--   identity running ClickPipes inserts can always materialize the views.
--   Creating the MVs first could break ClickPipes inserts upstream.
--
--   ⚠ DEPENDENCY WARNING: this service's ingestion now depends on the REMOTE tables.
--   Each MV writes into its proxy on every ClickPipes insert, and the proxy forwards
--   that write to app.orders on the corresponding tenant service. If a remote table
--   (or a tenant service) is dropped/unreachable, the forwarded INSERT fails, which
--   fails that MV, which fails the ClickPipes insert into the source table. ClickPipes
--   will report data as pulled from the source, but the push into ClickHouse will NOT
--   complete (rows do not land). Before dropping a tenant's app.orders, pause the
--   ClickPipe or drop the routing MV that targets it first.
--
-- SOURCE LAYOUT -- two variants are supported (pick whichever matches you):
--   (a) tenants share one table with a tenant_id column
--         -> the MV's WHERE tenant_id = '<...>' is the tenant boundary; all
--            per-tenant MVs read from the SAME source table.
--   (b) tenants were ingested into DIFFERENT databases in this service
--         (raw_tenant_a.orders, raw_tenant_b.orders, ...)
--         -> each per-tenant MV reads from that tenant's own source DB. The
--            WHERE filter is then belt-and-suspenders (kept for safety).
--   The example below uses variant (b): one source DB per tenant.
--
-- PLACEHOLDERS TO FILL (per tenant): <TENANT_x_HOST>, <TENANT_x_PASSWORD>,
--   the source db/table, the WHERE value, and <CLICKPIPES_USER>.
-- =============================================================================

-- 0) REFERENCE ONLY -- source table shape (ClickPipe owns it). Carries tenant_id;
--    every target drops it. Shown for variant (b): raw_tenant_a.orders. A
--    variant-(a) shared table would be e.g. multiplexed.orders with tenant_id.
--
--    CREATE TABLE raw_tenant_a.orders
--    (
--        tenant_id   LowCardinality(String),   -- filtered on, then dropped
--        order_id    UInt64,
--        customer_id UInt64,
--        amount      Decimal(18, 2),
--        created_at  DateTime,
--        updated_at  DateTime
--    )
--    ENGINE = ReplacingMergeTree(updated_at)
--    ORDER BY (tenant_id, customer_id, order_id);

-- 1) USERS & ROLES.
--    A single push role authorizes every remote write. Attach it to whoever your
--    ClickPipe inserts as (the MVs materialize as part of that insert).
CREATE ROLE IF NOT EXISTS remote_push_role;
-- GRANT remote_push_role TO <CLICKPIPES_USER>;

-- 2) CONNECTIONS -- one named collection per destination tenant, so passwords
--    never appear inline. Created before the grants and proxy tables that use them.
CREATE NAMED COLLECTION IF NOT EXISTS tenant_a_conn AS
    host = '<TENANT_A_HOST>', port = 9440,        -- Cloud native secure port
    user = 'remote_writer', password = '<TENANT_A_PASSWORD>';

CREATE NAMED COLLECTION IF NOT EXISTS tenant_b_conn AS
    host = '<TENANT_B_HOST>', port = 9440,
    user = 'remote_writer', password = '<TENANT_B_PASSWORD>';

-- 3) PERMISSIONS -- granted up front, before any proxy table or MV exists.
--    REMOTE (once) + access to each named collection + read on each source DB.
GRANT REMOTE ON *.* TO remote_push_role;
GRANT NAMED COLLECTION ON tenant_a_conn TO remote_push_role;
GRANT NAMED COLLECTION ON tenant_b_conn TO remote_push_role;
GRANT SELECT ON raw_tenant_a.* TO remote_push_role;
GRANT SELECT ON raw_tenant_b.* TO remote_push_role;

-- 4) ROUTE TABLES (proxies) -- each local handle's storage IS a tenant's remote
--    `app.orders`. Any INSERT is forwarded to that tenant's service over TLS.
CREATE TABLE IF NOT EXISTS raw_tenant_a.orders_outbox
    AS remoteSecure(tenant_a_conn, database = 'app', table = 'orders');

CREATE TABLE IF NOT EXISTS raw_tenant_b.orders_outbox
    AS remoteSecure(tenant_b_conn, database = 'app', table = 'orders');

-- 5) MATERIALIZED VIEWS -- one per tenant. Each filters to its tenant, DROPS the
--    tenant_id column, and forwards the row through the proxy.
CREATE MATERIALIZED VIEW IF NOT EXISTS raw_tenant_a.mv_route_orders
    TO raw_tenant_a.orders_outbox
AS
SELECT order_id, customer_id, amount, created_at, updated_at
FROM raw_tenant_a.orders             -- variant (b): tenant A's own source DB
WHERE tenant_id = 'A';               -- tenant boundary; tenant_id NOT projected

CREATE MATERIALIZED VIEW IF NOT EXISTS raw_tenant_b.mv_route_orders
    TO raw_tenant_b.orders_outbox
AS
SELECT order_id, customer_id, amount, created_at, updated_at
FROM raw_tenant_b.orders
WHERE tenant_id = 'B';

-- (Repeat the per-tenant lines in steps 2-5 for each additional tenant.)

-- 6) BACKFILL (optional, run ONCE per tenant) -- MVs only fire on NEW inserts, so
--    pre-existing rows must be sent explicitly. Mirrors each MV's SELECT; the
--    receiver's ReplacingMergeTree de-dupes any overlap. Uncomment to run.
--
-- INSERT INTO raw_tenant_a.orders_outbox
-- SELECT order_id, customer_id, amount, created_at, updated_at
-- FROM raw_tenant_a.orders WHERE tenant_id = 'A';
--
-- INSERT INTO raw_tenant_b.orders_outbox
-- SELECT order_id, customer_id, amount, created_at, updated_at
-- FROM raw_tenant_b.orders WHERE tenant_id = 'B';

-- -----------------------------------------------------------------------------
-- Verify (optional):
--   Insert a test row for tenant A into raw_tenant_a.orders, then on tenant A's
--   service:  SELECT * FROM app.orders FINAL;   -- row present, no tenant_id col
-- -----------------------------------------------------------------------------

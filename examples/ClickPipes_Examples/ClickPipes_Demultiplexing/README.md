# ClickPipes Demultiplexing (fan-out / separation)

Take a **single multiplexed ClickHouse Cloud service** that ingested many tenants together,
and **route each tenant's slice out to that tenant's own isolated service** — dropping the
`tenant_id` column on the way out, since each destination is single-tenant.

## Why you'd do this

- **Save money on ingestion** — run one shared ClickPipe (or a few) into one service
  instead of one pipe per tenant. Ingestion is where the cost is; consolidating it is
  cheaper and simpler to operate.
- **Then isolate for compliance** — once landed, separate each tenant into its own service
  so tenants never share storage or a query surface.

## How it works

The cross-service hop uses the `remoteSecure` table function, driven by **incremental
materialized views** — no external orchestrator. An MV is an insert trigger that can't
target a remote table directly, but it *can* re-route its output through a proxy table
built from `remoteSecure()`:

```sql
CREATE TABLE orders_outbox AS remoteSecure(tenant_a_conn, database='app', table='orders');
CREATE MATERIALIZED VIEW mv TO orders_outbox AS
  SELECT /* tenant_id dropped */ … FROM raw_tenant_a.orders WHERE tenant_id = 'A';
```

There is **one such block per destination tenant**. Every insert on the multiplexed service
fires the tenant's MV, which filters to that tenant, drops the `tenant_id` column, and
forwards the row to the tenant's own service — into a **uniformly named** `app` database, so
each tenant's config differs only by host and credentials.

Source data may live in **one shared table with a `tenant_id` column** *or* in **separate
per-tenant databases** on the multiplexed service — both layouts are supported (the example
uses per-tenant source databases).

```text
                 ┌─────────────────────────────────────────────┐
                 │        Multiplexed service (all tenants)      │
                 │                                               │
                 │  ClickPipe(s) → raw_tenant_a.orders           │
                 │                 raw_tenant_b.orders   …       │
                 │            (rows carry tenant_id)             │
                 │                                               │
                 │   per-tenant incremental MVs                  │
                 │   WHERE tenant_id = 'A' | 'B' | …             │
                 │   (DROP tenant_id column)                     │
                 │        │              │                       │
                 │        ▼              ▼                       │
                 │  proxy(A)          proxy(B)                   │
                 │  AS remoteSecure   AS remoteSecure            │
                 └────────┬──────────────┬──────────────────────┘
       remoteSecure :9440 │              │ remoteSecure :9440
              (TLS)       │              │       (TLS)
                          ▼              ▼
              ┌───────────────────┐  ┌───────────────────┐
              │ Tenant A service  │  │ Tenant B service  │
              │  app.orders       │  │  app.orders       │
              │  (no tenant_id)   │  │  (no tenant_id)   │
              └───────────────────┘  └───────────────────┘
```

Isolation is enforced by grants: each tenant service's `remote_writer` can only `INSERT`
into its own `app` database.

## Files & run order

Run in numeric order: `01_` (receiver) first on every tenant service, then `02_` (sender)
on the multiplexed service — each sender proxy needs its tenant's `app.orders` to exist.

1. [`01_clickpipes_demultiplexing_receiving_service.sql`](01_clickpipes_demultiplexing_receiving_service.sql)
   — runs on **each tenant** service (once per tenant). Creates the ingest role and
   `remote_writer` user, the uniform `app` database, the scoped grant, and the `app.orders`
   table (original PK minus `tenant_id`).
2. [`02_clickpipes_demultiplexing_sender_service.sql`](02_clickpipes_demultiplexing_sender_service.sql)
   — runs on the **multiplexed** service. Creates the push role, one named collection +
   `remoteSecure` proxy + routing MV per tenant, and the grants.

## Before you run

Fill the per-tenant placeholders: `<TENANT_x_HOST>`, `<TENANT_x_PASSWORD>`, the source
db/table, the `WHERE tenant_id = …` value, and `<CLICKPIPES_USER>`. Add one full per-tenant
block to the sender file for each destination. In the Cloud console, add the multiplexed
service's egress IP to each tenant service's **IP Access List**.

Materialized views only fire on **new** inserts — use the commented one-time backfill at the
bottom of the sender file to move pre-existing rows. `ReplacingMergeTree` on the receiver
de-duplicates any overlap.

> ⚠️ **The multiplexed service's ingestion now depends on the remote tables.** Each MV
> writes to its `remoteSecure` proxy on every ClickPipes insert, which forwards to
> `app.orders` on the corresponding tenant service. If a remote table (or a tenant service)
> is **dropped or unreachable**, the forwarded INSERT fails → that MV fails → the ClickPipes
> insert into the source table fails. ClickPipes will show data as **pulled from the source,
> but the push into ClickHouse will not complete** (rows never land). Before dropping a
> tenant's `app.orders`, pause the ClickPipe or drop the routing MV that targets it first.

## Verify

Insert a test row for tenant A into `raw_tenant_a.orders`, then on tenant A's service:

```sql
SELECT * FROM app.orders FINAL;   -- row present, no tenant_id column
SHOW GRANTS FOR remote_writer;    -- expect ONLY: INSERT ON app.*
```

## See also

- [Multiplexing](../ClickPipes_Multiplexing/) — the inverse pattern (fan-in).
- [remoteSecure table function](https://clickhouse.com/docs/sql-reference/table-functions/remote)
- [ClickPipes Postgres CDC billing](https://clickhouse.com/docs/cloud/reference/billing/clickpipes/postgres-cdc) ·
  [Cloud billing overview](https://clickhouse.com/docs/cloud/manage/billing/overview)

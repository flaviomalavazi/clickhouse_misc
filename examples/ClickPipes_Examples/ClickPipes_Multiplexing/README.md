# ClickPipes Multiplexing (fan-in / consolidation)

Consolidate data from **many isolated per-tenant ClickHouse Cloud services** into a
**single consolidated service** — anonymizing each tenant's data *before* it leaves the
tenant service, so the consolidated service only ever holds the anonymized copy.

## Why you'd do this

- **Compliance / data isolation** — each tenant's raw data (including PII) never leaves
  that tenant's own service. The consolidated service physically never receives PII.
- **One place to analyze everything** — cross-tenant analytics, dashboards, and models run
  against a single `ReplacingMergeTree` keyed on the original id + a `tenant_id`, instead of
  fanning queries across N services.

## How it works

The cross-service hop uses the `remoteSecure` table function, driven by an **incremental
materialized view** — no external orchestrator. An MV is an insert trigger that can't
target a remote table directly, but it *can* re-route its output through a proxy table
built from `remoteSecure()`:

```sql
CREATE TABLE outbox AS remoteSecure(consolidated_conn, database='consolidated', table='customers');
CREATE MATERIALIZED VIEW mv TO outbox AS SELECT /* PII dropped */ … FROM app.customers;
```

Every ClickPipes insert into the tenant's landing table fires the MV, which projects only
the non-PII columns (dropping `email`, `full_name`, `phone`), stamps the tenant's
`tenant_id`, and writes the row to the proxy — which forwards it over TLS to the
consolidated service.

```text
   Tenant A service (isolated)              Tenant B service (isolated)
  ┌───────────────────────────┐           ┌───────────────────────────┐
  │ ClickPipe → app.customers │           │ ClickPipe → app.customers │
  │        (raw + PII)        │           │        (raw + PII)        │
  │            │              │           │            │              │
  │            ▼              │           │            ▼              │
  │   incremental MV          │           │   incremental MV          │
  │   (DROP PII, add          │           │   (DROP PII, add          │
  │    tenant_id='A')         │           │    tenant_id='B')         │
  │            │              │           │            │              │
  │            ▼              │           │            ▼              │
  │  proxy table              │           │  proxy table              │
  │  AS remoteSecure(...)     │           │  AS remoteSecure(...)     │
  └────────────┬──────────────┘           └────────────┬──────────────┘
               │  anonymized rows only                 │  anonymized rows only
               │        (remoteSecure, :9440, TLS)     │
               └───────────────────┬───────────────────┘
                                   ▼
                    ┌──────────────────────────────────┐
                    │     Consolidated service          │
                    │  consolidated.customers           │
                    │  ReplacingMergeTree(updated_at)   │
                    │  ORDER BY (tenant_id, customer_id)│
                    │  ── never receives PII ──         │
                    └──────────────────────────────────┘
```

Isolation is enforced twice: the sender never *projects* PII, and the receiver's ingest
user is granted only `INSERT` on the `consolidated` database.

## Files & run order

Run in numeric order: `01_` (receiver) first, then `02_` (sender) — the sender's proxy
table needs the consolidated table to already exist.

1. [`01_clickpipes_multiplexing_receiving_service.sql`](01_clickpipes_multiplexing_receiving_service.sql)
   — runs on the **consolidated** service. Creates the ingest role and `remote_writer`
   user, the `consolidated` database, the scoped grants, and the `consolidated.customers`
   table.
2. [`02_clickpipes_multiplexing_sender_service.sql`](02_clickpipes_multiplexing_sender_service.sql)
   — runs on **each tenant** service (once per tenant). Creates the push role, the named
   collection, the grants, the `remoteSecure` proxy, and the anonymizing MV.

## Before you run

Fill the placeholders: `<RECEIVER_HOST>`, `<STRONG_PASSWORD>`, `<TENANT_ID>`,
`<CLICKPIPES_USER>`. Then, in the Cloud console, add each sender's egress IP to the
consolidated service's **IP Access List**.

`ReplacingMergeTree` is used so re-runs and the optional one-time backfill (commented at
the bottom of the sender file) de-duplicate automatically on `(tenant_id, customer_id)`.

> ⚠️ **The tenant service's ingestion now depends on the remote table.** The MV writes to
> the `remoteSecure` proxy on every ClickPipes insert, which forwards to
> `consolidated.customers` on the receiver. If that remote table (or the consolidated
> service) is **dropped or unreachable**, the forwarded INSERT fails → the MV fails → the
> ClickPipes insert into `app.customers` fails. ClickPipes will show data as **pulled from
> the source, but the push into ClickHouse will not complete** (rows never land). Before
> dropping `consolidated.customers`, pause the ClickPipe or drop the MV first.

## Verify

Insert a test row into a tenant's `app.customers`, then on the consolidated service:

```sql
SELECT * FROM consolidated.customers FINAL WHERE tenant_id = '<TENANT_ID>';
SHOW GRANTS FOR remote_writer;   -- expect ONLY: INSERT ON consolidated.*
```

## See also

- [Demultiplexing](../ClickPipes_Demultiplexing/) — the inverse pattern (fan-out).
- [remoteSecure table function](https://clickhouse.com/docs/sql-reference/table-functions/remote)
- [SharedMergeTree](https://clickhouse.com/docs/cloud/reference/shared-merge-tree) ·
  [Cloud architecture](https://clickhouse.com/docs/cloud/reference/architecture)

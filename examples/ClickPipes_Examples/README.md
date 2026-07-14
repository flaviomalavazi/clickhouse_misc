# ClickPipes Examples

Cross-service data-movement patterns for **ClickHouse Cloud multi-tenant workloads**.
Both examples solve the same tension — *save money on ingestion* while *keeping tenant
data isolated for compliance* — using the same building block:

> An incremental **materialized view** feeds a **proxy table backed by the `remoteSecure`
> table function**. Every ClickPipes insert fires the MV, which transforms the row and
> writes it to the proxy, which forwards it over TLS to another service. Movement is
> continuous with **no external orchestrator**.

Each example ships DDL split by role — `01_..._receiving_service.sql` (run first) and
`02_..._sender_service.sql` — including the users and role grants for the remote actions,
plus its own README with a diagram and step-by-step notes.

## The two patterns

### 1. Multiplexing — fan-in / consolidation

Many isolated per-tenant services each ingest raw data (with PII) via their own ClickPipes.
Each tenant service **anonymizes locally** (drops PII) and pushes only the anonymized copy
into **one consolidated service**, a `ReplacingMergeTree` keyed on the original id + a
`tenant_id`. The consolidated service never receives PII.

- **Use it when:** you need one place for cross-tenant analytics but raw/PII data must stay
  inside each tenant's own service.

```text
  Tenant A ─┐
  Tenant B ─┤ anonymize locally ──▶  Consolidated service
  Tenant C ─┘  (remoteSecure)        (anonymized, keyed on tenant_id + id)
```

→ Deep dive: [`ClickPipes_Multiplexing/`](ClickPipes_Multiplexing/)

### 2. Demultiplexing — fan-out / separation

One service ingests **all tenants together** via a shared ClickPipe (cheaper, simpler),
then routes each tenant's slice out to **that tenant's own isolated service**, filtering on
and dropping the `tenant_id` column before writing.

- **Use it when:** you want the cost/ops savings of one shared ingestion pipeline but must
  end up with each tenant physically isolated in its own service.

```text
                        ┌──▶ Tenant A service
  Multiplexed service ──┼──▶ Tenant B service   (remoteSecure, tenant_id dropped)
  (all tenants)         └──▶ Tenant C service
```

→ Deep dive: [`ClickPipes_Demultiplexing/`](ClickPipes_Demultiplexing/)

## They are inverses

| | Multiplexing | Demultiplexing |
|---|---|---|
| Direction | many senders → one receiver | one sender → many receivers |
| Sender does | anonymize (drop PII), stamp `tenant_id` | filter by tenant, drop `tenant_id` |
| Primary goal | consolidate + isolate PII | cheap ingestion + isolate tenants |
| Receiver key | original id + `tenant_id` | original PK minus `tenant_id` |

## Heads-up before you use these

The sender service's ingestion becomes **dependent on the remote table**: if the remote
target is dropped or unreachable, the forwarded INSERT fails, the MV fails, and the
ClickPipes insert fails — ClickPipes shows data as *pulled from the source but never
completing the push* into ClickHouse. Each child README documents the safe-teardown order.
These create real Cloud resources and can incur cost.

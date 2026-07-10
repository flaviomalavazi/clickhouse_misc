# ClickPipes Operator

A small Python project that uses the **ClickHouse Cloud OpenAPI** to create and
scale **Postgres CDC ClickPipes** — with a focus on making the *initial load*
(the first full snapshot of your Postgres tables into ClickHouse) as fast as
possible.

If you've never touched ClickPipes before, that's fine: this README walks you
through everything from zero to a running pipe.

---

## Table of contents

1. [What this project does](#what-this-project-does)
2. [Background: the concepts you need](#background-the-concepts-you-need)
3. [Repository layout](#repository-layout)
4. [Prerequisites](#prerequisites)
5. [Setup](#setup)
6. [Configuration (`.env`)](#configuration-env)
7. [Postgres source requirements](#postgres-source-requirements)
8. [Running it](#running-it)
9. [What happens step by step](#what-happens-step-by-step)
10. [Using the library directly](#using-the-library-directly)
11. [Troubleshooting](#troubleshooting)
12. [Cost & safety notes](#cost--safety-notes)

---

## What this project does

A [ClickPipe](https://clickhouse.com/docs/integrations/clickpipes) is ClickHouse
Cloud's managed ingestion service. A **Postgres CDC ClickPipe** continuously
replicates data from a PostgreSQL database into ClickHouse: it first takes a
one-time **snapshot** (the "initial load") of the selected tables, then streams
ongoing changes via **CDC** (Change Data Capture) using Postgres logical
replication.

The initial load is often the slow part. Two levers make it faster, and this
project pulls both **at pipe-creation time**, which is the only moment one of
them can be set:

- **`initialLoadParallelism`** — how many table partitions are snapshotted in
  parallel (platform default `4`; this project raises it, e.g. to `16`).
  ⚠️ This is **immutable after the pipe is created** — you cannot change it
  later, which is exactly why the project bakes it in during creation.
- **CDC compute (vCPU / RAM)** — the compute backing the pipe. This project
  scales it up (e.g. to 24 vCPU / 96 GiB) so there's enough horsepower to
  actually use the higher parallelism, and can scale it back down once the
  snapshot finishes to avoid paying for idle compute.

The `main.py` script reads all of its configuration from a `.env` file and runs
the full flow: connect → create the pipe with high parallelism → scale compute
up → (optionally) wait for the snapshot to finish → scale compute back down.

---

## Background: the concepts you need

| Term | Meaning |
| --- | --- |
| **ClickHouse Cloud** | Managed ClickHouse. Everything here targets a Cloud *service*. |
| **OpenAPI key** | A Cloud API key (key ID + secret) used for HTTP Basic auth. Must have the **Admin** role. |
| **Organization / Service ID** | Identifiers for your Cloud org and the specific service the pipe belongs to. |
| **ClickPipe** | A managed ingestion pipeline. Here: Postgres → ClickHouse. |
| **Initial load / snapshot** | The one-time full copy of existing Postgres rows into ClickHouse. |
| **CDC** | Ongoing replication of inserts/updates/deletes after the snapshot. |
| **Publication** | A Postgres object listing which tables are published for logical replication. ClickPipes either creates it or reuses one you made. |
| **Pipe lifecycle states** | `Provisioning` → `Setup` → `Snapshot` → `Running`. `Failed` means it errored. |

---

## Repository layout

```
ClickPipes_Operator/
├── clickpipes_operator.py   # Library: the ClickPipeOperator class + error types (no side effects)
├── main.py                  # Runnable, step-by-step workflow driven by .env
├── .env                     # Your credentials & settings (gitignored — never commit real values)
├── pyproject.toml           # uv project definition + dependencies
├── uv.lock                  # Locked dependency versions
├── .gitignore               # Ignores .venv/ and .env
└── README.md                # This file
```

- **`clickpipes_operator.py`** only *defines* things — importing it does nothing
  on its own. It contains `ClickPipeOperator` (the client) and a typed
  exception hierarchy (`ClickPipesAuthError`, `ClickPipesNotFoundError`,
  `ClickPipesClientError`, etc.).
- **`main.py`** is the entry point. It loads `.env`, constructs the operator,
  and calls the high-level `create_pipe_with_scaled_initial_load(...)` method.

---

## Prerequisites

1. **`uv`** — the Python package/environment manager used here.
   Install (macOS/Linux):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```
   or via Homebrew: `brew install uv`. uv provisions the right Python version
   automatically (this project targets **Python ≥ 3.14**), so you don't need a
   pre-installed interpreter.

2. **A ClickHouse Cloud service** you can administer, plus:
   - an **OpenAPI key** (key ID + secret) with the **Admin** role
     — create one under *Cloud console → API keys*
     ([docs](https://clickhouse.com/docs/cloud/manage/openapi)),
   - your **Organization ID** and **Service ID** (both visible in the console).

3. **A reachable PostgreSQL source** (self-managed, RDS, or Aurora) with
   logical replication available, plus a database user for ClickPipes. See
   [Postgres source requirements](#postgres-source-requirements).

---

## Setup

From inside the `ClickPipes_Operator/` directory:

```bash
# Install the exact locked dependencies into a local .venv
uv sync
```

That's it — `uv sync` reads `pyproject.toml` / `uv.lock` and creates a `.venv`
with `requests` and `python-dotenv`. You do **not** need to activate the venv
manually; use `uv run ...` to execute commands inside it.

> If you're starting this project from scratch (no `pyproject.toml` yet), the
> equivalent bootstrap is:
> ```bash
> uv init --bare --name clickpipes-operator
> uv add requests python-dotenv
> ```

---

## Configuration (`.env`)

All configuration lives in `.env`, read at startup by `main.py` via
`python-dotenv`. Variables already exported in your shell take precedence over
`.env` values (`override=False`).

> **Security:** `.env` holds live secrets (API key secret, DB password) and is
> gitignored. Never commit it. If secrets leak, rotate them.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `CH_ORG_ID` | ✅ | — | ClickHouse Cloud organization ID |
| `CH_KEY_ID` | ✅ | — | OpenAPI key ID |
| `CH_KEY_SECRET` | ✅ | — | OpenAPI key secret |
| `CH_SERVICE_ID` | ✅ | — | Target Cloud service ID |
| `PG_HOST` | ✅ | — | Postgres host |
| `PG_PORT` | | `5432` | Postgres port |
| `PG_DATABASE` | ✅ | — | Postgres database name |
| `PG_USERNAME` | ✅ | — | Postgres user for ClickPipes |
| `PG_PASSWORD` | ✅ | — | Password for that user |
| `CLICKPIPE_NAME` | | `postgres-cdc-pipe` | Name for the new pipe |
| `CLICKPIPE_DESTINATION_DATABASE` | | `default` | Target ClickHouse database |
| `CLICKPIPE_TABLE_MAPPINGS` | | `public.orders → public_orders` | JSON array of `{sourceSchemaName, sourceTable, targetTable}` |
| `CLICKPIPE_PUBLICATION_NAME` | | *(empty)* | Reuse an existing Postgres publication; blank = ClickPipes creates one |
| `CLICKPIPE_INITIAL_LOAD_PARALLELISM` | | `16` | Snapshot parallelism (**must be > 4**) |
| `CLICKPIPE_INITIAL_LOAD_CPU_MILLICORES` | | `24000` | CDC compute vCPU in millicores (1000 = 1 vCPU; 1–32 cores) |
| `CLICKPIPE_INITIAL_LOAD_MEMORY_GB` | | `96` | CDC compute RAM — **must equal 4 × cores** |
| `CLICKPIPE_WAIT_AND_SCALE_DOWN` | | `false` | If `true`, block until snapshot completes then scale compute down |
| `CLICKPIPE_WAIT_TIMEOUT_S` | | `3600` | Seconds to wait when the above is `true` (`none` = wait forever) |

**Notes on the tuning fields:**

- Parallelism must be **greater than 4** — otherwise
  `create_pipe_with_scaled_initial_load` refuses to run, because scaling compute
  without raising parallelism just costs money without speeding anything up.
- CDC compute is validated: cores must be a whole number in **1–32**, and
  `MEMORY_GB` must be exactly **4 × cores** (so 24 vCPU → 96 GiB).

Example `CLICKPIPE_TABLE_MAPPINGS` with multiple tables:

```env
CLICKPIPE_TABLE_MAPPINGS=[{"sourceSchemaName":"public","sourceTable":"orders","targetTable":"public_orders"},{"sourceSchemaName":"public","sourceTable":"customers","targetTable":"public_customers"}]
```

---

## Postgres source requirements

For CDC to work, the source Postgres must have **logical replication enabled**
and the ClickPipes user must have the right privileges.

- **RDS / Aurora:** set `rds.logical_replication = 1` in the cluster parameter
  group (requires a reboot), and grant the replication role.
- **Self-managed:** set `wal_level = logical` in `postgresql.conf` and restart.

Grants (run as a Postgres admin / master user):

```sql
-- read access to the replicated tables
GRANT USAGE ON SCHEMA public TO clickpipes_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO clickpipes_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO clickpipes_user;

-- replication role (RDS/Aurora)
GRANT rds_replication TO clickpipes_user;
```

**Publication — pick one:**

- **Let ClickPipes create it** (default). The user then also needs
  `GRANT CREATE ON DATABASE <db> TO clickpipes_user;` so it can run
  `CREATE PUBLICATION`. Leave `CLICKPIPE_PUBLICATION_NAME` blank.
- **Pre-create it yourself** (no CREATE-on-database privilege needed):
  ```sql
  CREATE PUBLICATION clickpipes_pub FOR TABLE public.orders;
  ```
  then set `CLICKPIPE_PUBLICATION_NAME=clickpipes_pub` in `.env`.

See the official
[Postgres source setup docs](https://clickhouse.com/docs/integrations/clickpipes/postgres)
for the authoritative, provider-specific steps.

---

## Running it

Once `.env` is filled in and `uv sync` has been run:

```bash
uv run python main.py
```

You'll see logging as it works, followed by two printed blocks: the created
pipe (JSON) and its live status.

- With `CLICKPIPE_WAIT_AND_SCALE_DOWN=false` (default), the script returns as
  soon as the pipe is created and compute is scaled up — the snapshot continues
  in the background.
- With `CLICKPIPE_WAIT_AND_SCALE_DOWN=true`, it blocks until the pipe leaves the
  `Provisioning`/`Setup`/`Snapshot` states (or `CLICKPIPE_WAIT_TIMEOUT_S`
  elapses), then scales CDC compute back down.

---

## What happens step by step

`main.py` is intentionally written as explicit, commented steps:

0. **Load `.env`** via `python-dotenv`.
1. **Connect** — build a `ClickPipeOperator` from the ClickHouse Cloud
   credentials (no `clickpipe_id`, since we're creating one).
2. **Describe the Postgres source** — host/port/database + username/password.
3. **Choose what to replicate** — table mappings + destination database, and the
   optional existing publication name.
4. **Pick tuning** — parallelism, CDC compute, and wait/scale-down behavior.
5. **Create the pipe** via `create_pipe_with_scaled_initial_load(...)`, which:
   - creates the Postgres CDC pipe with `initialLoadParallelism` baked in,
   - scales CDC compute up and waits for it to propagate,
   - if `wait_and_scale_down=True`, waits for the snapshot and scales down
     afterward (guaranteed to run even if the wait fails or times out).
6. **Report status** — print the new pipe's lifecycle state and scaling.

---

## Using the library directly

You don't have to use `main.py`. The `ClickPipeOperator` class can be imported
and driven however you like — e.g. from a notebook or your own script:

```python
from clickpipes_operator import ClickPipeOperator, HIGH_INITIAL_LOAD_PARALLELISM

operator = ClickPipeOperator(
    organization_id="...",
    key_id="...",
    key_secret="...",
    service_id="...",
)

# Inspect an existing pipe
print(operator.list_clickpipes())
print(operator.get_status(clickpipe_id="..."))

# Scale an existing pipe up for its initial load, wait, then scale down
operator.run_scaled_initial_load(clickpipe_id="...", timeout_s=None)

# Or scale CDC compute directly (1–32 cores; RAM must be 4× cores)
operator.set_cdc_scaling(cpu_millicores=8000, memory_gb=32, service_id="...")
```

Key methods worth knowing:

- `get_status(...)` — combined lifecycle state + pipe scaling + CDC scaling.
- `set_cdc_scaling(...)` / `wait_for_cdc_scaling(...)` — change compute and wait
  for it to propagate (3–5 min per ClickHouse docs).
- `wait_for_snapshot_completion(...)` — block until the snapshot finishes.
- `scale_up_for_initial_load(...)` / `scale_down_after_initial_load(...)`.
- `run_scaled_initial_load(...)` — orchestrates up → wait → down on an existing
  pipe (scale-down always runs, even on failure/timeout).
- `create_pipe_with_scaled_initial_load(...)` — the create-time path used by
  `main.py`.

---

## Troubleshooting

| Symptom | Likely cause & fix |
| --- | --- |
| `[HTTP 401]` / `[HTTP 403]` `ClickPipesAuthError` | Bad API key, or the key lacks the **Admin** role. Recreate a key with Admin. |
| `[HTTP 404]` `ClickPipesNotFoundError` | Wrong `CH_ORG_ID` / `CH_SERVICE_ID` (or pipe id). Copy them from the Cloud console. |
| `[HTTP 400] ... permission denied for database ... (SQLSTATE 42501)` | The PG user can't `CREATE PUBLICATION`. Either `GRANT CREATE ON DATABASE ... TO <user>` or pre-create a publication and set `CLICKPIPE_PUBLICATION_NAME`. |
| `[HTTP 400] ...` mentioning replication/`SELECT` | PG user missing `rds_replication` / `SELECT` on tables, or logical replication not enabled. See [Postgres source requirements](#postgres-source-requirements). |
| `ScalingValidationError: initial_load_parallelism ... must be greater than ... 4` | Set `CLICKPIPE_INITIAL_LOAD_PARALLELISM` above `4`. |
| `ScalingValidationError: replicaMemoryGb must be 4x the core count` | Make `CLICKPIPE_INITIAL_LOAD_MEMORY_GB` equal `4 × (millicores/1000)`. |
| `Missing required environment variable '...'` | Fill that variable in `.env`. |
| `SnapshotTimeoutError` | The snapshot took longer than `CLICKPIPE_WAIT_TIMEOUT_S`. Increase it or set `none`. |
| `ClickPipesConnectionError` | Network/DNS/timeout reaching the Cloud API. Check connectivity. |

Enable more detail: logging is set to `INFO` in `main.py` — change to
`logging.basicConfig(level=logging.DEBUG)` for verbose output.

---

## Cost & safety notes

- **Running `main.py` creates real infrastructure and costs money.** It scales
  CDC compute up (24 vCPU / 96 GiB by default). Use `CLICKPIPE_WAIT_AND_SCALE_DOWN=true`,
  or manually call `scale_down_after_initial_load()` afterward, so you don't pay
  for high compute longer than the snapshot needs.
- CDC scaling changes are **asynchronous** — ClickHouse notes propagation
  typically takes **3–5 minutes**; the wait helpers account for this.
- `initialLoadParallelism` **cannot be changed after creation** — decide it up
  front. To change it later you must create a new pipe.

---

## References

- [Scaling DB ClickPipes via OpenAPI](https://clickhouse.com/docs/integrations/clickpipes/postgres/scaling)
- [ClickPipes OpenAPI reference](https://clickhouse.com/docs/cloud/manage/api/swagger)
- [Parallel initial load](https://clickhouse.com/docs/integrations/clickpipes/postgres/parallel_initial_load)
- [Postgres ClickPipe lifecycle](https://clickhouse.com/docs/integrations/clickpipes/postgres/lifecycle)
- [ClickHouse Cloud OpenAPI overview](https://clickhouse.com/docs/cloud/manage/openapi)

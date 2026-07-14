# clickhouse-misc

A grab-bag of **purpose-built ClickHouse code snippets and utilities**, each
self-contained and easy to share on its own.

Think of this repo as a toolbox rather than a single application. Every entry
solves one specific ClickHouse problem — a scaling operator, an ingestion
helper, a query pattern, a migration script — and is written so you can drop it
into your own project (or hand it to a colleague) without dragging the rest of
the repo along.

---

## How this repo is organized

Each utility lives in its own directory under [`utils/`](utils/) and is
**fully self-contained**:

- it manages its **own dependencies** (e.g. its own `pyproject.toml` / `uv.lock`
  for Python projects),
- it ships its **own `README.md`** explaining what it does and how to run it,
- it keeps its **own configuration** (e.g. a `.env` / `.env.example`),

so you can copy a single folder and it will still work in isolation.

```text
clickhouse-misc/
├── README.md          # you are here — the index
├── utils/
│   └── ClickPipes_Operator/   # each subfolder is an independent, shareable snippet
└── examples/
    └── ClickPipes_Examples/             # cross-service ClickPipes patterns
        ├── ClickPipes_Multiplexing/     # fan-in: anonymize per-tenant data into one service
        └── ClickPipes_Demultiplexing/   # fan-out: split a multiplexed service per tenant
```

Standalone runnable snippets (Python, uv-based) live under [`utils/`](utils/);
copy-paste DDL walkthroughs live under [`examples/`](examples/).

---

## What's inside

| Utility | Language | What it does |
| --- | --- | --- |
| [ClickPipes Operator](utils/ClickPipes_Operator/) | Python (uv) | Create and scale **Postgres CDC ClickPipes** on ClickHouse Cloud via the OpenAPI, optimizing the initial-load snapshot (high `initialLoadParallelism` + scaled CDC compute, baked in at creation time). |

> More snippets will be added over time — see [Adding a new snippet](#adding-a-new-snippet).

### Examples (DDL walkthroughs)

Cross-service data-movement patterns for ClickHouse Cloud multi-tenant workloads, under
[`examples/ClickPipes_Examples/`](examples/ClickPipes_Examples/). Each uses an incremental
materialized view feeding a `remoteSecure`-backed proxy table, so movement is continuous
with **no external orchestrator**. Every case ships DDL split by role —
`01_..._receiving_service.sql` (run first) and `02_..._sender_service.sql` — including the
users and role grants for the remote actions. See the
[ClickPipes Examples README](examples/ClickPipes_Examples/) for the high-level overview.

| Example | What it does |
| --- | --- |
| [ClickPipes Multiplexing](examples/ClickPipes_Examples/ClickPipes_Multiplexing/) | **Fan-in.** Many isolated per-tenant services anonymize their data (PII dropped on the sender) and push it into one consolidated `ReplacingMergeTree` keyed on original id + tenant id. The consolidated service never receives PII. |
| [ClickPipes Demultiplexing](examples/ClickPipes_Examples/ClickPipes_Demultiplexing/) | **Fan-out.** One multiplexed service (cheaper ingestion) routes each tenant's slice to that tenant's own service, filtering on and dropping `tenant_id` before writing. |

---

## Using a snippet

1. Open the utility's own directory under `utils/`.
2. Read its `README.md` — each one documents its prerequisites, setup, and how
   to run it.
3. Follow that README. Nothing at the repo root needs to be installed first;
   dependencies are scoped per-utility.

For example, to use the ClickPipes Operator:

```bash
cd utils/ClickPipes_Operator
# then follow utils/ClickPipes_Operator/README.md (uv sync, fill in .env, uv run python main.py)
```

---

## Conventions

To keep everything easy to share, each utility should:

- **Be self-contained** — its own dependencies, config, and README; no reliance
  on repo-root tooling.
- **Ship a README** — a newcomer with no prior context should be able to
  reproduce and run it from that README alone.
- **Keep secrets out of git** — real credentials live in a gitignored `.env`;
  commit a `.env.example` template with placeholder values instead.
- **Pin dependencies** — for Python, prefer [`uv`](https://docs.astral.sh/uv/)
  with a committed `pyproject.toml` and `uv.lock`.

---

## Adding a new snippet

1. Create a new folder under `utils/` with a descriptive name.
2. Make it self-contained (dependencies, config template, README) per the
   [conventions](#conventions) above.
3. Add a row to the [What's inside](#whats-inside) table pointing at it.

---

## Disclaimer

These are utilities and examples, not officially supported products. Several of
them create real ClickHouse Cloud resources and can incur cost — read each
utility's README (and its cost/safety notes, where present) before running it.

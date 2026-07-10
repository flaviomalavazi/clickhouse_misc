"""
main.py

Step-by-step workflow that uses `ClickPipeOperator` (from clickpipes_operator.py)
to connect to ClickHouse Cloud and create a Postgres CDC ClickPipe, with a
raised initialLoadParallelism and scaled-up CDC compute baked in at creation
time (the only point at which parallelism can be set).

Everything the script needs is read from the `.env` file sitting next to it,
so there are no secrets in this file. Fill in `.env` first, then run:

    uv run python main.py

The steps below are deliberately spelled out one at a time so you can comment
any of them out, reorder them, or copy them into a notebook.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from clickpipes_operator import (
    ClickPipeOperator,
    HIGH_INITIAL_LOAD_PARALLELISM,
    INITIAL_LOAD_CPU_MILLICORES,
    INITIAL_LOAD_MEMORY_GB,
)


def require(name: str) -> str:
    """Fetch a required env var or exit with a clear message."""
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing required environment variable {name!r}; set it in .env.")
    return value


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    # ----------------------------------------------------------------------- #
    # Step 0: load credentials and settings from the sibling .env file.
    #
    # override=False keeps any variables already exported in the shell winning
    # over the .env values.
    # ----------------------------------------------------------------------- #
    load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

    # ----------------------------------------------------------------------- #
    # Step 1: connect to ClickHouse Cloud.
    #
    # HTTP Basic auth with a Cloud API key (Admin role) on the target service.
    # No clickpipe_id here -- we're about to create one.
    # ----------------------------------------------------------------------- #
    operator = ClickPipeOperator(
        organization_id=require("CH_ORG_ID"),
        key_id=require("CH_KEY_ID"),
        key_secret=require("CH_KEY_SECRET"),
        service_id=require("CH_SERVICE_ID"),
    )

    # ----------------------------------------------------------------------- #
    # Step 2: describe the Postgres CDC source to replicate from.
    #
    # `credentials` uses basic username/password auth. For RDS/Aurora IAM auth
    # or custom TLS, swap in the fields ClickHouse documents for the Postgres
    # source instead -- they're passed through to the API as-is.
    # ----------------------------------------------------------------------- #
    postgres_source = {
        "host": require("PG_HOST"),
        "port": int(os.environ.get("PG_PORT", "5432")),
        "database": require("PG_DATABASE"),
        "credentials": {
            "username": require("PG_USERNAME"),
            "password": require("PG_PASSWORD"),
        },
    }

    # ----------------------------------------------------------------------- #
    # Step 3: choose what to replicate and where it lands.
    #
    # CLICKPIPE_TABLE_MAPPINGS is a JSON array of
    #   {"sourceSchemaName": ..., "sourceTable": ..., "targetTable": ...}
    # objects; defaults to a single public.orders -> public_orders mapping.
    # ----------------------------------------------------------------------- #
    table_mappings = json.loads(
        os.environ.get(
            "CLICKPIPE_TABLE_MAPPINGS",
            '[{"sourceSchemaName": "public", "sourceTable": "orders", '
            '"targetTable": "public_orders"}]',
        )
    )
    destination = {"database": os.environ.get("CLICKPIPE_DESTINATION_DATABASE", "default")}

    # If CLICKPIPE_PUBLICATION_NAME is set, point the pipe at that existing
    # Postgres publication (via the source `publicationName` setting) so
    # ClickPipes doesn't try to CREATE PUBLICATION itself -- which needs
    # CREATE-on-database privileges the source user may not have. Left blank,
    # ClickPipes creates and manages the publication as usual.
    extra_postgres_settings = {}
    publication_name = os.environ.get("CLICKPIPE_PUBLICATION_NAME", "").strip()
    if publication_name:
        extra_postgres_settings["publicationName"] = publication_name

    # ----------------------------------------------------------------------- #
    # Step 4: pick initial-load tuning and wait behaviour (all from .env).
    #
    # Parallelism MUST be > 4 (the platform default); create_pipe_with_scaled_
    # initial_load refuses to just bump compute without also raising parallelism.
    # ----------------------------------------------------------------------- #
    initial_load_parallelism = int(
        os.environ.get("CLICKPIPE_INITIAL_LOAD_PARALLELISM", HIGH_INITIAL_LOAD_PARALLELISM)
    )
    cpu_millicores = int(
        os.environ.get("CLICKPIPE_INITIAL_LOAD_CPU_MILLICORES", INITIAL_LOAD_CPU_MILLICORES)
    )
    memory_gb = float(
        os.environ.get("CLICKPIPE_INITIAL_LOAD_MEMORY_GB", INITIAL_LOAD_MEMORY_GB)
    )

    # If truthy, block until the snapshot finishes then scale CDC compute back
    # down. Otherwise the call returns as soon as the pipe is created + scaled up.
    wait_and_scale_down = (
        os.environ.get("CLICKPIPE_WAIT_AND_SCALE_DOWN", "false").strip().lower()
        in ("1", "true", "yes")
    )
    wait_timeout_raw = os.environ.get("CLICKPIPE_WAIT_TIMEOUT_S", "3600").strip()
    wait_timeout_s = None if wait_timeout_raw.lower() in ("", "none") else int(wait_timeout_raw)

    # ----------------------------------------------------------------------- #
    # Step 5: create the pipe with parallelism + CDC compute baked in.
    # ----------------------------------------------------------------------- #
    created = operator.create_pipe_with_scaled_initial_load(
        name=os.environ.get("CLICKPIPE_NAME", "postgres-cdc-pipe"),
        postgres_source=postgres_source,
        table_mappings=table_mappings,
        destination=destination,
        initial_load_parallelism=initial_load_parallelism,
        cpu_millicores=cpu_millicores,
        memory_gb=memory_gb,
        extra_postgres_settings=extra_postgres_settings or None,
        wait_and_scale_down=wait_and_scale_down,
        wait_timeout_s=wait_timeout_s,
    )

    print(json.dumps(created, indent=2, default=str))

    # ----------------------------------------------------------------------- #
    # Step 6: report the new pipe's live status.
    #
    # If you set wait_and_scale_down=False above, you can run the wait/scale-down
    # yourself later against created["clickpipe_id"]:
    #     operator.wait_for_snapshot_completion(clickpipe_id=created["clickpipe_id"])
    #     operator.scale_down_after_initial_load()
    # ----------------------------------------------------------------------- #
    print(operator.get_status(clickpipe_id=created["clickpipe_id"]))


if __name__ == "__main__":
    main()

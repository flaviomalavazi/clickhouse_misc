"""
clickpipes_operator.py

Defines `ClickPipeOperator`, a thin client over the ClickHouse Cloud OpenAPI
for inspecting, scaling and creating DB ClickPipes. This module only defines
the class (and its exceptions) -- see `main.py` for a runnable, step-by-step
workflow that wires it up from the `.env` file.

Scale a ClickHouse DB ClickPipe (Postgres/MySQL/MongoDB CDC pipe) up for its
initial load, wait for the snapshot to finish, then scale it back down.

Built from:
  - Scaling DB ClickPipes via OpenAPI
    https://clickhouse.com/docs/integrations/clickpipes/postgres/scaling
  - ClickPipes OpenAPI reference (clickPipeCdcScalingUpdate & friends)
    https://clickhouse.com/docs/cloud/manage/api/swagger
  - How parallel snapshot works (initialLoadParallelism)
    https://clickhouse.com/docs/integrations/clickpipes/postgres/parallel_initial_load
  - Lifecycle of a Postgres ClickPipe (state names)
    https://clickhouse.com/docs/integrations/clickpipes/postgres/lifecycle

IMPORTANT CAVEAT ABOUT PARALLELISM
-----------------------------------
The compute scaling (CPU/RAM) done via `clickpipesCdcScaling` is a
documented, editable, org+service level setting and works at any time.

The *initial load parallelism* (4 -> 16 partitions processed in parallel)
is a different, per-pipe setting (`initialLoadParallelism`). ClickHouse's
own docs on parallel snapshot explicitly say:

    "The snapshot parameters can't be edited after pipe creation. If you
    want to change them, you will have to create a new ClickPipe."

`update_initial_load_parallelism()` below calls the documented "Update
ClickPipe settings" endpoint because it was asked for, but on an
already-existing pipe you should expect the API to reject the change (or
silently ignore it) once the pipe has passed the `Setup` state. Use it
right after pipe creation / before the `Snapshot` state begins, not as a
knob to twist on a pipe that's already snapshotting. The method surfaces
whatever the API actually says rather than assuming success.

Because of that limitation, the *reliable* way to get a higher initial-load
parallelism is to set it at pipe-creation time. `create_pipe_with_scaled_initial_load()`
does exactly that: it creates a brand-new Postgres CDC ClickPipe with
`initialLoadParallelism` baked into the creation payload, then immediately
scales the CDC compute up so there's enough compute to actually use that
parallelism. The two steps are intentionally not separable through that
method -- scaling CDC compute up for a pipe that's still snapshotting at
the default parallelism just spends more money without loading any faster,
so the method refuses to do the resize unless it's also raising parallelism
above the default.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("clickpipe_operator")

API_BASE_URL = "https://api.clickhouse.cloud/v1"
DEFAULT_TIMEOUT_S = 30

# -- CDC compute scaling bounds, per the scaling doc -------------------------
MIN_CDC_CPU_CORES = 1
MAX_CDC_CPU_CORES = 32          # doc's own PATCH example uses 24 cores / 96GB
MEM_GB_PER_CORE = 4

# Requested "initial load" scale-up target: 24 vCPU / 96 GiB
INITIAL_LOAD_CPU_MILLICORES = 24_000
INITIAL_LOAD_MEMORY_GB = 96

# Requested parallelism change: 4 -> 16
DEFAULT_INITIAL_LOAD_PARALLELISM = 4
HIGH_INITIAL_LOAD_PARALLELISM = 16

# Pipe lifecycle states, from the lifecycle doc
SNAPSHOT_IN_PROGRESS_STATES = {"Provisioning", "Setup", "Snapshot"}
SNAPSHOT_DONE_STATES = {"Running", "Completed"}
FAILURE_STATES = {"Failed"}


# ============================================================================
# Errors
#
# The ClickPipes API reference (clickPipeCdcScalingUpdate and the other
# ClickPipes endpoints) documents two response types besides 200:
#   400 - "The request cannot be processed due to a client error. Please
#          verify your request parameters and try again."
#   500 - "An internal server error has occurred. If this issue persists,
#          please contact ClickHouse Cloud support for assistance."
# In practice the Cloud API also uses 401/403 (bad key / insufficient role),
# 404 (unknown org/service/pipe) and 429 (rate limited); those are modeled
# as subclasses of the 400 client-error family below.
# ============================================================================

class ClickPipesAPIError(Exception):
    """Base class for anything that goes wrong talking to the Cloud API."""

    def __init__(self, message: str, status_code: Optional[int] = None,
                 request_id: Optional[str] = None, payload: Optional[dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.request_id = request_id
        self.payload = payload or {}

    def __str__(self) -> str:
        base = super().__str__()
        if self.status_code is not None:
            return f"[HTTP {self.status_code}] {base} (requestId={self.request_id})"
        return base


class ClickPipesClientError(ClickPipesAPIError):
    """HTTP 400 - malformed request / invalid parameters."""


class ClickPipesAuthError(ClickPipesClientError):
    """HTTP 401/403 - bad API key, or key lacks the Admin role this API needs."""


class ClickPipesNotFoundError(ClickPipesClientError):
    """HTTP 404 - organization, service or ClickPipe id doesn't exist."""


class ClickPipesRateLimitError(ClickPipesClientError):
    """HTTP 429 - too many requests, back off and retry."""


class ClickPipesServerError(ClickPipesAPIError):
    """HTTP 5xx - internal server error. Retry, then contact support if persistent."""


class ClickPipesConnectionError(ClickPipesAPIError):
    """Network-level failure: timeout, DNS failure, connection refused, etc."""


class ScalingValidationError(ValueError):
    """Requested scaling/parallelism values are outside documented bounds."""


class SnapshotFailedError(RuntimeError):
    """The ClickPipe entered the `Failed` state while waiting for the snapshot."""


class SnapshotTimeoutError(RuntimeError):
    """Timed out waiting for the initial load / snapshot to finish."""


# ============================================================================
# The operator
# ============================================================================

class ClickPipeOperator:
    """
    Talks to the ClickHouse Cloud OpenAPI to inspect and scale DB ClickPipes.

    Authentication is HTTP Basic auth using a ClickHouse Cloud API key
    (key id / key secret) with Admin permissions on the target service, as
    required by https://clickhouse.com/docs/cloud/manage/openapi.

    `service_id` and `clickpipe_id` are optional at construction time:

    - Pass neither to use one operator against many services/pipes,
      supplying `service_id`/`clickpipe_id` on every method call.
    - Pass `service_id` only to bind the operator to one service but manage
      several pipes on it (pass `clickpipe_id` per call).
    - Pass both to bind the operator to one specific pipe; methods then need
      no ids at all.

    Passing `clickpipe_id` without `service_id` is invalid (a pipe always
    belongs to a service) and raises ValueError immediately.
    """

    def __init__(
        self,
        organization_id: str,
        key_id: str,
        key_secret: str,
        service_id: Optional[str] = None,
        clickpipe_id: Optional[str] = None,
        base_url: str = API_BASE_URL,
        timeout: int = DEFAULT_TIMEOUT_S,
        session: Optional[requests.Session] = None,
    ):
        if not organization_id or not key_id or not key_secret:
            raise ValueError("organization_id, key_id and key_secret are all required.")
        if clickpipe_id and not service_id:
            raise ValueError(
                "clickpipe_id was provided without service_id; a ClickPipe "
                "always belongs to a service, so service_id is required too."
            )

        self.organization_id = organization_id
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.default_service_id = service_id
        self.default_clickpipe_id = clickpipe_id

        self.session = session or requests.Session()
        self.session.auth = (key_id, key_secret)
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    # ------------------------------------------------------------------ #
    # id resolution helpers
    # ------------------------------------------------------------------ #
    def _service(self, service_id: Optional[str]) -> str:
        sid = service_id or self.default_service_id
        if not sid:
            raise ValueError(
                "No service_id given and this ClickPipeOperator wasn't bound "
                "to one at init time."
            )
        return sid

    def _pipe(self, clickpipe_id: Optional[str]) -> str:
        pid = clickpipe_id or self.default_clickpipe_id
        if not pid:
            raise ValueError(
                "No clickpipe_id given and this ClickPipeOperator wasn't bound "
                "to one at init time."
            )
        return pid

    # ------------------------------------------------------------------ #
    # low-level HTTP
    # ------------------------------------------------------------------ #
    def _request(self, method: str, path: str, **kwargs) -> dict:
        url = f"{self.base_url}{path}"
        try:
            response = self.session.request(method, url, timeout=self.timeout, **kwargs)
        except requests.exceptions.Timeout as exc:
            raise ClickPipesConnectionError(f"Timed out calling {method} {url}") from exc
        except requests.exceptions.ConnectionError as exc:
            raise ClickPipesConnectionError(f"Connection error calling {method} {url}: {exc}") from exc
        except requests.exceptions.RequestException as exc:
            raise ClickPipesConnectionError(f"Request to {method} {url} failed: {exc}") from exc

        if response.content:
            try:
                body = response.json()
            except ValueError:
                body = {"message": response.text}
        else:
            body = {}

        if response.status_code == 200:
            return body

        request_id = body.get("requestId")
        message = (
            body.get("message")
            or body.get("error")
            or response.text
            or f"HTTP {response.status_code} with no body"
        )

        if response.status_code in (401, 403):
            raise ClickPipesAuthError(message, response.status_code, request_id, body)
        if response.status_code == 404:
            raise ClickPipesNotFoundError(message, response.status_code, request_id, body)
        if response.status_code == 429:
            raise ClickPipesRateLimitError(message, response.status_code, request_id, body)
        if response.status_code == 400:
            raise ClickPipesClientError(message, response.status_code, request_id, body)
        if response.status_code >= 500:
            raise ClickPipesServerError(message, response.status_code, request_id, body)

        raise ClickPipesAPIError(message, response.status_code, request_id, body)

    # ------------------------------------------------------------------ #
    # CDC compute scaling  (org+service level; this is what the "Scaling
    # DB ClickPipes via OpenAPI" doc page is about)
    # ------------------------------------------------------------------ #
    @staticmethod
    def _validate_cdc_scaling(cpu_millicores: int, memory_gb: float) -> None:
        cores = cpu_millicores / 1000
        if not (MIN_CDC_CPU_CORES <= cores <= MAX_CDC_CPU_CORES) or cores != int(cores):
            raise ScalingValidationError(
                f"replicaCpuMillicores must be a whole number of cores between "
                f"{MIN_CDC_CPU_CORES} and {MAX_CDC_CPU_CORES} (got {cpu_millicores})."
            )
        expected_mem = cores * MEM_GB_PER_CORE
        if memory_gb != expected_mem:
            raise ScalingValidationError(
                f"replicaMemoryGb must be {MEM_GB_PER_CORE}x the core count "
                f"({expected_mem} for {cores} cores), got {memory_gb}."
            )

    def get_cdc_scaling(self, service_id: Optional[str] = None) -> Dict[str, Any]:
        """GET the current CDC compute scaling (replicaCpuMillicores/replicaMemoryGb)."""
        sid = self._service(service_id)
        body = self._request("GET", f"/organizations/{self.organization_id}/services/{sid}/clickpipesCdcScaling")
        return body.get("result", {})

    def set_cdc_scaling(
        self,
        cpu_millicores: int,
        memory_gb: float,
        service_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        PATCH the CDC compute scaling. Supported range per the docs is
        1-32 vCPU with memory fixed at 4x the core count in GB.

        Note: the API applies this asynchronously; ClickHouse's docs say
        propagation typically takes 3-5 minutes. Use `wait_for_cdc_scaling`
        if you need to block until it's actually live.
        """
        self._validate_cdc_scaling(cpu_millicores, memory_gb)
        sid = self._service(service_id)
        body = self._request(
            "PATCH",
            f"/organizations/{self.organization_id}/services/{sid}/clickpipesCdcScaling",
            json={"replicaCpuMillicores": cpu_millicores, "replicaMemoryGb": memory_gb},
        )
        return body.get("result", {})

    def wait_for_cdc_scaling(
        self,
        cpu_millicores: int,
        memory_gb: float,
        service_id: Optional[str] = None,
        poll_interval_s: int = 20,
        timeout_s: int = 600,
    ) -> Dict[str, Any]:
        """Poll GET clickpipesCdcScaling until it reflects the requested values."""
        deadline = time.monotonic() + timeout_s
        while True:
            current = self.get_cdc_scaling(service_id)
            if (
                current.get("replicaCpuMillicores") == cpu_millicores
                and current.get("replicaMemoryGb") == memory_gb
            ):
                return current
            if time.monotonic() >= deadline:
                raise SnapshotTimeoutError(
                    f"CDC scaling did not converge to {cpu_millicores}m/{memory_gb}GB "
                    f"within {timeout_s}s (last seen: {current})."
                )
            logger.info("Waiting for CDC scaling to propagate... current=%s", current)
            time.sleep(poll_interval_s)

    # ------------------------------------------------------------------ #
    # ClickPipe resource (status / lifecycle state)
    # ------------------------------------------------------------------ #
    def list_clickpipes(self, service_id: Optional[str] = None) -> List[Dict[str, Any]]:
        sid = self._service(service_id)
        body = self._request("GET", f"/organizations/{self.organization_id}/services/{sid}/clickpipes")
        return body.get("result", [])

    def get_clickpipe(
        self,
        service_id: Optional[str] = None,
        clickpipe_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        sid = self._service(service_id)
        pid = self._pipe(clickpipe_id)
        body = self._request(
            "GET", f"/organizations/{self.organization_id}/services/{sid}/clickpipes/{pid}"
        )
        return body.get("result", {})

    def get_clickpipe_settings(
        self,
        service_id: Optional[str] = None,
        clickpipe_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        sid = self._service(service_id)
        pid = self._pipe(clickpipe_id)
        body = self._request(
            "GET", f"/organizations/{self.organization_id}/services/{sid}/clickpipes/{pid}/settings"
        )
        return body.get("result", {})

    def get_status(
        self,
        service_id: Optional[str] = None,
        clickpipe_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Convenience status check combining:
          - the pipe's lifecycle state (Provisioning/Setup/Snapshot/Running/...)
          - the pipe's own `scaling` block (replicas/concurrency/cpu/mem)
          - the org+service level CDC compute scaling
        into a single dict so callers don't have to make 2-3 calls themselves.
        """
        sid = self._service(service_id)
        pid = self._pipe(clickpipe_id)

        pipe = self.get_clickpipe(sid, pid)
        state = pipe.get("state")

        try:
            cdc_scaling = self.get_cdc_scaling(sid)
        except ClickPipesAPIError as exc:
            # CDC scaling endpoints only exist once a DB ClickPipe has been
            # provisioned at least once in the service; surface but don't fail.
            logger.warning("Could not fetch CDC scaling for service %s: %s", sid, exc)
            cdc_scaling = None

        return {
            "clickpipe_id": pid,
            "service_id": sid,
            "name": pipe.get("name"),
            "state": state,
            "is_snapshot_in_progress": state in SNAPSHOT_IN_PROGRESS_STATES,
            "is_snapshot_done": state in SNAPSHOT_DONE_STATES,
            "is_failed": state in FAILURE_STATES,
            "pipe_scaling": pipe.get("scaling"),
            "cdc_scaling": cdc_scaling,
        }

    # ------------------------------------------------------------------ #
    # Initial load parallelism (per-pipe setting; see module docstring
    # caveat -- this is documented as immutable post-creation)
    # ------------------------------------------------------------------ #
    def update_initial_load_parallelism(
        self,
        parallelism: int,
        service_id: Optional[str] = None,
        clickpipe_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Attempt to set `initialLoadParallelism` via the "Update ClickPipe
        settings" endpoint (PUT .../clickpipes/{id}/settings).

        CAVEAT: ClickHouse's parallel-initial-load documentation states this
        and the other snapshot parameters "can't be edited after pipe
        creation." This call fetches the current settings, patches just this
        field, and PUTs the result -- but on a pipe that's already past
        `Setup`, expect this to be rejected (ClickPipesClientError) or to have
        no effect. It's included because it was asked for, and because the
        raw endpoint is used correctly; it's only reliable if run immediately
        after pipe creation / before the `Snapshot` state begins.
        """
        if parallelism < 1:
            raise ScalingValidationError(f"parallelism must be >= 1, got {parallelism}")

        sid = self._service(service_id)
        pid = self._pipe(clickpipe_id)

        current_settings = self.get_clickpipe_settings(sid, pid)
        new_settings = dict(current_settings)
        new_settings["initialLoadParallelism"] = parallelism

        logger.warning(
            "Attempting to change initialLoadParallelism to %s on pipe %s. "
            "ClickHouse docs state snapshot parameters are immutable after "
            "pipe creation -- this may be rejected by the API.",
            parallelism, pid,
        )

        body = self._request(
            "PUT",
            f"/organizations/{self.organization_id}/services/{sid}/clickpipes/{pid}/settings",
            json=new_settings,
        )
        return body.get("result", {})

    # ------------------------------------------------------------------ #
    # Waiting on the snapshot / initial load
    # ------------------------------------------------------------------ #
    def wait_for_snapshot_completion(
        self,
        service_id: Optional[str] = None,
        clickpipe_id: Optional[str] = None,
        poll_interval_s: int = 30,
        timeout_s: Optional[int] = 3600,
    ) -> Dict[str, Any]:
        """
        Block until the pipe leaves the Provisioning/Setup/Snapshot states.

        Raises SnapshotFailedError if the pipe reaches `Failed`, and
        SnapshotTimeoutError if `timeout_s` elapses first (pass None to wait
        forever).
        """
        sid = self._service(service_id)
        pid = self._pipe(clickpipe_id)
        deadline = None if timeout_s is None else time.monotonic() + timeout_s

        while True:
            pipe = self.get_clickpipe(sid, pid)
            state = pipe.get("state")

            if state in FAILURE_STATES:
                raise SnapshotFailedError(
                    f"ClickPipe {pid} entered state '{state}' while waiting for the "
                    f"initial load to complete."
                )
            if state in SNAPSHOT_DONE_STATES:
                return pipe

            if deadline is not None and time.monotonic() >= deadline:
                raise SnapshotTimeoutError(
                    f"ClickPipe {pid} did not finish its initial load within "
                    f"{timeout_s}s (last seen state: '{state}')."
                )

            logger.info("ClickPipe %s still in state '%s', waiting...", pid, state)
            time.sleep(poll_interval_s)

    # ------------------------------------------------------------------ #
    # High level scale up / down helpers
    # ------------------------------------------------------------------ #
    def scale_up_for_initial_load(
        self,
        service_id: Optional[str] = None,
        clickpipe_id: Optional[str] = None,
        cpu_millicores: int = INITIAL_LOAD_CPU_MILLICORES,
        memory_gb: float = INITIAL_LOAD_MEMORY_GB,
        parallelism: int = HIGH_INITIAL_LOAD_PARALLELISM,
        update_parallelism: bool = True,
        wait_for_propagation: bool = True,
    ) -> Dict[str, Any]:
        """
        Scale the CDC compute up to `cpu_millicores`/`memory_gb` (defaults:
        24 vCPU / 96 GiB) and, if `update_parallelism`, also try to bump
        `initialLoadParallelism` to `parallelism` (default 16). See the
        caveat on `update_initial_load_parallelism` -- the parallelism part
        is best-effort and only reliable pre-Snapshot.
        """
        sid = self._service(service_id)
        pid = self._pipe(clickpipe_id) if (clickpipe_id or self.default_clickpipe_id) else None

        result: Dict[str, Any] = {}
        result["cdc_scaling"] = self.set_cdc_scaling(cpu_millicores, memory_gb, sid)

        if wait_for_propagation:
            result["cdc_scaling"] = self.wait_for_cdc_scaling(cpu_millicores, memory_gb, sid)

        if update_parallelism:
            if pid is None:
                raise ValueError("update_parallelism=True requires a clickpipe_id.")
            try:
                result["parallelism"] = self.update_initial_load_parallelism(parallelism, sid, pid)
            except ClickPipesClientError as exc:
                logger.warning(
                    "initialLoadParallelism update was rejected (expected if the "
                    "pipe already passed the Setup state): %s", exc,
                )
                result["parallelism_error"] = str(exc)

        return result

    def scale_down_after_initial_load(
        self,
        service_id: Optional[str] = None,
        cpu_millicores: int = 2000,
        memory_gb: float = 8,
        wait_for_propagation: bool = True,
    ) -> Dict[str, Any]:
        """
        Scale the CDC compute back down once the snapshot is done, to avoid
        the ongoing higher compute cost the scaling doc warns about. Defaults
        to 2 vCPU / 8 GiB (the doc's own baseline example) -- pass whatever
        your steady-state CDC workload actually needs.
        """
        sid = self._service(service_id)
        scaling = self.set_cdc_scaling(cpu_millicores, memory_gb, sid)
        if wait_for_propagation:
            scaling = self.wait_for_cdc_scaling(cpu_millicores, memory_gb, sid)
        return scaling

    def run_scaled_initial_load(
        self,
        service_id: Optional[str] = None,
        clickpipe_id: Optional[str] = None,
        scale_up_cpu_millicores: int = INITIAL_LOAD_CPU_MILLICORES,
        scale_up_memory_gb: float = INITIAL_LOAD_MEMORY_GB,
        parallelism: int = HIGH_INITIAL_LOAD_PARALLELISM,
        update_parallelism: bool = True,
        scale_down_cpu_millicores: int = 2000,
        scale_down_memory_gb: float = 8,
        poll_interval_s: int = 30,
        timeout_s: Optional[int] = 3600,
    ) -> Dict[str, Any]:
        """
        End-to-end orchestration:
          1. Scale CDC compute up (and try to bump parallelism).
          2. Wait for the pipe to leave Provisioning/Setup/Snapshot.
          3. Scale CDC compute back down.

        Steps 1 and 2 are both wrapped so that scale-down (step 3) always
        runs -- whether the snapshot finishes normally, fails, times out
        waiting, *or* the scale-up step itself errors out (e.g. the CDC
        scaling PATCH times out waiting to propagate). In every one of those
        cases some amount of extra compute may already have been requested,
        so scale-down is attempted regardless and the original error is then
        re-raised. The one thing this can't protect against is scale-down
        itself failing -- if that happens, that error replaces the original
        one and you should check `get_cdc_scaling()` by hand.
        """
        sid = self._service(service_id)
        pid = self._pipe(clickpipe_id)

        summary: Dict[str, Any] = {"clickpipe_id": pid, "service_id": sid}

        try:
            summary["scale_up"] = self.scale_up_for_initial_load(
                service_id=sid,
                clickpipe_id=pid,
                cpu_millicores=scale_up_cpu_millicores,
                memory_gb=scale_up_memory_gb,
                parallelism=parallelism,
                update_parallelism=update_parallelism,
            )

            summary["final_pipe_state"] = self.wait_for_snapshot_completion(
                service_id=sid,
                clickpipe_id=pid,
                poll_interval_s=poll_interval_s,
                timeout_s=timeout_s,
            )
        finally:
            summary["scale_down"] = self.scale_down_after_initial_load(
                service_id=sid,
                cpu_millicores=scale_down_cpu_millicores,
                memory_gb=scale_down_memory_gb,
            )

        return summary

    # ------------------------------------------------------------------ #
    # Pipe creation (this is the only point at which initialLoadParallelism
    # can actually be set, per the parallel-load docs)
    # ------------------------------------------------------------------ #
    def create_clickpipe(
        self,
        payload: Dict[str, Any],
        service_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Low-level POST /clickpipes. `payload` is sent verbatim as the JSON
        body, so it must already match the ClickPipes creation schema (see
        https://clickhouse.com/docs/integrations/clickpipes/programmatic-access/openapi).
        Prefer `create_pipe_with_scaled_initial_load` for the Postgres CDC
        case this module is built around.
        """
        sid = self._service(service_id)
        body = self._request(
            "POST",
            f"/organizations/{self.organization_id}/services/{sid}/clickpipes",
            json=payload,
        )
        return body.get("result", {})

    @staticmethod
    def _build_postgres_clickpipe_payload(
        name: str,
        postgres_source: Dict[str, Any],
        table_mappings: List[Dict[str, Any]],
        destination: Optional[Dict[str, Any]] = None,
        replication_mode: str = "cdc",
        initial_load_parallelism: int = HIGH_INITIAL_LOAD_PARALLELISM,
        extra_postgres_settings: Optional[Dict[str, Any]] = None,
        top_level_settings: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Assemble a `POST /clickpipes` body for a Postgres CDC source.

        `postgres_source` should carry the connection-specific fields exactly
        as ClickHouse documents them for the Postgres source -- `host`,
        `port`, `database`, and whichever auth fields apply (`credentials`
        with username/password for basic auth, or `authentication`/`iamRole`
        for RDS/Aurora IAM auth, plus any TLS options). This builder
        deliberately does not try to validate or synthesize those fields,
        since they vary by auth method -- see
        https://clickhouse.com/docs/integrations/clickpipes/postgres/auth
        and the OpenAPI/Terraform references for the exact shape your setup
        needs. Pass whatever that source needs; it's merged in as-is.

        What this builder *is* responsible for is `settings.initialLoadParallelism`,
        since that's the one field this whole workflow revolves around and
        it can only be set here, at creation time.
        """
        postgres = dict(postgres_source)  # don't mutate the caller's dict
        settings = dict(postgres.get("settings", {}))
        settings["replicationMode"] = replication_mode
        settings["initialLoadParallelism"] = initial_load_parallelism
        if extra_postgres_settings:
            settings.update(extra_postgres_settings)
        postgres["settings"] = settings
        postgres.setdefault("type", "postgres")
        postgres["tableMappings"] = table_mappings

        payload: Dict[str, Any] = {
            "name": name,
            "source": {"postgres": postgres},
            "destination": destination or {"database": "default"},
        }
        if top_level_settings:
            payload["settings"] = top_level_settings
        return payload

    def create_pipe_with_scaled_initial_load(
        self,
        name: str,
        postgres_source: Dict[str, Any],
        table_mappings: List[Dict[str, Any]],
        destination: Optional[Dict[str, Any]] = None,
        service_id: Optional[str] = None,
        initial_load_parallelism: int = HIGH_INITIAL_LOAD_PARALLELISM,
        cpu_millicores: int = INITIAL_LOAD_CPU_MILLICORES,
        memory_gb: float = INITIAL_LOAD_MEMORY_GB,
        replication_mode: str = "cdc",
        extra_postgres_settings: Optional[Dict[str, Any]] = None,
        top_level_settings: Optional[Dict[str, Any]] = None,
        wait_for_scaling_propagation: bool = True,
        wait_and_scale_down: bool = False,
        wait_poll_interval_s: int = 30,
        wait_timeout_s: Optional[int] = 3600,
        scale_down_cpu_millicores: int = 2000,
        scale_down_memory_gb: float = 8,
    ) -> Dict[str, Any]:
        """
        Create a new Postgres CDC ClickPipe with a raised `initialLoadParallelism`
        baked in at creation time -- the only time that setting is editable
        -- then immediately scale the CDC compute up so there's enough
        compute behind it for the initial load.

        These two steps are chained deliberately and can't be split apart
        through this method: bumping CDC compute to `cpu_millicores`/`memory_gb`
        (24 vCPU / 96 GiB by default) only pays off if there's more
        parallelism to actually use that compute. If `initial_load_parallelism`
        isn't above the platform default (4), this raises
        `ScalingValidationError` instead of creating a pipe and resizing
        compute that wouldn't help it.

        By default this returns as soon as the pipe is created and CDC
        compute is scaled up -- it does *not* wait for the snapshot to
        finish, since that means picking a timeout on your behalf. Pass
        `wait_and_scale_down=True` to opt into blocking here instead: this
        method will then wait for the pipe to leave Provisioning/Setup/Snapshot
        (via `wait_for_snapshot_completion`, using `wait_poll_interval_s` /
        `wait_timeout_s`) and scale CDC compute back down to
        `scale_down_cpu_millicores`/`scale_down_memory_gb` afterward --
        guaranteed to run whether the wait succeeds, the pipe fails, or the
        wait times out, mirroring `run_scaled_initial_load`'s guarantee. If
        you'd rather manage that wait yourself, leave this False and call
        `wait_for_snapshot_completion` / `scale_down_after_initial_load`
        (or `run_scaled_initial_load`) on the returned `clickpipe_id` later.

        Returns a dict with the created pipe, its id, and the resulting CDC
        scaling (plus `final_pipe_state` and `scale_down` if
        `wait_and_scale_down=True`). If the creation succeeds but the
        compute resize fails, the pipe is left in place (at default CDC
        compute) and the resize error is re-raised -- it will not silently
        create a pipe that ends up under-provisioned for the parallelism
        you asked for.
        """
        if initial_load_parallelism <= DEFAULT_INITIAL_LOAD_PARALLELISM:
            raise ScalingValidationError(
                f"initial_load_parallelism ({initial_load_parallelism}) must be "
                f"greater than the platform default ({DEFAULT_INITIAL_LOAD_PARALLELISM}). "
                "Scaling up CDC compute without also raising parallelism won't make "
                "the initial load any faster, so this method won't do just the resize."
            )
        self._validate_cdc_scaling(cpu_millicores, memory_gb)

        sid = self._service(service_id)

        payload = self._build_postgres_clickpipe_payload(
            name=name,
            postgres_source=postgres_source,
            table_mappings=table_mappings,
            destination=destination,
            replication_mode=replication_mode,
            initial_load_parallelism=initial_load_parallelism,
            extra_postgres_settings=extra_postgres_settings,
            top_level_settings=top_level_settings,
        )

        created_pipe = self.create_clickpipe(payload, service_id=sid)
        pipe_id = created_pipe.get("id")
        if not pipe_id:
            raise ClickPipesAPIError(
                "ClickPipe creation response did not include an 'id'.",
                payload=created_pipe,
            )

        logger.info(
            "Created ClickPipe %s with initialLoadParallelism=%s; scaling CDC "
            "compute to %sm/%sGB.", pipe_id, initial_load_parallelism, cpu_millicores, memory_gb,
        )

        try:
            cdc_scaling = self.set_cdc_scaling(cpu_millicores, memory_gb, service_id=sid)
            if wait_for_scaling_propagation:
                cdc_scaling = self.wait_for_cdc_scaling(cpu_millicores, memory_gb, service_id=sid)
        except ClickPipesAPIError as exc:
            logger.error(
                "ClickPipe %s was created with initialLoadParallelism=%s but scaling "
                "CDC compute up failed (%s). The pipe now exists at default CDC "
                "compute; call set_cdc_scaling() manually once resolved.",
                pipe_id, initial_load_parallelism, exc,
            )
            raise

        result = {
            "clickpipe": created_pipe,
            "clickpipe_id": pipe_id,
            "service_id": sid,
            "initial_load_parallelism": initial_load_parallelism,
            "cdc_scaling": cdc_scaling,
        }

        if not wait_and_scale_down:
            return result

        try:
            result["final_pipe_state"] = self.wait_for_snapshot_completion(
                service_id=sid,
                clickpipe_id=pipe_id,
                poll_interval_s=wait_poll_interval_s,
                timeout_s=wait_timeout_s,
            )
        finally:
            result["scale_down"] = self.scale_down_after_initial_load(
                service_id=sid,
                cpu_millicores=scale_down_cpu_millicores,
                memory_gb=scale_down_memory_gb,
            )

        return result
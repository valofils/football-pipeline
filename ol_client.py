"""
lineage/ol_client.py
OpenLineage client wrapper for football-pipeline.

Emits OpenLineage RunEvents (START / COMPLETE / FAIL) to the Marquez
HTTP transport.  All pipeline components import `emit_run_event` from
here so the transport URL is configured in exactly one place.

Environment variables
---------------------
MARQUEZ_URL          Marquez API base URL (default: http://marquez:5000)
OPENLINEAGE_NAMESPACE Logical namespace for all jobs (default: football_pipeline)
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import List, Optional

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    Dataset,
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.facet_v2 import (
    documentation_job,
    schema_dataset,
    sql_job,
)
from openlineage.client.transport.http import HttpConfig, HttpTransport

MARQUEZ_URL: str = os.getenv("MARQUEZ_URL", "http://marquez:5000")
NAMESPACE: str = os.getenv("OPENLINEAGE_NAMESPACE", "football_pipeline")


def _build_client() -> OpenLineageClient:
    transport = HttpTransport(
        HttpConfig(url=MARQUEZ_URL, endpoint="api/v1/lineage")
    )
    return OpenLineageClient(transport=transport)


_CLIENT: Optional[OpenLineageClient] = None


def get_client() -> OpenLineageClient:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = _build_client()
    return _CLIENT


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _schema_facet(fields: List[dict]) -> schema_dataset.SchemaDatasetFacet:
    """Build a SchemaDatasetFacet from a list of {name, type} dicts."""
    return schema_dataset.SchemaDatasetFacet(
        fields=[
            schema_dataset.SchemaDatasetFacetFields(name=f["name"], type=f["type"])
            for f in fields
        ]
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def emit_run_event(
    *,
    job_name: str,
    run_id: str,
    state: RunState,
    inputs: Optional[List[InputDataset]] = None,
    outputs: Optional[List[OutputDataset]] = None,
    sql: Optional[str] = None,
    description: Optional[str] = None,
    event_time: Optional[datetime] = None,
) -> None:
    """Emit a single OpenLineage RunEvent."""
    job_facets: dict = {}
    if description:
        job_facets["documentation"] = documentation_job.DocumentationJobFacet(
            description=description
        )
    if sql:
        job_facets["sql"] = sql_job.SQLJobFacet(query=sql)

    event = RunEvent(
        eventType=state,
        eventTime=(event_time or _now()).isoformat(),
        run=Run(runId=run_id),
        job=Job(namespace=NAMESPACE, name=job_name, facets=job_facets),
        inputs=inputs or [],
        outputs=outputs or [],
    )
    get_client().emit(event)


@contextmanager
def lineage_run(
    job_name: str,
    inputs: Optional[List[InputDataset]] = None,
    outputs: Optional[List[OutputDataset]] = None,
    sql: Optional[str] = None,
    description: Optional[str] = None,
    run_id: Optional[str] = None,
):
    """
    Context manager that emits START on enter and COMPLETE / FAIL on exit.

    Usage::

        with lineage_run("my_job", inputs=[...], outputs=[...]) as run_id:
            do_work()
    """
    rid = run_id or str(uuid.uuid4())
    emit_run_event(
        job_name=job_name,
        run_id=rid,
        state=RunState.START,
        inputs=inputs,
        outputs=outputs,
        sql=sql,
        description=description,
    )
    try:
        yield rid
        emit_run_event(
            job_name=job_name,
            run_id=rid,
            state=RunState.COMPLETE,
            inputs=inputs,
            outputs=outputs,
        )
    except Exception:
        emit_run_event(
            job_name=job_name,
            run_id=rid,
            state=RunState.FAIL,
            inputs=inputs,
            outputs=outputs,
        )
        raise


# ---------------------------------------------------------------------------
# Dataset factory helpers
# ---------------------------------------------------------------------------

def kafka_dataset(topic: str) -> InputDataset:
    return InputDataset(namespace="kafka://kafka:9092", name=topic)


def delta_dataset(
    path: str,
    fields: Optional[List[dict]] = None,
    as_output: bool = False,
) -> Dataset:
    cls = OutputDataset if as_output else InputDataset
    facets = {}
    if fields:
        facets["schema"] = _schema_facet(fields)
    return cls(namespace="s3://football-data", name=path, facets=facets)


def postgres_dataset(
    table: str,
    fields: Optional[List[dict]] = None,
    as_output: bool = False,
) -> Dataset:
    cls = OutputDataset if as_output else InputDataset
    db_ns = "postgres://football-db:5432/football"
    facets = {}
    if fields:
        facets["schema"] = _schema_facet(fields)
    return cls(namespace=db_ns, name=table, facets=facets)

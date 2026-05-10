"""
lineage/ol_client.py — OpenLineage client wrapper for Marquez.

Provides:
  - get_client()          : singleton OpenLineage client via HttpTransport
  - emit_run_event()      : low-level raw event emitter
  - lineage_run()         : context manager → START / COMPLETE / FAIL
  - kafka_dataset()       : dataset factory for Kafka sources
  - delta_dataset()       : dataset factory for Delta Lake (S3) tables
  - postgres_dataset()    : dataset factory for PostgreSQL tables
"""

from __future__ import annotations

import os
import uuid
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import lru_cache
from typing import List, Optional

from openlineage.client import OpenLineageClient
from openlineage.client.transport.http import HttpConfig, HttpTransport
from openlineage.client.run import (
    RunEvent,
    RunState,
    Run,
    Job,
    Dataset,
)
from openlineage.client.facet import (
    SchemaDatasetFacet,
    SchemaField,
    SqlJobFacet,
    DataSourceDatasetFacet,
    DocumentationJobFacet,
)

logger = logging.getLogger(__name__)

_MARQUEZ_URL = os.environ.get("MARQUEZ_URL", "http://localhost:5002")
_NAMESPACE = os.environ.get("OPENLINEAGE_NAMESPACE", "football_pipeline")


# ---------------------------------------------------------------------------
# Client singleton
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def get_client() -> OpenLineageClient:
    """Return a cached OpenLineage client pointed at Marquez."""
    transport = HttpTransport(
        HttpConfig(url=_MARQUEZ_URL, endpoint="api/v1/lineage")
    )
    return OpenLineageClient(transport=transport)


# ---------------------------------------------------------------------------
# Low-level emitter
# ---------------------------------------------------------------------------

def emit_run_event(
    *,
    job_name: str,
    run_id: str,
    state: RunState,
    inputs: Optional[List[Dataset]] = None,
    outputs: Optional[List[Dataset]] = None,
    job_facets: Optional[dict] = None,
) -> None:
    """Emit a single RunEvent to Marquez. Swallows errors to avoid breaking pipelines."""
    try:
        client = get_client()
        event = RunEvent(
            eventType=state,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=run_id),
            job=Job(
                namespace=_NAMESPACE,
                name=job_name,
                facets=job_facets or {},
            ),
            inputs=inputs or [],
            outputs=outputs or [],
            producer="https://github.com/your-org/football-pipeline",
        )
        client.emit(event)
        logger.debug("Emitted %s for job=%s run=%s", state, job_name, run_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("OpenLineage emit failed (non-fatal): %s", exc)


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

@contextmanager
def lineage_run(
    job_name: str,
    *,
    run_id: Optional[str] = None,
    inputs: Optional[List[Dataset]] = None,
    outputs: Optional[List[Dataset]] = None,
    sql: Optional[str] = None,
    description: Optional[str] = None,
):
    """
    Context manager that wraps a pipeline stage with START / COMPLETE / FAIL events.

    Usage::

        with lineage_run("validate_matches", inputs=[src], outputs=[dst]) as run_id:
            do_work()
    """
    run_id = run_id or str(uuid.uuid4())
    job_facets: dict = {}
    if sql:
        job_facets["sql"] = SqlJobFacet(query=sql)
    if description:
        job_facets["documentation"] = DocumentationJobFacet(description=description)

    emit_run_event(
        job_name=job_name,
        run_id=run_id,
        state=RunState.START,
        inputs=inputs,
        outputs=outputs,
        job_facets=job_facets,
    )
    try:
        yield run_id
        emit_run_event(
            job_name=job_name,
            run_id=run_id,
            state=RunState.COMPLETE,
            inputs=inputs,
            outputs=outputs,
            job_facets=job_facets,
        )
    except Exception:
        emit_run_event(
            job_name=job_name,
            run_id=run_id,
            state=RunState.FAIL,
            inputs=inputs,
            outputs=outputs,
            job_facets=job_facets,
        )
        raise


# ---------------------------------------------------------------------------
# Dataset factories
# ---------------------------------------------------------------------------

def _schema_facet(fields: List[tuple]) -> SchemaDatasetFacet:
    """Build a SchemaDatasetFacet from [(name, type), ...] tuples."""
    return SchemaDatasetFacet(
        fields=[SchemaField(name=n, type=t) for n, t in fields]
    )


def kafka_dataset(topic: str, fields: Optional[List[tuple]] = None) -> Dataset:
    """Dataset representing a Kafka topic."""
    facets: dict = {
        "dataSource": DataSourceDatasetFacet(
            name=f"kafka://kafka:9092/{topic}",
            uri=f"kafka://kafka:9092/{topic}",
        )
    }
    if fields:
        facets["schema"] = _schema_facet(fields)
    return Dataset(
        namespace="kafka://kafka:9092",
        name=topic,
        facets=facets,
    )


def delta_dataset(path: str, fields: Optional[List[tuple]] = None) -> Dataset:
    """Dataset representing a Delta Lake table on S3."""
    uri = f"s3://football-data/delta/{path}"
    facets: dict = {
        "dataSource": DataSourceDatasetFacet(name=uri, uri=uri)
    }
    if fields:
        facets["schema"] = _schema_facet(fields)
    return Dataset(
        namespace="s3://football-data",
        name=f"delta/{path}",
        facets=facets,
    )


def postgres_dataset(table: str, fields: Optional[List[tuple]] = None) -> Dataset:
    """Dataset representing a PostgreSQL table."""
    uri = f"postgresql://postgres:5432/football/{table}"
    facets: dict = {
        "dataSource": DataSourceDatasetFacet(name=uri, uri=uri)
    }
    if fields:
        facets["schema"] = _schema_facet(fields)
    return Dataset(
        namespace="postgresql://postgres:5432",
        name=table,
        facets=facets,
    )

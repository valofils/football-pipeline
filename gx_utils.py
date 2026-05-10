"""
gx_utils.py — Great Expectations helpers for football-pipeline-v9.

Responsibilities:
  - Build a FileDataContext pointed at gx/
  - Register a Spark DataFrame as a runtime batch
  - Run the matches checkpoint and parse results
  - Raise DataQualityError with a structured summary if any expectations fail
  - Upload Data Docs to S3 (triggered automatically by the checkpoint action)
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import FileDataContext

log = logging.getLogger(__name__)

GX_ROOT = Path(__file__).parent.parent / "gx"
SUITE_NAME = "matches_suite"
CHECKPOINT_NAME = "matches_checkpoint"
DATASOURCE_NAME = "spark_delta_datasource"
CONNECTOR_NAME = "runtime_connector"
DATA_ASSET_NAME = "matches_delta"


class DataQualityError(RuntimeError):
    """Raised when one or more GE expectations fail."""

    def __init__(self, summary: dict[str, Any]) -> None:
        self.summary = summary
        failed = summary.get("failed_expectations", [])
        msg = (
            f"Data quality check FAILED — "
            f"{len(failed)} expectation(s) violated.\n"
            + "\n".join(f"  • {e}" for e in failed)
        )
        super().__init__(msg)


def _build_context() -> FileDataContext:
    """Return a GE FileDataContext rooted at gx/."""
    return gx.get_context(context_root_dir=str(GX_ROOT))


def _load_suite(context: FileDataContext):
    """Load the matches expectation suite from the JSON file.

    GE 0.18 will pick it up from the ExpectationsStore automatically
    if it has already been synced; on first run we load from disk.
    """
    suite_path = GX_ROOT / "expectations" / f"{SUITE_NAME}.json"
    try:
        return context.get_expectation_suite(SUITE_NAME)
    except Exception:
        with suite_path.open() as fh:
            suite_dict = json.load(fh)
        suite = context.create_expectation_suite(
            SUITE_NAME, overwrite_existing=True
        )
        for exp in suite_dict["expectations"]:
            suite.add_expectation_configuration(
                gx.core.ExpectationConfiguration(
                    expectation_type=exp["expectation_type"],
                    kwargs=exp["kwargs"],
                )
            )
        context.save_expectation_suite(suite)
        return suite


def _parse_results(validation_result) -> dict[str, Any]:
    """Extract a concise summary from a GE ValidationResult."""
    stats = validation_result.statistics
    results = validation_result.results

    failed = [
        f"{r.expectation_config.expectation_type}"
        f"({r.expectation_config.kwargs})"
        for r in results
        if not r.success
    ]

    return {
        "success": validation_result.success,
        "evaluated": stats.get("evaluated_expectations", 0),
        "successful": stats.get("successful_expectations", 0),
        "failed_count": stats.get("unsuccessful_expectations", 0),
        "failed_expectations": failed,
        "run_id": str(validation_result.meta.get("run_id", "")),
    }


def validate_dataframe(
    spark_df,
    run_id: str,
    season: str | None = None,
    raise_on_failure: bool = True,
) -> dict[str, Any]:
    """
    Validate *spark_df* against the matches_suite expectations.

    Parameters
    ----------
    spark_df:
        A live PySpark DataFrame (already read from Delta).
    run_id:
        Logical run identifier — e.g. Airflow run_id or ISO timestamp.
    season:
        Optional season label for the batch identifier (informational).
    raise_on_failure:
        If True (default) raise DataQualityError on any failed expectation.

    Returns
    -------
    dict
        Concise validation summary (always returned, even on failure before
        the exception is raised).
    """
    context = _build_context()
    _load_suite(context)

    batch_request = RuntimeBatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name=CONNECTOR_NAME,
        data_asset_name=DATA_ASSET_NAME,
        runtime_parameters={"batch_data": spark_df},
        batch_identifiers={
            "run_id": run_id,
            "season": season or "all",
        },
    )

    checkpoint_result = context.run_checkpoint(
        checkpoint_name=CHECKPOINT_NAME,
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": SUITE_NAME,
            }
        ],
    )

    # checkpoint_result.run_results is a dict keyed by ValidationResultIdentifier
    validation_result = next(iter(checkpoint_result.run_results.values()))[
        "validation_result"
    ]
    summary = _parse_results(validation_result)

    log.info(
        "GE validation complete | success=%s | %d/%d expectations passed",
        summary["success"],
        summary["successful"],
        summary["evaluated"],
    )

    if not summary["success"] and raise_on_failure:
        raise DataQualityError(summary)

    return summary

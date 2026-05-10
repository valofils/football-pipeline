"""
dags/gx_utils.py
----------------
Great Expectations helper utilities (carried forward from v9/v10).
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

GX_ROOT = os.getenv("GX_ROOT", "/opt/airflow/gx")


class DataQualityError(Exception):
    """Raised when a Great Expectations checkpoint fails."""


def _build_context():
    import great_expectations as ge
    from great_expectations.data_context import FileDataContext

    return FileDataContext(context_root_dir=GX_ROOT)


def _parse_results(results) -> list[str]:
    failures = []
    for r in results.run_results.values():
        for res in r["validation_result"].results:
            if not res.success:
                failures.append(str(res.expectation_config))
    return failures


def validate_dataframe(df: DataFrame, suite_name: str = "matches_suite") -> None:
    """Validate a Spark DataFrame against the GE matches suite.

    Raises DataQualityError if any expectation fails.
    """
    ctx = _build_context()
    suite = ctx.get_expectation_suite(suite_name)
    validator = ctx.get_validator(
        batch_request=None,
        expectation_suite=suite,
    )
    validator.active_batch.data = df
    results = validator.validate()

    if not results.success:
        failures = _parse_results(results)
        raise DataQualityError(
            f"Data quality check failed ({len(failures)} expectation(s)):\n"
            + "\n".join(failures)
        )

    logger.info("GE validation passed for suite '%s'.", suite_name)

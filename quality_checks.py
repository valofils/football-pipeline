"""Data quality checks beyond Great Expectations."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import psycopg2
from prometheus_client import Counter, Gauge

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
quality_check_failures_total = Counter(
    "quality_check_failures_total",
    "Total number of data quality check failures",
    ["check_name"],
)
row_count_delta = Gauge(
    "row_count_delta",
    "Difference between Kafka offset count and Delta Lake row count",
)
data_freshness_seconds = Gauge(
    "data_freshness_seconds",
    "Seconds since the latest row timestamp in the matches table",
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
POSTGRES_DSN = os.getenv(
    "QUALITY_POSTGRES_DSN",
    "host=localhost port=5432 dbname=football user=airflow password=airflow",
)
FRESHNESS_THRESHOLD_HOURS = int(os.getenv("FRESHNESS_THRESHOLD_HOURS", "25"))


@dataclass
class QualityCheckResult:
    check_name: str
    passed: bool
    details: str
    measured_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "details": self.details,
            "measured_at": self.measured_at.isoformat(),
        }


class QualityChecks:
    """Runs four post-load quality checks against PostgreSQL and Delta Lake metadata."""

    def __init__(
        self,
        postgres_dsn: Optional[str] = None,
        freshness_threshold_hours: int = FRESHNESS_THRESHOLD_HOURS,
        db_conn=None,
    ) -> None:
        self.postgres_dsn = postgres_dsn or POSTGRES_DSN
        self.freshness_threshold_hours = freshness_threshold_hours
        self._db_conn = db_conn

    def _get_conn(self):
        if self._db_conn is not None:
            return self._db_conn
        return psycopg2.connect(self.postgres_dsn)

    # ------------------------------------------------------------------
    # 1. Row-count reconciliation (Kafka offset vs Delta rows)
    # ------------------------------------------------------------------
    def check_row_count_reconciliation(
        self,
        kafka_offset_count: int,
        delta_row_count: int,
        tolerance: int = 0,
    ) -> QualityCheckResult:
        diff = abs(kafka_offset_count - delta_row_count)
        row_count_delta.set(kafka_offset_count - delta_row_count)
        passed = diff <= tolerance
        details = (
            f"kafka_offsets={kafka_offset_count}, delta_rows={delta_row_count}, "
            f"diff={diff}, tolerance={tolerance}"
        )
        if not passed:
            quality_check_failures_total.labels(check_name="row_count_reconciliation").inc()
            logger.warning("QUALITY FAIL row_count_reconciliation: %s", details)
        else:
            logger.info("QUALITY PASS row_count_reconciliation: %s", details)
        return QualityCheckResult(
            check_name="row_count_reconciliation", passed=passed, details=details
        )

    # ------------------------------------------------------------------
    # 2. Duplicate primary-key detection in postgres
    # ------------------------------------------------------------------
    def check_duplicate_primary_keys(
        self, table: str = "public.matches", pk_column: str = "match_id"
    ) -> QualityCheckResult:
        sql = f"""
            SELECT COUNT(*) AS dup_count
            FROM (
                SELECT {pk_column}, COUNT(*) AS cnt
                FROM {table}
                GROUP BY {pk_column}
                HAVING COUNT(*) > 1
            ) t
        """
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
            dup_count = row[0] if row else 0
        except Exception as exc:
            details = f"Query failed: {exc}"
            quality_check_failures_total.labels(check_name="duplicate_primary_keys").inc()
            logger.exception("QUALITY FAIL duplicate_primary_keys")
            return QualityCheckResult(
                check_name="duplicate_primary_keys", passed=False, details=details
            )

        passed = dup_count == 0
        details = f"table={table}, pk={pk_column}, duplicate_groups={dup_count}"
        if not passed:
            quality_check_failures_total.labels(check_name="duplicate_primary_keys").inc()
            logger.warning("QUALITY FAIL duplicate_primary_keys: %s", details)
        else:
            logger.info("QUALITY PASS duplicate_primary_keys: %s", details)
        return QualityCheckResult(
            check_name="duplicate_primary_keys", passed=passed, details=details
        )

    # ------------------------------------------------------------------
    # 3. Referential integrity (mart references all match IDs in matches)
    # ------------------------------------------------------------------
    def check_referential_integrity(
        self,
        source_table: str = "public.matches",
        mart_table: str = "public.mart_team_season_stats",
        source_pk: str = "match_id",
        mart_fk: str = "match_id",
    ) -> QualityCheckResult:
        sql = f"""
            SELECT COUNT(*) AS orphan_count
            FROM {mart_table} m
            LEFT JOIN {source_table} s ON m.{mart_fk} = s.{source_pk}
            WHERE s.{source_pk} IS NULL
        """
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
            orphan_count = row[0] if row else 0
        except Exception as exc:
            details = f"Query failed: {exc}"
            quality_check_failures_total.labels(check_name="referential_integrity").inc()
            logger.exception("QUALITY FAIL referential_integrity")
            return QualityCheckResult(
                check_name="referential_integrity", passed=False, details=details
            )

        passed = orphan_count == 0
        details = (
            f"mart={mart_table}, source={source_table}, "
            f"orphaned_mart_rows={orphan_count}"
        )
        if not passed:
            quality_check_failures_total.labels(check_name="referential_integrity").inc()
            logger.warning("QUALITY FAIL referential_integrity: %s", details)
        else:
            logger.info("QUALITY PASS referential_integrity: %s", details)
        return QualityCheckResult(
            check_name="referential_integrity", passed=passed, details=details
        )

    # ------------------------------------------------------------------
    # 4. Freshness check (latest row timestamp within threshold hours)
    # ------------------------------------------------------------------
    def check_data_freshness(
        self,
        table: str = "public.matches",
        timestamp_column: str = "event_time",
    ) -> QualityCheckResult:
        sql = f"SELECT MAX({timestamp_column}) FROM {table}"
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
            latest_ts = row[0] if row else None
        except Exception as exc:
            details = f"Query failed: {exc}"
            quality_check_failures_total.labels(check_name="data_freshness").inc()
            logger.exception("QUALITY FAIL data_freshness")
            return QualityCheckResult(
                check_name="data_freshness", passed=False, details=details
            )

        if latest_ts is None:
            details = f"table={table} has no rows"
            quality_check_failures_total.labels(check_name="data_freshness").inc()
            logger.warning("QUALITY FAIL data_freshness: %s", details)
            return QualityCheckResult(
                check_name="data_freshness", passed=False, details=details
            )

        # Make tz-aware
        if latest_ts.tzinfo is None:
            latest_ts = latest_ts.replace(tzinfo=timezone.utc)
        now = datetime.now(tz=timezone.utc)
        age_seconds = (now - latest_ts).total_seconds()
        data_freshness_seconds.set(age_seconds)

        threshold_seconds = self.freshness_threshold_hours * 3600
        passed = age_seconds <= threshold_seconds
        details = (
            f"table={table}, latest_ts={latest_ts.isoformat()}, "
            f"age_seconds={age_seconds:.0f}, threshold_seconds={threshold_seconds}"
        )
        if not passed:
            quality_check_failures_total.labels(check_name="data_freshness").inc()
            logger.warning("QUALITY FAIL data_freshness: %s", details)
        else:
            logger.info("QUALITY PASS data_freshness: %s", details)
        return QualityCheckResult(
            check_name="data_freshness", passed=passed, details=details
        )

    # ------------------------------------------------------------------
    # Run all checks
    # ------------------------------------------------------------------
    def run_all(
        self,
        kafka_offset_count: int = 0,
        delta_row_count: int = 0,
        row_count_tolerance: int = 0,
    ) -> List[QualityCheckResult]:
        results = [
            self.check_row_count_reconciliation(
                kafka_offset_count, delta_row_count, row_count_tolerance
            ),
            self.check_duplicate_primary_keys(),
            self.check_referential_integrity(),
            self.check_data_freshness(),
        ]
        failures = [r for r in results if not r.passed]
        logger.info(
            "Quality checks complete: %d/%d passed, %d failed",
            len(results) - len(failures),
            len(results),
            len(failures),
        )
        return results

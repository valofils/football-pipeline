"""Breach handler — consumes SLABreachEvent, logs, increments metrics, alerts, persists."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Optional

import psycopg2
import requests
from prometheus_client import Counter

from sla.sla_monitor import SLABreachEvent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prometheus counter
# ---------------------------------------------------------------------------
sla_breaches_total = Counter(
    "sla_breaches_total",
    "Total number of SLA breaches",
    ["stage", "severity"],
)

# ---------------------------------------------------------------------------
# Config (env-overridable)
# ---------------------------------------------------------------------------
GRAFANA_WEBHOOK_URL = os.getenv(
    "GRAFANA_WEBHOOK_URL", "http://localhost:3000/api/alertmanager/grafana/api/v2/alerts"
)
GRAFANA_API_KEY = os.getenv("GRAFANA_API_KEY", "")

POSTGRES_DSN = os.getenv(
    "BREACH_POSTGRES_DSN",
    "host=localhost port=5432 dbname=football user=airflow password=airflow",
)

DDL = """
CREATE TABLE IF NOT EXISTS public.sla_breaches (
    id              SERIAL PRIMARY KEY,
    stage           TEXT        NOT NULL,
    expected_sec    INTEGER     NOT NULL,
    actual_sec      NUMERIC     NOT NULL,
    overrun_sec     NUMERIC     NOT NULL,
    severity        TEXT        NOT NULL,
    breach_time     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


class BreachHandler:
    """Handles SLABreachEvent: log → metrics → grafana alert → postgres record."""

    def __init__(
        self,
        postgres_dsn: Optional[str] = None,
        grafana_webhook_url: Optional[str] = None,
        grafana_api_key: Optional[str] = None,
        requests_session: Optional[requests.Session] = None,
        db_conn=None,
    ) -> None:
        self.postgres_dsn = postgres_dsn or POSTGRES_DSN
        self.grafana_webhook_url = grafana_webhook_url or GRAFANA_WEBHOOK_URL
        self.grafana_api_key = grafana_api_key or GRAFANA_API_KEY
        self._session = requests_session or requests.Session()
        self._db_conn = db_conn  # allow injection for tests
        self._ensure_table()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def handle(self, event: SLABreachEvent) -> None:
        self._log_structured(event)
        self._increment_metric(event)
        self._send_grafana_alert(event)
        self._persist_breach(event)

    def __call__(self, event: SLABreachEvent) -> None:
        self.handle(event)

    # ------------------------------------------------------------------
    # Internal steps
    # ------------------------------------------------------------------
    def _log_structured(self, event: SLABreachEvent) -> None:
        payload = event.to_dict()
        payload["event"] = "sla_breach"
        logger.warning(json.dumps(payload))

    def _increment_metric(self, event: SLABreachEvent) -> None:
        sla_breaches_total.labels(stage=event.stage, severity=event.severity).inc()

    def _send_grafana_alert(self, event: SLABreachEvent) -> None:
        alert_payload = [
            {
                "labels": {
                    "alertname": "SLABreach",
                    "stage": event.stage,
                    "severity": event.severity,
                },
                "annotations": {
                    "summary": f"SLA breach on stage '{event.stage}'",
                    "description": (
                        f"Stage '{event.stage}' took {event.actual_seconds:.1f}s "
                        f"(SLA={event.expected_seconds}s, "
                        f"overrun={event.overrun_seconds:.1f}s)"
                    ),
                },
                "startsAt": event.breach_time.isoformat() + "Z",
            }
        ]
        headers = {"Content-Type": "application/json"}
        if self.grafana_api_key:
            headers["Authorization"] = f"Bearer {self.grafana_api_key}"
        try:
            resp = self._session.post(
                self.grafana_webhook_url,
                json=alert_payload,
                headers=headers,
                timeout=5,
            )
            resp.raise_for_status()
            logger.info("Grafana alert sent for stage=%s status=%d", event.stage, resp.status_code)
        except Exception:
            logger.exception("Failed to send Grafana alert for stage=%s", event.stage)

    def _get_conn(self):
        if self._db_conn is not None:
            return self._db_conn
        return psycopg2.connect(self.postgres_dsn)

    def _ensure_table(self) -> None:
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(DDL)
            conn.commit()
        except Exception:
            logger.exception("Failed to ensure sla_breaches table")

    def _persist_breach(self, event: SLABreachEvent) -> None:
        sql = """
            INSERT INTO public.sla_breaches
                (stage, expected_sec, actual_sec, overrun_sec, severity, breach_time)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        event.stage,
                        event.expected_seconds,
                        round(event.actual_seconds, 3),
                        round(event.overrun_seconds, 3),
                        event.severity,
                        event.breach_time,
                    ),
                )
            conn.commit()
            logger.info("SLA breach persisted for stage=%s", event.stage)
        except Exception:
            logger.exception("Failed to persist SLA breach for stage=%s", event.stage)

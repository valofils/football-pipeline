"""SLA Monitor — tracks per-stage expected vs actual completion times."""
from __future__ import annotations

import time
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)

# Default SLA thresholds per stage (seconds)
DEFAULT_SLA_THRESHOLDS: Dict[str, int] = {
    "streaming_ingest": 5 * 60,
    "validate": 2 * 60,
    "load_postgres": 10 * 60,
    "dbt_transform": 15 * 60,
    "ml_train": 30 * 60,
    "batch_predict": 10 * 60,
}


@dataclass
class SLABreachEvent:
    stage: str
    expected_seconds: int
    actual_seconds: float
    breach_time: datetime = field(default_factory=datetime.utcnow)
    severity: str = "warning"

    def __post_init__(self) -> None:
        ratio = self.actual_seconds / max(self.expected_seconds, 1)
        if ratio >= 2.0:
            self.severity = "critical"
        elif ratio >= 1.5:
            self.severity = "high"
        else:
            self.severity = "warning"

    @property
    def overrun_seconds(self) -> float:
        return self.actual_seconds - self.expected_seconds

    def to_dict(self) -> dict:
        return {
            "stage": self.stage,
            "expected_seconds": self.expected_seconds,
            "actual_seconds": round(self.actual_seconds, 3),
            "overrun_seconds": round(self.overrun_seconds, 3),
            "breach_time": self.breach_time.isoformat(),
            "severity": self.severity,
        }


class SLAMonitor:
    """Tracks stage start/end times and fires breach events when SLA is exceeded."""

    def __init__(
        self,
        thresholds: Optional[Dict[str, int]] = None,
        on_breach: Optional[Callable[[SLABreachEvent], None]] = None,
    ) -> None:
        self.thresholds: Dict[str, int] = thresholds or DEFAULT_SLA_THRESHOLDS.copy()
        self._start_times: Dict[str, float] = {}
        self._breach_callbacks: list[Callable[[SLABreachEvent], None]] = []
        if on_breach:
            self._breach_callbacks.append(on_breach)

    def add_breach_callback(self, cb: Callable[[SLABreachEvent], None]) -> None:
        self._breach_callbacks.append(cb)

    def stage_started(self, stage: str) -> None:
        self._start_times[stage] = time.monotonic()
        logger.info("SLA tracking started for stage=%s", stage)

    def stage_completed(self, stage: str) -> Optional[SLABreachEvent]:
        if stage not in self._start_times:
            logger.warning("stage_completed called for untracked stage=%s", stage)
            return None

        elapsed = time.monotonic() - self._start_times.pop(stage)
        threshold = self.thresholds.get(stage)

        if threshold is None:
            logger.debug("No SLA threshold configured for stage=%s", stage)
            return None

        logger.info(
            "Stage %s completed in %.1fs (SLA=%ds)", stage, elapsed, threshold
        )

        if elapsed > threshold:
            event = SLABreachEvent(
                stage=stage,
                expected_seconds=threshold,
                actual_seconds=elapsed,
            )
            logger.warning("SLA BREACH: %s", event.to_dict())
            for cb in self._breach_callbacks:
                try:
                    cb(event)
                except Exception:
                    logger.exception("Breach callback failed for stage=%s", stage)
            return event

        return None

    def check_in_progress(self, stage: str) -> Optional[SLABreachEvent]:
        """Check if a currently-running stage has already exceeded its SLA."""
        if stage not in self._start_times:
            return None
        elapsed = time.monotonic() - self._start_times[stage]
        threshold = self.thresholds.get(stage)
        if threshold is not None and elapsed > threshold:
            return SLABreachEvent(
                stage=stage,
                expected_seconds=threshold,
                actual_seconds=elapsed,
            )
        return None

    def set_threshold(self, stage: str, seconds: int) -> None:
        self.thresholds[stage] = seconds

    def get_threshold(self, stage: str) -> Optional[int]:
        return self.thresholds.get(stage)

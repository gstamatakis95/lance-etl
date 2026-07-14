"""Low-cardinality Datadog emission for reconciler queue and retention SLOs."""

from __future__ import annotations

from dataclasses import dataclass

from lance_etl.reconciler.service import SloStatus
from lance_etl.telemetry import Telemetry


@dataclass(frozen=True, slots=True)
class TelemetrySloEmitter:
    """Emit reconciler SLO gauges without tenant or work identifiers."""

    telemetry: Telemetry

    def emit(self, status: SloStatus) -> None:
        """Emit one evaluated SLO status as infallible gauges.

        Args:
            status: Low-cardinality evaluated status.
        """
        self.telemetry.gauge("reconciler.healthy", float(status.healthy))
        self.telemetry.gauge("reconciler.due_work", float(status.due_work))
        self.telemetry.gauge("reconciler.blocked_work", float(status.blocked_work))
        self.telemetry.gauge("reconciler.blocked_source_windows", float(status.blocked_source_windows))
        self.telemetry.gauge("reconciler.oldest_open_age_seconds", status.oldest_open_age_seconds)
        self.telemetry.gauge("reconciler.retention_age_seconds", status.retention_age_seconds)

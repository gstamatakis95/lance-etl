"""Tests for the Lance trace-event bridge in telemetry.

Asserts that the object-store request-count field (``requests``, emitted by lance-datafusion's execution-stats trace
event distinct from the coalesced ``iops``) is forwarded as a Datadog distribution alongside the other execution stats.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from lance_etl.telemetry import EXECUTION_DISTRIBUTION_KEYS, Telemetry, build_lance_event_callback


def fire_execution_event(telemetry: Telemetry, args: dict[str, str]) -> None:
    """Build the bridge callback and fire one execution trace event through it.

    Args:
        telemetry: The telemetry facade the bridge emits through.
        args: The trace event argument strings.
    """
    callback = build_lance_event_callback(telemetry)
    event = SimpleNamespace(target="lance_datafusion::exec::execution", args=args)
    callback(event)


def test_requests_is_a_bridged_distribution_key() -> None:
    """The request-count field is in the execution distribution key set, alongside iops."""
    assert "requests" in EXECUTION_DISTRIBUTION_KEYS
    assert "iops" in EXECUTION_DISTRIBUTION_KEYS


def test_requests_forwarded_as_distribution() -> None:
    """An execution event with a requests field emits a lance.execution.requests distribution."""
    statsd: MagicMock = MagicMock()
    telemetry: Telemetry = Telemetry(MagicMock(), statsd, MagicMock())
    fire_execution_event(telemetry, {"iops": "3", "requests": "5", "bytes_read": "100"})

    distributed: dict[str, float] = {call.args[0]: call.args[1] for call in statsd.distribution.call_args_list}
    assert distributed["lance.execution.requests"] == 5.0
    assert distributed["lance.execution.iops"] == 3.0
    assert distributed["lance.execution.bytes_read"] == 100.0

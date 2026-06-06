"""Datadog telemetry helpers shared by the Lance pipeline jobs.

Provides traces (ddtrace), metrics (DogStatsD), and trace-correlated logs, and bridges Lance's own structured trace
events into Datadog. Telemetry clients are created per process via :meth:`Telemetry.create`, so the lightweight
:class:`TelemetryConfig` is the only object pickled into executor closures.

Lance emits structured trace events (file audits, dataset events, object-store throttling, index I/O, and execution
stats) through a non-blocking callback that :func:`attach_lance_event_bridge` registers. The bridge turns those events
into Datadog counters, gauges, and distributions and forwards them as logs. The bridge attaches automatically the first
time :meth:`Telemetry.create` runs in a process, so driver and executors both report Lance internals. The performance
tuning that the Lance guide describes (compute and I/O thread pools, the scan read-ahead buffer, log and trace levels)
is applied through :class:`LanceRuntimeConfig` and :func:`apply_lance_runtime`.

Assumes a Datadog Agent reachable from every node for DogStatsD on the configured host and port. The ddtrace, datadog,
and lance imports use version and availability fallbacks. Adjust the import block if installed versions differ.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from datadog.dogstatsd import DogStatsd

try:
    from ddtrace.trace import tracer as dd_tracer
except ImportError:
    from ddtrace import tracer as dd_tracer

try:
    from ddtrace.trace import Context as DDContext
except ImportError:
    from ddtrace.context import Context as DDContext

try:
    from lance.tracing import capture_trace_events
except ImportError:
    capture_trace_events = None

logger: logging.Logger = logging.getLogger(__name__)

EXECUTION_DISTRIBUTION_KEYS: tuple[str, ...] = (
    "output_rows",
    "iops",
    "bytes_read",
    "indices_loaded",
    "parts_loaded",
    "index_comparisons",
)
THROTTLE_GAUGE_KEYS: tuple[str, ...] = ("previous_rate", "new_rate")
EVENT_TAG_KEYS: tuple[str, ...] = ("type", "mode", "event", "operation")
HIGH_VOLUME_EVENTS: tuple[str, ...] = ("execution", "io_events")

lance_bridge_attached: bool = False


@dataclass
class TelemetryConfig:
    """Picklable configuration for :class:`Telemetry`.

    Attributes:
        service: Datadog service name.
        env: Deployment environment tag.
        version: Optional code version tag.
        statsd_host: Host of the local Datadog Agent DogStatsD endpoint.
        statsd_port: Port of the DogStatsD endpoint.
        metric_prefix: Namespace prepended to every metric name.
        constant_tags: Tags attached to every metric, each formatted as a DogStatsD ``"key:value"`` string (e.g.
            ``["team:data", "region:us-east-1"]``). Passing plain keys without values is accepted by the DogStatsD
            protocol but loses the value dimension. Always include the colon and value.
    """

    service: str = "lance-pipeline"
    env: str = "prod"
    version: str = ""
    statsd_host: str = "localhost"
    statsd_port: int = 8125
    metric_prefix: str = "lance.pipeline"
    constant_tags: list[str] = field(default_factory=list)


@dataclass
class LanceRuntimeConfig:
    """Lance runtime tuning drawn from the Lance performance guide.

    The thread-pool and buffer settings must be in the environment before Lance initializes its pools, so on Spark they
    belong in the executor environment rather than being set inside a task. :func:`apply_lance_runtime` sets them on the
    current process for the driver, and returns the variables it set so the caller can mirror them onto executors.

    Attributes:
        cpu_threads: Value for ``LANCE_CPU_THREADS``. Set this below the executor core count when several tasks share
            an executor to avoid compute oversubscription.
        io_threads: Value for ``LANCE_IO_THREADS``. Cloud stores often need 128 or 256 to saturate bandwidth.
        io_buffer_size_bytes: Value for ``LANCE_DEFAULT_IO_BUFFER_SIZE``. Raise it alongside the I/O thread count.
        lance_log: Value for ``LANCE_LOG`` controlling log filtering by level and target.
        lance_tracing: Value for ``LANCE_TRACING`` controlling the trace level the event bridge observes. The key events
            are emitted at ``info``.
    """

    cpu_threads: int | None = None
    io_threads: int | None = None
    io_buffer_size_bytes: int | None = None
    lance_log: str | None = None
    lance_tracing: str | None = None

    def as_env(self) -> dict[str, str]:
        """Return the environment variables for the set fields.

        Returns:
            A mapping of Lance environment variable names to string values,
            containing only the fields that are set.
        """
        candidates: list[tuple[str, int | str | None]] = [
            ("LANCE_CPU_THREADS", self.cpu_threads),
            ("LANCE_IO_THREADS", self.io_threads),
            ("LANCE_DEFAULT_IO_BUFFER_SIZE", self.io_buffer_size_bytes),
            ("LANCE_LOG", self.lance_log),
            ("LANCE_TRACING", self.lance_tracing),
        ]
        return {name: str(value) for name, value in candidates if value is not None}


def apply_lance_runtime(config: LanceRuntimeConfig) -> dict[str, str]:
    """Apply Lance runtime tuning to the current process environment.

    Args:
        config: The runtime tuning to apply.

    Returns:
        The environment variables that were set, for mirroring onto executors.
    """
    variables: dict[str, str] = config.as_env()
    for name, value in variables.items():
        os.environ[name] = value
    return variables


def commit_with_retries(
    action: Callable[[], Any],
    retries: int,
    backoff_seconds: float,
    on_conflict: Callable[[], None] | None = None,
) -> Any:
    """Run a commit action, retrying optimistic-concurrency conflicts.

    The action should re-read any dataset state it needs so each retry observes the latest committed version. Backoff
    doubles per attempt up to a cap. Lance surfaces commit conflicts as ``OSError`` or ``RuntimeError`` with a message
    containing ``"Commit conflict"`` or ``"Retryable commit conflict"``.

    Args:
        action: The commit to attempt, returning any result.
        retries: Conflict retries allowed beyond the first attempt.
        backoff_seconds: Base backoff in seconds between retries.
        on_conflict: Called on each conflict, for example to emit a metric.

    Returns:
        Whatever ``action`` returns on its first non-conflicting attempt.

    Raises:
        OSError | RuntimeError: The original exception if every attempt conflicts.
    """
    last_exc: BaseException | None = None
    for attempt in range(retries + 1):
        try:
            return action()
        except (OSError, RuntimeError) as exc:
            msg = str(exc)
            if "Commit conflict" not in msg and "Retryable commit conflict" not in msg:
                raise
            last_exc = exc
            if on_conflict is not None:
                on_conflict()
            time.sleep(backoff_seconds * (2 ** min(attempt, 6)))
    assert last_exc is not None
    raise last_exc


def build_lance_event_callback(telemetry: Telemetry) -> object:
    """Build the callback that reports Lance trace events to Datadog.

    Args:
        telemetry: The telemetry facade used to emit metrics and logs.

    Returns:
        A callable suitable for :func:`lance.tracing.capture_trace_events`.
    """

    def on_event(event: object) -> None:
        """Translate one Lance trace event into Datadog metrics and a log.

        Args:
            event: A Lance ``TraceEvent`` carrying a target and string args.
        """
        try:
            target: str = event.target
            args: dict[str, str] = dict(event.args)
        except (AttributeError, TypeError):
            return
        short: str = target.rsplit("::", 1)[-1]
        base_tags: list[str] = [f"event:{short}"]
        telemetry.incr("lance.event", tags=base_tags)

        for key in EVENT_TAG_KEYS:
            value: str | None = args.get(key)
            if value:
                telemetry.incr(f"lance.{short}", tags=base_tags + [f"{key}:{value}"])

        if short == "execution":
            for key in EXECUTION_DISTRIBUTION_KEYS:
                raw: str | None = args.get(key)
                if raw is not None:
                    try:
                        telemetry.distribution(f"lance.execution.{key}", float(raw), tags=base_tags)
                    except ValueError:
                        continue
        elif short == "throttle":
            for key in THROTTLE_GAUGE_KEYS:
                raw = args.get(key)
                if raw is not None:
                    try:
                        telemetry.gauge(f"lance.throttle.{key}", float(raw), tags=base_tags)
                    except ValueError:
                        continue
            if args.get("error"):
                telemetry.incr("lance.throttle.error", tags=base_tags)
                logger.warning("lance object store throttle: %s", args)

        if short in HIGH_VOLUME_EVENTS:
            logger.debug("lance event target=%s args=%s", target, args)
        else:
            logger.info("lance event target=%s args=%s", target, args)

    return on_event


def attach_lance_event_bridge(telemetry: Telemetry) -> bool:
    """Register the Lance trace-event bridge once per process.

    Subsequent calls within the same process are no-ops, and the call is a no-op when the installed pylance does not
    expose the trace-event API.

    Args:
        telemetry: The telemetry facade the bridge emits through.

    Returns:
        ``True`` if the bridge was registered by this call.
    """
    global lance_bridge_attached
    if lance_bridge_attached or capture_trace_events is None:
        return False
    capture_trace_events(build_lance_event_callback(telemetry))
    lance_bridge_attached = True
    logger.info("lance trace-event bridge attached")
    return True


class TraceContextFilter(logging.Filter):
    """Logging filter that injects the active Datadog trace and span ids."""

    def __init__(self, tracer: object) -> None:
        """Initialize the filter.

        Args:
            tracer: The ddtrace tracer to read the active context from.
        """
        super().__init__()
        self.tracer: object = tracer

    def filter(self, record: logging.LogRecord) -> bool:
        """Attach ``dd_trace_id`` and ``dd_span_id`` to the record.

        Args:
            record: The log record being emitted.

        Returns:
            Always true so the record is kept.
        """
        context = self.tracer.current_trace_context()
        record.dd_trace_id = context.trace_id if context is not None else 0
        record.dd_span_id = context.span_id if context is not None else 0
        return True


def configure_logging(config: TelemetryConfig, level: int = logging.INFO) -> None:
    """Configure trace-correlated structured logging on the current process.

    Args:
        config: Telemetry configuration supplying service and env tags.
        level: Root log level.
    """
    handler: logging.StreamHandler = logging.StreamHandler()
    fmt: str = (
        "%(asctime)s %(levelname)s %(name)s "
        f"[dd.service={config.service} dd.env={config.env} "
        "dd.trace_id=%(dd_trace_id)s dd.span_id=%(dd_span_id)s] %(message)s"
    )
    handler.setFormatter(logging.Formatter(fmt))
    handler.addFilter(TraceContextFilter(dd_tracer))
    root: logging.Logger = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


class Telemetry:
    """Thin facade over the ddtrace tracer and a DogStatsD client."""

    def __init__(self, config: TelemetryConfig, statsd: DogStatsd, tracer: object) -> None:
        """Initialize the facade.

        Args:
            config: Telemetry configuration.
            statsd: A configured DogStatsD client.
            tracer: The ddtrace tracer.
        """
        self.config: TelemetryConfig = config
        self.statsd: DogStatsd = statsd
        self.tracer: object = tracer

    @classmethod
    def create(cls, config: TelemetryConfig, attach_lance_bridge: bool = True) -> Telemetry:
        """Build a telemetry facade for the current process.

        Call this on the driver and again inside each executor partition so the clients are local and never pickled. The
        Lance trace-event bridge is attached on the first call per process unless disabled.

        Args:
            config: Telemetry configuration.
            attach_lance_bridge: Whether to register the Lance event bridge.

        Returns:
            A ready telemetry facade.
        """
        tags: list[str] = list(config.constant_tags)
        tags.append(f"env:{config.env}")
        tags.append(f"service:{config.service}")
        if config.version:
            tags.append(f"version:{config.version}")
        statsd: DogStatsd = DogStatsd(
            host=config.statsd_host,
            port=config.statsd_port,
            namespace=config.metric_prefix,
            constant_tags=tags,
        )
        telemetry: Telemetry = cls(config, statsd, dd_tracer)
        if attach_lance_bridge:
            attach_lance_event_bridge(telemetry)
        return telemetry

    @contextmanager
    def span(self, name: str, resource: str | None = None, tags: dict[str, object] | None = None) -> Iterator[object]:
        """Open a trace span as a context manager.

        Args:
            name: Span operation name.
            resource: Optional resource label.
            tags: Optional span tags.

        Yields:
            The active span.
        """
        with self.tracer.trace(name, service=self.config.service, resource=resource) as active:
            for key, value in (tags or {}).items():
                active.set_tag(key, value)
            yield active

    def incr(self, name: str, value: float = 1, tags: list[str] | None = None) -> None:
        """Increment a counter metric.

        Args:
            name: Metric name relative to the configured prefix.
            value: Amount to add.
            tags: Optional per-call tags.
        """
        self.statsd.increment(name, value, tags=tags)

    def gauge(self, name: str, value: float, tags: list[str] | None = None) -> None:
        """Record a gauge metric.

        Args:
            name: Metric name relative to the configured prefix.
            value: Current value.
            tags: Optional per-call tags.
        """
        self.statsd.gauge(name, value, tags=tags)

    def distribution(self, name: str, value: float, tags: list[str] | None = None) -> None:
        """Record a distribution metric for global percentiles.

        Args:
            name: Metric name relative to the configured prefix.
            value: Sample value.
            tags: Optional per-call tags.
        """
        self.statsd.distribution(name, value, tags=tags)

    def event(self, name: str, tags: list[str] | None = None) -> None:
        """Record a discrete domain event as a counter.

        Args:
            name: Event metric name relative to the configured prefix.
            tags: Optional per-call tags.
        """
        self.statsd.increment(name, 1, tags=tags)

    def error(self, message: str, tags: list[str] | None = None) -> None:
        """Log the current exception and increment an error counter.

        Call this from an ``except`` block. The active exception and traceback are logged before the caller re-raises to
        fail fast.

        Args:
            message: A description of the failed operation.
            tags: Optional per-call tags.
        """
        logger.exception(message)
        self.statsd.increment("errors", 1, tags=tags)

    @contextmanager
    def timed(self, name: str, tags: list[str] | None = None) -> Iterator[None]:
        """Time a block and record its duration as a distribution in ms.

        Args:
            name: Metric name relative to the configured prefix.
            tags: Optional per-call tags.

        Yields:
            Control to the timed block.
        """
        started: float = time.perf_counter()
        try:
            yield
        finally:
            self.distribution(name, (time.perf_counter() - started) * 1000.0, tags=tags)

    def current_context_ids(self) -> tuple[int, int] | None:
        """Return the active trace and span ids for executor propagation.

        Returns:
            A ``(trace_id, span_id)`` tuple, or ``None`` when no span is active.
        """
        context = self.tracer.current_trace_context()
        if context is None:
            return None
        return (context.trace_id, context.span_id)

    def activate(self, trace_id: int, span_id: int) -> None:
        """Activate a remote trace context so new spans link to the driver.

        Args:
            trace_id: Trace id captured on the driver.
            span_id: Span id captured on the driver.
        """
        self.tracer.context_provider.activate(DDContext(trace_id=trace_id, span_id=span_id))

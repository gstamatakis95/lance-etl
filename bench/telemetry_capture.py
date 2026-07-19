"""Local telemetry capture for benchmark runs.

Provides three independent capture components that together record all telemetry emitted by the
Python pipeline jobs and the Rust gRPC search service to plain files under a workspace telemetry
directory:

- :class:`DogStatsDListener` — asyncio UDP server that parses DogStatsD datagrams and appends
  JSON lines to ``telemetry/metrics.jsonl``.
- :class:`OtlpGrpcReceiver` — minimal gRPC server implementing ``opentelemetry.proto.collector``
  that decodes ``ExportTraceServiceRequest`` protobuf messages and appends span JSON lines to
  ``telemetry/traces.jsonl``.
- :class:`TelemetryCapture` — context manager that starts all listeners, sets the required
  environment variables for child processes, and stops everything cleanly on exit.

None of the components block or raise when their sockets are unavailable: the DogStatsD emitters
in both Python and Rust are documented as infallible fire-and-forget UDP, so a missing listener
never breaks a pipeline run.

Environment variables configured by :meth:`TelemetryCapture.env_overrides`:

Python (``TelemetryConfig`` fields, passed to worker processes):
  ``LANCE_BENCH_STATSD_HOST``, ``LANCE_BENCH_STATSD_PORT``

Rust (read by ``config.rs``):
  ``SEARCH_API_STATSD_ADDR``, ``OTEL_EXPORTER_OTLP_ENDPOINT``

Rust telemetry can also be disabled entirely via ``SEARCH_API_TELEMETRY_DISABLED=true`` (not
set here; the capture infrastructure replaces the Datadog agent, not the Rust telemetry).

See ``bench/TELEMETRY.md`` for full documentation and jq recipes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import threading
import time
from concurrent import futures
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import grpc
from google.protobuf import json_format
from opentelemetry.proto.collector.trace.v1 import (
    trace_service_pb2,
    trace_service_pb2_grpc,
)

logger: logging.Logger = logging.getLogger(__name__)

DEFAULT_STATSD_HOST: str = "127.0.0.1"
DEFAULT_STATSD_PORT: int = 19125
DEFAULT_OTLP_PORT: int = 14317


def parse_dogstatsd_datagram(raw: bytes, received_at: float) -> dict[str, Any] | None:
    """Parse one DogStatsD UDP datagram into a JSON-serializable record.

    Handles the wire format ``<name>:<value>|<type>[|@<rate>][|#<tags>]``. Multi-metric
    datagrams (newline-delimited) are each parsed independently; this function handles
    exactly one metric line.  Returns ``None`` when the line does not match the expected
    format or is blank.

    Supported type codes: ``c`` (counter), ``g`` (gauge), ``ms`` (timer), ``h``
    (histogram), ``s`` (set), ``d`` (distribution).  Unknown types are recorded verbatim
    so no data is silently dropped.

    Sampled datagrams include ``|@<rate>`` between the type and tag section.  The sample
    rate is preserved in the output record but does not affect parsing.

    Args:
        raw: The raw UDP datagram payload bytes (UTF-8).
        received_at: POSIX timestamp at which the datagram arrived.

    Returns:
        A dictionary with keys ``received_at``, ``name``, ``value``, ``metric_type``,
        ``sample_rate``, and ``tags``, or ``None`` when the line cannot be parsed.
    """
    text: str = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return None
    colon_idx: int = text.find(":")
    if colon_idx < 0:
        return None
    name: str = text[:colon_idx]
    rest: str = text[colon_idx + 1 :]
    pipe_idx: int = rest.find("|")
    if pipe_idx < 0:
        return None
    raw_value: str = rest[:pipe_idx]
    remainder: str = rest[pipe_idx + 1 :]
    parts: list[str] = remainder.split("|")
    metric_type: str = parts[0] if parts else ""
    sample_rate: float = 1.0
    tags: list[str] = []
    for part in parts[1:]:
        if part.startswith("@"):
            with suppress(ValueError):
                sample_rate = float(part[1:])
        elif part.startswith("#"):
            tags = [t.strip() for t in part[1:].split(",") if t.strip()]
    try:
        value: float = float(raw_value)
    except ValueError:
        value = 0.0
    return {
        "received_at": received_at,
        "name": name,
        "value": value,
        "metric_type": metric_type,
        "sample_rate": sample_rate,
        "tags": tags,
    }


class DogStatsDProtocol(asyncio.DatagramProtocol):
    """asyncio datagram protocol that parses DogStatsD packets and appends them as JSON lines.

    One datagram can carry multiple newline-delimited metric lines (the DogStatsD multi-metric
    extension). Each line is parsed independently. Unparseable lines are counted and logged at
    debug level rather than silently dropped.

    Args:
        output_path: The file to append JSON records to.
    """

    def __init__(self, output_path: Path) -> None:
        """Initialise the protocol.

        Args:
            output_path: JSON-lines output file. Created if it does not exist.
        """
        self.output_path: Path = output_path
        self.dropped: int = 0
        self.received: int = 0

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Record the transport reference on connection establishment.

        Args:
            transport: The UDP transport provided by asyncio.
        """
        self.transport: asyncio.BaseTransport = transport

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        """Parse and persist one UDP datagram.

        Multi-metric datagrams are split on newlines and each line is parsed
        independently.

        Args:
            data: The raw datagram payload.
            addr: The sender address (ignored).
        """
        now: float = time.time()
        lines: list[bytes] = data.split(b"\n")
        with self.output_path.open("a", encoding="utf-8") as fh:
            for line in lines:
                record: dict[str, Any] | None = parse_dogstatsd_datagram(line, now)
                if record is None:
                    if line.strip():
                        self.dropped += 1
                        logger.debug("DogStatsD: unparseable line from %s: %r", addr, line)
                    continue
                self.received += 1
                fh.write(json.dumps(record) + "\n")

    def error_received(self, exc: Exception) -> None:
        """Log a transport-level error without raising.

        Args:
            exc: The exception from the asyncio transport.
        """
        logger.warning("DogStatsD socket error: %s", exc)

    def connection_lost(self, exc: Exception | None) -> None:
        """Log unexpected connection closure.

        Args:
            exc: The exception that caused the loss, or ``None`` for a clean close.
        """
        if exc is not None:
            logger.debug("DogStatsD connection lost: %s", exc)


class DogStatsDListener:
    """Asyncio UDP listener for DogStatsD datagrams.

    Binds a loopback UDP socket on the configured host and port, runs an asyncio event loop in a
    background thread, and writes parsed metric records as JSON lines to
    ``{telemetry_dir}/metrics.jsonl``.

    Both the Python pipeline jobs (via ``datadog.dogstatsd.DogStatsd``) and the Rust service (via
    ``cadence``'s buffered UDP sink) direct their metrics to this address when the capture
    environment variables are set.

    Args:
        telemetry_dir: Directory that will receive ``metrics.jsonl``.
        host: UDP bind address (loopback only).
        port: UDP port to listen on.
    """

    def __init__(
        self,
        telemetry_dir: Path,
        host: str = DEFAULT_STATSD_HOST,
        port: int = DEFAULT_STATSD_PORT,
    ) -> None:
        """Initialise the listener.

        Args:
            telemetry_dir: Output directory.
            host: Bind address.
            port: Bind port.
        """
        self.telemetry_dir: Path = telemetry_dir
        self.host: str = host
        self.port: int = port
        self.output_path: Path = telemetry_dir / "metrics.jsonl"
        self.loop: asyncio.AbstractEventLoop | None = None
        self.thread: threading.Thread | None = None
        self.transport: asyncio.BaseTransport | None = None

    def start(self) -> None:
        """Start the UDP listener in a background daemon thread.

        Creates the telemetry directory and output file, then binds the UDP socket and runs the
        asyncio loop until :meth:`stop` is called.  A ready event is set once the transport is
        bound so the caller can wait for the listener to be accepting packets.
        """
        self.telemetry_dir.mkdir(parents=True, exist_ok=True)
        self.output_path.touch(exist_ok=True)
        ready: threading.Event = threading.Event()
        self.loop = asyncio.new_event_loop()

        def run_loop() -> None:
            """Run the asyncio event loop until stop() signals it."""
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self.bind_and_signal(ready))
            self.loop.run_forever()
            self.loop.close()

        self.thread = threading.Thread(target=run_loop, daemon=True, name="statsd-listener")
        self.thread.start()
        ready.wait(timeout=5.0)
        logger.info("DogStatsD listener bound on %s:%d -> %s", self.host, self.port, self.output_path)

    async def bind_and_signal(self, ready: threading.Event) -> None:
        """Bind the UDP socket and signal readiness.

        Args:
            ready: Event to set once the transport is created.
        """
        protocol = DogStatsDProtocol(self.output_path)
        transport, _ = await self.loop.create_datagram_endpoint(
            lambda: protocol,
            local_addr=(self.host, self.port),
            family=socket.AF_INET,
            reuse_port=False,
        )
        self.transport = transport
        ready.set()

    def stop(self) -> None:
        """Close the UDP socket and stop the background thread."""
        if self.transport is not None:
            self.loop.call_soon_threadsafe(self.transport.close)
        if self.loop is not None:
            self.loop.call_soon_threadsafe(self.loop.stop)
        if self.thread is not None:
            self.thread.join(timeout=5.0)
        logger.info("DogStatsD listener stopped")


class OtlpTraceServicer(trace_service_pb2_grpc.TraceServiceServicer):
    """gRPC servicer implementing the OTLP ExportTraceService.

    Decodes ``ExportTraceServiceRequest`` messages, converts each span to a JSON-serializable
    dictionary, and appends one JSON line per span to ``{telemetry_dir}/traces.jsonl``.  The
    ``message`` field of each line contains the full span as rendered by
    ``google.protobuf.json_format.MessageToDict``.

    Args:
        output_path: The file to append span JSON lines to.
    """

    def __init__(self, output_path: Path) -> None:
        """Initialise the servicer.

        Args:
            output_path: JSON-lines output file for decoded spans.
        """
        self.output_path: Path = output_path
        self.span_count: int = 0

    def Export(
        self,
        request: trace_service_pb2.ExportTraceServiceRequest,
        context: grpc.ServicerContext,
    ) -> trace_service_pb2.ExportTraceServiceResponse:
        """Accept one OTLP export batch and write spans to the output file.

        Each ``ResourceSpans`` entry may carry multiple ``ScopeSpans``, each of which may carry
        multiple ``Span`` messages.  Every span is written as one JSON line with a
        ``received_at`` POSIX timestamp prepended.

        Args:
            request: The decoded ``ExportTraceServiceRequest``.
            context: gRPC server context (unused).

        Returns:
            An empty ``ExportTraceServiceResponse`` indicating success.
        """
        del context
        now: float = time.time()
        with self.output_path.open("a", encoding="utf-8") as fh:
            for resource_spans in request.resource_spans:
                resource_dict: dict[str, Any] = json_format.MessageToDict(
                    resource_spans.resource,
                    preserving_proto_field_name=True,
                )
                for scope_spans in resource_spans.scope_spans:
                    for span in scope_spans.spans:
                        span_dict: dict[str, Any] = json_format.MessageToDict(
                            span,
                            preserving_proto_field_name=True,
                        )
                        record: dict[str, Any] = {
                            "received_at": now,
                            "resource": resource_dict,
                            "span": span_dict,
                        }
                        fh.write(json.dumps(record) + "\n")
                        self.span_count += 1
        return trace_service_pb2.ExportTraceServiceResponse()


class OtlpGrpcReceiver:
    """Minimal OTLP gRPC receiver that captures Rust service spans to a JSONL file.

    Binds a gRPC server on the configured port implementing only the
    ``opentelemetry.proto.collector.trace.v1.TraceService`` service, which is the single
    endpoint the Rust tonic-based OTLP exporter contacts.  Span records are appended as JSON
    lines to ``{telemetry_dir}/traces.jsonl``.

    The Rust service is pointed at this receiver by setting
    ``OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:{port}`` before the process starts.  The
    tonic exporter uses gRPC transport; HTTP/protobuf is not supported.

    Args:
        telemetry_dir: Directory that will receive ``traces.jsonl``.
        host: Bind address (loopback).
        port: gRPC port to listen on.
    """

    def __init__(
        self,
        telemetry_dir: Path,
        host: str = DEFAULT_STATSD_HOST,
        port: int = DEFAULT_OTLP_PORT,
    ) -> None:
        """Initialise the receiver.

        Args:
            telemetry_dir: Output directory.
            host: Bind address.
            port: Bind port.
        """
        self.telemetry_dir: Path = telemetry_dir
        self.host: str = host
        self.port: int = port
        self.output_path: Path = telemetry_dir / "traces.jsonl"
        self.server: grpc.Server | None = None
        self.servicer: OtlpTraceServicer | None = None

    def start(self) -> None:
        """Start the gRPC server in a background thread pool.

        Creates the telemetry directory and output file, then binds the gRPC server and starts
        serving.  The server runs in a ``futures.ThreadPoolExecutor`` with two workers, which is
        sufficient for the low-rate benchmark OTLP export.
        """
        self.telemetry_dir.mkdir(parents=True, exist_ok=True)
        self.output_path.touch(exist_ok=True)
        self.servicer = OtlpTraceServicer(self.output_path)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        trace_service_pb2_grpc.add_TraceServiceServicer_to_server(self.servicer, self.server)
        self.server.add_insecure_port(f"{self.host}:{self.port}")
        self.server.start()
        logger.info("OTLP gRPC receiver bound on %s:%d -> %s", self.host, self.port, self.output_path)

    def stop(self) -> None:
        """Stop the gRPC server with a short grace period."""
        if self.server is not None:
            self.server.stop(grace=2.0)
        logger.info("OTLP gRPC receiver stopped (captured %d spans)", self.servicer.span_count if self.servicer else 0)


@dataclass
class CaptureConfig:
    """Configuration for the telemetry capture session.

    Attributes:
        telemetry_dir: Directory receiving all capture output files.
        statsd_host: Loopback address for the DogStatsD UDP listener.
        statsd_port: UDP port for the DogStatsD listener.
        otlp_port: gRPC port for the OTLP trace receiver.
        capture_metrics: Whether to start the DogStatsD listener.
        capture_traces: Whether to start the OTLP gRPC receiver.
    """

    telemetry_dir: Path
    statsd_host: str = DEFAULT_STATSD_HOST
    statsd_port: int = DEFAULT_STATSD_PORT
    otlp_port: int = DEFAULT_OTLP_PORT
    capture_metrics: bool = True
    capture_traces: bool = True
    extra_env: dict[str, str] = field(default_factory=dict)


class TelemetryCapture:
    """Context manager that starts capture listeners and sets environment variables for child processes.

    On entry, starts the configured listeners (DogStatsD and/or OTLP) and returns an
    ``env_overrides`` dict that must be merged into the environment of any child processes
    (Spark, gRPC server) that should emit to the capture listeners.  On exit, the listeners are
    stopped gracefully.

    When ``capture_metrics`` is False or ``capture_traces`` is False the corresponding listener
    is not started and the corresponding env var is not emitted — the child process falls back to
    its default (usually ``localhost:8125``/``localhost:4317``), which is a no-op if nothing
    listens there.  This means that disabling capture never fails a pipeline run.

    Usage::

        capture_cfg = CaptureConfig(telemetry_dir=workspace / "telemetry")
        with TelemetryCapture(capture_cfg) as capture:
            env = {**os.environ, **capture.env_overrides}
            subprocess.run(["python", "-m", "bench", "e2e", ...], env=env)

    Args:
        config: Capture configuration.
    """

    def __init__(self, config: CaptureConfig) -> None:
        """Initialise the capture session.

        Args:
            config: Capture configuration.
        """
        self.config: CaptureConfig = config
        self.statsd_listener: DogStatsDListener | None = None
        self.otlp_receiver: OtlpGrpcReceiver | None = None
        self.env_overrides: dict[str, str] = {}

    def start(self) -> None:
        """Start all configured capture listeners and populate ``env_overrides``.

        Listeners that fail to bind log a warning and are skipped.  The env overrides dict
        is populated only for listeners that started successfully.
        """
        self.config.telemetry_dir.mkdir(parents=True, exist_ok=True)
        if self.config.capture_metrics:
            try:
                self.statsd_listener = DogStatsDListener(
                    self.config.telemetry_dir,
                    host=self.config.statsd_host,
                    port=self.config.statsd_port,
                )
                self.statsd_listener.start()
                statsd_addr: str = f"{self.config.statsd_host}:{self.config.statsd_port}"
                self.env_overrides["SEARCH_API_STATSD_ADDR"] = statsd_addr
                self.env_overrides["LANCE_BENCH_STATSD_HOST"] = self.config.statsd_host
                self.env_overrides["LANCE_BENCH_STATSD_PORT"] = str(self.config.statsd_port)
            except OSError as exc:
                logger.warning("DogStatsD listener failed to start (metrics will not be captured): %s", exc)
                self.statsd_listener = None
        if self.config.capture_traces:
            try:
                self.otlp_receiver = OtlpGrpcReceiver(
                    self.config.telemetry_dir,
                    host=self.config.statsd_host,
                    port=self.config.otlp_port,
                )
                self.otlp_receiver.start()
                self.env_overrides["OTEL_EXPORTER_OTLP_ENDPOINT"] = (
                    f"http://{self.config.statsd_host}:{self.config.otlp_port}"
                )
            except Exception as exc:
                logger.warning("OTLP gRPC receiver failed to start (traces will not be captured): %s", exc)
                self.otlp_receiver = None
        self.env_overrides.update(self.config.extra_env)

    def stop(self) -> None:
        """Stop all running capture listeners."""
        if self.statsd_listener is not None:
            self.statsd_listener.stop()
        if self.otlp_receiver is not None:
            self.otlp_receiver.stop()

    def __enter__(self) -> TelemetryCapture:
        """Start capture and return self.

        Returns:
            This instance (access ``env_overrides`` after entry).
        """
        self.start()
        return self

    def __exit__(self, exc_type: type | None, exc_val: BaseException | None, exc_tb: object | None) -> None:
        """Stop capture regardless of whether the body raised.

        Args:
            exc_type: Exception type, if any.
            exc_val: Exception value, if any.
            exc_tb: Traceback, if any.
        """
        self.stop()


@contextmanager
def capture_telemetry(
    telemetry_dir: Path,
    statsd_port: int = DEFAULT_STATSD_PORT,
    otlp_port: int = DEFAULT_OTLP_PORT,
) -> Any:
    """Convenience context manager starting both listeners with default loopback binding.

    Yields a dictionary of environment variable overrides that must be applied to any child
    process that should emit telemetry to the capture listeners.

    Args:
        telemetry_dir: Directory for all capture output files.
        statsd_port: UDP port for the DogStatsD listener.
        otlp_port: gRPC port for the OTLP receiver.

    Yields:
        ``env_overrides`` dict.
    """
    cfg = CaptureConfig(
        telemetry_dir=telemetry_dir,
        statsd_port=statsd_port,
        otlp_port=otlp_port,
    )
    with TelemetryCapture(cfg) as capture:
        yield capture.env_overrides

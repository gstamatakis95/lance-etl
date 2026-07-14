"""Tests for the local telemetry capture helpers.

Covers:

- :func:`~bench.telemetry_capture.parse_dogstatsd_datagram`: pure-function parser exercised
  against representative wire-format inputs including counters, gauges, distributions, sampled
  lines, multi-tag lines, and malformed inputs.
- :class:`~bench.telemetry_capture.DogStatsDListener`: start/receive/stop round-trip over a
  loopback UDP socket, verifying that the JSON-lines file is populated.
- :class:`~bench.telemetry_capture.OtlpGrpcReceiver`: start/Export/stop round-trip using a
  synthetic ``ExportTraceServiceRequest``, verifying that span JSON lines are written.
- :class:`~bench.telemetry_capture.TelemetryCapture`: context-manager integration test
  verifying that ``env_overrides`` is populated and listeners are stopped cleanly on exit.

All network communication is loopback-only.  No external services are required.
"""

from __future__ import annotations

import json
import socket
import time
from pathlib import Path
from typing import Any

import grpc
import pytest
from opentelemetry.proto.collector.trace.v1 import (
    trace_service_pb2,
    trace_service_pb2_grpc,
)
from opentelemetry.proto.trace.v1 import trace_pb2

from bench.telemetry_capture import (
    CaptureConfig,
    DogStatsDListener,
    OtlpGrpcReceiver,
    TelemetryCapture,
    parse_dogstatsd_datagram,
)


def drain_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read all JSON lines from a file and return them as parsed dicts.

    Args:
        path: The JSON-lines file to read.

    Returns:
        A list of parsed record dicts, one per non-empty line.
    """
    lines: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            lines.append(json.loads(line))
    return lines


def send_udp(host: str, port: int, payload: bytes) -> None:
    """Send a single UDP datagram to a loopback address.

    Args:
        host: Destination host.
        port: Destination port.
        payload: Datagram payload bytes.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.sendto(payload, (host, port))


class TestParseDogstatsdDatagram:
    """Unit tests for the DogStatsD datagram parser."""

    def test_counter_no_tags(self) -> None:
        """A bare counter line is parsed with an empty tag list and sample_rate 1.0."""
        record = parse_dogstatsd_datagram(b"requests:42|c", 1000.0)
        assert record is not None
        assert record["name"] == "requests"
        assert record["value"] == 42.0
        assert record["metric_type"] == "c"
        assert record["sample_rate"] == 1.0
        assert record["tags"] == []
        assert record["received_at"] == 1000.0

    def test_gauge_with_tags(self) -> None:
        """A gauge line with DogStatsD tags is parsed and tags are split correctly."""
        record = parse_dogstatsd_datagram(b"cpu:0.75|g|#host:web1,env:prod", 2000.0)
        assert record is not None
        assert record["metric_type"] == "g"
        assert record["value"] == pytest.approx(0.75)
        assert "host:web1" in record["tags"]
        assert "env:prod" in record["tags"]

    def test_distribution_with_sample_rate_and_tags(self) -> None:
        """Sample rate and tags are both extracted when present together."""
        record = parse_dogstatsd_datagram(b"latency:12.5|d|@0.5|#rpc:search", 0.0)
        assert record is not None
        assert record["metric_type"] == "d"
        assert record["sample_rate"] == pytest.approx(0.5)
        assert record["tags"] == ["rpc:search"]

    def test_timer_type(self) -> None:
        """Timer type ``ms`` is preserved verbatim."""
        record = parse_dogstatsd_datagram(b"response_time:55|ms", 0.0)
        assert record is not None
        assert record["metric_type"] == "ms"

    def test_histogram_type(self) -> None:
        """Histogram type ``h`` is recognised."""
        record = parse_dogstatsd_datagram(b"queue_depth:7|h", 0.0)
        assert record is not None
        assert record["metric_type"] == "h"

    def test_set_type(self) -> None:
        """Set type ``s`` is recognised."""
        record = parse_dogstatsd_datagram(b"unique_users:abc|s", 0.0)
        assert record is not None
        assert record["metric_type"] == "s"
        assert record["value"] == 0.0

    def test_blank_line_returns_none(self) -> None:
        """A blank datagram payload returns ``None``."""
        assert parse_dogstatsd_datagram(b"", 0.0) is None
        assert parse_dogstatsd_datagram(b"   ", 0.0) is None

    def test_missing_pipe_returns_none(self) -> None:
        """A line with no pipe separator returns ``None``."""
        assert parse_dogstatsd_datagram(b"badline", 0.0) is None

    def test_missing_colon_returns_none(self) -> None:
        """A line with no colon separator returns ``None``."""
        assert parse_dogstatsd_datagram(b"no_colon|c", 0.0) is None

    def test_non_numeric_string_value_defaults_to_zero(self) -> None:
        """A truly non-numeric value field (not parseable as float) does not crash."""
        record = parse_dogstatsd_datagram(b"metric:not_a_number|c", 0.0)
        assert record is not None
        assert record["value"] == 0.0

    def test_malformed_sample_rate_is_ignored(self) -> None:
        """A non-numeric sample rate token does not crash — rate stays 1.0."""
        record = parse_dogstatsd_datagram(b"metric:1|c|@bad", 0.0)
        assert record is not None
        assert record["sample_rate"] == pytest.approx(1.0)

    def test_multi_tag_ordering_preserved(self) -> None:
        """All tags in a comma-separated list are returned in order."""
        record = parse_dogstatsd_datagram(b"m:1|c|#a:1,b:2,c:3", 0.0)
        assert record is not None
        assert record["tags"] == ["a:1", "b:2", "c:3"]

    def test_search_api_metric_roundtrip(self) -> None:
        """A representative Rust search-api metric parses without data loss."""
        raw = b"search_api.rpc.requests:1|c|#rpc:vector_search,status:ok"
        record = parse_dogstatsd_datagram(raw, 42.0)
        assert record is not None
        assert record["name"] == "search_api.rpc.requests"
        assert record["value"] == 1.0
        assert "rpc:vector_search" in record["tags"]
        assert "status:ok" in record["tags"]


class TestDogStatsDListener:
    """Integration tests for :class:`~bench.telemetry_capture.DogStatsDListener`."""

    def test_start_receive_stop(self, tmp_path: Path) -> None:
        """Listener writes parsed JSON lines for received DogStatsD datagrams."""
        port: int = 19200
        listener = DogStatsDListener(tmp_path, host="127.0.0.1", port=port)
        listener.start()
        try:
            send_udp("127.0.0.1", port, b"search_api.rpc.requests:1|c|#rpc:vector_search")
            send_udp("127.0.0.1", port, b"search_api.rpc.duration_ms:55.5|d|#rpc:text_search")
            time.sleep(0.15)
        finally:
            listener.stop()

        output: Path = tmp_path / "metrics.jsonl"
        assert output.exists(), "metrics.jsonl must be created"
        records = drain_jsonl(output)
        assert len(records) >= 2
        names = {r["name"] for r in records}
        assert "search_api.rpc.requests" in names
        assert "search_api.rpc.duration_ms" in names

    def test_multi_metric_datagram(self, tmp_path: Path) -> None:
        """A newline-delimited multi-metric datagram is split and each line written."""
        port: int = 19201
        listener = DogStatsDListener(tmp_path, host="127.0.0.1", port=port)
        listener.start()
        try:
            multi = b"metric.a:1|c\nmetric.b:2|g"
            send_udp("127.0.0.1", port, multi)
            time.sleep(0.15)
        finally:
            listener.stop()

        records = drain_jsonl(tmp_path / "metrics.jsonl")
        names = {r["name"] for r in records}
        assert "metric.a" in names
        assert "metric.b" in names

    def test_garbage_datagram_does_not_crash(self, tmp_path: Path) -> None:
        """Unparseable datagrams are silently dropped without crashing the listener."""
        port: int = 19202
        listener = DogStatsDListener(tmp_path, host="127.0.0.1", port=port)
        listener.start()
        try:
            send_udp("127.0.0.1", port, b"\x00\xff\xfe garbage")
            send_udp("127.0.0.1", port, b"good:1|c")
            time.sleep(0.15)
        finally:
            listener.stop()

        records = drain_jsonl(tmp_path / "metrics.jsonl")
        assert any(r["name"] == "good" for r in records)


class TestOtlpGrpcReceiver:
    """Integration tests for :class:`~bench.telemetry_capture.OtlpGrpcReceiver`."""

    def make_export_request(self) -> trace_service_pb2.ExportTraceServiceRequest:
        """Build a synthetic ExportTraceServiceRequest with one span.

        Returns:
            A populated protobuf request suitable for testing the receiver.
        """
        span = trace_pb2.Span(
            trace_id=b"\x01" * 16,
            span_id=b"\x02" * 8,
            name="test.operation",
            start_time_unix_nano=1_000_000_000,
            end_time_unix_nano=2_000_000_000,
        )
        scope_spans = trace_pb2.ScopeSpans(spans=[span])
        resource_spans = trace_pb2.ResourceSpans(scope_spans=[scope_spans])
        return trace_service_pb2.ExportTraceServiceRequest(resource_spans=[resource_spans])

    def test_start_export_stop(self, tmp_path: Path) -> None:
        """Receiver decodes ExportTraceServiceRequest and writes span JSON lines."""
        port: int = 14400
        receiver = OtlpGrpcReceiver(tmp_path, host="127.0.0.1", port=port)
        receiver.start()
        try:
            channel = grpc.insecure_channel(f"127.0.0.1:{port}")
            stub = trace_service_pb2_grpc.TraceServiceStub(channel)
            request = self.make_export_request()
            stub.Export(request)
            time.sleep(0.15)
        finally:
            receiver.stop()

        output: Path = tmp_path / "traces.jsonl"
        assert output.exists(), "traces.jsonl must be created"
        records = drain_jsonl(output)
        assert len(records) >= 1
        span_record = records[0]
        assert "received_at" in span_record
        assert "span" in span_record
        assert span_record["span"].get("name") == "test.operation"

    def test_multiple_spans_in_one_export(self, tmp_path: Path) -> None:
        """Multiple spans in one ExportTraceServiceRequest are each written as a separate line."""
        port: int = 14401
        receiver = OtlpGrpcReceiver(tmp_path, host="127.0.0.1", port=port)
        receiver.start()
        try:
            channel = grpc.insecure_channel(f"127.0.0.1:{port}")
            stub = trace_service_pb2_grpc.TraceServiceStub(channel)
            spans = [
                trace_pb2.Span(
                    trace_id=b"\x01" * 16,
                    span_id=bytes([i + 1]) * 8,
                    name=f"op.{i}",
                    start_time_unix_nano=i * 1_000_000_000,
                    end_time_unix_nano=(i + 1) * 1_000_000_000,
                )
                for i in range(3)
            ]
            scope_spans = trace_pb2.ScopeSpans(spans=spans)
            resource_spans = trace_pb2.ResourceSpans(scope_spans=[scope_spans])
            request = trace_service_pb2.ExportTraceServiceRequest(resource_spans=[resource_spans])
            stub.Export(request)
            time.sleep(0.15)
        finally:
            receiver.stop()

        records = drain_jsonl(tmp_path / "traces.jsonl")
        names = {r["span"]["name"] for r in records}
        assert names == {"op.0", "op.1", "op.2"}


class TestTelemetryCapture:
    """Integration tests for :class:`~bench.telemetry_capture.TelemetryCapture`."""

    def test_env_overrides_populated_on_entry(self, tmp_path: Path) -> None:
        """env_overrides contains the expected keys after capture start."""
        cfg = CaptureConfig(
            telemetry_dir=tmp_path,
            statsd_port=19300,
            otlp_port=14500,
        )
        with TelemetryCapture(cfg) as capture:
            overrides = capture.env_overrides
            assert "SEARCH_API_STATSD_ADDR" in overrides
            assert overrides["SEARCH_API_STATSD_ADDR"] == "127.0.0.1:19300"
            assert "LANCE_BENCH_STATSD_HOST" in overrides
            assert "LANCE_BENCH_STATSD_PORT" in overrides
            assert overrides["LANCE_BENCH_STATSD_PORT"] == "19300"
            assert "OTEL_EXPORTER_OTLP_ENDPOINT" in overrides
            assert "14500" in overrides["OTEL_EXPORTER_OTLP_ENDPOINT"]

    def test_output_files_created(self, tmp_path: Path) -> None:
        """Both output files are created when capture starts."""
        cfg = CaptureConfig(
            telemetry_dir=tmp_path,
            statsd_port=19301,
            otlp_port=14501,
        )
        with TelemetryCapture(cfg):
            assert (tmp_path / "metrics.jsonl").exists()
            assert (tmp_path / "traces.jsonl").exists()

    def test_context_manager_stops_cleanly_on_exception(self, tmp_path: Path) -> None:
        """Listeners are stopped even when the body raises."""
        cfg = CaptureConfig(
            telemetry_dir=tmp_path,
            statsd_port=19302,
            otlp_port=14502,
        )
        with pytest.raises(RuntimeError, match="intentional"), TelemetryCapture(cfg):
            raise RuntimeError("intentional")

    def test_metrics_only_mode(self, tmp_path: Path) -> None:
        """capture_traces=False starts only the DogStatsD listener."""
        cfg = CaptureConfig(
            telemetry_dir=tmp_path,
            statsd_port=19303,
            otlp_port=14503,
            capture_metrics=True,
            capture_traces=False,
        )
        with TelemetryCapture(cfg) as capture:
            assert "SEARCH_API_STATSD_ADDR" in capture.env_overrides
            assert "OTEL_EXPORTER_OTLP_ENDPOINT" not in capture.env_overrides

    def test_traces_only_mode(self, tmp_path: Path) -> None:
        """capture_metrics=False starts only the OTLP receiver."""
        cfg = CaptureConfig(
            telemetry_dir=tmp_path,
            statsd_port=19304,
            otlp_port=14504,
            capture_metrics=False,
            capture_traces=True,
        )
        with TelemetryCapture(cfg) as capture:
            assert "SEARCH_API_STATSD_ADDR" not in capture.env_overrides
            assert "OTEL_EXPORTER_OTLP_ENDPOINT" in capture.env_overrides

    def test_end_to_end_metric_and_span_captured(self, tmp_path: Path) -> None:
        """A DogStatsD datagram and an OTLP export both land in their respective files."""
        cfg = CaptureConfig(
            telemetry_dir=tmp_path,
            statsd_port=19305,
            otlp_port=14505,
        )
        with TelemetryCapture(cfg):
            send_udp("127.0.0.1", 19305, b"pipeline.etl.rows:1000|c|#env:bench")
            span = trace_pb2.Span(
                trace_id=b"\xab" * 16,
                span_id=b"\xcd" * 8,
                name="bench.ingest",
                start_time_unix_nano=0,
                end_time_unix_nano=1_000_000_000,
            )
            scope_spans = trace_pb2.ScopeSpans(spans=[span])
            resource_spans = trace_pb2.ResourceSpans(scope_spans=[scope_spans])
            request = trace_service_pb2.ExportTraceServiceRequest(resource_spans=[resource_spans])
            channel = grpc.insecure_channel("127.0.0.1:14505")
            stub = trace_service_pb2_grpc.TraceServiceStub(channel)
            stub.Export(request)
            time.sleep(0.2)

        metric_records = drain_jsonl(tmp_path / "metrics.jsonl")
        assert any(r["name"] == "pipeline.etl.rows" for r in metric_records)

        trace_records = drain_jsonl(tmp_path / "traces.jsonl")
        assert any(r["span"]["name"] == "bench.ingest" for r in trace_records)

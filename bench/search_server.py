"""Self-hosted ``search-api`` subprocess management for the benchmark.

The benchmark's control plane lives in an ephemeral, randomly named PostgreSQL schema for the
duration of one ``e2e`` or ``search`` run (see ``bench/reconcile.py:isolated_control_plane``). An
externally started ``search-api`` server connects with the default ``search_path`` and can never
see that schema's rows, so the only way to measure a real search leg is to spawn the release
binary as a subprocess from *inside* the isolation window, pointed at the exact isolated database
URL, and tear it down before the schema is dropped.

Readiness is checked with the standard gRPC health protocol (``grpc.health.v1.Health/Check``)
against the server's fixed health port. The health and reflection messages are trivial enough
(one optional string field in, one enum field out) that they are hand-encoded here in the raw
protobuf wire format rather than pulling in the ``grpcio-health-checking`` package as an extra
benchmark dependency.
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import grpc

logger: logging.Logger = logging.getLogger(__name__)

SEARCH_API_HEALTH_PORT: int = 8081
"""Fixed plaintext gRPC health port the ``search-api`` binary always binds (not configurable)."""

DEFAULT_SEARCH_API_PORT: int = 8080
"""Default search port matching the ``search-api`` binary's own ``SEARCH_API_PORT`` default."""

HEALTH_STARTUP_TIMEOUT_SECONDS: float = 60.0
"""Upper bound on waiting for the self-hosted server to report ``SERVING``."""

HEALTH_POLL_INTERVAL_SECONDS: float = 0.5
"""Delay between health-check polls while waiting for ``SERVING``."""

HEALTH_CHECK_RPC_TIMEOUT_SECONDS: float = 2.0
"""Per-poll gRPC deadline for the health check call."""

SHUTDOWN_TIMEOUT_SECONDS: float = 10.0
"""Upper bound on waiting for a terminated subprocess to exit before it is killed."""

HEALTH_CHECK_METHOD: str = "/grpc.health.v1.Health/Check"
"""Fully qualified method name of the standard gRPC health-checking protocol."""

SERVING_STATUS: int = 1
"""``HealthCheckResponse.ServingStatus.SERVING`` wire value."""


def encode_health_check_request(service: str) -> bytes:
    """Encode a ``grpc.health.v1.HealthCheckRequest`` in raw protobuf wire format.

    Args:
        service: The service name to check. An empty string checks overall server health, which
            is what ``search-api`` reports on its single unnamed health service.

    Returns:
        The serialized request bytes. Field 1 (``service``, a length-delimited string) is omitted
        entirely when empty, matching proto3 default-value encoding.
    """
    if not service:
        return b""
    encoded: bytes = service.encode("utf-8")
    return bytes([0x0A, len(encoded)]) + encoded


def decode_health_check_status(payload: bytes) -> int:
    """Decode the ``status`` enum field of a ``grpc.health.v1.HealthCheckResponse``.

    Args:
        payload: Raw response bytes from the health RPC.

    Returns:
        The wire value of field 1 (``status``), or ``0`` (``UNKNOWN``) when the field is absent.
    """
    if len(payload) >= 2 and payload[0] == 0x08:
        return payload[1]
    return 0


def grpc_health_check(port: int, timeout_seconds: float = HEALTH_CHECK_RPC_TIMEOUT_SECONDS) -> int:
    """Issue one standard gRPC health check against the ``search-api`` health port.

    Args:
        port: The health port, always :data:`SEARCH_API_HEALTH_PORT` for this binary.
        timeout_seconds: Per-call gRPC deadline.

    Returns:
        The reported serving-status wire value.
    """
    channel: grpc.Channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        check = channel.unary_unary(
            HEALTH_CHECK_METHOD,
            request_serializer=encode_health_check_request,
            response_deserializer=decode_health_check_status,
        )
        return check("", timeout=timeout_seconds)
    finally:
        channel.close()


def wait_for_serving(
    process: subprocess.Popen[bytes],
    port: int = SEARCH_API_HEALTH_PORT,
    timeout_seconds: float = HEALTH_STARTUP_TIMEOUT_SECONDS,
    log_path: Path | None = None,
) -> None:
    """Poll the health port until the self-hosted server reports ``SERVING``.

    Fails fast, without waiting out the full timeout, when the subprocess has already exited.

    Args:
        process: The spawned ``search-api`` subprocess.
        port: The health port to poll.
        timeout_seconds: Upper bound on the wait.
        log_path: Optional log file whose tail is included in a timeout or crash error.

    Raises:
        RuntimeError: If the process exits before reporting ``SERVING``, or the deadline elapses.
    """
    deadline: float = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        exit_code: int | None = process.poll()
        if exit_code is not None:
            raise RuntimeError(
                f"search-api exited with code {exit_code} before becoming ready{log_tail_suffix(log_path)}"
            )
        try:
            status: int = grpc_health_check(port)
            if status == SERVING_STATUS:
                return
        except grpc.RpcError as exc:
            last_error = exc
        time.sleep(HEALTH_POLL_INTERVAL_SECONDS)
    raise RuntimeError(
        f"search-api did not report SERVING within {timeout_seconds}s (last error: {last_error})"
        f"{log_tail_suffix(log_path)}"
    )


def log_tail_suffix(log_path: Path | None, lines: int = 20) -> str:
    """Build a diagnostic suffix carrying the tail of the server log, when available.

    Args:
        log_path: The server's log file, or ``None`` when logging was not captured.
        lines: Maximum trailing lines to include.

    Returns:
        An empty string when no log is available, otherwise a formatted tail block.
    """
    if log_path is None or not log_path.exists():
        return ""
    tail: list[str] = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:]
    return "\nsearch-api.log tail:\n" + "\n".join(tail)


def terminate_process(process: subprocess.Popen[bytes], timeout_seconds: float = SHUTDOWN_TIMEOUT_SECONDS) -> None:
    """Terminate a subprocess gracefully, escalating to a kill if it does not exit in time.

    Args:
        process: The subprocess to stop.
        timeout_seconds: Grace period for a clean exit after ``SIGTERM``.
    """
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout_seconds)


@contextmanager
def self_hosted_search_api(
    binary: Path,
    database_url: str,
    base_uri: Path,
    port: int = DEFAULT_SEARCH_API_PORT,
    log_path: Path | None = None,
    statsd_addr: str | None = None,
    startup_timeout_seconds: float = HEALTH_STARTUP_TIMEOUT_SECONDS,
) -> Iterator[str]:
    """Spawn, wait for, and tear down a self-hosted ``search-api`` subprocess.

    The subprocess inherits the current process environment (so ``--capture-telemetry``'s
    ``SEARCH_API_STATSD_ADDR``/``OTEL_EXPORTER_OTLP_ENDPOINT`` overrides apply automatically) with
    ``LANCE_ETL_DATABASE_URL``, ``LANCE_ETL_BASE_URI``, and ``SEARCH_API_PORT`` set explicitly.
    Must be used from inside the PostgreSQL isolation window whose URL is passed in, and the
    caller must not exit that window until this context manager has torn the subprocess down.

    Args:
        binary: Path to the ``search-api`` release binary.
        database_url: The isolated ``postgresql://`` control-plane URL (with the
            ``options=-csearch_path=<schema>`` parameter) the subprocess should read the catalog
            through.
        base_uri: Allowlisted Lance storage namespace root.
        port: Search gRPC port to bind. The health port is always :data:`SEARCH_API_HEALTH_PORT`.
        log_path: Optional file to append the subprocess's stdout/stderr to.
        statsd_addr: Optional explicit ``SEARCH_API_STATSD_ADDR`` override. When ``None`` the
            inherited environment (or the server's own default) applies unchanged.
        startup_timeout_seconds: Upper bound on waiting for ``SERVING``.

    Yields:
        The ``host:port`` endpoint of the ready server.

    Raises:
        FileNotFoundError: If ``binary`` does not exist.
        RuntimeError: If the server does not become ready within the timeout.
    """
    if not binary.exists():
        raise FileNotFoundError(f"search-api binary not found at {binary}")
    env: dict[str, str] = dict(os.environ)
    env["LANCE_ETL_DATABASE_URL"] = database_url
    env["LANCE_ETL_BASE_URI"] = str(base_uri)
    env["SEARCH_API_PORT"] = str(port)
    if statsd_addr is not None:
        env["SEARCH_API_STATSD_ADDR"] = statsd_addr
    log_file = None
    stdout_target: int | object = subprocess.DEVNULL
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = log_path.open("ab")
        stdout_target = log_file
    logger.info("spawning self-hosted search-api %s on port %d", binary, port)
    process: subprocess.Popen[bytes] = subprocess.Popen(
        [str(binary)], env=env, stdout=stdout_target, stderr=subprocess.STDOUT
    )
    try:
        wait_for_serving(process, timeout_seconds=startup_timeout_seconds, log_path=log_path)
        logger.info("self-hosted search-api ready at 127.0.0.1:%d", port)
        yield f"127.0.0.1:{port}"
    finally:
        terminate_process(process)
        if log_file is not None:
            log_file.close()

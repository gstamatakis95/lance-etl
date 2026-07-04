"""Managed lifecycle for the Rust ``search-api`` server during an experiment run.

The experiment loop owns the server so an agent can vary server-side knobs (cache backend,
cache budgets) per iteration and measure true cold starts. :class:`ServerHandle` resolves the
binary (release first, debug fallback, optional ``cargo build --release``), spawns it against
the workspace's Lance root on a free port, waits for gRPC readiness, captures its output to
``{run_dir}/server.log``, and kills it on exit. :meth:`ServerHandle.restart` respawns on the
same port for cold first-query measurement. When no binary is available and building is
disabled, :func:`resolve_binary` returns ``None`` so callers record a graceful skip, the same
pattern the ghz load leg uses.
"""

from __future__ import annotations

import logging
import os
import socket
import subprocess
import time
from pathlib import Path

import grpc

from bench.config import BenchConfig

logger: logging.Logger = logging.getLogger(__name__)

REPO_ROOT: Path = Path(__file__).resolve().parent.parent

SERVER_CRATE_DIR: Path = REPO_ROOT / "rust" / "search-api"

RELEASE_BINARY: Path = SERVER_CRATE_DIR / "target" / "release" / "search-api"

DEBUG_BINARY: Path = SERVER_CRATE_DIR / "target" / "debug" / "search-api"

READY_TIMEOUT_SECONDS: float = 30.0


def free_port() -> int:
    """Reserve a free localhost TCP port.

    Returns:
        A port number that was free at probe time.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def resolve_binary(config: BenchConfig) -> Path | None:
    """Resolve the search-api binary to spawn, optionally building it first.

    Resolution order: an explicit ``--server-bin`` path, else the release binary, else the
    debug binary. With ``--build-server`` a ``cargo build --release`` runs first.

    Args:
        config: Benchmark configuration.

    Returns:
        The binary path, or ``None`` when nothing exists and building is disabled (callers
        record a skip).
    """
    if config.build_server:
        logger.info("building search-api (cargo build --release) in %s", SERVER_CRATE_DIR)
        subprocess.run(["cargo", "build", "--release"], cwd=SERVER_CRATE_DIR, check=True)
    if config.server_bin is not None:
        explicit: Path = Path(config.server_bin)
        return explicit if explicit.exists() else None
    for candidate in (RELEASE_BINARY, DEBUG_BINARY):
        if candidate.exists():
            return candidate
    return None


class ServerHandle:
    """One spawned search-api process bound to the workspace's Lance root.

    Use as a context manager. The endpoint is ``localhost:{port}`` with a port reserved at
    construction, so :attr:`endpoint` is stable across :meth:`restart`.
    """

    def __init__(self, config: BenchConfig, binary: Path) -> None:
        """Initialize the handle without spawning.

        Args:
            config: Benchmark configuration supplying the Lance root, run dir, and server env.
            binary: The search-api binary to spawn, from :func:`resolve_binary`.
        """
        self.config: BenchConfig = config
        self.binary: Path = binary
        self.port: int = free_port()
        self.endpoint: str = f"localhost:{self.port}"
        self.process: subprocess.Popen | None = None
        self.log_path: Path = config.run_dir() / "server.log"

    def environment(self) -> dict[str, str]:
        """Build the child environment: base URI, port, telemetry off, plus overrides.

        The repeatable ``--server-env KEY=VALUE`` flags are applied last, so an agent can
        override anything, including the cache backend and budgets.

        Returns:
            The complete child process environment.
        """
        env: dict[str, str] = dict(os.environ)
        env["LANCE_ETL_BASE_URI"] = f"file-object-store://{self.config.lance_root()}"
        env["SEARCH_API_PORT"] = str(self.port)
        env["SEARCH_API_TELEMETRY_DISABLED"] = "true"
        env.update(self.config.server_env)
        return env

    def spawn(self) -> None:
        """Start the server and wait for gRPC readiness.

        Raises:
            RuntimeError: If the server does not become ready within the timeout or exits.
        """
        with open(self.log_path, "ab") as log_file:
            self.process = subprocess.Popen(
                [str(self.binary)],
                env=self.environment(),
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=REPO_ROOT,
            )
        deadline: float = time.monotonic() + READY_TIMEOUT_SECONDS
        while True:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"search-api exited with code {self.process.returncode} during startup, see {self.log_path}"
                )
            try:
                channel = grpc.insecure_channel(self.endpoint)
                grpc.channel_ready_future(channel).result(timeout=1.0)
                channel.close()
                logger.info("search-api ready at %s (pid %d)", self.endpoint, self.process.pid)
                return
            except Exception:
                if time.monotonic() > deadline:
                    self.stop()
                    raise RuntimeError(
                        f"search-api did not become ready at {self.endpoint} within "
                        f"{READY_TIMEOUT_SECONDS}s, see {self.log_path}"
                    ) from None

    def stop(self) -> None:
        """Kill the server process if it is running."""
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            self.process.wait()
        self.process = None

    def restart(self) -> None:
        """Kill and respawn the server on the same port, for true cold-start measurement."""
        self.stop()
        self.spawn()

    def __enter__(self) -> ServerHandle:
        """Spawn on entry.

        Returns:
            This handle, ready to serve.
        """
        self.spawn()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Stop on exit.

        Args:
            exc_type: Exception type, if any.
            exc: Exception value, if any.
            tb: Traceback, if any.
        """
        del exc_type, exc, tb
        self.stop()

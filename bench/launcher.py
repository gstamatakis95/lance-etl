"""Lance-free process launcher for every benchmark command."""

from __future__ import annotations

import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Never

from bench.config import DEFAULT_ICEBERG_PACKAGE, DEFAULT_WORKSPACE, REPO_ROOT
from lance_etl.spark_process import SPARK_CORE_CONF_PINS, resolve_spark_installation, spark_launch_environment

SPARK_COMMANDS: frozenset[str] = frozenset({"prepare", "e2e", "experiment", "fuzz"})
"""Benchmark commands that must enter Python through ``spark-submit``."""


def benchmark_command(arguments: Sequence[str]) -> str | None:
    """Resolve the benchmark subcommand without importing the application driver.

    Args:
        arguments: Original command-line arguments after the module name.

    Returns:
        The subcommand token, or None when the top-level arguments are incomplete.
    """
    position: int = 0
    while position < len(arguments):
        token: str = arguments[position]
        if token == "--log-level":
            position += 2
            continue
        if token.startswith("--log-level="):
            position += 1
            continue
        return token
    return None


def option_value(arguments: Sequence[str], option: str, default: str) -> str:
    """Return the final well-formed value of a launcher-relevant CLI option.

    Invalid or incomplete option syntax is left for the full benchmark parser in the driver. The
    launcher uses the default only to start that driver safely.

    Args:
        arguments: Original command-line arguments after the module name.
        option: Long option name including the leading hyphens.
        default: Value used when the option is absent or incomplete.

    Returns:
        The last complete option value, or the default.
    """
    value: str = default
    prefix: str = f"{option}="
    for position, token in enumerate(arguments):
        if token.startswith(prefix):
            candidate: str = token[len(prefix) :]
            value = candidate
        elif token == option and position + 1 < len(arguments):
            candidate = arguments[position + 1]
            if not candidate.startswith("--"):
                value = candidate
    return value


def launch_environment(spark_home: Path | None = None) -> dict[str, str]:
    """Build the deterministic environment inherited by the benchmark driver and executors.

    Args:
        spark_home: Optional resolved PySpark package root for a Spark-bearing command.

    Returns:
        A copy of the current environment with benchmark process invariants applied.
    """
    environment: dict[str, str]
    if spark_home is not None:
        environment = spark_launch_environment(spark_home, (REPO_ROOT, REPO_ROOT / "src"))
    else:
        environment = dict(os.environ)
        existing_pythonpath: list[str] = [
            entry for entry in environment.get("PYTHONPATH", "").split(os.pathsep) if entry
        ]
        required_pythonpath: list[str] = [str(REPO_ROOT), str(REPO_ROOT / "src")]
        environment["PYTHONPATH"] = os.pathsep.join(dict.fromkeys([*required_pythonpath, *existing_pythonpath]))
    environment.setdefault("MPLBACKEND", "Agg")
    environment.setdefault("DD_TRACE_ENABLED", "false")
    return environment


def spark_submit_arguments(arguments: Sequence[str], spark_submit: Path, driver: Path) -> list[str]:
    """Build the spark-submit invocation for one Spark-bearing benchmark command.

    Args:
        arguments: Original benchmark arguments preserved for the driver.
        spark_submit: Resolved spark-submit executable.
        driver: Benchmark driver script.

    Returns:
        Complete spark-submit argument vector.
    """
    master: str = option_value(arguments, "--spark-master", "local[*]")
    driver_memory: str = option_value(arguments, "--driver-memory", "8g")
    iceberg_package: str = option_value(arguments, "--iceberg-package", DEFAULT_ICEBERG_PACKAGE)
    workspace: Path = Path(option_value(arguments, "--workspace", str(DEFAULT_WORKSPACE))).resolve()
    ivy_path: Path = workspace / "ivy"
    ivy_path.mkdir(parents=True, exist_ok=True)
    invocation: list[str] = [
        str(spark_submit),
        "--master",
        master,
        "--driver-memory",
        driver_memory,
        "--packages",
        iceberg_package,
        "--conf",
        f"spark.jars.ivy={ivy_path}",
    ]
    key: str
    value: str
    for key, value in SPARK_CORE_CONF_PINS.items():
        invocation.extend(("--conf", f"{key}={value}"))
    invocation.append(str(driver))
    invocation.extend(arguments)
    return invocation


def main(argv: Sequence[str] | None = None) -> Never:
    """Replace this import-light process with the benchmark driver.

    Args:
        argv: Optional arguments after the module name. Defaults to ``sys.argv[1:]``.

    Raises:
        RuntimeError: If process replacement unexpectedly returns.
    """
    arguments: list[str] = list(sys.argv[1:] if argv is None else argv)
    driver: Path = REPO_ROOT / "bench" / "driver.py"
    if benchmark_command(arguments) in SPARK_COMMANDS:
        spark_home, spark_submit = resolve_spark_installation()
        environment: dict[str, str] = launch_environment(spark_home)
        invocation: list[str] = spark_submit_arguments(arguments, spark_submit, driver)
    else:
        environment = launch_environment()
        invocation = [sys.executable, str(driver), *arguments]
    os.execvpe(invocation[0], invocation, environment)
    raise RuntimeError("benchmark process replacement unexpectedly returned")

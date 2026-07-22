"""Import-light process launcher for the local reconciler command."""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Never

from lance_etl.spark_process import (
    DEFAULT_ICEBERG_PACKAGE,
    SPARK_CORE_CONF_PINS,
    resolve_spark_installation,
    spark_launch_environment,
)

SPARK_COMMANDS: frozenset[str] = frozenset({"run", "run-once"})
"""Reconciler commands that create local Spark and must enter through spark-submit."""

LOCAL_SPARK_MASTER_PATTERN: re.Pattern[str] = re.compile(r"local(?:\[(?:\*|[1-9][0-9]*)\])?")
"""Allowlist enforced before spark-submit can select a master."""


def reconciler_command(arguments: Sequence[str]) -> str | None:
    """Resolve the closed reconciler subcommand without importing its driver.

    Args:
        arguments: Original command-line arguments after the executable name.

    Returns:
        The first argument, or None when the command is incomplete.
    """
    return arguments[0] if arguments else None


def spark_submit_arguments(arguments: Sequence[str], spark_submit: Path, driver: Path) -> list[str]:
    """Build the safe local spark-submit invocation for a write-path command.

    Args:
        arguments: Original reconciler arguments preserved for the driver.
        spark_submit: Resolved spark-submit executable.
        driver: Reconciler driver script.

    Returns:
        Complete spark-submit argument vector.

    Raises:
        RuntimeError: If a launcher-critical environment value is empty or selects a remote master.
    """
    master: str = os.environ.get("LANCE_ETL_SPARK_MASTER", "local[*]").strip()
    if LOCAL_SPARK_MASTER_PATTERN.fullmatch(master) is None:
        raise RuntimeError("LANCE_ETL_SPARK_MASTER must be local, local[*], or local[N]")
    iceberg_package: str = os.environ.get("LANCE_ETL_SPARK_ICEBERG_PACKAGE", DEFAULT_ICEBERG_PACKAGE).strip()
    if not iceberg_package:
        raise RuntimeError("LANCE_ETL_SPARK_ICEBERG_PACKAGE must be non-empty")
    local_root_value: str = os.environ.get("LANCE_ETL_LOCAL_ROOT", str(Path.cwd() / ".lance-etl"))
    local_root: Path = Path(local_root_value).expanduser().resolve()
    warehouse_value: str = os.environ.get("LANCE_ETL_SPARK_WAREHOUSE", str(local_root / "iceberg")).strip()
    if not warehouse_value:
        raise RuntimeError("LANCE_ETL_SPARK_WAREHOUSE must be non-empty")
    ivy_path: Path = Path(warehouse_value).expanduser().resolve().parent / "ivy"
    ivy_path.mkdir(parents=True, exist_ok=True)
    invocation: list[str] = [
        str(spark_submit),
        "--master",
        master,
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
    """Replace the import-light process with the appropriate reconciler driver.

    Args:
        argv: Optional arguments after the executable name. Defaults to ``sys.argv[1:]``.

    Raises:
        RuntimeError: If process replacement unexpectedly returns.
    """
    arguments: list[str] = list(sys.argv[1:] if argv is None else argv)
    driver: Path = Path(__file__).resolve().with_name("driver.py")
    if reconciler_command(arguments) in SPARK_COMMANDS:
        spark_home, spark_submit = resolve_spark_installation()
        source_root: Path = Path(__file__).resolve().parents[2]
        environment: dict[str, str] = spark_launch_environment(spark_home, (source_root,))
        invocation: list[str] = spark_submit_arguments(arguments, spark_submit, driver)
    else:
        environment = dict(os.environ)
        invocation = [sys.executable, str(driver), *arguments]
    os.execvpe(invocation[0], invocation, environment)
    raise RuntimeError("reconciler process replacement unexpectedly returned")

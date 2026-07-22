"""Lance-free Spark process safety invariants shared by launchers and session builders."""

from __future__ import annotations

import importlib.util
import logging
import os
import sys
from collections.abc import Sequence
from importlib.machinery import ModuleSpec
from pathlib import Path
from typing import Any

logger: logging.Logger = logging.getLogger(__name__)

DEFAULT_ICEBERG_PACKAGE: str = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
"""Iceberg runtime compatible with the pinned local PySpark release."""

SPARK_CORE_CONF_PINS: dict[str, str] = {
    "spark.speculation": "false",
    "spark.python.use.daemon": "false",
    "spark.python.worker.faulthandler.enabled": "true",
    "spark.sql.execution.pyspark.udf.faulthandler.enabled": "true",
}
"""Correctness pins that must be applied before a Spark context starts.

Speculation would duplicate commit-producing task attempts. The Python daemon forks workers after
native Arrow modules may already be loaded, which is unsafe for PyArrow and pylance thread pools.
Direct worker launch avoids that fork, while the fault handlers preserve actionable tracebacks.
"""


def resolve_spark_installation() -> tuple[Path, Path]:
    """Locate PySpark and its launcher without importing the package.

    Returns:
        The PySpark package root and spark-submit executable.

    Raises:
        RuntimeError: If PySpark or spark-submit cannot be located.
    """
    specification: ModuleSpec | None = importlib.util.find_spec("pyspark")
    locations: list[str] = list(specification.submodule_search_locations or []) if specification else []
    if not locations:
        raise RuntimeError("pyspark is not installed in the active Python environment")
    spark_home: Path = Path(locations[0]).resolve()
    environment_submit: Path = Path(sys.executable).parent / "spark-submit"
    package_submit: Path = spark_home / "bin" / "spark-submit"
    spark_submit: Path = environment_submit if os.access(environment_submit, os.X_OK) else package_submit
    if not os.access(spark_submit, os.X_OK):
        raise RuntimeError(f"spark-submit is missing from both {environment_submit} and {package_submit}")
    return spark_home, spark_submit.resolve()


def spark_launch_environment(spark_home: Path, python_paths: Sequence[Path] = ()) -> dict[str, str]:
    """Build the environment inherited by a local spark-submit driver and its workers.

    Args:
        spark_home: Resolved PySpark package root.
        python_paths: Import roots prepended to any existing Python path.

    Returns:
        A copy of the current environment with interpreter and import-path pins.
    """
    environment: dict[str, str] = dict(os.environ)
    environment["SPARK_HOME"] = str(spark_home)
    environment["PYSPARK_PYTHON"] = sys.executable
    environment["PYSPARK_DRIVER_PYTHON"] = sys.executable
    existing_pythonpath: list[str] = [entry for entry in environment.get("PYTHONPATH", "").split(os.pathsep) if entry]
    required_pythonpath: list[str] = [str(path) for path in python_paths]
    environment["PYTHONPATH"] = os.pathsep.join(dict.fromkeys([*required_pythonpath, *existing_pythonpath]))
    return environment


def ensure_spark_process_safety(session: Any, workload: str) -> None:
    """Reject and stop an active Spark session with unsafe task process settings.

    Args:
        session: Newly obtained or reused Spark session.
        workload: Human-readable operation appended to the corrective error message.

    Raises:
        RuntimeError: If the active Spark context enables speculation or the fork-based Python daemon.
    """
    speculation: str = session.sparkContext.getConf().get("spark.speculation", "false")
    python_daemon: str = session.sparkContext.getConf().get("spark.python.use.daemon", "true")
    unsafe: list[str] = []
    if speculation.strip().lower() != "false":
        unsafe.append("spark.speculation enabled")
    if python_daemon.strip().lower() != "false":
        unsafe.append("spark.python.use.daemon enabled")
    if not unsafe:
        return
    failure: RuntimeError = RuntimeError(
        f"the active SparkContext is unsafe ({', '.join(unsafe)}). Restart it with "
        f"spark.speculation=false and spark.python.use.daemon=false before {workload}"
    )
    try:
        session.stop()
    except BaseException:
        logger.exception("failed to stop the rejected Spark session")
    raise failure

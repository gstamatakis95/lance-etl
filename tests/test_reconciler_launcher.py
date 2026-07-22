"""Tests for the import-light reconciler process launcher."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Never

import pytest

import lance_etl.reconciler.launcher as launcher
from lance_etl.spark_process import DEFAULT_ICEBERG_PACKAGE, SPARK_CORE_CONF_PINS


class ProcessReplaced(RuntimeError):
    """Sentinel raised by the exec replacement used in launcher tests."""


def test_importing_reconciler_launcher_loads_no_native_job_modules() -> None:
    """The pre-exec launcher must not initialize Lance, PySpark, Arrow, or telemetry clients."""
    script: str = (
        "import json, sys; import lance_etl.reconciler.launcher; "
        "blocked = {'lance', 'pyspark', 'pyarrow', 'datadog', 'ddtrace'}; "
        "print(json.dumps(sorted(name for name in sys.modules if name.split('.', 1)[0] in blocked)))"
    )
    completed: subprocess.CompletedProcess[str] = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(completed.stdout) == []


@pytest.mark.parametrize("command", ["run", "run-once"])
def test_spark_command_execs_spark_submit(
    command: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A write-path command starts through spark-submit with local process invariants.

    Args:
        command: Spark-bearing reconciler command.
        monkeypatch: Scoped process and environment replacements.
        tmp_path: Isolated Spark and reconciler workspace.
    """
    spark_home: Path = tmp_path / "pyspark"
    spark_submit: Path = tmp_path / "venv" / "bin" / "spark-submit"
    captured: dict[str, object] = {}
    monkeypatch.setattr(launcher, "resolve_spark_installation", lambda: (spark_home, spark_submit))
    monkeypatch.setenv("LANCE_ETL_SPARK_MASTER", "local[2]")
    monkeypatch.setenv("LANCE_ETL_LOCAL_ROOT", str(tmp_path / "local"))
    monkeypatch.setenv("PYTHONPATH", "/existing/pythonpath")

    def replace_process(executable: str, invocation: list[str], environment: dict[str, str]) -> Never:
        """Capture process replacement and stop the test.

        Args:
            executable: Selected executable.
            invocation: Complete argument vector.
            environment: Child process environment.

        Raises:
            ProcessReplaced: Always.
        """
        captured.update(executable=executable, invocation=invocation, environment=environment)
        raise ProcessReplaced

    monkeypatch.setattr(launcher.os, "execvpe", replace_process)

    with pytest.raises(ProcessReplaced):
        launcher.main([command])

    driver: str = str(Path(launcher.__file__).resolve().with_name("driver.py"))
    ivy_path: Path = tmp_path / "local" / "ivy"
    invocation = captured["invocation"]
    assert isinstance(invocation, list)
    assert invocation[:7] == [
        str(spark_submit),
        "--master",
        "local[2]",
        "--packages",
        DEFAULT_ICEBERG_PACKAGE,
        "--conf",
        f"spark.jars.ivy={ivy_path}",
    ]
    expected_pins: list[str] = []
    for key, value in SPARK_CORE_CONF_PINS.items():
        expected_pins.extend(("--conf", f"{key}={value}"))
    assert invocation[7 : 7 + len(expected_pins)] == expected_pins
    assert invocation[7 + len(expected_pins) :] == [driver, command]
    environment = captured["environment"]
    assert isinstance(environment, dict)
    assert environment["SPARK_HOME"] == str(spark_home)
    assert environment["PYSPARK_PYTHON"] == sys.executable
    assert environment["PYSPARK_DRIVER_PYTHON"] == sys.executable
    assert environment["PYTHONPATH"].split(os.pathsep)[-1] == "/existing/pythonpath"


@pytest.mark.parametrize("command", ["migrate", "status", "repair"])
def test_postgres_only_command_execs_python(
    command: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PostgreSQL-only command never resolves or starts Spark.

    Args:
        command: PostgreSQL-only reconciler command.
        monkeypatch: Scoped process replacement.
    """
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        launcher,
        "resolve_spark_installation",
        lambda: pytest.fail("PostgreSQL-only commands must not resolve PySpark"),
    )

    def replace_process(executable: str, invocation: list[str], environment: dict[str, str]) -> Never:
        """Capture process replacement and stop the test.

        Args:
            executable: Selected executable.
            invocation: Complete argument vector.
            environment: Child process environment.

        Raises:
            ProcessReplaced: Always.
        """
        captured.update(executable=executable, invocation=invocation, environment=environment)
        raise ProcessReplaced

    monkeypatch.setattr(launcher.os, "execvpe", replace_process)

    with pytest.raises(ProcessReplaced):
        launcher.main([command])

    driver: str = str(Path(launcher.__file__).resolve().with_name("driver.py"))
    assert captured["executable"] == sys.executable
    assert captured["invocation"] == [sys.executable, driver, command]


def test_launcher_rejects_remote_spark_master(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The launcher rejects a remote master before process replacement.

    Args:
        monkeypatch: Scoped environment replacement.
        tmp_path: Isolated launcher paths.
    """
    monkeypatch.setenv("LANCE_ETL_SPARK_MASTER", "spark://remote.example:7077")
    with pytest.raises(RuntimeError, match="must be local"):
        launcher.spark_submit_arguments(["run-once"], tmp_path / "spark-submit", tmp_path / "driver.py")

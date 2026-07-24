"""Tests for the import-light benchmark process launcher."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Never

import pytest

import bench.launcher as launcher
from lance_etl.spark_process import SPARK_CORE_CONF_PINS


class ProcessReplaced(RuntimeError):
    """Sentinel raised by the exec replacement used in launcher unit tests."""


def test_importing_launcher_loads_no_lance_or_native_telemetry() -> None:
    """The pre-exec launcher must not initialize Lance, PySpark, Arrow, or telemetry clients."""
    script: str = (
        "import json, sys; import bench.launcher; "
        "blocked = {'lance', 'pyspark', 'pyarrow', 'datadog', 'ddtrace'}; "
        "print(json.dumps(sorted(name for name in sys.modules if name.split('.', 1)[0] in blocked)))"
    )
    completed: subprocess.CompletedProcess[str] = subprocess.run(
        [sys.executable, "-c", script],
        cwd=launcher.REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(completed.stdout) == []


def test_spark_command_execs_spark_submit_with_process_invariants(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A Spark-bearing command preserves CLI args and starts through the package spark-submit.

    Args:
        monkeypatch: Scoped process and environment replacements.
        tmp_path: Isolated Spark installation and benchmark workspace.
    """
    spark_home: Path = tmp_path / "pyspark"
    spark_submit: Path = tmp_path / "venv" / "bin" / "spark-submit"
    arguments: list[str] = [
        "--log-level",
        "DEBUG",
        "fuzz",
        "--workspace",
        str(tmp_path / "workspace"),
        "--spark-master=local[2]",
        "--driver-memory",
        "3g",
        "--iceberg-package",
        "iceberg:test",
        "--seed",
        "99",
    ]
    captured: dict[str, object] = {}
    monkeypatch.setattr(launcher, "resolve_spark_installation", lambda: (spark_home, spark_submit))
    monkeypatch.setenv("PYTHONPATH", "/existing/pythonpath")
    monkeypatch.setenv("DD_TRACE_ENABLED", "true")

    def replace_process(executable: str, invocation: list[str], environment: dict[str, str]) -> Never:
        """Capture the process replacement and stop the test.

        Args:
            executable: Selected executable.
            invocation: Full argument vector.
            environment: Child process environment.

        Raises:
            ProcessReplaced: Always.
        """
        captured.update(executable=executable, invocation=invocation, environment=environment)
        raise ProcessReplaced

    monkeypatch.setattr(launcher.os, "execvpe", replace_process)

    with pytest.raises(ProcessReplaced):
        launcher.main(arguments)

    invocation = captured["invocation"]
    assert isinstance(invocation, list)
    driver: str = str(launcher.REPO_ROOT / "bench" / "driver.py")
    assert invocation[:9] == [
        str(spark_submit),
        "--master",
        "local[2]",
        "--driver-memory",
        "3g",
        "--packages",
        "iceberg:test",
        "--conf",
        f"spark.jars.ivy={tmp_path / 'workspace' / 'ivy'}",
    ]
    expected_pins: list[str] = []
    for key, value in SPARK_CORE_CONF_PINS.items():
        expected_pins.extend(("--conf", f"{key}={value}"))
    assert invocation[9 : 9 + len(expected_pins)] == expected_pins
    assert invocation[9 + len(expected_pins) :] == [driver, *arguments]
    environment = captured["environment"]
    assert isinstance(environment, dict)
    assert environment["SPARK_HOME"] == str(spark_home)
    assert environment["PYSPARK_PYTHON"] == sys.executable
    assert environment["PYSPARK_DRIVER_PYTHON"] == sys.executable
    assert environment["MPLBACKEND"] == "Agg"
    assert environment["DD_TRACE_ENABLED"] == "true"
    assert environment["PYTHONPATH"].split(os.pathsep) == [
        str(launcher.REPO_ROOT),
        str(launcher.REPO_ROOT / "src"),
        "/existing/pythonpath",
    ]


@pytest.mark.parametrize("command", ["download", "search", "report", "qualify"])
def test_non_spark_command_execs_python_driver(
    command: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A non-Spark command replaces the launcher with the same Python interpreter.

    Args:
        command: Non-Spark benchmark command.
        monkeypatch: Scoped process replacement.
        tmp_path: Isolated environment fixture.
    """
    captured: dict[str, object] = {}
    del tmp_path
    monkeypatch.setattr(
        launcher,
        "resolve_spark_installation",
        lambda: pytest.fail("non-Spark commands must not resolve PySpark"),
    )
    monkeypatch.delenv("DD_TRACE_ENABLED", raising=False)
    monkeypatch.delenv("SPARK_HOME", raising=False)
    monkeypatch.delenv("PYSPARK_PYTHON", raising=False)
    monkeypatch.delenv("PYSPARK_DRIVER_PYTHON", raising=False)

    def replace_process(executable: str, invocation: list[str], environment: dict[str, str]) -> Never:
        """Capture the process replacement and stop the test.

        Args:
            executable: Selected executable.
            invocation: Full argument vector.
            environment: Child process environment.

        Raises:
            ProcessReplaced: Always.
        """
        captured.update(executable=executable, invocation=invocation, environment=environment)
        raise ProcessReplaced

    monkeypatch.setattr(launcher.os, "execvpe", replace_process)

    with pytest.raises(ProcessReplaced):
        launcher.main([command, "--run-id", "unchanged"])

    assert captured["executable"] == sys.executable
    assert captured["invocation"] == [
        sys.executable,
        str(launcher.REPO_ROOT / "bench" / "driver.py"),
        command,
        "--run-id",
        "unchanged",
    ]
    environment = captured["environment"]
    assert isinstance(environment, dict)
    assert environment["DD_TRACE_ENABLED"] == "false"
    assert "SPARK_HOME" not in environment
    assert "PYSPARK_PYTHON" not in environment
    assert "PYSPARK_DRIVER_PYTHON" not in environment

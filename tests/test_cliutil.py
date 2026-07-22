"""Tests for shared Spark session correctness pins and lifecycle handling."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import lance_etl.cliutil as cliutil
from lance_etl.cliutil import SPARK_CONF_DEFAULTS, build_spark, run_with_spark
from lance_etl.spark_process import SPARK_CORE_CONF_PINS


def spark_builder_fixture(speculation: str) -> tuple[MagicMock, MagicMock, MagicMock]:
    """Build a fluent Spark builder, session, and explicit-config fixture.

    Args:
        speculation: Effective ``spark.speculation`` value exposed by the SparkContext.

    Returns:
        Builder, session, and SparkConf mocks.
    """
    builder: MagicMock = MagicMock()
    session: MagicMock = MagicMock()
    explicit: MagicMock = MagicMock()
    builder.appName.return_value = builder
    builder.config.return_value = builder
    builder.getOrCreate.return_value = session
    session.sparkContext.getConf.return_value = explicit
    explicit.get.return_value = speculation
    explicit.contains.return_value = False
    return builder, session, explicit


def test_build_spark_rejects_speculation_on_reused_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """A pre-existing unsafe core config cannot evade the builder-time correctness pin.

    Args:
        monkeypatch: Scoped SparkSession builder replacement.
    """
    builder, session, explicit = spark_builder_fixture("true")
    monkeypatch.setattr(cliutil, "SparkSession", SimpleNamespace(builder=builder))

    with pytest.raises(RuntimeError, match="spark.speculation enabled"):
        build_spark("test")

    assert {call.args for call in builder.config.call_args_list} == set(SPARK_CORE_CONF_PINS.items())
    assert {call.args for call in explicit.get.call_args_list} == {
        ("spark.speculation", "false"),
        ("spark.python.use.daemon", "true"),
    }
    session.conf.set.assert_not_called()
    session.stop.assert_called_once_with()


def test_build_spark_rejects_fork_based_python_daemon_on_reused_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """A reused fork-based worker context cannot execute native Arrow and Lance code.

    Args:
        monkeypatch: Scoped SparkSession builder replacement.
    """
    builder, session, explicit = spark_builder_fixture("false")

    def config_value(key: str, default: str) -> str:
        """Return the unsafe daemon value and safe values for every other key.

        Args:
            key: Requested Spark configuration key.
            default: Spark configuration fallback value.

        Returns:
            The configured test value.
        """
        del default
        return "true" if key == "spark.python.use.daemon" else "false"

    explicit.get.side_effect = config_value
    monkeypatch.setattr(cliutil, "SparkSession", SimpleNamespace(builder=builder))

    with pytest.raises(RuntimeError, match="spark.python.use.daemon enabled"):
        build_spark("test")

    session.conf.set.assert_not_called()
    session.stop.assert_called_once_with()


def test_build_spark_applies_sql_defaults_after_validating_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """A safe context receives every non-explicit memory-bound SQL default.

    Args:
        monkeypatch: Scoped SparkSession builder replacement.
    """
    builder, session, explicit = spark_builder_fixture("false")
    monkeypatch.setattr(cliutil, "SparkSession", SimpleNamespace(builder=builder))

    assert build_spark("test") is session

    assert session.conf.set.call_count == len(SPARK_CONF_DEFAULTS)
    assert {call.args for call in session.conf.set.call_args_list} == set(SPARK_CONF_DEFAULTS.items())
    assert explicit.contains.call_count == len(SPARK_CONF_DEFAULTS)


def test_run_with_spark_preserves_job_error_when_shutdown_also_fails() -> None:
    """A secondary Spark shutdown error cannot mask the actionable job failure."""
    spark: MagicMock = MagicMock()
    spark.stop.side_effect = RuntimeError("shutdown failed")

    def work() -> None:
        """Raise the primary job failure.

        Raises:
            ValueError: Always.
        """
        raise ValueError("job failed first")

    with pytest.raises(ValueError, match="job failed first"):
        run_with_spark(spark, "test job", logging.getLogger("test.cliutil"), work)


def test_run_with_spark_preserves_base_job_error_when_shutdown_is_interrupted() -> None:
    """Cleanup cannot replace a primary failure outside the Exception hierarchy."""
    spark: MagicMock = MagicMock()
    spark.stop.side_effect = KeyboardInterrupt("shutdown interrupted")

    def work() -> None:
        """Raise the primary job failure.

        Raises:
            SystemExit: Always.
        """
        raise SystemExit("job stopped first")

    with pytest.raises(SystemExit, match="job stopped first"):
        run_with_spark(spark, "test job", logging.getLogger("test.cliutil"), work)


def test_run_with_spark_surfaces_shutdown_error_after_success() -> None:
    """A failed Spark shutdown remains visible when the job itself succeeded."""
    spark: MagicMock = MagicMock()
    spark.stop.side_effect = RuntimeError("shutdown failed")

    with pytest.raises(RuntimeError, match="shutdown failed"):
        run_with_spark(spark, "test job", logging.getLogger("test.cliutil"), lambda: 7)

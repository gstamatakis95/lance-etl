"""Per-dataset failure isolation for the ETL merge and bulk-append paths (ADR 0034).

One poisoned org must not fail the whole ETL run. The merge path catches each routing-key group's
exception inside the executor closure and reports it as a ``failed`` stats row, the bulk path drops
every transaction of a failed trio without committing anything partial, and the run returns the
isolated failure count, which maps to the fleet-wide partial-failure exit code ``3`` through
``cliutil.resolve_exit_code``.

The end-to-end tests poison one org through the ``max_keys_per_map`` boundary guard (its group's
pivot raises inside the executor) and assert every other org's dataset is still written. The
driver-side unit tests pin the bulk phase's isolation and the non-idempotent ``commit_batch``
policy: zero outer retries and a loud post-commit row-count assertion.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import lance
import pyarrow as pa
import pytest
from pyspark.sql import DataFrame, SparkSession

import lance_etl.etl.bulk as bulk_module
import lance_etl.etl.job as job_module
from lance_etl.cliutil import EXIT_PARTIAL_FAILURE, resolve_exit_code
from lance_etl.etl import ETLConfig, IcebergToLanceETL, dataset_uri
from lance_etl.etl.bulk import append_partition_trios, commit_bulk_transactions
from lance_etl.etl.plan import RoutingPlan
from lance_etl.telemetry import TelemetryConfig

SOURCE_DDL: str = (
    "org_id string, tenant_id string, namespace string, vector_id string, op string, "
    "event_timestamp timestamp, processing_timestamp timestamp, metadata map<string, string>"
)
"""Contract-satisfying Spark DDL for the isolation sources (no vectors or texts needed)."""

TS: datetime = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)
"""Single UTC timestamp reused for both required timestamp columns."""


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """Provide a two-core local Spark session pinned to the test interpreter and UTC.

    Yields:
        The module-scoped local session.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session: SparkSession = (
        SparkSession.builder.master("local[2]")
        .appName("lance-etl-failure-isolation-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def org_rows(org: str, keys_per_row: list[list[str]]) -> list[tuple]:
    """Build insert rows for one org whose metadata maps carry the given keys.

    Args:
        org: The ``org_id`` literal for every row.
        keys_per_row: For each row, the metadata keys it carries.

    Returns:
        Rows conforming to :data:`SOURCE_DDL`.
    """
    return [
        (org, "t1", "ns1", f"{org}-{i}", "insert", TS, TS, {key: "x" for key in keys})
        for i, keys in enumerate(keys_per_row)
    ]


def test_poisoned_group_does_not_block_other_datasets(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """One org whose pivot raises is isolated: every other org is written and the run reports one failure.

    The poisoned org's group carries three distinct metadata keys against ``max_keys_per_map=2``,
    so its executor-side pivot raises. The two healthy orgs must still land, the poisoned org must
    have no dataset, and the returned failure count must map to exit code 3.

    Args:
        spark: The module-scoped local Spark session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config, max_keys_per_map=2)
    rows: list[tuple] = [
        *org_rows("orgHealthyA", [["lang"], ["lang"]]),
        *org_rows("orgPoisoned", [["k0"], ["k1"], ["k2"]]),
        *org_rows("orgHealthyB", [["lang", "cat"]]),
    ]
    frame: DataFrame = spark.createDataFrame(rows, schema=SOURCE_DDL)

    failed: int = IcebergToLanceETL(config).run_on_dataframe(frame)

    assert failed == 1
    assert resolve_exit_code(failed) == EXIT_PARTIAL_FAILURE
    assert lance.dataset(dataset_uri(config, "orgHealthyA", "t1", "ns1")).count_rows() == 2
    assert lance.dataset(dataset_uri(config, "orgHealthyB", "t1", "ns1")).count_rows() == 1
    assert not Path(dataset_uri(config, "orgPoisoned", "t1", "ns1")).exists(), (
        "the poisoned group must not have been written"
    )


def test_clean_run_reports_zero_failures(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """A run with no poisoned group returns zero isolated failures and exit code 0.

    Args:
        spark: The module-scoped local Spark session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
    """
    config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
    frame: DataFrame = spark.createDataFrame(org_rows("orgClean", [["lang"], ["lang"]]), schema=SOURCE_DDL)
    failed: int = IcebergToLanceETL(config).run_on_dataframe(frame)
    assert failed == 0
    assert resolve_exit_code(failed) == 0
    assert lance.dataset(dataset_uri(config, "orgClean", "t1", "ns1")).count_rows() == 2


def make_bulk_phase_mocks(
    monkeypatch: pytest.MonkeyPatch,
    collected: list[tuple[str, str, str, int, object | None, int]],
    commit_results: dict[tuple[str, str, str], int | Exception],
) -> list[tuple[tuple[str, str, str], list[object]]]:
    """Patch the bulk-phase seams in the job module and record commit calls.

    Args:
        monkeypatch: The pytest monkeypatch fixture.
        collected: The fabricated ``run_bulk_append`` result rows.
        commit_results: Per-trio commit outcome, either an appended count or an exception to raise.

    Returns:
        The mutable list that records each ``commit_bulk_transactions`` call as
        ``(trio, transactions)``.
    """
    trios: list[tuple[str, str, str, int]] = [
        ("o1", "t1", "n1", 4),
        ("o2", "t1", "n1", 4),
    ]
    schemas: dict[tuple[str, str, str], tuple[pa.Schema, dict[str, str], dict[str, int]]] = {
        ("o1", "t1", "n1"): (pa.schema([("vector_id", pa.string())]), {}, {}),
        ("o2", "t1", "n1"): (pa.schema([("vector_id", pa.string())]), {}, {}),
    }
    commit_calls: list[tuple[tuple[str, str, str], list[object]]] = []

    def fake_commit(
        config: ETLConfig,
        telemetry: object,
        uri: str,
        transactions: list[object],
        roles: dict[str, str],
    ) -> int:
        """Record the commit call and return or raise the configured outcome."""
        del telemetry, roles
        trio: tuple[str, str, str] = next(t for t in commit_results if dataset_uri(config, *t) == uri)
        commit_calls.append((trio, transactions))
        outcome: int | Exception = commit_results[trio]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def fake_plan(*args: object) -> list[tuple[str, str, str, int]]:
        """Return the fixed big-trio list."""
        del args
        return trios

    def fake_derive(*args: object) -> dict[tuple[str, str, str], tuple[pa.Schema, dict[str, str], dict[str, int]]]:
        """Return the fixed per-trio schemas."""
        del args
        return schemas

    def fake_bootstrap(*args: object) -> list[tuple[str, str, str]]:
        """Report every trio as bootstrapped empty."""
        del args
        return list(schemas)

    def fake_run_bulk_append(*args: object) -> list[tuple[str, str, str, int, object | None, int]]:
        """Return the fabricated fan-out result rows."""
        del args
        return collected

    monkeypatch.setattr(job_module, "plan_bulk_append", fake_plan)
    monkeypatch.setattr(job_module, "derive_bulk_schemas", fake_derive)
    monkeypatch.setattr(job_module, "bootstrap_bulk_datasets", fake_bootstrap)
    monkeypatch.setattr(job_module, "run_bulk_append", fake_run_bulk_append)
    monkeypatch.setattr(job_module, "commit_bulk_transactions", fake_commit)
    return commit_calls


def bulk_phase_plan() -> RoutingPlan:
    """Return a two-big-trio routing plan for the bulk-phase isolation tests.

    Returns:
        The routing plan stub.
    """
    return RoutingPlan(
        total_rows=200,
        trio_count=2,
        big_trios=[("o1", "t1", "n1", 4), ("o2", "t1", "n1", 4)],
        num_partitions=8,
        null_routing_rows=0,
        big_trio_rows={("o1", "t1", "n1"): 100, ("o2", "t1", "n1"): 100},
    )


def test_bulk_phase_drops_all_transactions_of_a_failed_trio(
    tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A trio with any failed append task is never committed, and the run counts it as one failure.

    The failed trio contributed one successful task transaction and one failure marker: nothing of
    it may reach ``commit_batch`` (a partial commit would half-write the dataset), while the
    healthy trio commits normally. Both trios stay excluded from the merge input.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        monkeypatch: The pytest monkeypatch fixture.
    """
    config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
    healthy_txn: object = object()
    orphaned_txn: object = object()
    collected: list[tuple[str, str, str, int, object | None, int]] = [
        ("o1", "t1", "n1", 100, healthy_txn, 0),
        ("o2", "t1", "n1", 60, orphaned_txn, 0),
        ("o2", "t1", "n1", 0, None, 1),
    ]
    commit_calls = make_bulk_phase_mocks(monkeypatch, collected, {("o1", "t1", "n1"): 100})

    etl: IcebergToLanceETL = IcebergToLanceETL(config)
    seen, appended, exclusions, failed = etl.run_bulk_phase(MagicMock(), bulk_phase_plan(), MagicMock())

    assert seen == {("o1", "t1", "n1")}
    assert appended == 100
    assert failed == 1
    assert {(o, t, n) for o, t, n, _ in exclusions} == {("o1", "t1", "n1"), ("o2", "t1", "n1")}
    assert commit_calls == [(("o1", "t1", "n1"), [healthy_txn])], "the failed trio must never be committed"


def test_bulk_phase_isolates_a_commit_failure(
    tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A trio whose driver-side commit raises is counted as failed without failing the run.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        monkeypatch: The pytest monkeypatch fixture.
    """
    config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
    collected: list[tuple[str, str, str, int, object | None, int]] = [
        ("o1", "t1", "n1", 100, object(), 0),
        ("o2", "t1", "n1", 60, object(), 0),
    ]
    outcomes: dict[tuple[str, str, str], int | Exception] = {
        ("o1", "t1", "n1"): 100,
        ("o2", "t1", "n1"): ValueError("bulk append committed 60 rows but the dataset holds 120"),
    }
    make_bulk_phase_mocks(monkeypatch, collected, outcomes)

    etl: IcebergToLanceETL = IcebergToLanceETL(config)
    seen, appended, exclusions, failed = etl.run_bulk_phase(MagicMock(), bulk_phase_plan(), MagicMock())

    assert seen == {("o1", "t1", "n1")}
    assert appended == 100
    assert failed == 1
    assert len(exclusions) == 2


def commit_stub_environment(
    monkeypatch: pytest.MonkeyPatch, committed_rows: int, dataset_rows: int
) -> list[dict[str, Any]]:
    """Patch the commit seams so commit_bulk_transactions runs without a real dataset.

    Args:
        monkeypatch: The pytest monkeypatch fixture.
        committed_rows: Row total carried by the merged transaction's fragments.
        dataset_rows: Row count the post-commit ``count_rows`` re-read reports.

    Returns:
        The mutable list recording each ``commit_with_retries`` call's keyword-free arguments as
        ``{"retries": ..., "backoff": ...}``.
    """
    calls: list[dict[str, Any]] = []
    merged: SimpleNamespace = SimpleNamespace(
        operation=SimpleNamespace(fragments=[SimpleNamespace(num_rows=committed_rows)])
    )

    def fake_commit_with_retries(
        action: Any, retries: int, backoff_seconds: float, on_conflict: Any = None
    ) -> dict[str, Any]:
        """Record the retry budget and return a merged-transaction stub without committing."""
        del action, on_conflict
        calls.append({"retries": retries, "backoff": backoff_seconds})
        return {"merged": merged}

    def fake_merge_column_roles(*args: object, **kwargs: object) -> None:
        """Skip the role-persistence write."""
        del args, kwargs

    def fake_dataset(*args: object, **kwargs: object) -> SimpleNamespace:
        """Return a dataset stub reporting the configured row count."""
        del args, kwargs
        return SimpleNamespace(count_rows=lambda: dataset_rows)

    monkeypatch.setattr("lance_etl.etl.bulk.commit_with_retries", fake_commit_with_retries)
    monkeypatch.setattr("lance_etl.etl.bulk.merge_column_roles", fake_merge_column_roles)
    monkeypatch.setattr(lance, "dataset", fake_dataset)
    return calls


def test_bulk_commit_batch_never_retries(
    tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The non-idempotent commit_batch runs with a zero outer retry budget.

    Re-running a raw append after an ambiguous commit outcome would silently duplicate every row,
    so the outer wrapper must attempt exactly once and let the rerun demote the trio to the merge
    path through the emptiness check.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        monkeypatch: The pytest monkeypatch fixture.
    """
    config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
    calls: list[dict[str, Any]] = commit_stub_environment(monkeypatch, committed_rows=7, dataset_rows=7)
    appended: int = commit_bulk_transactions(config, MagicMock(), str(tmp_path / "x.lance"), [object()], {})
    assert appended == 7
    assert calls == [{"retries": 0, "backoff": config.retry_backoff_seconds}]


def test_bulk_commit_rowcount_mismatch_raises_loudly(
    tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A post-commit row count differing from the committed fragment total raises and meters.

    The freshly bootstrapped dataset must hold exactly the committed rows, so any excess is a
    duplicate append. The mismatch increments ``dataset.bulk_rowcount_mismatch`` before raising.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        monkeypatch: The pytest monkeypatch fixture.
    """
    config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)
    commit_stub_environment(monkeypatch, committed_rows=7, dataset_rows=14)
    telemetry_mock: MagicMock = MagicMock()
    with pytest.raises(ValueError, match="committed 7 rows but the dataset holds 14"):
        commit_bulk_transactions(config, telemetry_mock, str(tmp_path / "x.lance"), [object()], {})
    telemetry_mock.incr.assert_any_call("dataset.bulk_rowcount_mismatch")


def one_row_group(trio: tuple[str, str, str]) -> tuple[tuple[str, str, str], pa.Table]:
    """Build one ``(key, table)`` routing-group tuple for a trio.

    Args:
        trio: The routing key naming the group.

    Returns:
        A ``(trio, table)`` group whose table carries a single ``vector_id`` row.
    """
    return trio, pa.table({"vector_id": [f"{trio[0]}-row"]})


def test_bulk_partition_shared_stream_failure_fails_task(
    tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure raised by the shared batch stream fails the whole task instead of dropping later trios.

    This reproduces D4: when the shared routing-group generator raises mid-consumption while an
    earlier trio is being appended, the per-trio handler must not swallow it. If it did, the
    generator would be dead, the outer group loop would end normally, the task would succeed with
    partial results, and every subsequent trio in the partition would be silently dropped, its
    dataset stamped while missing this partition's rows. The fix re-raises the shared-stream failure
    so nothing partial commits and no later trio is silently stamped.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        monkeypatch: The pytest monkeypatch fixture.
    """
    config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)

    def poisoned_stream() -> Iterator[tuple[tuple[str, str, str], pa.Table]]:
        """Yield one group for trio A, then raise as trio B would be advanced to."""
        yield one_row_group(("orgA", "t1", "n1"))
        raise RuntimeError("arrow read failed mid-stream")

    def consuming_append(
        trio_key: tuple[Any, ...],
        trio_groups: Iterator[tuple[tuple[Any, ...], pa.Table]],
        schemas: dict[tuple[str, str, str], tuple[pa.Schema, dict[str, str], dict[str, int]]],
        config: ETLConfig,
        results: list[tuple[Any, ...]],
    ) -> int:
        """Drain the trio's groups so the shared stream advances into the poisoned pull."""
        del trio_key, schemas, config, results
        for _ in trio_groups:
            pass
        return 0

    monkeypatch.setattr(bulk_module, "append_one_trio", consuming_append)
    telemetry: MagicMock = MagicMock()
    results: list[tuple[Any, ...]] = []

    with pytest.raises(RuntimeError, match="arrow read failed mid-stream"):
        append_partition_trios(poisoned_stream(), {}, config, telemetry, results)

    assert results == [], "no trio may be recorded as appended or failed when the shared stream dies"
    telemetry.incr.assert_not_called()


def test_bulk_partition_isolates_single_trio_processing_failure(
    tmp_path: Path, telemetry_config: TelemetryConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure confined to one trio's own groups marks only that trio failed and processes the next.

    This guards the isolation contract the fix must preserve: when the shared stream is healthy and
    the failure lives entirely in appending one trio's already-materialised groups, the handler must
    mark just that trio failed and continue, not fail the whole task. The later trio still appends.

    Args:
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The test telemetry configuration.
        monkeypatch: The pytest monkeypatch fixture.
    """
    config: ETLConfig = ETLConfig(base_uri=str(tmp_path), telemetry=telemetry_config)

    def healthy_stream() -> Iterator[tuple[tuple[str, str, str], pa.Table]]:
        """Yield one clean group each for trio A and trio B, then stop."""
        yield one_row_group(("orgA", "t1", "n1"))
        yield one_row_group(("orgB", "t1", "n1"))

    def selective_append(
        trio_key: tuple[Any, ...],
        trio_groups: Iterator[tuple[tuple[Any, ...], pa.Table]],
        schemas: dict[tuple[str, str, str], tuple[pa.Schema, dict[str, str], dict[str, int]]],
        config: ETLConfig,
        results: list[tuple[Any, ...]],
    ) -> int:
        """Raise on trio A's own data, append a success row for trio B."""
        del schemas, config
        for _ in trio_groups:
            pass
        if trio_key[0] == "orgA":
            raise ValueError("pivot failed on this trio's data")
        results.append((trio_key[0], trio_key[1], trio_key[2], 5, object(), 0))
        return 5

    monkeypatch.setattr(bulk_module, "append_one_trio", selective_append)
    telemetry: MagicMock = MagicMock()
    results: list[tuple[Any, ...]] = []

    append_partition_trios(healthy_stream(), {}, config, telemetry, results)

    assert ("orgA", "t1", "n1", 0, None, 1) in results, "the poisoned trio must be recorded as failed"
    appended_trios: set[tuple[str, str, str]] = {(o, t, n) for o, t, n, _, txn, failed in results if not failed}
    assert appended_trios == {("orgB", "t1", "n1")}, "the healthy later trio must still append"
    telemetry.incr.assert_any_call("dataset.bulk_group_failed")

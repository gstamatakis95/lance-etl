"""The indexing plan-phase failure marker.

``LanceIndexer.run`` records a plan fan-out failure as a dataset-level error tagged
``error_phase="plan"`` (runner.py:871), isolating that dataset while the rest of the fleet run
proceeds. This drives the stable indexing runner under an in-process fake Spark session, so it does
not depend on the concurrently-edited reconciler surface.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import lance
import pyarrow as pa
import pytest
from conftest import FakeRdd, FakeSpark

import lance_etl.indexing.runner as indexing_runner
from lance_etl.fanout import count_failed
from lance_etl.indexing import IndexJobConfig, LanceIndexer
from lance_etl.telemetry import TelemetryConfig


def test_index_plan_failure_records_plan_phase(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A plan fan-out failure lands a dataset-level error tagged ``error_phase='plan'``."""
    uri: str = str(tmp_path / "plan.lance")
    lance.write_dataset(pa.table({"id": pa.array([1, 2, 3], pa.int64())}), uri)

    def boom(dataset_uri: str, config: IndexJobConfig, telemetry: object) -> dict[str, Any]:
        """Fail the plan phase for the dataset.

        Args:
            dataset_uri: Ignored dataset URI.
            config: Ignored indexing configuration.
            telemetry: Ignored executor telemetry facade.

        Raises:
            RuntimeError: Always, to drive the plan-phase failure path.
        """
        del dataset_uri, config, telemetry
        raise RuntimeError("plan resolution failed")

    monkeypatch.setattr(indexing_runner, "plan_dataset_indexes", boom)
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(service="lance-etl-tests", env="test"),
        scalar_columns=["id"],
        commit_backoff_seconds=0.0,
    )
    results: list[dict[str, Any]] = LanceIndexer(config).run(FakeSpark(), [uri])

    assert len(results) == 1
    assert results[0]["error_phase"] == "plan"
    assert "plan resolution failed" in str(results[0]["error"])


def test_fleet_run_counts_unopenable_dataset_as_failed(tmp_path: Path) -> None:
    """A fleet-level LanceIndexer.run counts a nonexistent dataset URI as failed.

    Regression guard for PR-02 finding 1: `plan_dataset_indexes`'s own open-failure catch must
    return an `{"error", "phase"}` marker, not a benign `"skipped"` one, so it is visible to
    :func:`~lance_etl.fanout.count_failed`.
    """
    missing_uri: str = str(tmp_path / "does_not_exist.lance")
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(service="lance-etl-tests", env="test"),
        scalar_columns=["id"],
        commit_backoff_seconds=0.0,
    )
    results: list[dict[str, Any]] = LanceIndexer(config).run(FakeSpark(), [missing_uri])
    assert count_failed(results) == 1
    assert results[0]["error_phase"] == "open"


def test_indexing_deduplicates_dataset_uris(monkeypatch: pytest.MonkeyPatch) -> None:
    """A repeated URI is planned once instead of being scheduled concurrently with itself.

    Args:
        monkeypatch: Pytest monkeypatch used to record plan calls.
    """
    planned: list[str] = []

    def record_plan(dataset_uri: str, config: IndexJobConfig, telemetry: object) -> dict[str, Any]:
        """Record one URI and return a terminal no-work plan.

        Args:
            dataset_uri: URI being planned.
            config: Ignored indexing configuration.
            telemetry: Ignored executor telemetry facade.

        Returns:
            A terminal skipped plan.
        """
        del config, telemetry
        planned.append(dataset_uri)
        return {"uri": dataset_uri, "indexes": [], "skipped": "nothing to do"}

    monkeypatch.setattr(indexing_runner, "plan_dataset_indexes", record_plan)
    config: IndexJobConfig = IndexJobConfig(telemetry=TelemetryConfig())
    results: list[dict[str, Any]] = LanceIndexer(config).run(FakeSpark(), ["a", "a", "b", "a"])

    assert planned == ["a", "b"]
    assert [result["uri"] for result in results] == ["a", "b"]


def test_duplicate_index_names_are_rejected_before_build(tmp_path: Path) -> None:
    """Repeated targets cannot collapse into the same name-keyed shard payload.

    Args:
        tmp_path: Pytest-provided temporary directory.
    """
    uri: str = str(tmp_path / "duplicate_target.lance")
    lance.write_dataset(pa.table({"id": pa.array([1, 2, 3], pa.int64())}), uri)
    config: IndexJobConfig = IndexJobConfig(telemetry=TelemetryConfig(), scalar_columns=["id", "id"])

    with pytest.raises(ValueError, match="configured more than once"):
        indexing_runner.resolve_index_targets(lance.dataset(uri), config)


def test_high_fragment_plan_returns_one_bounded_index_seed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A million-fragment FTS rebuild never returns fragment IDs to the driver."""
    dataset: MagicMock = MagicMock()
    dataset.version = 17
    dataset.describe_indices.return_value = []
    dataset.stats.dataset_stats.return_value = {"num_fragments": 1_000_000}
    monkeypatch.setattr(indexing_runner.lance, "dataset", MagicMock(return_value=dataset))
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(),
        text_columns=["text"],
        fragments_per_index_task=8,
    )

    plan: dict[str, Any] = indexing_runner.plan_dataset_indexes("large", config, MagicMock())
    spec: dict[str, Any] = plan["specs"][0]
    seeds: list[dict[str, Any]] = indexing_runner.flatten_shard_tasks(
        {"large": plan["specs"]}, {"large": plan["version"]}
    )

    assert spec["fragments"] == 1_000_000
    assert spec["shard_count"] == 125_000
    assert "shards" not in spec
    assert len(seeds) == 1
    assert seeds[0]["shard"] == []
    assert len(repr(seeds[0])) < 512
    dataset.get_fragments.assert_not_called()


def test_build_and_commit_collects_only_bounded_index_outcomes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serialized segment metadata reduces to commit executors without crossing the driver."""
    segment_marker: str = "large-segment-metadata-" + ("x" * 262_144)
    committed_documents: list[str] = []

    def enumerate_two_shards(seed: dict[str, Any], config: IndexJobConfig) -> list[dict[str, Any]]:
        """Return two deterministic shard tasks for the bounded seed.

        Args:
            seed: Bounded index seed.
            config: Ignored indexing configuration.

        Returns:
            Two exact-version shard tasks.
        """
        del config
        return [
            indexing_runner.build_shard_task(seed, str(seed["uri"]), int(seed["version"]), [fragment_id])
            for fragment_id in range(2)
        ]

    def build_large_segment(
        task: dict[str, Any], config: IndexJobConfig, telemetry: object
    ) -> tuple[str, str, dict[str, Any]]:
        """Return a deliberately large serialized segment payload.

        Args:
            task: Exact-version shard task.
            config: Ignored indexing configuration.
            telemetry: Ignored executor telemetry facade.

        Returns:
            A normal build result carrying the large segment marker.
        """
        del config, telemetry
        document: str = f"{segment_marker}-{task['shard'][0]}"
        return str(task["uri"]), str(task["index_name"]), {"segment": document}

    def commit_segments_on_reducer(
        uri: str,
        spec: dict[str, Any],
        payloads: list[dict[str, Any]],
        config: IndexJobConfig,
        telemetry: object,
    ) -> dict[str, Any]:
        """Record the segment documents received by the simulated commit executor.

        Args:
            uri: Dataset URI.
            spec: Bounded commit specification.
            payloads: Executor-reduced segment payloads.
            config: Ignored indexing configuration.
            telemetry: Ignored executor telemetry facade.

        Returns:
            Bounded terminal index statistics.
        """
        del uri, config, telemetry
        committed_documents.extend(str(payload["segment"]) for payload in payloads)
        return {
            "column": spec["column"],
            "index": spec["index_name"],
            "segments": len(payloads),
            "fragments": int(spec["fragments"]),
        }

    def collect_without_segment_metadata(rdd: FakeRdd) -> list[object]:
        """Fail if a Spark collection attempts to return segment payloads to the driver.

        Args:
            rdd: Fake RDD whose items would cross the collection boundary.

        Returns:
            The bounded collected items.
        """
        items: list[object] = list(rdd.items)
        assert segment_marker not in repr(items)
        return items

    monkeypatch.setattr(indexing_runner, "enumerate_shard_tasks", enumerate_two_shards)
    monkeypatch.setattr(indexing_runner, "build_one_shard", build_large_segment)
    monkeypatch.setattr(indexing_runner, "commit_one_index", commit_segments_on_reducer)
    monkeypatch.setattr(FakeRdd, "collect", collect_without_segment_metadata)
    seed: dict[str, Any] = {
        "kind": indexing_runner.BTREE_KIND,
        "column": "id",
        "index_name": "id_idx",
        "mode": "segments",
        "uri": "large",
        "version": 9,
        "shard": [],
        "fragments": 2,
        "shard_count": 2,
    }
    config: IndexJobConfig = IndexJobConfig(telemetry=TelemetryConfig())

    outcomes: list[tuple[str, dict[str, Any]]] = LanceIndexer(config).build_and_commit_fleet(FakeSpark(), [seed])

    assert outcomes == [("large", {"column": "id", "index": "id_idx", "segments": 2, "fragments": 2})]
    assert committed_documents == [f"{segment_marker}-0", f"{segment_marker}-1"]


def test_build_reducer_discards_segments_after_any_shard_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """One failed shard prevents commit and drops successful serialized segment metadata."""
    task: dict[str, Any] = {
        "kind": indexing_runner.BTREE_KIND,
        "column": "id",
        "index_name": "id_idx",
        "mode": "segments",
        "uri": "large",
        "version": 9,
        "shard": [0],
        "fragments": 2,
    }
    successful: dict[str, Any] = indexing_runner.build_aggregate(task, {"segment": "successful-segment"})
    failed: dict[str, Any] = indexing_runner.build_aggregate(
        {**task, "shard": [1]},
        {"error": "shard failed", "phase": "build", "column": "id"},
    )
    commit: MagicMock = MagicMock(side_effect=AssertionError("partial index committed"))
    monkeypatch.setattr(indexing_runner, "commit_one_index", commit)

    reduced: dict[str, Any] = indexing_runner.merge_build_aggregates(successful, failed)
    _, outcome = indexing_runner.commit_build_aggregate(
        ("large", "id_idx"),
        reduced,
        IndexJobConfig(telemetry=TelemetryConfig()),
        MagicMock(),
    )

    assert reduced["segments"] == []
    assert outcome["error"] == "shard failed"
    assert outcome["phase"] == "build"
    commit.assert_not_called()


def test_executor_enumeration_reproduces_exact_n_shards(monkeypatch: pytest.MonkeyPatch) -> None:
    """One bounded scalar seed expands to N complete fragment shards only on the executor."""
    dataset: MagicMock = MagicMock()
    dataset.get_fragments.return_value = [SimpleNamespace(fragment_id=value) for value in range(33)]
    dataset.describe_indices.return_value = []
    monkeypatch.setattr(indexing_runner.lance, "dataset", MagicMock(return_value=dataset))
    config: IndexJobConfig = IndexJobConfig(
        telemetry=TelemetryConfig(),
        scalar_columns=["id"],
        fragments_per_index_task=8,
    )
    seed: dict[str, Any] = {
        "kind": indexing_runner.BTREE_KIND,
        "column": "id",
        "index_name": "id_idx",
        "mode": "segments",
        "uri": "large",
        "version": 9,
        "shard": [],
        "fragments": 33,
        "shard_count": 5,
    }

    tasks: list[dict[str, Any]] = list(indexing_runner.enumerate_shard_tasks(seed, config))

    assert len(tasks) == 5
    assert sorted(fragment_id for task in tasks for fragment_id in task["shard"]) == list(range(33))
    assert max(len(task["shard"]) for task in tasks) == 7


def test_vector_artifacts_are_cached_per_worker_generation(monkeypatch: pytest.MonkeyPatch) -> None:
    """N vector shards load artifacts once until dataset version or index identity changes."""
    dataset: MagicMock = MagicMock()
    dataset.describe_indices.return_value = []
    handler: MagicMock = MagicMock()
    handler.prepare.return_value = ("centroids", "rotation", 1, 4)
    handler.build_segment.return_value = object()
    monkeypatch.setattr(indexing_runner.lance, "dataset", MagicMock(return_value=dataset))
    monkeypatch.setattr(
        indexing_runner,
        "load_vector_config",
        MagicMock(return_value={"num_partitions": 4, "rabitq_model": "rotation"}),
    )
    monkeypatch.setattr(indexing_runner, "make_handler", MagicMock(return_value=handler))
    monkeypatch.setattr(indexing_runner, "serialize_segment", MagicMock(return_value="segment"))
    config: IndexJobConfig = IndexJobConfig(telemetry=TelemetryConfig(), vector_columns=["vector"])
    telemetry: MagicMock = MagicMock()
    indexing_runner.VECTOR_ARTIFACT_CACHE.clear()
    task: dict[str, Any] = {
        "kind": indexing_runner.VECTOR_KIND,
        "column": "vector",
        "index_name": "vector_idx",
        "mode": "segments",
        "uri": "cache-dataset",
        "version": 7,
        "shard": [0],
        "artifact_generation": "generation",
        "artifact_num_partitions": 4,
    }

    cache_size: int = 0
    try:
        for fragment_id in range(12):
            indexing_runner.build_one_shard({**task, "shard": [fragment_id]}, config, telemetry)
        indexing_runner.build_one_shard({**task, "version": 8}, config, telemetry)
        indexing_runner.build_one_shard({**task, "index_name": "other_vector_idx"}, config, telemetry)
        cache_size = len(indexing_runner.VECTOR_ARTIFACT_CACHE)
    finally:
        indexing_runner.VECTOR_ARTIFACT_CACHE.clear()

    assert handler.prepare.call_count == 3
    assert handler.build_segment.call_count == 14
    assert telemetry.incr.call_count >= 11
    assert cache_size == 1
    assert indexing_runner.vector_artifact_cache_key(
        dataset, task, config
    ) != indexing_runner.vector_artifact_cache_key(
        dataset,
        {**task, "artifact_num_partitions": 8},
        config,
    )

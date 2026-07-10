"""End-to-end idempotency tests for the unified fleet pipeline.

The production guarantee under test is convergence: a scheduled pipeline that keeps re-running
over an already-maintained fleet must not thrash it. Running :class:`PipelineJob` a second time
over the same datasets has to be a clean no-op — no new dataset version is committed, no new
index segment or delta is built, and every maintenance and index plan reaches a skip or terminal
state. If the second run does real work, that is a genuine idempotency bug and these tests fail
loudly rather than papering over it.

Because the pipeline order is prune -> maintenance -> index -> stamp, the first run compacts a
multi-fragment dataset before indexing the post-compaction fragments, so the second run finds
nothing uncovered and skips both the compaction and the index phases. The tests build a real
local Lance fleet, drive the unmocked :class:`PipelineJob` on a real local Spark session (the
same executor closures production uses), and assert the second run leaves the dataset version and
the index layout byte-for-byte where the first run left them.

Tag pruning and stamping are intentionally out of scope here (``tag_keep_last=None``,
``tag_stamp=None``, ``serve_tag=False``) so the no-op assertions focus on data and index state.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import lance
import pytest
from conftest import make_vector_table, write_fragmented_dataset
from pyspark.sql import SparkSession

from lance_etl.indexing.config import IndexJobConfig
from lance_etl.indexing.runner import LanceIndexer
from lance_etl.maintenance.job import MaintenanceConfig, MaintenanceJob
from lance_etl.pipeline.job import PipelineConfig, PipelineJob
from lance_etl.telemetry import TelemetryConfig

pytestmark = pytest.mark.integration

DIM: int = 8
"""Vector dimension, divisible by 8 as ``build_rq_model`` requires."""

MULTI_ROWS: int = 1024
"""Row count of the multi-fragment dataset, above the vector build floor used here."""

SINGLE_ROWS: int = 512
"""Row count of the single-fragment dataset, above the vector build floor used here."""

ROWS_PER_FRAGMENT: int = 256
"""Fragment size that splits the multi-fragment dataset into four fragments to compact."""

EXPECTED_INDEX_NAMES: frozenset[str] = frozenset({"vector_idx", "id_idx", "category_bitmap_idx", "text_fts_idx"})
"""The four index names the mixed configuration builds on every dataset."""


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """Provide a local Spark session pinned to the test interpreter.

    Yields:
        A two-core local session with a UTC timezone and four shuffle partitions, created once
        for the module and stopped on teardown.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session: SparkSession = (
        SparkSession.builder.master("local[2]")
        .appName("lance-etl-fleet-idempotency-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def build_fleet(tmp_path: Path) -> tuple[str, str]:
    """Write a heterogeneous two-dataset fleet: one multi-fragment, one single-fragment.

    The multi-fragment dataset is split into four fragments so the first maintenance run performs
    real compaction work, while the single-fragment dataset is a trivial compaction no-op. Both
    carry enough rows for the vector index to bootstrap with the tiny partition count the test
    configures.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        The ``(multi_fragment_uri, single_fragment_uri)`` pair.
    """
    multi_uri: str = str(tmp_path / "multi.lance")
    single_uri: str = str(tmp_path / "single.lance")
    write_fragmented_dataset(multi_uri, make_vector_table(rows=MULTI_ROWS, dim=DIM, seed=1), ROWS_PER_FRAGMENT)
    write_fragmented_dataset(single_uri, make_vector_table(rows=SINGLE_ROWS, dim=DIM, seed=2), SINGLE_ROWS)
    return multi_uri, single_uri


def pipeline_config(telemetry_config: TelemetryConfig) -> PipelineConfig:
    """Build a pipeline configuration exercising compaction and a mix of index types.

    Compaction is enabled with the default target fragment size (so the four small fragments
    merge into one), TTL stays off, and the indexing sub-config targets a vector index, a BTREE
    scalar index, a BITMAP scalar index, and an FTS/INVERTED index. Tag pruning and stamping are
    disabled so the no-op assertions concern only data and index state.

    Args:
        telemetry_config: The shared telemetry configuration.

    Returns:
        A pipeline configuration ready to drive both fleet runs.
    """
    maintenance: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config, commit_backoff_seconds=0.0)
    indexing: IndexJobConfig = IndexJobConfig(
        telemetry=telemetry_config,
        vector_columns=["vector"],
        num_partitions=4,
        vector_min_rows=1,
        scalar_columns=["id"],
        bitmap_columns=["category"],
        text_columns=["text"],
        commit_backoff_seconds=0.0,
    )
    return PipelineConfig(
        telemetry=telemetry_config,
        maintenance=maintenance,
        indexing=indexing,
        tag_keep_last=None,
        tag_stamp=None,
        serve_tag=False,
    )


def index_signature(uri: str) -> dict[str, tuple[int, frozenset[int]]]:
    """Return a comparable fingerprint of a dataset's committed index layout.

    ``list_indices`` returns one entry per index delta (a bootstrap plus each incremental
    segment), so the per-name entry count captures whether a new segment or delta was built and
    the union of covered fragment ids captures whether coverage changed. Equal signatures across
    two runs therefore prove the second run added no index work.

    Args:
        uri: The dataset URI.

    Returns:
        A mapping of index name to ``(delta_count, covered_fragment_ids)``.
    """
    counts: dict[str, int] = {}
    fragments: dict[str, set[int]] = {}
    for item in lance.dataset(uri).list_indices():
        name: str = item["name"]
        counts[name] = counts.get(name, 0) + 1
        fragments.setdefault(name, set()).update(item.get("fragment_ids", []) or [])
    return {name: (counts[name], frozenset(fragments[name])) for name in counts}


def result_by_uri(results: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Index a list of per-dataset result dicts by their URI.

    Args:
        results: The per-dataset result dicts from a fleet phase.

    Returns:
        The results keyed by URI.
    """
    return {result["uri"]: result for result in results}


def test_pipeline_second_run_is_noop(spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig) -> None:
    """A second identical pipeline run over an already-maintained fleet is a clean no-op.

    Run 1 does real work: it compacts the multi-fragment dataset and builds all four index types
    on both datasets. Run 2 must commit no new dataset version, add no index segment or delta,
    and report every dataset skipped by both the maintenance and index phases. The version
    equality is the strongest signal because neither a no-op compaction plan nor a no-op index
    plan bumps the current version (version cleanup prunes old versions but never advances the
    current one).

    Args:
        spark: The module-scoped local Spark session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The shared telemetry configuration.
    """
    multi_uri, single_uri = build_fleet(tmp_path)
    uris: list[str] = [multi_uri, single_uri]
    config: PipelineConfig = pipeline_config(telemetry_config)

    assert len(lance.dataset(multi_uri).get_fragments()) == 4, "the multi dataset must start fragmented"
    assert len(lance.dataset(single_uri).get_fragments()) == 1, "the single dataset must start with one fragment"

    first: dict[str, Any] = PipelineJob(config).run(spark, uris)

    assert first["counts"]["failed"] == 0, f"run 1 had failures: {first['index_results']}"
    first_maint: dict[str, dict[str, Any]] = result_by_uri(first["maintenance_results"])
    assert int(first_maint[multi_uri].get("tasks", 0)) > 0, "run 1 must plan compaction tasks for the multi dataset"
    assert int(first_maint[multi_uri].get("fragments_removed", 0)) > 0, "run 1 must compact the multi dataset"
    assert len(lance.dataset(multi_uri).get_fragments()) == 1, "run 1 must merge the multi dataset to one fragment"
    for uri in uris:
        built: set[str] = set(index_signature(uri))
        assert built >= EXPECTED_INDEX_NAMES, f"run 1 must build every configured index on {uri}, got {built}"

    version_after_first: dict[str, int] = {uri: lance.dataset(uri).version for uri in uris}
    signature_after_first: dict[str, dict[str, tuple[int, frozenset[int]]]] = {
        uri: index_signature(uri) for uri in uris
    }

    second: dict[str, Any] = PipelineJob(config).run(spark, uris)

    assert second["counts"]["failed"] == 0, f"run 2 had failures: {second['index_results']}"
    assert second["counts"]["maintenance_skipped"] == len(uris), "run 2 must skip maintenance on every dataset"
    assert second["counts"]["index_skipped"] == len(uris), "run 2 must skip indexing on every dataset"

    for uri in uris:
        assert lance.dataset(uri).version == version_after_first[uri], (
            f"run 2 advanced the version of {uri} from {version_after_first[uri]} to "
            f"{lance.dataset(uri).version}; a no-op run must not commit"
        )
        assert index_signature(uri) == signature_after_first[uri], (
            f"run 2 changed the index layout of {uri}: {index_signature(uri)} != {signature_after_first[uri]}"
        )

    second_maint: dict[str, dict[str, Any]] = result_by_uri(second["maintenance_results"])
    for uri in uris:
        maint: dict[str, Any] = second_maint[uri]
        assert int(maint.get("tasks", 0)) == 0, f"run 2 planned compaction tasks for {uri}: {maint}"
        assert int(maint.get("fragments_removed", 0)) == 0, f"run 2 rewrote fragments for {uri}: {maint}"

    second_index: dict[str, dict[str, Any]] = result_by_uri(second["index_results"])
    for uri in uris:
        index_result: dict[str, Any] = second_index[uri]
        assert "error" not in index_result, f"run 2 index result carries a dataset error for {uri}: {index_result}"
        assert index_result.get("skipped"), f"run 2 must skip indexing {uri} as current: {index_result}"
        for entry in index_result.get("indexes", []):
            assert "error" not in entry, f"run 2 recorded an index error on {uri}: {entry}"
            assert int(entry.get("segments", 0)) == 0, f"run 2 built new index segments on {uri}: {entry}"


def test_indexer_and_maintenance_standalone_second_run_noop(
    spark: SparkSession, tmp_path: Path, telemetry_config: TelemetryConfig
) -> None:
    """The maintenance and index jobs are each idempotent when driven directly, not via the pipeline.

    Running :meth:`MaintenanceJob.run` twice and then :meth:`LanceIndexer.run` twice over the same
    single-fragment dataset must converge: the second maintenance call plans no compaction, the
    second index call skips as current, and neither advances the dataset version past its first
    fully-indexed state. This is the pipeline's guarantee reduced to its two heavy sub-jobs.

    Args:
        spark: The module-scoped local Spark session.
        tmp_path: Pytest-provided temporary directory.
        telemetry_config: The shared telemetry configuration.
    """
    uri: str = str(tmp_path / "standalone.lance")
    write_fragmented_dataset(uri, make_vector_table(rows=SINGLE_ROWS, dim=DIM, seed=3), SINGLE_ROWS)
    maintenance_config: MaintenanceConfig = MaintenanceConfig(telemetry=telemetry_config, commit_backoff_seconds=0.0)
    index_config: IndexJobConfig = IndexJobConfig(
        telemetry=telemetry_config,
        vector_columns=["vector"],
        num_partitions=4,
        vector_min_rows=1,
        scalar_columns=["id"],
        commit_backoff_seconds=0.0,
    )

    MaintenanceJob(maintenance_config).run(spark, [uri])
    first_index: list[dict[str, Any]] = LanceIndexer(index_config).run(spark, [uri])
    assert not any("error" in stats for stats in first_index), f"standalone run 1 indexing failed: {first_index}"
    assert {"vector_idx", "id_idx"} <= set(index_signature(uri)), "standalone run 1 must build both indexes"

    version_after_first: int = lance.dataset(uri).version
    signature_after_first: dict[str, tuple[int, frozenset[int]]] = index_signature(uri)

    second_maint: list[dict[str, Any]] = MaintenanceJob(maintenance_config).run(spark, [uri])
    assert int(second_maint[0].get("tasks", 0)) == 0, f"standalone maintenance run 2 did work: {second_maint}"
    second_index: list[dict[str, Any]] = LanceIndexer(index_config).run(spark, [uri])
    assert second_index[0].get("skipped"), f"standalone indexing run 2 must skip as current: {second_index}"

    assert lance.dataset(uri).version == version_after_first, "standalone run 2 must not advance the version"
    assert index_signature(uri) == signature_after_first, "standalone run 2 must not change the index layout"

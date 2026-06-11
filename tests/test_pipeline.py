"""Tests for the unified pipeline job: phase ordering, tag retention, stamp gating, and config propagation.

Uses the FakeSpark/FakeSparkContext pattern from test_maintenance.py so that fan-out callables run
in the driver process and can be observed and intercepted with monkeypatching.  Tag-retention tests
use a real tiny Lance dataset so that ``dataset.tags`` operations exercise actual Lance behaviour.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import lance
import pyarrow as pa
import pytest

import lance_etl.pipeline.job as pipeline_job
from lance_etl.indexing.config import IndexJobConfig
from lance_etl.maintenance.job import MaintenanceConfig
from lance_etl.maintenance.tools import prune_interval_tags
from lance_etl.pipeline.job import PipelineConfig, PipelineJob, stamp_eligible
from lance_etl.telemetry import Telemetry, TelemetryConfig


class FakeRdd:
    """Minimal in-process stand-in for a Spark RDD."""

    def __init__(self, items: Iterable[object]) -> None:
        """Initialize the fake RDD.

        Args:
            items: The items to distribute.
        """
        self.items: list[object] = list(items)

    def map(self, fn: Callable[[object], object]) -> FakeRdd:
        """Apply a function to every item eagerly.

        Args:
            fn: The mapper.

        Returns:
            A new fake RDD with the mapped items.
        """
        return FakeRdd([fn(item) for item in self.items])

    def mapPartitions(self, fn: Callable[[Iterator[object]], Iterator[object]]) -> FakeRdd:
        """Apply a partition function to the single in-process partition.

        Args:
            fn: The partition mapper yielding outputs.

        Returns:
            A new fake RDD with the collected outputs.
        """
        return FakeRdd(list(fn(iter(self.items))))

    def collect(self) -> list[object]:
        """Return the items.

        Returns:
            The current items.
        """
        return list(self.items)


class FakeSparkContext:
    """Minimal stand-in for a SparkContext running everything in process."""

    def parallelize(self, items: Iterable[object], slices: int) -> FakeRdd:
        """Wrap items into a fake RDD.

        Args:
            items: The items to distribute.
            slices: Ignored partition count.

        Returns:
            The fake RDD.
        """
        del slices
        return FakeRdd(items)

    def setLocalProperty(self, key: str, value: str | None) -> None:
        """Accept and ignore scheduler-pool properties.

        Args:
            key: The property name.
            value: The property value.
        """
        del key, value


class FakeSpark:
    """Minimal stand-in for a SparkSession driving fan-out in the driver process."""

    def __init__(self) -> None:
        """Initialize the fake session with its fake context."""
        self.sparkContext: FakeSparkContext = FakeSparkContext()


def make_config(telemetry_config: TelemetryConfig, **kwargs: Any) -> PipelineConfig:
    """Build a PipelineConfig with test-safe sub-configs.

    Args:
        telemetry_config: The shared telemetry config.
        **kwargs: Overrides forwarded to PipelineConfig.

    Returns:
        A PipelineConfig ready for use in tests.
    """
    maintenance = MaintenanceConfig(
        telemetry=telemetry_config,
        commit_backoff_seconds=0.0,
    )
    indexing = IndexJobConfig(telemetry=telemetry_config)
    return PipelineConfig(
        telemetry=telemetry_config,
        maintenance=maintenance,
        indexing=indexing,
        **kwargs,
    )


def write_tiny_dataset(path: Path) -> str:
    """Write a tiny one-fragment Lance dataset.

    Args:
        path: Destination path; a subdirectory named ``tiny.lance`` is created inside it.

    Returns:
        The dataset URI string.
    """
    uri: str = str(path / "tiny.lance")
    lance.write_dataset(pa.table({"id": pa.array([1, 2, 3], pa.int64())}), uri)
    return uri


def noop_maintenance_run(self: Any, spark: Any, uris: list[str]) -> list[dict[str, Any]]:
    """Substitute for MaintenanceJob.run that returns a skipped result per URI.

    Args:
        self: Unused bound instance.
        spark: Unused Spark session.
        uris: Dataset URIs to echo.

    Returns:
        One skipped-tier result per URI.
    """
    del self, spark
    return [{"uri": u, "tier": "small", "bytes_removed": 0} for u in uris]


def noop_indexer_run(self: Any, spark: Any, uris: list[str]) -> list[dict[str, Any]]:
    """Substitute for LanceIndexer.run that returns an empty-index result per URI.

    Args:
        self: Unused bound instance.
        spark: Unused Spark session.
        uris: Dataset URIs to echo.

    Returns:
        One empty-index result per URI.
    """
    del self, spark
    return [{"uri": u, "indexes": [], "tier": "small"} for u in uris]


def noop_prune_fleet(
    spark: Any,
    uris: Any,
    telemetry_cfg: Any,
    storage_options: Any,
    tag_keep_last: Any,
    partitions: Any = 512,
) -> list[dict[str, Any]]:
    """Substitute for prune_interval_tags_fleet that does nothing.

    Args:
        spark: Unused Spark session.
        uris: Unused URI list.
        telemetry_cfg: Unused telemetry config.
        storage_options: Unused storage options.
        tag_keep_last: Unused retention count.
        partitions: Unused partition count.

    Returns:
        An empty list.
    """
    del spark, uris, telemetry_cfg, storage_options, tag_keep_last, partitions
    return []


def noop_update_serving_tags(
    spark: Any,
    dataset_uris: Any,
    telemetry_cfg: Any,
    storage_options: Any,
    tag: str = "HEAD",
    target_version: Any = None,
    partitions: int = 512,
) -> list[dict[str, Any]]:
    """Substitute for update_serving_tags that does nothing.

    Args:
        spark: Unused Spark session.
        dataset_uris: Unused URI iterable.
        telemetry_cfg: Unused telemetry config.
        storage_options: Unused storage options.
        tag: Unused tag name.
        target_version: Unused target version.
        partitions: Unused partition count.

    Returns:
        An empty list.
    """
    del spark, dataset_uris, telemetry_cfg, storage_options, tag, target_version, partitions
    return []


class TestPhaseOrdering:
    """The pipeline executes phases in the order prune, maintenance, index, stamp."""

    def test_phases_run_in_order(
        self,
        telemetry_config: TelemetryConfig,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Monkeypatched phase callables append markers; the order must be prune/maintenance/index/stamp."""
        uri: str = write_tiny_dataset(tmp_path)
        order: list[str] = []

        def fake_prune_fleet(
            spark: Any,
            uris: Any,
            telemetry_cfg: Any,
            storage_options: Any,
            tag_keep_last: Any,
            partitions: Any = 512,
        ) -> list[dict[str, Any]]:
            """Record the prune phase."""
            del spark, uris, telemetry_cfg, storage_options, tag_keep_last, partitions
            order.append("prune")
            return [{"uri": uri, "tags_pruned": 0, "tags_kept": 0}]

        def fake_maintenance_run(self_inner: Any, spark: Any, uris: Any) -> list[dict[str, Any]]:
            """Record the maintenance phase."""
            del self_inner, spark, uris
            order.append("maintenance")
            return [{"uri": uri, "tier": "small", "bytes_removed": 0}]

        def fake_indexer_run(self_inner: Any, spark: Any, uris: Any) -> list[dict[str, Any]]:
            """Record the index phase."""
            del self_inner, spark, uris
            order.append("index")
            return [{"uri": uri, "indexes": [], "tier": "small"}]

        def fake_update_serving_tags(
            spark: Any,
            dataset_uris: Any,
            telemetry_cfg: Any,
            storage_options: Any,
            tag: str = "HEAD",
            target_version: Any = None,
            partitions: int = 512,
        ) -> list[dict[str, Any]]:
            """Record each stamp call."""
            del spark, dataset_uris, telemetry_cfg, storage_options, target_version, partitions
            order.append(f"stamp:{tag}")
            return [{"uri": uri, "tag": tag, "version": 1, "created": True}]

        monkeypatch.setattr(pipeline_job, "prune_interval_tags_fleet", fake_prune_fleet)
        monkeypatch.setattr(pipeline_job.MaintenanceJob, "run", fake_maintenance_run)
        monkeypatch.setattr(pipeline_job.LanceIndexer, "run", fake_indexer_run)
        monkeypatch.setattr(pipeline_job, "update_serving_tags", fake_update_serving_tags)

        config = make_config(
            telemetry_config,
            tag_keep_last=10,
            tag_stamp="20260611T120000Z",
            serve_tag=True,
        )
        PipelineJob(config).run(FakeSpark(), [uri])
        assert order == ["prune", "maintenance", "index", "stamp:20260611T120000Z", "stamp:HEAD"]

    def test_prune_skipped_when_tag_keep_last_none(
        self,
        telemetry_config: TelemetryConfig,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """When tag_keep_last is None the prune phase must not run."""
        uri: str = write_tiny_dataset(tmp_path)
        prune_called: list[bool] = []

        def fail_prune(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            """Fail if prune is called with tag_keep_last=None."""
            del args, kwargs
            prune_called.append(True)
            raise AssertionError("prune_interval_tags_fleet must not run when tag_keep_last is None")

        monkeypatch.setattr(pipeline_job, "prune_interval_tags_fleet", fail_prune)
        monkeypatch.setattr(pipeline_job.MaintenanceJob, "run", noop_maintenance_run)
        monkeypatch.setattr(pipeline_job.LanceIndexer, "run", noop_indexer_run)

        config = make_config(telemetry_config, tag_keep_last=None, tag_stamp=None)
        PipelineJob(config).run(FakeSpark(), [uri])
        assert not prune_called


class TestPruneIntervalTags:
    """prune_interval_tags keeps the newest N interval tags and deletes the rest."""

    def make_tagged_dataset(self, tmp_path: Path, tags: list[str]) -> str:
        """Write a dataset and apply a list of tags to it.

        Args:
            tmp_path: Temporary directory.
            tags: Tag names to create.

        Returns:
            The dataset URI.
        """
        uri: str = str(tmp_path / "tagged.lance")
        ds: lance.LanceDataset = lance.write_dataset(pa.table({"id": pa.array([1], pa.int64())}), uri)
        for tag in tags:
            ds.tags.create(tag, ds.version)
        return uri

    def test_non_interval_tags_never_pruned(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """HEAD and other non-interval tags are never deleted."""
        uri: str = self.make_tagged_dataset(
            tmp_path,
            ["HEAD", "release-1.0", "20260601T000000Z", "20260602T000000Z"],
        )
        result: dict[str, Any] = prune_interval_tags(uri, None, 1, telemetry)
        remaining: list[str] = list(lance.dataset(uri).tags.list())
        assert "HEAD" in remaining
        assert "release-1.0" in remaining
        assert result["tags_pruned"] == 1
        assert result["tags_kept"] == 1

    def test_keep_last_boundary_keeps_newest_n(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """The newest tag_keep_last interval tags are kept; older ones are deleted."""
        interval_tags: list[str] = [
            "20260601T000000Z",
            "20260602T000000Z",
            "20260603T000000Z",
            "20260604T000000Z",
            "20260605T000000Z",
        ]
        uri: str = self.make_tagged_dataset(tmp_path, interval_tags)
        result: dict[str, Any] = prune_interval_tags(uri, None, 3, telemetry)
        remaining: list[str] = list(lance.dataset(uri).tags.list())
        assert result["tags_pruned"] == 2
        assert result["tags_kept"] == 3
        assert "20260605T000000Z" in remaining
        assert "20260604T000000Z" in remaining
        assert "20260603T000000Z" in remaining
        assert "20260602T000000Z" not in remaining
        assert "20260601T000000Z" not in remaining

    def test_keep_last_zero_deletes_all_interval_tags(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """Passing tag_keep_last=0 removes every interval tag."""
        uri: str = self.make_tagged_dataset(
            tmp_path,
            ["HEAD", "20260601T000000Z", "20260602T000000Z"],
        )
        result: dict[str, Any] = prune_interval_tags(uri, None, 0, telemetry)
        remaining: list[str] = list(lance.dataset(uri).tags.list())
        assert "HEAD" in remaining
        assert "20260601T000000Z" not in remaining
        assert "20260602T000000Z" not in remaining
        assert result["tags_pruned"] == 2
        assert result["tags_kept"] == 0

    def test_weird_names_ignored(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """Tag names that do not match %Y%m%dT%H%M%SZ are silently ignored.

        Lance only allows alphanumeric, '.', '-', and '_' in tag names, so the
        non-matching names used here are restricted to those characters.  The
        format %Y%m%dT%H%M%SZ contains the literal 'T' and 'Z' characters, which
        differ from hyphenated date strings like '2026-06-01'.
        """
        uri: str = self.make_tagged_dataset(
            tmp_path,
            ["HEAD", "not-a-date", "release.v1", "20260601T000000Z"],
        )
        result: dict[str, Any] = prune_interval_tags(uri, None, 1, telemetry)
        remaining: list[str] = list(lance.dataset(uri).tags.list())
        assert "HEAD" in remaining
        assert "not-a-date" in remaining
        assert "release.v1" in remaining
        assert "20260601T000000Z" in remaining
        assert result["tags_pruned"] == 0
        assert result["tags_kept"] == 1

    def test_no_interval_tags_is_noop(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """When there are no interval tags pruning is a no-op."""
        uri: str = self.make_tagged_dataset(tmp_path, ["HEAD"])
        result: dict[str, Any] = prune_interval_tags(uri, None, 5, telemetry)
        assert result["tags_pruned"] == 0
        assert result["tags_kept"] == 0


class TestStampGating:
    """stamp_eligible gates which datasets receive the interval tag."""

    def test_clean_result_is_eligible(self) -> None:
        """A dataset result with no error key is stamp-eligible."""
        stats: dict[str, Any] = {"uri": "s3://bucket/ds.lance", "indexes": [], "tier": "small"}
        assert stamp_eligible(stats) is True

    def test_skipped_result_is_eligible(self) -> None:
        """A skipped-as-current result (no error) is stamp-eligible."""
        stats: dict[str, Any] = {
            "uri": "s3://bucket/ds.lance",
            "indexes": [],
            "tier": "small",
            "skipped": "all indices current",
        }
        assert stamp_eligible(stats) is True

    def test_error_result_is_not_eligible(self) -> None:
        """A result with an error key is not stamp-eligible."""
        stats: dict[str, Any] = {"uri": "s3://bucket/ds.lance", "error": "index build failed"}
        assert stamp_eligible(stats) is False

    def test_stamp_phase_skips_error_datasets(
        self,
        telemetry_config: TelemetryConfig,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Datasets with an error marker are excluded from the stamp fan-out."""
        uri_ok: str = str(tmp_path / "ok.lance")
        uri_err: str = str(tmp_path / "err.lance")
        stamped_uris: list[str] = []

        def fake_indexer_run_mixed(self_inner: Any, spark: Any, uris: Any) -> list[dict[str, Any]]:
            """Return a clean result for uri_ok and an error result for uri_err."""
            del self_inner, spark, uris
            return [
                {"uri": uri_ok, "indexes": [], "tier": "small"},
                {"uri": uri_err, "error": "build failed"},
            ]

        def fake_update_serving_tags(
            spark: Any,
            dataset_uris: Any,
            telemetry_cfg: Any,
            storage_options: Any,
            tag: str = "HEAD",
            target_version: Any = None,
            partitions: int = 512,
        ) -> list[dict[str, Any]]:
            """Capture the URIs passed to the stamp fan-out."""
            del spark, telemetry_cfg, storage_options, target_version, partitions
            stamped_uris.extend(list(dataset_uris))
            return [{"uri": u, "tag": tag, "version": 1, "created": True} for u in dataset_uris]

        monkeypatch.setattr(pipeline_job, "prune_interval_tags_fleet", noop_prune_fleet)
        monkeypatch.setattr(pipeline_job.MaintenanceJob, "run", noop_maintenance_run)
        monkeypatch.setattr(pipeline_job.LanceIndexer, "run", fake_indexer_run_mixed)
        monkeypatch.setattr(pipeline_job, "update_serving_tags", fake_update_serving_tags)

        config = make_config(
            telemetry_config,
            tag_keep_last=None,
            tag_stamp="20260611T120000Z",
        )
        PipelineJob(config).run(FakeSpark(), [uri_ok, uri_err])
        assert uri_ok in stamped_uris
        assert uri_err not in stamped_uris

    def test_no_tag_stamp_skips_update_serving_tags(
        self,
        telemetry_config: TelemetryConfig,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """When tag_stamp is None update_serving_tags is never called."""
        uri: str = write_tiny_dataset(tmp_path)
        stamp_called: list[bool] = []

        def fail_stamp(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            """Fail if stamp is called without tag_stamp configured."""
            del args, kwargs
            stamp_called.append(True)
            raise AssertionError("update_serving_tags must not run when tag_stamp is None")

        monkeypatch.setattr(pipeline_job, "prune_interval_tags_fleet", noop_prune_fleet)
        monkeypatch.setattr(pipeline_job.MaintenanceJob, "run", noop_maintenance_run)
        monkeypatch.setattr(pipeline_job.LanceIndexer, "run", noop_indexer_run)
        monkeypatch.setattr(pipeline_job, "update_serving_tags", fail_stamp)

        config = make_config(telemetry_config, tag_keep_last=None, tag_stamp=None)
        PipelineJob(config).run(FakeSpark(), [uri])
        assert not stamp_called


class TestPipelineConfigPropagation:
    """PipelineConfig.__post_init__ pushes scheduler_pool and telemetry into both sub-configs."""

    def test_scheduler_pool_propagated(self, telemetry_config: TelemetryConfig) -> None:
        """The configured scheduler_pool is present on both sub-configs after construction."""
        maintenance = MaintenanceConfig(telemetry=telemetry_config)
        indexing = IndexJobConfig(telemetry=telemetry_config)
        config = PipelineConfig(
            telemetry=telemetry_config,
            maintenance=maintenance,
            indexing=indexing,
            scheduler_pool="my-pool",
        )
        assert config.maintenance.scheduler_pool == "my-pool"
        assert config.indexing.scheduler_pool == "my-pool"

    def test_telemetry_propagated(self, telemetry_config: TelemetryConfig) -> None:
        """The top-level telemetry config is set on both sub-configs after construction."""
        maintenance = MaintenanceConfig(telemetry=TelemetryConfig())
        indexing = IndexJobConfig(telemetry=TelemetryConfig())
        config = PipelineConfig(
            telemetry=telemetry_config,
            maintenance=maintenance,
            indexing=indexing,
        )
        assert config.maintenance.telemetry is telemetry_config
        assert config.indexing.telemetry is telemetry_config

    def test_storage_options_propagated(self, telemetry_config: TelemetryConfig) -> None:
        """The top-level storage_options dict is set on both sub-configs after construction."""
        opts: dict[str, str] = {"aws_region": "us-east-1"}
        maintenance = MaintenanceConfig(telemetry=telemetry_config)
        indexing = IndexJobConfig(telemetry=telemetry_config)
        config = PipelineConfig(
            telemetry=telemetry_config,
            storage_options=opts,
            maintenance=maintenance,
            indexing=indexing,
        )
        assert config.maintenance.storage_options is opts
        assert config.indexing.storage_options is opts

    def test_default_scheduler_pool(self, telemetry_config: TelemetryConfig) -> None:
        """The default scheduler_pool is 'lance-pipeline'."""
        config = make_config(telemetry_config)
        assert config.scheduler_pool == "lance-pipeline"
        assert config.maintenance.scheduler_pool == "lance-pipeline"
        assert config.indexing.scheduler_pool == "lance-pipeline"


class TestReturnShape:
    """PipelineJob.run returns the expected result dictionary shape."""

    def test_result_keys_present(
        self,
        telemetry_config: TelemetryConfig,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """The returned dict has datasets, raw phase lists, tag_stamp, and counts."""
        uri: str = write_tiny_dataset(tmp_path)
        monkeypatch.setattr(pipeline_job, "prune_interval_tags_fleet", noop_prune_fleet)
        monkeypatch.setattr(pipeline_job.MaintenanceJob, "run", noop_maintenance_run)
        monkeypatch.setattr(pipeline_job.LanceIndexer, "run", noop_indexer_run)
        monkeypatch.setattr(pipeline_job, "update_serving_tags", noop_update_serving_tags)

        config = make_config(telemetry_config, tag_keep_last=None, tag_stamp=None)
        result: dict[str, Any] = PipelineJob(config).run(FakeSpark(), [uri])

        assert "datasets" in result
        assert "maintenance_results" in result
        assert "index_results" in result
        assert "prune_results" in result
        assert "stamp_results" in result
        assert "tag_stamp" in result
        assert "counts" in result
        counts: dict[str, int] = result["counts"]
        assert "total" in counts
        assert "pruned_tags" in counts
        assert "maintenance_skipped" in counts
        assert "index_skipped" in counts
        assert "stamped" in counts

    def test_datasets_merges_maintenance_and_index(
        self,
        telemetry_config: TelemetryConfig,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Each entry in 'datasets' merges keys from both maintenance and index results."""
        uri: str = write_tiny_dataset(tmp_path)

        def maint_run_with_bytes(self_inner: Any, spark: Any, uris: list[str]) -> list[dict[str, Any]]:
            """Return maintenance results with a specific bytes_removed value.

            Args:
                self_inner: Unused bound instance.
                spark: Unused Spark session.
                uris: Dataset URIs to echo.

            Returns:
                One result per URI with bytes_removed=99.
            """
            del self_inner, spark
            return [{"uri": u, "tier": "small", "bytes_removed": 99} for u in uris]

        def index_run_with_marker(self_inner: Any, spark: Any, uris: list[str]) -> list[dict[str, Any]]:
            """Return index results with a sentinel index list.

            Args:
                self_inner: Unused bound instance.
                spark: Unused Spark session.
                uris: Dataset URIs to echo.

            Returns:
                One result per URI with indexes=['idx1'].
            """
            del self_inner, spark
            return [{"uri": u, "indexes": ["idx1"], "tier": "small"} for u in uris]

        monkeypatch.setattr(pipeline_job, "prune_interval_tags_fleet", noop_prune_fleet)
        monkeypatch.setattr(pipeline_job.MaintenanceJob, "run", maint_run_with_bytes)
        monkeypatch.setattr(pipeline_job.LanceIndexer, "run", index_run_with_marker)
        monkeypatch.setattr(pipeline_job, "update_serving_tags", noop_update_serving_tags)

        config = make_config(telemetry_config, tag_keep_last=None, tag_stamp=None)
        result: dict[str, Any] = PipelineJob(config).run(FakeSpark(), [uri])
        datasets: list[dict[str, Any]] = result["datasets"]
        assert len(datasets) == 1
        merged: dict[str, Any] = datasets[0]
        assert merged["uri"] == uri
        assert merged["bytes_removed"] == 99
        assert merged["indexes"] == ["idx1"]


class TestPruneTiming:
    """Ensure that the newest interval tags survive based on parsed datetime, not string order."""

    def test_newest_by_datetime_not_string_order(self, tmp_path: Path, telemetry: Telemetry) -> None:
        """Tags are sorted by parsed datetime so '20260101T...' < '20261201T...' regardless of string sort."""
        now: datetime = datetime.now(tz=UTC)
        tags: list[str] = []
        for offset_hours in range(6):
            dt: datetime = now - timedelta(hours=offset_hours)
            tags.append(dt.strftime("%Y%m%dT%H%M%SZ"))

        uri: str = str(tmp_path / "time_order.lance")
        ds: lance.LanceDataset = lance.write_dataset(pa.table({"id": pa.array([1], pa.int64())}), uri)
        for tag in tags:
            ds.tags.create(tag, ds.version)

        result: dict[str, Any] = prune_interval_tags(uri, None, 3, telemetry)
        remaining: list[str] = list(lance.dataset(uri).tags.list())
        assert result["tags_pruned"] == 3
        assert result["tags_kept"] == 3
        assert tags[0] in remaining
        assert tags[1] in remaining
        assert tags[2] in remaining
        assert tags[3] not in remaining
        assert tags[4] not in remaining
        assert tags[5] not in remaining

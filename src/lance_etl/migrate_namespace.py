"""Namespace copy/migrate utility for a fleet of per-tenant Lance datasets.

A dataset lives at ``base_uri/<val1>/<val2>/.../<valN>.lance`` where the path components are the values of the
configured ``partition_cols`` (default ``org_id``, ``tenant_id``, ``namespace``). One namespace is a single path
component and a namespace therefore spans many datasets, one per ``(org, tenant)`` pair that uses it. This job migrates
a whole namespace: for every dataset whose namespace component equals ``source_namespace`` it writes a copy at the same
address with the namespace component swapped to ``target_namespace``.

The default behaviour is copy plus optimize, keep source. The source datasets are never deleted, so an operator can
build the new namespace, verify it, and only then flip serving to it through the blue-green tag helpers in
:mod:`lance_etl.maintenance`. During the copy the targets are optimized in the same order as the production pipeline:
write, then recompact, then reindex. Recompaction reuses :class:`lance_etl.maintenance.MaintenanceJob` and reindexing
reuses :class:`lance_etl.indexing.LanceIndexer`, so the segment-API index flows and the unified compaction
orchestration are shared rather than reimplemented.

The copy itself scales in two tiers of its own. The set of source datasets is classified by fragment count in one
distributed job that also resolves each target URI and tests whether it already exists. Small datasets are copied whole
inside one executor task each, batched into a single Spark job. Large datasets follow the fleet phase shape: a
per-dataset plan fan-out pins each source version and shards its fragment ids on executors, ONE flat ``(dataset,
shard)`` Spark job writes new fragment files into every target across the tier, and a per-dataset commit fan-out
commits each target's collected fragments in one transaction. The driver only plans, groups, and dispatches. All heavy
read and write I/O runs in executors. Every commit goes through :func:`lance_etl.telemetry.commit_with_retries`.

The source and target namespace names are validated to be non-empty, non-traversal path components (rejecting the
literal values ``.`` and ``..``) before any work runs.

Requires pylance and the Datadog Agent on the executors.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

import lance
import pyarrow as pa
from lance.fragment import FragmentMetadata, write_fragments
from pyspark.sql import SparkSession

from lance_etl.cloud_storage import discover_datasets
from lance_etl.etl import ROUTING_COLS
from lance_etl.etl.sink import DATA_STORAGE_VERSION
from lance_etl.indexing import IndexJobConfig, LanceIndexer, split_evenly
from lance_etl.maintenance import MaintenanceConfig, MaintenanceJob, fan_out_per_dataset
from lance_etl.telemetry import DEFAULT_COMMIT_RETRIES, Telemetry, TelemetryConfig, commit_with_retries

logger: logging.Logger = logging.getLogger(__name__)

NAMESPACE_COL: str = "namespace"
"""The partition column carrying the namespace component of a dataset path."""

LARGE_DATASET_FRAGMENT_THRESHOLD: int = 128
"""Fragment count at or above which a dataset is copied with the distributed per-dataset fan-out
instead of in one executor task."""

BATCH_PARTITIONS: int = 512
"""Maximum Spark partitions for the small-tier batch copy, the classification job, and the
large-tier commit fan-out."""

NUM_SHARDS: int = 64
"""Fragment shards per large dataset, one flat-job task each."""

MAX_TASKS: int = 256
"""Upper bound on shards per large dataset and on the flat shard job's Spark partitions."""

COMMIT_BACKOFF_SECONDS: float = 0.5
"""Base backoff between copy-commit retries."""

LANCE_SUFFIX: str = ".lance"
"""Suffix that marks a dataset directory in a discovered path component."""

RESERVED_PATH_COMPONENTS: frozenset[str] = frozenset({".", ".."})
"""Path components rejected everywhere a routing value becomes a dataset-URI segment.

A non-empty check alone still admits the literal components ``.`` and ``..``, which are valid path segments that
walk up or stay within the directory tree instead of naming a routing key, letting a configured or discovered value
escape the ``base_uri`` tree.
"""


def uri_components(base_uri: str, uri: str) -> list[str]:
    """Split a dataset URI into its routing-value path components relative to a base URI.

    Strips the base URI prefix and the trailing ``.lance`` suffix, then splits on ``/`` to yield one
    value per routing column in path order.

    Args:
        base_uri: Root location the dataset lives under.
        uri: Full dataset URI ending in ``.lance``.

    Returns:
        Routing values in path order with the ``.lance`` suffix removed from the last.

    Raises:
        ValueError: If the URI is not rooted at ``base_uri`` or does not end in ``.lance``.
    """
    root: str = base_uri.rstrip("/")
    if not uri.startswith(f"{root}/") or not uri.endswith(LANCE_SUFFIX):
        raise ValueError(f"dataset URI {uri!r} is not a .lance dataset rooted at {base_uri!r}")
    relative: str = uri[len(root) + 1 :]
    parts: list[str] = relative.split("/")
    parts[-1] = parts[-1].removesuffix(LANCE_SUFFIX)
    return parts


@dataclass
class MigrateConfig:
    """Configuration for :class:`NamespaceMigrator`.

    Attributes:
        source_namespace: Namespace component value of the datasets to copy.
        target_namespace: Namespace component value the copies are written under. Must differ from
            ``source_namespace`` so a copy can never clobber its own source.
        base_uri: Root location under which all per-tenant datasets live.
        telemetry: Telemetry configuration created once per process.
        partition_cols: Columns whose values build each dataset path in order, used to locate datasets and to know which
            path component carries the namespace. Defaults to the stable ``org_id``, ``tenant_id``, ``namespace`` trio.
            Must contain :data:`NAMESPACE_COL`.
        storage_options: Object-store options forwarded to pylance and pyarrow.
        recompact: Recompact every target after the copy by reusing :class:`lance_etl.maintenance.MaintenanceJob`.
        reindex: Rebuild indexes on every target after compaction by reusing :class:`lance_etl.indexing.LanceIndexer`.
            A copy carries no indexes, so this is the only way the migrated namespace becomes searchable. Has no effect
            unless ``index`` supplies an index specification.
        overwrite_target: Permit overwriting a target dataset that already exists. When ``False`` (the default) an
            existing target fails the whole run rather than clobbering data silently.
        index: Index specification for the reindex step, naming the vector, scalar, bitmap, and text columns to build.
            When ``None`` reindexing is skipped because the columns to index cannot be guessed.
    """

    source_namespace: str
    target_namespace: str
    base_uri: str
    telemetry: TelemetryConfig
    partition_cols: list[str] = field(default_factory=lambda: list(ROUTING_COLS))
    storage_options: dict[str, Any] | None = None
    recompact: bool = True
    reindex: bool = True
    overwrite_target: bool = False
    index: IndexJobConfig | None = None

    def namespace_index(self) -> int:
        """Return the position of the namespace component in the dataset path.

        Returns:
            The index of :data:`NAMESPACE_COL` within ``partition_cols``.
        """
        return self.partition_cols.index(NAMESPACE_COL)

    def compaction_config(self) -> MaintenanceConfig:
        """Return the compaction configuration for the recompact step.

        Returns:
            A default compaction config sharing this config's telemetry and storage options.
        """
        return MaintenanceConfig(telemetry=self.telemetry, storage_options=self.storage_options)


@dataclass
class MigrateReport:
    """Outcome of one namespace migration.

    Attributes:
        source_namespace: The namespace that was copied from.
        target_namespace: The namespace that was copied to.
        datasets_found: Number of source datasets in the namespace.
        copied: Target dataset URIs successfully written.
        compacted: Number of targets compacted in the recompact step.
        indexed: Number of targets processed by the reindex step.
        skipped: One ``{"source", "target", "reason"}`` mapping per source dataset not copied.
    """

    source_namespace: str
    target_namespace: str
    datasets_found: int
    copied: list[str] = field(default_factory=list)
    compacted: int = 0
    indexed: int = 0
    skipped: list[dict[str, str]] = field(default_factory=list)


def validate_config(config: MigrateConfig) -> None:
    """Validate a migrate configuration before any work runs.

    Args:
        config: The configuration to validate.

    Raises:
        ValueError: If the namespaces are equal, empty, or a directory-traversal component (``.`` or ``..``), are
            missing from the partition columns, or the partition list is empty or carries duplicates.
    """
    if not config.partition_cols:
        raise ValueError("partition_cols must list at least one column")
    duplicates: list[str] = sorted({c for c in config.partition_cols if config.partition_cols.count(c) > 1})
    if duplicates:
        raise ValueError(f"partition_cols carries duplicate columns: {duplicates}")
    if NAMESPACE_COL not in config.partition_cols:
        raise ValueError(f"{NAMESPACE_COL!r} is not in partition_cols {config.partition_cols}")
    if config.source_namespace == config.target_namespace:
        raise ValueError("source_namespace and target_namespace must differ; a copy cannot clobber its own source")
    for label, value in (("source_namespace", config.source_namespace), ("target_namespace", config.target_namespace)):
        if not value or value in RESERVED_PATH_COMPONENTS:
            raise ValueError(f"{label} must be a non-empty, non-traversal path component, got {value!r}")


def build_dataset_uri(base_uri: str, components: list[str]) -> str:
    """Build a validated dataset URI from routing values.

    Args:
        base_uri: The root location the dataset lives under.
        components: The routing values in path order. Each must be a non-empty string.

    Returns:
        The dataset URI ``base_uri/<val1>/.../<valN>.lance`` confined to the routing-key prefix.

    Raises:
        ValueError: If any component is not a non-empty string, or is a directory-traversal component (``.`` or
            ``..``) that would escape the ``base_uri`` prefix.
    """
    for component in components:
        if not component or component in RESERVED_PATH_COMPONENTS:
            raise ValueError(f"invalid routing component: {component!r}")
    base: str = base_uri.rstrip("/")
    return f"{base}/{'/'.join(components)}{LANCE_SUFFIX}"


def target_uri_for(config: MigrateConfig, source_uri: str) -> str:
    """Compute the target dataset URI for one source dataset by swapping the namespace component.

    Args:
        config: Migrate configuration.
        source_uri: A source dataset URI in the source namespace.

    Returns:
        The target dataset URI at the same address with the namespace component set to ``target_namespace``.

    Raises:
        ValueError: If the source URI does not carry one value per configured partition column.
    """
    components: list[str] = uri_components(config.base_uri, source_uri)
    if len(components) != len(config.partition_cols):
        raise ValueError(
            f"source URI {source_uri!r} has {len(components)} path components, expected {len(config.partition_cols)} "
            f"for partition_cols {config.partition_cols}"
        )
    components[config.namespace_index()] = config.target_namespace
    return build_dataset_uri(config.base_uri, components)


def source_dataset_uris(config: MigrateConfig, spark: SparkSession | None = None) -> list[str]:
    """Discover every source dataset whose namespace component equals ``source_namespace``.

    Args:
        config: Migrate configuration.
        spark: Active session forwarded to :func:`~lance_etl.cloud_storage.discover_datasets` for
            executor-fanned discovery, or ``None`` for the pure-driver walk.

    Returns:
        The matching source dataset URIs, sorted.
    """
    index: int = config.namespace_index()
    width: int = len(config.partition_cols)
    matched: list[str] = []
    for uri in discover_datasets(config.base_uri, config.storage_options, spark=spark):
        components: list[str] = uri_components(config.base_uri, uri)
        if len(components) == width and components[index] == config.source_namespace:
            matched.append(uri)
    return matched


def target_exists(uri: str, storage_options: dict[str, Any] | None) -> bool:
    """Report whether a target dataset already exists.

    Args:
        uri: Target dataset URI.
        storage_options: Object-store options forwarded to pylance.

    Returns:
        ``True`` when a dataset can be opened at the URI.
    """
    try:
        lance.dataset(uri, storage_options=storage_options)
    except (FileNotFoundError, ValueError):
        return False
    return True


def classify_source(uri: str, config: MigrateConfig, telemetry: Telemetry) -> dict[str, Any]:
    """Resolve one source dataset's target URI, fragment count, and target existence on an executor.

    Args:
        uri: Source dataset URI.
        config: Migrate configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        A mapping with ``source``, ``target``, ``fragments``, and ``exists``.
    """
    target: str = target_uri_for(config, uri)
    dataset: lance.LanceDataset = lance.dataset(uri, storage_options=config.storage_options)
    fragments: int = len(dataset.get_fragments())
    exists: bool = target_exists(target, config.storage_options)
    telemetry.incr("migrate.classified")
    return {"source": uri, "target": target, "fragments": fragments, "exists": exists}


def write_mode(overwrite_target: bool) -> str:
    """Return the Lance write mode for the copy.

    Args:
        overwrite_target: Whether an existing target may be overwritten.

    Returns:
        ``"overwrite"`` when overwriting is permitted, otherwise ``"create"`` which refuses to clobber an existing
        dataset.
    """
    return "overwrite" if overwrite_target else "create"


def copy_small_dataset(source_uri: str, target_uri: str, config: MigrateConfig, telemetry: Telemetry) -> dict[str, Any]:
    """Copy one whole dataset to its target inside the current executor task.

    Streams the source rows through a scanner reader into a single ``write_dataset`` call, leaving the source intact.
    ``enable_v2_manifest_paths=True`` is always passed so the new dataset opens in one object-store request. The
    source reader is rebuilt inside the retried action so each attempt rebases.

    Args:
        source_uri: Source dataset URI.
        target_uri: Target dataset URI.
        config: Migrate configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        A statistics mapping with ``source``, ``target``, ``tier``, and ``rows`` copied.
    """
    mode: str = write_mode(config.overwrite_target)
    write_kwargs: dict[str, Any] = {
        "mode": mode,
        "storage_options": config.storage_options,
        "enable_v2_manifest_paths": True,
        "data_storage_version": DATA_STORAGE_VERSION,
    }

    def action() -> int:
        """Stream the source into the target once, returning the row count copied."""
        source: lance.LanceDataset = lance.dataset(source_uri, storage_options=config.storage_options)
        reader: pa.RecordBatchReader = source.scanner().to_reader()
        written: lance.LanceDataset = lance.write_dataset(reader, target_uri, **write_kwargs)
        telemetry.incr("migrate.copied")
        return int(written.count_rows())

    with telemetry.timed("migrate.copy_ms", tags=["tier:small"]):
        rows: int = commit_with_retries(
            action,
            DEFAULT_COMMIT_RETRIES,
            COMMIT_BACKOFF_SECONDS,
            lambda: telemetry.incr("migrate.copy_conflict"),
        )
    return {"source": source_uri, "target": target_uri, "tier": "small", "rows": rows}


def write_fragment_shard(
    source_uri: str,
    target_uri: str,
    version: int,
    shard: list[int],
    schema: pa.Schema,
    config: MigrateConfig,
) -> list[str]:
    """Read one fragment shard from the source and write it as new target fragment files on an executor.

    The fragments are written but not committed: ``write_fragments`` in ``create`` mode assigns field ids from the
    shared schema and returns metadata the driver collects and commits in one transaction.

    Args:
        source_uri: Source dataset URI.
        target_uri: Target dataset URI the fragment files are written under.
        version: Source dataset version every shard reads, so the copy is a consistent snapshot.
        shard: Source fragment ids assigned to this task.
        schema: The source schema, shared across shards so field ids stay consistent.
        config: Migrate configuration.

    Returns:
        One JSON-serialized fragment metadata document per written fragment.
    """
    telemetry: Telemetry = Telemetry.create(config.telemetry)
    source: lance.LanceDataset = lance.dataset(source_uri, version=version, storage_options=config.storage_options)
    wanted: set[int] = set(shard)
    fragments: list[Any] = [f for f in source.get_fragments() if f.fragment_id in wanted]
    write_kwargs: dict[str, Any] = {
        "schema": schema,
        "mode": "create",
        "storage_options": config.storage_options,
        "data_storage_version": DATA_STORAGE_VERSION,
    }
    with telemetry.timed("migrate.shard_write_ms"):
        reader: pa.RecordBatchReader = source.scanner(fragments=fragments).to_reader()
        metadatas: list[FragmentMetadata] = write_fragments(reader, target_uri, **write_kwargs)
    telemetry.incr("migrate.shard_written")
    return [json.dumps(metadata.to_json()) for metadata in metadatas]


def plan_large_copy(source_uri: str, target_uri: str, config: MigrateConfig, telemetry: Telemetry) -> dict[str, Any]:
    """Plan one large dataset's distributed copy on an executor.

    Pins the source version so every shard reads a consistent snapshot, captures the schema the target is created
    with, and shards the fragment ids into flat-job tasks.

    Args:
        source_uri: Source dataset URI.
        target_uri: Target dataset URI.
        config: Migrate configuration.
        telemetry: Telemetry facade for the current process.

    Returns:
        A mapping with ``source``, ``target``, ``version``, ``schema``, and the fragment-id ``shards``.
    """
    dataset: lance.LanceDataset = lance.dataset(source_uri, storage_options=config.storage_options)
    fragment_ids: list[int] = [fragment.fragment_id for fragment in dataset.get_fragments()]
    shards: list[list[int]] = split_evenly(fragment_ids, min(NUM_SHARDS, MAX_TASKS))
    telemetry.incr("migrate.large_planned")
    return {
        "source": source_uri,
        "target": target_uri,
        "version": dataset.version,
        "schema": dataset.schema,
        "shards": shards,
    }


def commit_copied_fragments(
    target_uri: str, fragment_documents: list[str], schema: pa.Schema, config: MigrateConfig, telemetry: Telemetry
) -> None:
    """Commit copied fragment files into the target dataset in one transaction.

    Uses ``LanceOperation.Overwrite``, which creates the target when absent and replaces it when present, so it serves
    both the create and the overwrite path. The commit is retried for the raw manifest-write race.

    Args:
        target_uri: Target dataset URI.
        fragment_documents: JSON fragment metadata collected from the executors.
        schema: The source schema the target is created with.
        config: Migrate configuration.
        telemetry: Telemetry facade for the current process.
    """
    fragments: list[FragmentMetadata] = [FragmentMetadata.from_json(document) for document in fragment_documents]

    def action() -> None:
        """Commit the overwrite operation against the target."""
        operation = lance.LanceOperation.Overwrite(schema, fragments)
        lance.LanceDataset.commit(
            target_uri,
            operation,
            storage_options=config.storage_options,
            enable_v2_manifest_paths=True,
        )
        telemetry.incr("migrate.committed")

    commit_with_retries(
        action,
        DEFAULT_COMMIT_RETRIES,
        COMMIT_BACKOFF_SECONDS,
        lambda: telemetry.incr("migrate.copy_conflict"),
    )


class NamespaceMigrator:
    """Copies a whole namespace to a new namespace with two-tier orchestration."""

    def __init__(self, config: MigrateConfig) -> None:
        """Initialize the migrator, validating the configuration early.

        Args:
            config: Migrate configuration.

        Raises:
            ValueError: If the configuration fails :func:`validate_config`.
        """
        validate_config(config)
        self.config: MigrateConfig = config

    def classify(self, spark: SparkSession, source_uris: list[str], telemetry: Telemetry) -> list[dict[str, Any]]:
        """Resolve targets and fragment counts for every source dataset in one distributed job.

        Args:
            spark: Active Spark session.
            source_uris: Source dataset URIs in the namespace.
            telemetry: Driver telemetry facade.

        Returns:
            One classification mapping per source dataset.
        """
        config: MigrateConfig = self.config
        with telemetry.timed("run.classify_ms"):
            return fan_out_per_dataset(
                spark,
                source_uris,
                config.telemetry,
                lambda uri, executor_telemetry: classify_source(uri, config, executor_telemetry),
                BATCH_PARTITIONS,
            )

    def copy_small_tier(self, spark: SparkSession, plans: list[dict[str, Any]], telemetry: Telemetry) -> list[str]:
        """Copy small datasets in one batched Spark job, one task per dataset.

        Args:
            spark: Active Spark session.
            plans: Small-tier classification mappings.
            telemetry: Driver telemetry facade.

        Returns:
            The copied target URIs.
        """
        config: MigrateConfig = self.config
        targets: dict[str, str] = {plan["source"]: plan["target"] for plan in plans}
        with telemetry.timed("run.small_tier_ms"):
            outcomes: list[dict[str, Any]] = fan_out_per_dataset(
                spark,
                list(targets),
                config.telemetry,
                lambda uri, executor_telemetry: copy_small_dataset(uri, targets[uri], config, executor_telemetry),
                BATCH_PARTITIONS,
            )
        return [outcome["target"] for outcome in outcomes]

    def copy_large_tier(self, spark: SparkSession, plans: list[dict[str, Any]], telemetry: Telemetry) -> list[str]:
        """Copy large datasets with one flat ``(dataset, shard)`` Spark job across the whole tier.

        A per-dataset plan fan-out pins each source version, captures its schema, and shards its fragment ids
        (:func:`plan_large_copy`). ONE flat Spark job then writes every shard's fragment files across all large
        datasets at once, and a per-dataset commit fan-out commits each target's collected fragments in one
        transaction on an executor — each commit task ships only its own target's fragment documents, never the
        whole tier's. The driver only plans, groups, and dispatches, and any task failure fails the tier's Spark
        job. A source that was legitimately empty at plan time (zero fragments) still creates its empty target.

        Args:
            spark: Active Spark session.
            plans: Large-tier classification mappings.
            telemetry: Driver telemetry facade.

        Returns:
            The copied target URIs.

        Raises:
            ValueError: If a non-empty plan produced no fragment documents, instead of silently
                committing an empty overwrite over a target.
        """
        config: MigrateConfig = self.config
        targets: dict[str, str] = {plan["source"]: plan["target"] for plan in plans}
        with telemetry.timed("run.large_tier_ms"):
            with telemetry.timed("run.large_plan_ms"):
                copy_plans: list[dict[str, Any]] = fan_out_per_dataset(
                    spark,
                    list(targets),
                    config.telemetry,
                    lambda uri, executor_telemetry: plan_large_copy(uri, targets[uri], config, executor_telemetry),
                    BATCH_PARTITIONS,
                )

            shard_tasks: list[tuple[str, str, int, pa.Schema, list[int]]] = [
                (plan["source"], plan["target"], plan["version"], plan["schema"], shard)
                for plan in copy_plans
                for shard in plan["shards"]
                if shard
            ]

            def write_partition(items: Any) -> Any:
                """Write the fragment-copy shard tasks assigned to this executor task.

                Args:
                    items: The ``(source, target, version, schema, shard)`` tasks for this partition.

                Yields:
                    One ``(target, fragment_document)`` pair per written fragment.
                """
                for source, target, version, schema, shard in items:
                    for document in write_fragment_shard(source, target, version, shard, schema, config):
                        yield target, document

            documents_by_target: dict[str, list[str]] = {}
            if shard_tasks:
                slices: int = max(1, min(len(shard_tasks), MAX_TASKS))
                with telemetry.timed("run.large_write_ms"):
                    pairs: list[tuple[str, str]] = (
                        spark.sparkContext.parallelize(shard_tasks, slices).mapPartitions(write_partition).collect()
                    )
                for target, document in pairs:
                    documents_by_target.setdefault(target, []).append(document)

            commit_entries: list[tuple[str, list[str], pa.Schema]] = []
            for plan in copy_plans:
                target_uri: str = plan["target"]
                documents: list[str] = documents_by_target.get(target_uri, [])
                planned_fragments: int = sum(len(shard) for shard in plan["shards"])
                if not documents and planned_fragments > 0:
                    raise ValueError(
                        f"large-tier copy wrote no fragments for {target_uri} despite a plan covering "
                        f"{planned_fragments} source fragment(s); refusing to commit an empty overwrite"
                    )
                commit_entries.append((target_uri, documents, plan["schema"]))

            def commit_partition(items: Any) -> Any:
                """Commit the targets assigned to this executor task.

                Each item carries only its own target's fragment documents and schema, so no task
                deserializes the whole tier's metadata.

                Args:
                    items: The ``(target, documents, schema)`` entries for this partition.

                Yields:
                    One mapping with the committed ``target`` per entry.
                """
                executor_telemetry: Telemetry = Telemetry.create(config.telemetry)
                for target, documents, schema in items:
                    commit_copied_fragments(target, documents, schema, config, executor_telemetry)
                    executor_telemetry.incr("migrate.copied")
                    yield {"target": target}

            commit_slices: int = max(1, min(len(commit_entries), BATCH_PARTITIONS))
            with telemetry.timed("run.large_commit_ms"):
                outcomes: list[dict[str, Any]] = (
                    spark.sparkContext.parallelize(commit_entries, commit_slices)
                    .mapPartitions(commit_partition)
                    .collect()
                )
        return [outcome["target"] for outcome in outcomes]

    def optimize(self, spark: SparkSession, copied: list[str], telemetry: Telemetry) -> tuple[int, int]:
        """Recompact and reindex the copied targets, in pipeline order.

        Args:
            spark: Active Spark session.
            copied: The copied target URIs.
            telemetry: Driver telemetry facade.

        Returns:
            The counts of datasets compacted and indexed.
        """
        config: MigrateConfig = self.config
        compacted: int = 0
        indexed: int = 0
        if config.recompact:
            with telemetry.timed("run.recompact_ms"):
                compacted = len(MaintenanceJob(config.compaction_config()).run(spark, copied))
        if config.reindex:
            if config.index is None:
                logger.warning(
                    "reindex requested but no index specification supplied; skipping reindex of %d targets", len(copied)
                )
            else:
                with telemetry.timed("run.reindex_ms"):
                    indexed = len(LanceIndexer(config.index).run(spark, copied))
        return compacted, indexed

    def run(self, spark: SparkSession) -> MigrateReport:
        """Migrate the namespace: discover, copy, optimize, and report.

        Discovers every source dataset in ``source_namespace``, classifies them by fragment count, fails fast if any
        target already exists unless ``overwrite_target`` is set, copies small datasets in a batched job and large ones
        with the flat ``(dataset, shard)`` tier job, and then recompacts and reindexes the targets. The source datasets
        are never deleted, so serving can be flipped to the new namespace only after verification.

        Args:
            spark: Active Spark session.

        Returns:
            A :class:`MigrateReport` describing the run.

        Raises:
            ValueError: If a target dataset already exists and ``overwrite_target`` is ``False``.
        """
        config: MigrateConfig = self.config
        telemetry: Telemetry = Telemetry.create(config.telemetry)
        with telemetry.span("lance.migrate.run") as run_span:
            run_span.set_tag("source_namespace", config.source_namespace)
            run_span.set_tag("target_namespace", config.target_namespace)
            source_uris: list[str] = source_dataset_uris(config, spark)
            run_span.set_tag("datasets_found", len(source_uris))
            report: MigrateReport = MigrateReport(
                source_namespace=config.source_namespace,
                target_namespace=config.target_namespace,
                datasets_found=len(source_uris),
            )
            if not source_uris:
                logger.info("namespace migrate: no datasets found in namespace %r", config.source_namespace)
                return report

            plans: list[dict[str, Any]] = self.classify(spark, source_uris, telemetry)
            collisions: list[dict[str, Any]] = [plan for plan in plans if plan["exists"]]
            if collisions and not config.overwrite_target:
                listed: str = ", ".join(plan["target"] for plan in collisions)
                raise ValueError(
                    f"{len(collisions)} target dataset(s) already exist and overwrite_target is False: {listed}"
                )

            threshold: int = LARGE_DATASET_FRAGMENT_THRESHOLD
            small_plans: list[dict[str, Any]] = [plan for plan in plans if plan["fragments"] < threshold]
            large_plans: list[dict[str, Any]] = [plan for plan in plans if plan["fragments"] >= threshold]
            run_span.set_tag("small_datasets", len(small_plans))
            run_span.set_tag("large_datasets", len(large_plans))

            copied: list[str] = []
            if small_plans:
                copied.extend(self.copy_small_tier(spark, small_plans, telemetry))
            if large_plans:
                copied.extend(self.copy_large_tier(spark, large_plans, telemetry))
            report.copied = copied
            logger.info(
                "namespace migrate: copied %d datasets (%d small, %d large) from %r to %r",
                len(copied),
                len(small_plans),
                len(large_plans),
                config.source_namespace,
                config.target_namespace,
            )

            report.compacted, report.indexed = self.optimize(spark, copied, telemetry)
            telemetry.gauge("run.datasets_copied", len(copied))
            telemetry.gauge("run.datasets_compacted", report.compacted)
            telemetry.gauge("run.datasets_indexed", report.indexed)
            return report

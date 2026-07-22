"""Randomized CRUD fuzz run flow: append seeded op snapshots, reconcile, and verify content.

This subcommand drives the exact production PostgreSQL reconciler path used by ``bench e2e`` but
over a seeded randomized CRUD op program instead of an insert-only corpus. Each snapshot appends
one Iceberg commit of generated mutations through ``mapInArrow`` (payloads built in executors,
never on the driver), the reconciler carries every dataset through ingest, compaction, indexing,
validation, prewarm, and publication, and each org's published Lance dataset is verified against an
in-memory oracle with full row-content comparison.

The op program, every payload, and the oracle are all regenerable from ``(seed, knobs, now_us)`` by
:mod:`bench.fuzz_workload`. Verification opens each publication at its exact served version and
checks one physical row per key, key-set equality, per-key event timestamp, tombstone state, stored
Iceberg source sequence, digest and window-sequence presence, regenerated live vector plus text and
cluster, and full index coverage. Conflict mode additionally asserts a same-snapshot distinct
mutation blocks exactly the victim org while every other org advances.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import lance
import numpy as np
import pyarrow as pa
import sqlalchemy as sa
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sqlalchemy.engine import Engine

from bench.config import NAMESPACE, TENANT_ID, BenchConfig
from bench.fuzz_workload import (
    MAX_REPORTED_MISMATCHES,
    FuzzOp,
    FuzzSettings,
    FuzzWorkload,
    OracleRow,
    fuzz_payload,
    generate_workload,
    oracle_rows,
    retention_band_keys,
)
from bench.reconcile import (
    PRODUCTION_ROW_DDL,
    bench_spec_index_names,
    bench_spec_revision,
    bench_workspace_spark,
    build_reconciler_application,
    create_production_source_table,
    drain_reconciler,
    isolated_control_plane,
    production_arrow_schema,
    resolve_database_url,
    resolve_org_serving,
    source_table_identifier,
)
from bench.results import ensure_dir, save_phase
from lance_etl.etl.mutation import normalize_operation
from lance_etl.reconciler.iceberg import SparkIcebergCatalog
from lance_etl.state import (
    ControlPlaneRepository,
    DatasetSpecRevision,
    FieldRole,
    RoutingIdentity,
    ServingDataset,
    WorkState,
    derive_source_id,
    deterministic_dataset_id,
    ingest_uri,
)
from lance_etl.state.tables import dataset_work, datasets

logger: logging.Logger = logging.getLogger(__name__)

VERIFY_COLUMNS: tuple[str, ...] = (
    "record_id",
    "ts",
    "vector",
    "text",
    "cluster",
    "lance_etl_source_sequence",
    "lance_etl_event_digest",
    "lance_etl_window_seq",
    "is_deleted",
)
"""Nine persisted columns scanned from each publication for full-content verification."""

SAME_SNAPSHOT_CONFLICT: str = "SAME_SNAPSHOT_CONFLICT"
"""Error code the reconciler stamps on a work row carrying distinct unordered mutations."""

FUZZ_SOURCE_NAME: str = "bench-fuzz"
"""Distinct source identity isolating fuzz dataset URIs from the standard ``bench`` e2e/qualify/
experiment source. Dataset identity is a pure hash of source name plus routing identity
(``deterministic_dataset_id``), independent of the installed spec revision, so a fuzz run sharing
the standard ``bench`` name with a prior ``bench e2e`` run over the same ``--workspace`` would
resolve to the exact same physical Lance path while carrying an incompatible narrowed vector
dimension, corrupting the shared dataset schema on ingest and blocking publication with
``CANDIDATE_SCHEMA_MISMATCH``."""


def fuzz_dataset_paths(config: BenchConfig) -> list[Path]:
    """Return every physical Lance path the fuzz evaluator's own source can ever address.

    The fuzz evaluator regenerates its entire op program, every payload, and its oracle from
    ``(seed, knobs, now_us)`` alone, so its Lance datasets are pure derived state that must not
    outlive one run. Dataset identity is a deterministic hash of ``FUZZ_SOURCE_NAME`` plus the
    routing identity (``deterministic_dataset_id``) and is independent of the installed spec
    revision, so two fuzz runs sharing one ``--workspace`` with different knobs (for example a
    different ``--fuzz-dim``) resolve to the exact same physical path even though their PostgreSQL
    control-plane state is freshly isolated per run. Computing these paths offline from the fixed
    ``FUZZ_SOURCE_NAME`` lets a run reset only its own namespace before it starts, never touching
    the standard ``bench`` source's paths used by ``e2e``, ``qualify``, and ``experiment``.

    Args:
        config: Benchmark configuration.

    Returns:
        One Lance dataset path per configured organization.
    """
    source_id: uuid.UUID = derive_source_id(FUZZ_SOURCE_NAME)
    base_uri: str = str(config.lance_root())
    paths: list[Path] = []
    org: str
    for org in config.org_ids():
        identity: RoutingIdentity = RoutingIdentity(TENANT_ID, NAMESPACE, org)
        dataset_id: uuid.UUID = deterministic_dataset_id(source_id, identity)
        paths.append(Path(ingest_uri(base_uri, dataset_id)))
    return paths


def reset_fuzz_datasets(config: BenchConfig) -> None:
    """Delete every stale physical Lance dataset the fuzz evaluator's own source can address.

    Runs before any Iceberg or reconciler state is touched. Removing the dataset directory and its
    ``.artifacts`` sidecar (index-build and publication-manifest artifacts keyed off the same
    prefix) guarantees each fuzz invocation starts from an empty dataset regardless of what a prior
    fuzz run, at any seed or knob combination, left behind at the same deterministic path.

    Args:
        config: Benchmark configuration.
    """
    path: Path
    for path in fuzz_dataset_paths(config):
        shutil.rmtree(path, ignore_errors=True)
        shutil.rmtree(Path(f"{path}.artifacts"), ignore_errors=True)


def fuzz_spec_revision(config: BenchConfig, settings: FuzzSettings) -> DatasetSpecRevision:
    """Build the fuzz DRAFT spec: the bench spec narrowed to the fuzz vector dimension and retention.

    Wraps :func:`~bench.reconcile.bench_spec_revision` so all six production indexes stay declared,
    then overrides every vector field's ``data_type`` and ``vector_dimension`` with the synthetic
    fuzz dimension and sets ``record_retention_seconds`` in short retention mode.

    Args:
        config: Benchmark configuration.
        settings: Validated fuzz settings.

    Returns:
        A validated small DRAFT specification carrying the fuzz vector dimension and retention.
    """
    base: DatasetSpecRevision = bench_spec_revision(config)
    fields: tuple[Any, ...] = tuple(
        replace(field, data_type=f"fixed_size_list<float32,{settings.dim}>", vector_dimension=settings.dim)
        if field.role is FieldRole.VECTOR
        else field
        for field in base.fields
    )
    retention: int | None = settings.retention_seconds if settings.retention_mode == "short" else None
    return replace(base, fields=fields, record_retention_seconds=retention, configuration_digest=b"")


def build_op_maps(records: list[dict[str, Any]], settings: FuzzSettings) -> tuple[pa.Array, pa.Array, pa.Array]:
    """Build the vectors, texts, and metadata map arrays for one executor batch.

    Upsert rows carry single-entry maps regenerated by :func:`fuzz_payload`. Delete rows carry empty
    maps: the source contract columns are non-null but an empty map is valid, and downstream
    normalization nulls the tombstone payload.

    Args:
        records: The op tuples for this batch as Python dicts.
        settings: Validated fuzz settings carrying the seed, dimension, and cluster cardinality.

    Returns:
        The vectors, texts, and metadata map arrays aligned to the batch rows.
    """
    vectors: list[list[tuple[str, list[float]]]] = []
    texts: list[list[tuple[str, str]]] = []
    metadata: list[list[tuple[str, str]]] = []
    for record in records:
        if normalize_operation(str(record["op"])) == "delete":
            vectors.append([])
            texts.append([])
            metadata.append([])
            continue
        vector: np.ndarray
        text: str
        cluster: str
        vector, text, cluster = fuzz_payload(
            settings.seed,
            str(record["org_id"]),
            str(record["record_id"]),
            int(record["payload_version"]),
            settings.dim,
            settings.num_clusters,
        )
        vectors.append([("vector", vector.tolist())])
        texts.append([("text", text)])
        metadata.append([("cluster", cluster)])
    return (
        pa.array(vectors, pa.map_(pa.string(), pa.list_(pa.float32()))),
        pa.array(texts, pa.map_(pa.string(), pa.string())),
        pa.array(metadata, pa.map_(pa.string(), pa.string())),
    )


def append_fuzz_snapshot(
    spark: SparkSession,
    config: BenchConfig,
    settings: FuzzSettings,
    table: str,
    ops: tuple[FuzzOp, ...],
) -> int:
    """Append one snapshot of generated mutations and return its Iceberg snapshot id.

    The driver holds only op specifications. Payload bytes are generated inside executors so the
    driver never materializes vectors, mirroring :func:`~bench.reconcile.append_production_batch`.

    Args:
        spark: Local Iceberg-enabled Spark session.
        config: Benchmark configuration.
        settings: Validated fuzz settings.
        table: Source table identifier.
        ops: The ops delivered in this snapshot.

    Returns:
        The Iceberg snapshot id committed by the append.

    Raises:
        RuntimeError: If the append produced no Iceberg snapshot.
    """
    rows: list[tuple[str, str, str, int, int]] = [
        (op.org_id, op.record_id, op.op, op.ts_us, op.payload_version) for op in ops
    ]

    def generate(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """Generate production-contract source batches for the op specs on this task.

        Args:
            batches: Arrow batches of op tuples.

        Yields:
            One production source batch per input batch.
        """
        for batch in batches:
            records: list[dict[str, Any]] = pa.Table.from_batches([batch]).to_pylist()
            if not records:
                continue
            count: int = len(records)
            vectors: pa.Array
            texts: pa.Array
            metadata: pa.Array
            vectors, texts, metadata = build_op_maps(records, settings)
            arrays: list[pa.Array] = [
                pa.array([TENANT_ID] * count, pa.string()),
                pa.array([NAMESPACE] * count, pa.string()),
                pa.array([str(record["org_id"]) for record in records], pa.string()),
                pa.array([str(record["record_id"]) for record in records], pa.string()),
                pa.array([str(record["op"]) for record in records], pa.string()),
                pa.array([int(record["ts_us"]) for record in records], pa.int64()),
                vectors,
                texts,
                metadata,
            ]
            yield pa.RecordBatch.from_arrays(arrays, schema=production_arrow_schema())

    specs = spark.createDataFrame(
        rows, "org_id string, record_id string, op string, ts_us long, payload_version int"
    ).repartition(config.etl_partitions)
    generated = specs.mapInArrow(generate, schema=PRODUCTION_ROW_DDL)
    generated = generated.withColumn("ts", F.timestamp_micros(F.col("ts_us"))).drop("ts_us")
    generated.writeTo(table).append()
    snapshot = spark.read.format("iceberg").load(f"{table}.snapshots").orderBy("committed_at", ascending=False).first()
    if snapshot is None:
        raise RuntimeError("fuzz source append produced no Iceberg snapshot")
    return int(snapshot["snapshot_id"])


def snapshot_sequence_numbers(catalog: SparkIcebergCatalog, table: str, snapshot_ids: list[int]) -> dict[int, int]:
    """Map each appended snapshot id to its exact Iceberg sequence number.

    Args:
        catalog: Spark Iceberg metadata adapter.
        table: Source table identifier.
        snapshot_ids: The appended snapshot ids in delivery order.

    Returns:
        A snapshot-id to sequence-number mapping restricted to the appended snapshots.
    """
    document: dict[str, Any] = catalog.metadata_document(table)
    by_id: dict[int, int] = {
        int(entry["snapshot-id"]): int(entry["sequence-number"]) for entry in document.get("snapshots", ())
    }
    return {snapshot_id: by_id[snapshot_id] for snapshot_id in snapshot_ids}


def serving_versions(config: BenchConfig, repository: ControlPlaneRepository) -> dict[str, int | None]:
    """Read the current served Lance version of every org, or ``None`` when unpublished.

    Args:
        config: Benchmark configuration.
        repository: Migrated PostgreSQL repository.

    Returns:
        A mapping of org identifier to served Lance version or ``None``.
    """
    versions: dict[str, int | None] = {}
    for org in config.org_ids():
        serving: ServingDataset | None = resolve_org_serving(repository, org)
        versions[org] = serving.lance_version if serving is not None else None
    return versions


def scenario_counts(ops: tuple[FuzzOp, ...]) -> dict[str, int]:
    """Count ops by scenario label for one snapshot.

    Args:
        ops: The snapshot's ops.

    Returns:
        A scenario-to-count mapping.
    """
    return dict(Counter(op.scenario for op in ops))


def blocked_conflict_rows(engine: Engine) -> list[dict[str, Any]]:
    """Return every blocked work row carrying the same-snapshot conflict code, attributed to its org.

    Args:
        engine: Isolated migrated PostgreSQL engine.

    Returns:
        One record per blocked conflict work row with its org, error code, and kind.
    """
    statement = (
        sa.select(datasets.c.org_id, dataset_work.c.error_code, dataset_work.c.kind)
        .select_from(dataset_work.join(datasets, dataset_work.c.dataset_id == datasets.c.dataset_id))
        .where(
            dataset_work.c.state == WorkState.BLOCKED.value,
            dataset_work.c.error_code == SAME_SNAPSHOT_CONFLICT,
        )
    )
    with engine.connect() as connection:
        return [
            {"org_id": str(row.org_id), "error_code": str(row.error_code), "kind": str(row.kind)}
            for row in connection.execute(statement)
        ]


def drive_fuzz_snapshots(
    config: BenchConfig,
    settings: FuzzSettings,
    workload: FuzzWorkload,
    repository: ControlPlaneRepository,
) -> tuple[list[dict[str, Any]], dict[int, int], dict[str, int | None]]:
    """Append and reconcile every fuzz snapshot, returning records, sequences, and pre-final versions.

    Args:
        config: Benchmark configuration.
        settings: Validated fuzz settings.
        workload: The generated op program.
        repository: Migrated PostgreSQL repository.

    Returns:
        Per-snapshot records, the ordinal-to-Iceberg-sequence mapping, and the per-org served
        versions captured immediately before the final drain (populated only in conflict mode).
    """
    spark: SparkSession = bench_workspace_spark(config)
    records: list[dict[str, Any]] = []
    snapshot_ids: list[int] = []
    pre_final: dict[str, int | None] = {}
    try:
        table: str = source_table_identifier(config)
        create_production_source_table(spark, table)
        baseline_id: int = append_fuzz_snapshot(spark, config, settings, table, workload.ops_for_snapshot(0))
        snapshot_ids.append(baseline_id)
        application = build_reconciler_application(
            spark,
            repository,
            table,
            baseline_id,
            config,
            spec_revision=fuzz_spec_revision(config, settings),
            source_name=FUZZ_SOURCE_NAME,
        )
        for snapshot in range(settings.snapshots):
            final: bool = snapshot == settings.snapshots - 1
            if snapshot > 0:
                snapshot_ids.append(
                    append_fuzz_snapshot(spark, config, settings, table, workload.ops_for_snapshot(snapshot))
                )
            if settings.conflict and final:
                pre_final = serving_versions(config, repository)
                totals: dict[str, int] = drain_reconciler(application, raise_on_blocked=False)
            else:
                totals = drain_reconciler(application)
            records.append(
                {
                    "snapshot": snapshot,
                    "snapshot_id": snapshot_ids[snapshot],
                    "op_scenarios": scenario_counts(workload.ops_for_snapshot(snapshot)),
                    "reconcile": totals,
                    "servings": serving_versions(config, repository),
                }
            )
        seq_map: dict[int, int] = snapshot_sequence_numbers(SparkIcebergCatalog(spark), table, snapshot_ids)
    finally:
        spark.stop()
    sequence_by_ordinal: dict[int, int] = {
        ordinal: seq_map[snapshot_ids[ordinal]] for ordinal in range(len(snapshot_ids))
    }
    return records, sequence_by_ordinal, pre_final


def vector_fingerprint(vector: np.ndarray) -> str:
    """Return a stable content hash of one regenerated float32 vector.

    Args:
        vector: The regenerated vector.

    Returns:
        The hex sha256 of the vector bytes.
    """
    return hashlib.sha256(np.asarray(vector, dtype=np.float32).tobytes()).hexdigest()


def expected_key_fingerprint(
    settings: FuzzSettings,
    org: str,
    oracle_org: dict[str, OracleRow],
    sequence_by_ordinal: dict[int, int],
) -> str:
    """Compute a deterministic per-org fingerprint of the expected published content.

    The fingerprint is regenerable from ``(seed, knobs)`` alone and excludes the event timestamp and
    every storage URI, so two same-seed runs produce identical fingerprints regardless of wall clock
    or workspace. It commits the key set, stored source sequence, tombstone state, and live-row
    vector, text, and cluster content, making the determinism check a single equality.

    Args:
        settings: Validated fuzz settings.
        org: Organization identifier.
        oracle_org: Expected terminal state for this org.
        sequence_by_ordinal: Snapshot-ordinal to Iceberg-sequence mapping.

    Returns:
        The hex sha256 fingerprint.
    """
    hasher = hashlib.sha256()
    for record_id in sorted(oracle_org):
        row: OracleRow = oracle_org[record_id]
        sequence: int = sequence_by_ordinal[row.snapshot_ordinal]
        content: str = ""
        if not row.is_deleted:
            vector: np.ndarray
            text: str
            cluster: str
            vector, text, cluster = fuzz_payload(
                settings.seed, org, record_id, row.payload_version, settings.dim, settings.num_clusters
            )
            content = f"{vector_fingerprint(vector)}|{text}|{cluster}"
        hasher.update(f"{record_id}:{sequence}:{int(row.is_deleted)}:{content}\n".encode())
    return hasher.hexdigest()


def index_coverage(config: BenchConfig, dataset: Any) -> tuple[list[str], list[str]]:
    """Return the built index names and any spec-declared indexes missing from a publication.

    Args:
        config: Benchmark configuration.
        dataset: An opened Lance dataset at its served version.

    Returns:
        The sorted built index names and the sorted missing declared index names.
    """
    built: list[str] = sorted(description.name for description in dataset.describe_indices())
    missing: list[str] = sorted(bench_spec_index_names(config) - set(built))
    return built, missing


def compare_payload(
    settings: FuzzSettings,
    org: str,
    record_id: str,
    oracle: OracleRow,
    actual: dict[str, Any],
) -> list[dict[str, Any]]:
    """Compare a row's payload against its oracle, returning per-field mismatches.

    Tombstones must carry all-null payload. Live rows must carry the regenerated float32 vector plus
    the exact text and cluster.

    Args:
        settings: Validated fuzz settings.
        org: Organization identifier.
        record_id: Logical merge key.
        oracle: Expected terminal state for this key.
        actual: The published row's fields keyed by column name.

    Returns:
        A list of ``{record_id, field, expected, actual}`` payload mismatch records.
    """
    if oracle.is_deleted:
        return [
            {"record_id": record_id, "field": field, "expected": None, "actual": "present"}
            for field in ("vector", "text", "cluster")
            if actual[field] is not None
        ]
    expected_vector: np.ndarray
    expected_text: str
    expected_cluster: str
    expected_vector, expected_text, expected_cluster = fuzz_payload(
        settings.seed, org, record_id, oracle.payload_version, settings.dim, settings.num_clusters
    )
    mismatches: list[dict[str, Any]] = []
    if actual["vector"] is None or not np.array_equal(np.asarray(actual["vector"], dtype=np.float32), expected_vector):
        preview: Any = None if actual["vector"] is None else "differs"
        mismatches.append(
            {
                "record_id": record_id,
                "field": "vector",
                "expected": vector_fingerprint(expected_vector),
                "actual": preview,
            }
        )
    if actual["text"] != expected_text:
        mismatches.append(
            {"record_id": record_id, "field": "text", "expected": expected_text, "actual": actual["text"]}
        )
    if actual["cluster"] != expected_cluster:
        mismatches.append(
            {"record_id": record_id, "field": "cluster", "expected": expected_cluster, "actual": actual["cluster"]}
        )
    return mismatches


def compare_key(
    settings: FuzzSettings,
    org: str,
    record_id: str,
    oracle: OracleRow,
    actual: dict[str, Any],
    expected_sequence: int,
) -> list[dict[str, Any]]:
    """Compare one published row against its oracle, returning per-field mismatches.

    Args:
        settings: Validated fuzz settings.
        org: Organization identifier.
        record_id: Logical merge key.
        oracle: Expected terminal state for this key.
        actual: The published row's fields keyed by column name.
        expected_sequence: The Iceberg sequence the stored source sequence must equal.

    Returns:
        A list of ``{record_id, field, expected, actual}`` mismatch records.
    """
    mismatches: list[dict[str, Any]] = []

    def note(field: str, expected: Any, observed: Any) -> None:
        """Record one field mismatch.

        Args:
            field: The mismatched field name.
            expected: The oracle value.
            observed: The published value.
        """
        mismatches.append({"record_id": record_id, "field": field, "expected": expected, "actual": observed})

    if bool(actual["is_deleted"]) != oracle.is_deleted:
        note("is_deleted", oracle.is_deleted, bool(actual["is_deleted"]))
    if int(actual["ts_us"]) != oracle.ts_us:
        note("ts_us", oracle.ts_us, int(actual["ts_us"]))
    if int(actual["source_sequence"]) != expected_sequence:
        note("source_sequence", expected_sequence, int(actual["source_sequence"]))
    digest: Any = actual["digest"]
    if not isinstance(digest, bytes) or len(digest) != 32:
        note("event_digest", "32-byte binary", None if digest is None else len(digest))
    if actual["window_seq"] is None:
        note("window_seq", "non-null", None)
    mismatches.extend(compare_payload(settings, org, record_id, oracle, actual))
    return mismatches


def read_actual_rows(serving: ServingDataset) -> tuple[dict[str, dict[str, Any]], int, Any]:
    """Read one publication's rows keyed by record id plus its row count and opened dataset.

    Args:
        serving: Resolved active publication.

    Returns:
        A record-id to field-dict mapping, the physical row count, and the opened Lance dataset.
    """
    dataset: Any = lance.dataset(serving.lance_uri, version=serving.lance_version)
    table: pa.Table = dataset.to_table(columns=list(VERIFY_COLUMNS))
    record_ids: list[str] = table.column("record_id").to_pylist()
    ts_us: list[int] = table.column("ts").cast(pa.int64()).to_pylist()
    is_deleted: list[bool] = table.column("is_deleted").to_pylist()
    source_sequence: list[int] = table.column("lance_etl_source_sequence").to_pylist()
    digest: list[Any] = table.column("lance_etl_event_digest").to_pylist()
    window_seq: list[Any] = table.column("lance_etl_window_seq").to_pylist()
    vectors: list[Any] = table.column("vector").to_pylist()
    texts: list[Any] = table.column("text").to_pylist()
    clusters: list[Any] = table.column("cluster").to_pylist()
    rows: dict[str, dict[str, Any]] = {}
    for index, record_id in enumerate(record_ids):
        rows[str(record_id)] = {
            "is_deleted": is_deleted[index],
            "ts_us": ts_us[index],
            "source_sequence": source_sequence[index],
            "digest": digest[index],
            "window_seq": window_seq[index],
            "vector": vectors[index],
            "text": texts[index],
            "cluster": clusters[index],
        }
    return rows, table.num_rows, dataset


def retention_band_evidence(
    settings: FuzzSettings, org: str, actual: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]] | None:
    """Summarize the observed retention-band records for one org in short mode.

    Args:
        settings: Validated fuzz settings.
        org: Organization identifier.
        actual: The org's published rows keyed by record id.

    Returns:
        Per-band presence and tombstone evidence, or ``None`` when retention is off.
    """
    if settings.retention_mode != "short":
        return None
    evidence: dict[str, dict[str, Any]] = {}
    for band, key in retention_band_keys(org).items():
        present: bool = key in actual
        evidence[band] = {
            "key": key,
            "present": present,
            "is_deleted": bool(actual[key]["is_deleted"]) if present else None,
        }
    return evidence


def verify_org(
    config: BenchConfig,
    settings: FuzzSettings,
    serving: ServingDataset | None,
    org: str,
    oracle_org: dict[str, OracleRow],
    sequence_by_ordinal: dict[int, int],
) -> dict[str, Any]:
    """Verify one org's publication against its oracle with full row-content comparison.

    Args:
        config: Benchmark configuration.
        settings: Validated fuzz settings.
        serving: Resolved active publication, or ``None`` when unpublished.
        org: Organization identifier.
        oracle_org: Expected terminal state for this org.
        sequence_by_ordinal: Snapshot-ordinal to Iceberg-sequence mapping.

    Returns:
        The per-org verification outcome including the deterministic content fingerprint.
    """
    fingerprint: str = expected_key_fingerprint(settings, org, oracle_org, sequence_by_ordinal)
    if serving is None:
        return {
            "org": org,
            "published": False,
            "ok": len(oracle_org) == 0,
            "expected_total": len(oracle_org),
            "fingerprint": fingerprint,
        }
    actual: dict[str, dict[str, Any]]
    total: int
    dataset: Any
    actual, total, dataset = read_actual_rows(serving)
    built: list[str]
    missing_indexes: list[str]
    built, missing_indexes = index_coverage(config, dataset)
    expected_keys: set[str] = set(oracle_org)
    actual_keys: set[str] = set(actual)
    missing: list[str] = sorted(expected_keys - actual_keys)
    unexpected: list[str] = sorted(actual_keys - expected_keys)
    mismatches: list[dict[str, Any]] = []
    for record_id in sorted(expected_keys & actual_keys):
        mismatches.extend(
            compare_key(
                settings,
                org,
                record_id,
                oracle_org[record_id],
                actual[record_id],
                sequence_by_ordinal[oracle_org[record_id].snapshot_ordinal],
            )
        )
    field_totals: dict[str, int] = dict(Counter(entry["field"] for entry in mismatches))
    distinct_ok: bool = total == len(actual)
    ok: bool = distinct_ok and not missing and not unexpected and not mismatches and not missing_indexes
    return {
        "org": org,
        "published": True,
        "ok": ok,
        "distinct_equals_total": distinct_ok,
        "total": total,
        "expected_total": len(oracle_org),
        "missing_keys": missing,
        "unexpected_keys": unexpected,
        "mismatch_field_totals": field_totals,
        "mismatches": mismatches[:MAX_REPORTED_MISMATCHES],
        "indexes": built,
        "missing_indexes": missing_indexes,
        "retention_bands": retention_band_evidence(settings, org, actual),
        "fingerprint": fingerprint,
        "lance_version": serving.lance_version,
    }


def verify_fuzz(
    config: BenchConfig,
    settings: FuzzSettings,
    workload: FuzzWorkload,
    now_us: int,
    repository: ControlPlaneRepository,
    sequence_by_ordinal: dict[int, int],
) -> dict[str, Any]:
    """Verify every org against its oracle, capping the victim org in conflict mode.

    Args:
        config: Benchmark configuration.
        settings: Validated fuzz settings.
        workload: The generated op program.
        now_us: Generation wall-clock instant in epoch microseconds.
        repository: Migrated PostgreSQL repository.
        sequence_by_ordinal: Snapshot-ordinal to Iceberg-sequence mapping.

    Returns:
        The verification section with an overall ``ok`` flag and per-org results.
    """
    final_ordinal: int = settings.snapshots - 1
    full: dict[str, dict[str, OracleRow]] = oracle_rows(workload, settings, now_us, final_ordinal)
    capped: dict[str, dict[str, OracleRow]] = (
        oracle_rows(workload, settings, now_us, final_ordinal - 1) if settings.conflict else full
    )
    org_results: list[dict[str, Any]] = []
    overall_ok: bool = True
    for org in config.org_ids():
        source: dict[str, dict[str, OracleRow]] = capped if settings.conflict and org == "org0" else full
        oracle_org: dict[str, OracleRow] = source.get(org, {})
        result: dict[str, Any] = verify_org(
            config, settings, resolve_org_serving(repository, org), org, oracle_org, sequence_by_ordinal
        )
        org_results.append(result)
        overall_ok = overall_ok and bool(result["ok"])
    return {"ok": overall_ok, "orgs": org_results}


def assess_conflict(
    settings: FuzzSettings,
    conflict_rows: list[dict[str, Any]],
    pre_final: dict[str, int | None],
    post_versions: dict[str, int | None],
) -> dict[str, Any]:
    """Assess the conflict-mode assertions from the blocked work and serving versions.

    Args:
        settings: Validated fuzz settings.
        conflict_rows: Blocked work rows carrying the same-snapshot conflict code.
        pre_final: Per-org served versions captured before the final drain.
        post_versions: Per-org served versions after the final drain.

    Returns:
        The conflict evidence with an ``ok`` flag; ``ok`` is vacuously true when disabled.
    """
    if not settings.conflict:
        return {"enabled": False, "ok": True}
    victim_rows: list[dict[str, Any]] = [row for row in conflict_rows if row["org_id"] == "org0"]
    blocked_ok: bool = len(conflict_rows) == 1 and len(victim_rows) == 1
    victim_unchanged: bool = pre_final.get("org0") == post_versions.get("org0")
    others_advanced: bool = all(
        post_versions.get(org) is not None
        and pre_final.get(org) is not None
        and int(post_versions[org]) > int(pre_final[org])
        for org in post_versions
        if org != "org0"
    )
    return {
        "enabled": True,
        "ok": blocked_ok and victim_unchanged and others_advanced,
        "blocked_rows": conflict_rows,
        "victim_unchanged": victim_unchanged,
        "others_advanced": others_advanced,
        "pre_final_versions": pre_final,
        "post_versions": post_versions,
    }


def knobs_document(settings: FuzzSettings) -> dict[str, Any]:
    """Render the resolved fuzz knobs for the evidence document.

    Args:
        settings: Validated fuzz settings.

    Returns:
        A JSON-serializable knob mapping.
    """
    return {
        "seed": settings.seed,
        "ops": settings.ops,
        "snapshots": settings.snapshots,
        "keyspace": settings.keyspace,
        "mix": list(settings.mix),
        "dup_probability": settings.dup_probability,
        "retention_mode": settings.retention_mode,
        "retention_seconds": settings.retention_seconds,
        "conflict": settings.conflict,
        "dim": settings.dim,
        "tenants": settings.tenants,
        "num_clusters": settings.num_clusters,
    }


def run_fuzz(config: BenchConfig) -> dict[str, Any]:
    """Run the randomized CRUD fuzz evaluation end-to-end and verify against the oracle.

    Deletes every stale physical Lance path the fuzz evaluator's own source can address
    (``reset_fuzz_datasets``) before touching Iceberg or the reconciler, so the run is hermetic:
    its published state depends only on ``(seed, knobs, now_us)``, never on what an earlier fuzz
    invocation, at any seed or knob combination, left on disk at the same deterministic path.

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document saved as ``fuzz.json`` in the run directory.

    Raises:
        RuntimeError: If content verification or the conflict assertions fail.
    """
    settings: FuzzSettings = FuzzSettings.from_config(config)
    now_us: int = int(time.time() * 1_000_000)
    workload: FuzzWorkload = generate_workload(settings, now_us)
    ensure_dir(config.run_dir())
    reset_fuzz_datasets(config)
    database_url: str = resolve_database_url()
    with isolated_control_plane(database_url) as (repository, engine, isolated_url):
        del isolated_url
        records: list[dict[str, Any]]
        sequence_by_ordinal: dict[int, int]
        pre_final: dict[str, int | None]
        records, sequence_by_ordinal, pre_final = drive_fuzz_snapshots(config, settings, workload, repository)
        post_versions: dict[str, int | None] = serving_versions(config, repository)
        conflict_rows: list[dict[str, Any]] = blocked_conflict_rows(engine) if settings.conflict else []
        verification: dict[str, Any] = verify_fuzz(config, settings, workload, now_us, repository, sequence_by_ordinal)
        conflict: dict[str, Any] = assess_conflict(settings, conflict_rows, pre_final, post_versions)
    overall_ok: bool = bool(verification["ok"]) and bool(conflict["ok"])
    result: dict[str, Any] = save_phase(
        config,
        "fuzz",
        {
            "knobs": knobs_document(settings),
            "now_us": now_us,
            "sequence_by_ordinal": {str(ordinal): sequence for ordinal, sequence in sequence_by_ordinal.items()},
            "snapshots": records,
            "conflict": conflict,
            "verification": verification,
            "ok": overall_ok,
        },
    )
    if not verification["ok"]:
        raise RuntimeError("fuzz content verification failed; inspect fuzz.json verification section")
    if not conflict["ok"]:
        raise RuntimeError("fuzz conflict assertions failed; inspect fuzz.json conflict section")
    return result

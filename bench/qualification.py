"""Bounded deterministic scale and failure qualification artifact generation."""

from __future__ import annotations

import hashlib
import json
import time
import tracemalloc
from collections import Counter, defaultdict
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from bench.capacity import capacity_artifact
from bench.config import BenchConfig
from bench.results import save_phase
from lance_etl.etl.digest import encode_mapping
from lance_etl.etl.mutation import MutationInput, collapse_snapshot_mutations
from lance_etl.etl.pivot import ETLConfig
from lance_etl.etl.plan import MAX_SHUFFLE_PARTITIONS, bucket_count, shuffle_partition_count
from lance_etl.telemetry import TelemetryConfig

LOCAL_COHORT_ROW_LIMIT: int = 1_000_000
"""Largest synthetic cohort permitted without an explicit operator opt-in."""

WIDE_FIELD_COUNT: int = 32
"""Release-independent payload width exercised by every generated row."""

BASE_TIME: datetime = datetime(2025, 1, 1, tzinfo=UTC)
"""Stable timestamp origin for reproducible cohorts."""


@dataclass(frozen=True)
class QualificationRow:
    """One deterministic source delivery carrying business and partition clocks.

    Attributes:
        mutation: Business mutation passed to the production collapse contract.
        source_sequence: Iceberg snapshot sequence that totally orders arrival.
        processing_timestamp: Timestamp whose hour controls Iceberg partition placement.
        scenario: Named qualification feature represented by the row.
    """

    mutation: MutationInput
    source_sequence: int
    processing_timestamp: datetime
    scenario: str


def payload_for(index: int, wide: bool = False) -> dict[str, Any]:
    """Build a fixed-width deterministic post-image payload.

    Args:
        index: Stable row number.
        wide: Whether to attach the large-payload sentinel.

    Returns:
        Payload with a fixed 32-field schema plus text and vector-like values.
    """
    payload: dict[str, Any] = {f"field_{field:02d}": f"v{index % 101}-{field}" for field in range(WIDE_FIELD_COUNT)}
    payload["text"] = (f"wide-{index}-" + "x" * 8_192) if wide else f"row-{index}"
    payload["vector"] = [float((index + offset) % 17) for offset in range(16)]
    return payload


def base_row(index: int, row_count: int) -> QualificationRow:
    """Build one base upsert in a power-law-like target distribution.

    Args:
        index: Stable row number.
        row_count: Total requested base row count.

    Returns:
        Deterministic base row with occasional late and wide payload classification.
    """
    hot_cutoff: int = row_count * 8 // 10
    org_id: str = "org-hot" if index < hot_cutoff else f"org-tail-{index % 257:03d}"
    processing_timestamp: datetime = BASE_TIME + timedelta(hours=index % 24)
    event_timestamp: datetime = processing_timestamp
    scenario: str = "base"
    if index % 37 == 0:
        processing_timestamp += timedelta(hours=96)
        event_timestamp -= timedelta(hours=72)
        scenario = "late_hour"
    wide: bool = index % 97 == 0
    if wide:
        scenario = "wide_payload" if scenario == "base" else "late_wide_payload"
    mutation = MutationInput(
        tenant_id="tenant0",
        namespace="vectors",
        org_id=org_id,
        vector_id=f"vector-{index:012d}",
        operation="upsert",
        event_timestamp=event_timestamp,
        payload=payload_for(index, wide),
    )
    return QualificationRow(mutation, 1 + index // 1_000, processing_timestamp, scenario)


def generate_qualification_cohort(row_count: int) -> Iterator[QualificationRow]:
    """Yield a deterministic skew, duplicate, late, delete, recreate, and wide cohort.

    Args:
        row_count: Number of base upserts before planted redeliveries and lifecycle events.

    Yields:
        Qualification deliveries in deterministic arrival order.

    Raises:
        ValueError: If the requested base row count is not positive.
    """
    if row_count < 1:
        raise ValueError("qualification row count must be positive")
    lifecycle_sequence: int = row_count + 1
    for index in range(row_count):
        row = base_row(index, row_count)
        yield row
        if index % 29 == 0:
            yield QualificationRow(row.mutation, row.source_sequence, row.processing_timestamp, "exact_duplicate")
        if index % 53 == 0:
            deleted = MutationInput(
                tenant_id=row.mutation.tenant_id,
                namespace=row.mutation.namespace,
                org_id=row.mutation.org_id,
                vector_id=row.mutation.vector_id,
                operation="delete",
                event_timestamp=row.mutation.event_timestamp + timedelta(seconds=1),
                payload={},
            )
            yield QualificationRow(
                deleted,
                lifecycle_sequence,
                row.processing_timestamp + timedelta(hours=1),
                "delete",
            )
            recreated = MutationInput(
                tenant_id=row.mutation.tenant_id,
                namespace=row.mutation.namespace,
                org_id=row.mutation.org_id,
                vector_id=row.mutation.vector_id,
                operation="upsert",
                event_timestamp=row.mutation.event_timestamp + timedelta(seconds=2),
                payload=payload_for(index + row_count, index % 97 == 0),
            )
            yield QualificationRow(
                recreated,
                lifecycle_sequence + 1,
                row.processing_timestamp + timedelta(hours=2),
                "recreate",
            )
            lifecycle_sequence += 2


def row_fingerprint(row: QualificationRow) -> bytes:
    """Encode one delivery for a reproducible cohort fingerprint.

    Args:
        row: Generated qualification row.

    Returns:
        Canonical JSON bytes excluding runtime measurements.
    """
    document = {
        "target": row.mutation.target,
        "vector_id": row.mutation.vector_id,
        "operation": row.mutation.operation,
        "event_timestamp": row.mutation.event_timestamp.isoformat(),
        "processing_timestamp": row.processing_timestamp.isoformat(),
        "source_sequence": row.source_sequence,
        "scenario": row.scenario,
        "payload_digest": hashlib.sha256(encode_mapping(row.mutation.payload)).hexdigest(),
    }
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")


def external_scale_gates(capacity: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    """Describe exact evidence required before claiming 100M or 1B success.

    Args:
        capacity: Optional local capacity evidence used to name every unmet resource floor.

    Returns:
        Two explicit external infrastructure gates with resource floors and commands.
    """
    gates: list[dict[str, Any]] = [
        {
            "scale": "100M",
            "status": "EXTERNAL_GATE_REQUIRED",
            "minimum_resources": {
                "disk_free_bytes_at_start": 80 * 1024**3,
                "logical_cpu_count": 16,
                "physical_memory_bytes": 32 * 1024**3,
            },
            "command": "python -m bench e2e --dataset bigann --limit 100000000 --no-text",
            "required_evidence": ["capacity.json", "dataset checksums", "phase artifacts", "cold cache results"],
            "claim": "NOT_RUN",
        },
        {
            "scale": "1B",
            "status": "EXTERNAL_GATE_REQUIRED",
            "minimum_resources": {
                "disk_free_bytes_at_start": 800 * 1024**3,
                "logical_cpu_count": 64,
                "physical_memory_bytes": 128 * 1024**3,
            },
            "command": "python -m bench e2e --dataset bigann --limit 1000000000 --no-text",
            "required_evidence": ["capacity.json", "dataset checksums", "phase artifacts", "cold cache results"],
            "claim": "NOT_RUN",
        },
    ]
    if capacity is None:
        return gates
    hardware: Mapping[str, Any] = capacity.get("hardware", {})
    for gate in gates:
        unmet: dict[str, dict[str, int | None]] = {}
        for resource, required_value in gate["minimum_resources"].items():
            available_value: int | None = hardware.get(resource)
            if available_value is None or available_value < required_value:
                unmet[resource] = {"available": available_value, "required": required_value}
        gate["unmet_resources"] = unmet
        gate["status"] = "BLOCKED_EXTERNAL_CAPACITY" if unmet else "EXTERNAL_APPROVAL_REQUIRED"
    return gates


def validate_capacity_evidence(artifact: Mapping[str, Any]) -> None:
    """Reject incomplete hardware, software, source, workload, or cache evidence.

    Args:
        artifact: Candidate capacity artifact.

    Raises:
        ValueError: If a production qualification cannot be reproduced from the artifact.
    """
    required_paths: tuple[tuple[str, ...], ...] = (
        ("git_commit",),
        ("software", "pylance"),
        ("software", "python"),
        ("hardware", "logical_cpu_count"),
        ("hardware", "physical_memory_bytes"),
        ("hardware", "disk_free_bytes_at_start"),
        ("cache_state", "workspace_existed_at_start"),
        ("cache_state", "prepared_corpus_existed_at_start"),
        ("cache_state", "lance_root_existed_at_start"),
        ("workload", "seed"),
    )
    for path in required_paths:
        value: Any = artifact
        for component in path:
            value = value.get(component) if isinstance(value, Mapping) else None
        if value is None or value == "":
            raise ValueError(f"capacity evidence is missing {'.'.join(path)}")


def qualification_measurements(row_count: int) -> dict[str, Any]:
    """Execute one bounded collapse pass and measure deterministic scale characteristics.

    Args:
        row_count: Number of base rows in the generated cohort.

    Returns:
        Cohort identity, feature counts, collapse materialization, and shuffle-width evidence.
    """
    groups: dict[tuple[int, tuple[str, str, str]], list[MutationInput]] = defaultdict(list)
    input_by_target: Counter[tuple[str, str, str]] = Counter()
    scenario_counts: Counter[str] = Counter()
    payload_bytes: int = 0
    fingerprint = hashlib.sha256()
    tracemalloc.start()
    started: float = time.perf_counter()
    for row in generate_qualification_cohort(row_count):
        groups[(row.source_sequence, row.mutation.target)].append(row.mutation)
        input_by_target[row.mutation.target] += 1
        scenario_counts[row.scenario] += 1
        payload_bytes += len(encode_mapping(row.mutation.payload))
        fingerprint.update(row_fingerprint(row))
    terminal_rows: int = 0
    target_terminals: Counter[tuple[str, str, str]] = Counter()
    for (source_sequence, target), mutations in sorted(groups.items()):
        collapsed = collapse_snapshot_mutations(mutations, source_sequence, source_sequence)
        terminal_rows += len(collapsed)
        target_terminals[target] += len(collapsed)
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    elapsed_seconds: float = time.perf_counter() - started
    config = ETLConfig(base_uri="qualification://local", telemetry=TelemetryConfig())
    shuffle_width: int = shuffle_partition_count(terminal_rows, len(input_by_target), config)
    hot_rows: int = max(target_terminals.values())
    return {
        "schema_version": 1,
        "base_rows": row_count,
        "input_deliveries": sum(scenario_counts.values()),
        "terminal_mutations": terminal_rows,
        "target_count": len(input_by_target),
        "scenario_counts": dict(sorted(scenario_counts.items())),
        "cohort_sha256": fingerprint.hexdigest(),
        "collapse": {
            "passes_per_snapshot_target": 1,
            "snapshot_target_groups": len(groups),
            "largest_group_input_rows": max(len(mutations) for mutations in groups.values()),
            "largest_target_input_rows": max(input_by_target.values()),
            "largest_target_terminal_rows": hot_rows,
            "canonical_payload_bytes": payload_bytes,
            "python_peak_traced_bytes": peak_bytes,
            "elapsed_seconds": elapsed_seconds,
        },
        "routing": {
            "shuffle_width": shuffle_width,
            "shuffle_width_cap": MAX_SHUFFLE_PARTITIONS,
            "hot_target_bucket_count": bucket_count(
                hot_rows,
                config.bucket_rows,
                config.max_buckets_per_dataset,
            ),
            "within_internal_cap": shuffle_width <= MAX_SHUFFLE_PARTITIONS,
        },
    }


def run_qualification(config: BenchConfig) -> dict[str, Any]:
    """Run and persist the local qualification without implying external scale success.

    Args:
        config: Benchmark configuration carrying cohort size and opt-in state.

    Returns:
        Saved qualification phase artifact.

    Raises:
        ValueError: If an oversized cohort lacks explicit opt-in or capacity evidence is incomplete.
    """
    if config.qualification_rows > LOCAL_COHORT_ROW_LIMIT and not config.allow_large_qualification:
        raise ValueError(
            f"qualification_rows={config.qualification_rows} exceeds the local safety bound "
            f"{LOCAL_COHORT_ROW_LIMIT}; pass --allow-large-qualification only on qualified infrastructure"
        )
    capacity = capacity_artifact(config)
    validate_capacity_evidence(capacity)
    return save_phase(
        config,
        "qualify",
        {
            "capacity": capacity,
            "qualification": qualification_measurements(config.qualification_rows),
            "external_scale_gates": external_scale_gates(capacity),
        },
    )

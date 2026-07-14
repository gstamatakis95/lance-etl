"""Canonical immutable evidence for one exact Lance publication candidate."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import asdict, dataclass

import lance
import pyarrow as pa

from lance_etl.etl.profile import TargetProfile

PIN_PATTERN: re.Pattern[str] = re.compile(r"^candidate-[0-9a-f]{32}$")
"""Allowlist for immutable work-derived Lance publication pins."""


@dataclass(frozen=True, slots=True)
class CandidateCounts:
    """Distributed cardinality evidence for one exact candidate version."""

    total_rows: int
    live_rows: int
    distinct_live_vector_ids: int

    def validate(self) -> CandidateCounts:
        """Validate non-negative row counts and live-vector uniqueness.

        Returns:
            This validated count evidence.

        Raises:
            ValueError: If counts are impossible or live vector IDs are duplicated.
        """
        if min(self.total_rows, self.live_rows, self.distinct_live_vector_ids) < 0:
            raise ValueError("candidate counts must be non-negative")
        if self.live_rows > self.total_rows:
            raise ValueError("live row count cannot exceed total row count")
        if self.distinct_live_vector_ids != self.live_rows:
            raise ValueError("live vector_id uniqueness validation failed")
        return self


@dataclass(frozen=True, slots=True)
class IndexOutcome:
    """Validation outcome for one required or optional index."""

    name: str
    column: str
    kind: str
    required: bool
    valid: bool
    unindexed_fragments: int
    artifact_generation_digest: str | None = None
    error_code: str | None = None

    def validate(self) -> IndexOutcome:
        """Validate bounded and internally consistent index evidence.

        Returns:
            This validated outcome.

        Raises:
            ValueError: If an outcome is malformed or a required index is invalid.
        """
        if not self.name or not self.column or not self.kind:
            raise ValueError("index outcome identifiers must be non-empty")
        if self.unindexed_fragments < 0:
            raise ValueError("unindexed fragment count must be non-negative")
        if self.valid and self.unindexed_fragments:
            raise ValueError("a valid index cannot have unindexed fragments")
        if self.required and not self.valid:
            raise ValueError(f"required index {self.name!r} is invalid")
        if self.kind == "vector" and self.valid and not self.artifact_generation_digest:
            raise ValueError("a valid vector index requires one immutable artifact generation digest")
        return self


@dataclass(frozen=True, slots=True)
class ArtifactManifest:
    """Canonical exact-version publication evidence stored by content digest."""

    format_version: int
    work_id: str
    target_id: str
    candidate_lance_uri: str
    data_lance_version: int
    indexed_lance_version: int
    profile_id: str
    schema_fingerprint: str
    total_rows: int
    live_rows: int
    distinct_live_vector_ids: int
    indexes: tuple[IndexOutcome, ...]

    def canonical_bytes(self) -> bytes:
        """Serialize deterministic JSON without environment or clock fields.

        Returns:
            Canonical UTF-8 JSON bytes.
        """
        payload = asdict(self)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    def digest(self) -> bytes:
        """Return the content address of the canonical manifest.

        Returns:
            Raw SHA-256 digest.
        """
        return hashlib.sha256(self.canonical_bytes()).digest()

    def content_uri(self) -> str:
        """Return a dataset-local immutable content-addressed manifest URI.

        Returns:
            URI named by the canonical SHA-256 hex digest.
        """
        base = self.candidate_lance_uri.rstrip("/")
        return f"{base}.artifacts/publications/{self.digest().hex()}.json"


def candidate_pin_name(work_id: uuid.UUID) -> str:
    """Derive the immutable Lance pin name for one durable work generation.

    Args:
        work_id: Durable work identifier.

    Returns:
        Allowlisted tag name that is never shared by another work row.
    """
    name = f"candidate-{work_id.hex}"
    if PIN_PATTERN.fullmatch(name) is None:
        raise ValueError("invalid candidate publication pin")
    return name


def schema_fingerprint(schema: pa.Schema) -> str:
    """Hash the exact Arrow schema including field metadata.

    Args:
        schema: Candidate dataset schema.

    Returns:
        Lowercase SHA-256 hex digest.
    """
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def validate_profile_schema(schema: pa.Schema, profile: TargetProfile) -> None:
    """Validate required control fields and profile-owned vector dimensions.

    Args:
        schema: Exact candidate Arrow schema.
        profile: Release-owned target profile.

    Raises:
        ValueError: If required columns or vector dimensions differ.
    """
    required = {
        "vector_id": pa.string(),
        "lance_etl_window_seq": pa.int64(),
        "lance_etl_source_sequence": pa.int64(),
        "lance_etl_event_digest": pa.binary(32),
        "is_deleted": pa.bool_(),
    }
    missing = sorted(set(required) - set(schema.names))
    if missing:
        raise ValueError(f"candidate schema is missing required columns: {missing}")
    for name, expected in required.items():
        if schema.field(name).type != expected:
            raise ValueError(f"candidate column {name!r} must be {expected}")
    for vector in profile.vector_fields:
        if vector.name not in schema.names:
            raise ValueError(f"candidate schema is missing vector column {vector.name!r}")
        field_type = schema.field(vector.name).type
        if not pa.types.is_fixed_size_list(field_type) or field_type.list_size != vector.dimension:
            raise ValueError(f"vector {vector.name!r} must be fixed-size dimension {vector.dimension}")


def validate_index_outcomes(outcomes: tuple[IndexOutcome, ...]) -> tuple[IndexOutcome, ...]:
    """Validate unique per-index outcomes with required failures blocking publication.

    Args:
        outcomes: Per-index results from isolated validation.

    Returns:
        Outcomes sorted by index name for canonical serialization.

    Raises:
        ValueError: If names repeat or a required index is invalid.
    """
    ordered = tuple(sorted((outcome.validate() for outcome in outcomes), key=lambda item: item.name))
    names = [outcome.name for outcome in ordered]
    if len(names) != len(set(names)):
        raise ValueError("artifact manifest contains duplicate index outcomes")
    return ordered


def build_artifact_manifest(
    dataset: lance.LanceDataset,
    work_id: uuid.UUID,
    target_id: uuid.UUID,
    candidate_lance_uri: str,
    data_lance_version: int,
    indexed_lance_version: int,
    profile: TargetProfile,
    counts: CandidateCounts,
    indexes: tuple[IndexOutcome, ...],
) -> ArtifactManifest:
    """Validate exact candidate evidence and build its immutable manifest.

    Args:
        dataset: Candidate opened at the exact indexed version on an executor.
        work_id: Durable work generation.
        target_id: Opaque target identity.
        candidate_lance_uri: Persisted candidate URI.
        data_lance_version: Exact post-maintenance input version.
        indexed_lance_version: Exact post-index candidate version.
        profile: Release-owned schema and index policy.
        counts: Distributed row-count and uniqueness evidence.
        indexes: Isolated per-index validation outcomes.

    Returns:
        Canonical immutable artifact manifest.

    Raises:
        ValueError: If any exact-version, schema, count, or index invariant fails.
    """
    if dataset.uri.rstrip("/") != candidate_lance_uri.rstrip("/"):
        raise ValueError("candidate dataset URI differs from persisted work input")
    if dataset.version != indexed_lance_version:
        raise ValueError("candidate dataset is not open at the exact indexed version")
    if data_lance_version < 1 or indexed_lance_version < data_lance_version:
        raise ValueError("candidate versions are invalid or regressed")
    validated_counts = counts.validate()
    if dataset.count_rows() != validated_counts.total_rows:
        raise ValueError("candidate exact-version row count differs from distributed evidence")
    validate_profile_schema(dataset.schema, profile)
    validated_indexes = validate_index_outcomes(indexes)
    return ArtifactManifest(
        format_version=1,
        work_id=str(work_id),
        target_id=str(target_id),
        candidate_lance_uri=candidate_lance_uri,
        data_lance_version=data_lance_version,
        indexed_lance_version=indexed_lance_version,
        profile_id=profile.profile_id,
        schema_fingerprint=schema_fingerprint(dataset.schema),
        total_rows=validated_counts.total_rows,
        live_rows=validated_counts.live_rows,
        distinct_live_vector_ids=validated_counts.distinct_live_vector_ids,
        indexes=validated_indexes,
    )

"""Release-owned target profiles that bound schema, indexing, and execution policy."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class VectorField:
    """One allowed vector field and its immutable index contract.

    Attributes:
        name: Concrete Lance column name.
        dimension: Required vector dimension.
        metric: Lance distance metric.
    """

    name: str
    dimension: int
    metric: str


@dataclass(frozen=True)
class TargetProfile:
    """Versioned code-owned policy for one class of target datasets.

    Attributes:
        profile_id: Stable release profile identifier.
        vector_fields: Allowed vector map entries.
        text_fields: Allowed text map entries.
        metadata_fields: Allowed metadata map entries.
        scalar_fields: Allowed top-level scalar payload fields.
        rows_per_fragment: Qualified row budget per Lance fragment.
        merge_rows_per_chunk: Qualified mutation chunk row bound.
        max_index_deltas: Soft index-delta budget.
    """

    profile_id: str
    vector_fields: tuple[VectorField, ...]
    text_fields: tuple[str, ...]
    metadata_fields: tuple[str, ...]
    scalar_fields: tuple[str, ...]
    rows_per_fragment: int
    merge_rows_per_chunk: int
    max_index_deltas: int

    @property
    def payload_fields(self) -> tuple[str, ...]:
        """Return every flattened payload field in deterministic order."""
        vector_names = tuple(field.name for field in self.vector_fields)
        return vector_names + self.text_fields + self.metadata_fields + self.scalar_fields


PRODUCTION_PROFILE = TargetProfile(
    profile_id="production-v1",
    vector_fields=(VectorField(name="vector", dimension=128, metric="cosine"),),
    text_fields=("text",),
    metadata_fields=("cluster",),
    scalar_fields=("category", "event_timestamp", "ttl"),
    rows_per_fragment=1_000_000,
    merge_rows_per_chunk=250_000,
    max_index_deltas=8,
)

PROFILES: Mapping[str, TargetProfile] = {PRODUCTION_PROFILE.profile_id: PRODUCTION_PROFILE}


def profile_by_id(profile_id: str) -> TargetProfile:
    """Resolve a release-bundled target profile.

    Args:
        profile_id: Stable profile identifier stored on the target row.

    Returns:
        Release-owned target profile.

    Raises:
        ValueError: If the profile is not bundled with this release.
    """
    profile = PROFILES.get(profile_id)
    if profile is None:
        raise ValueError(f"unknown release target profile: {profile_id!r}")
    return profile


def normalize_vector(value: Sequence[Any], field: VectorField) -> list[float]:
    """Validate and normalize one profile-owned vector.

    Args:
        value: Incoming vector components.
        field: Immutable vector field contract.

    Returns:
        Finite float components.

    Raises:
        ValueError: If dimension or numeric values violate the profile.
    """
    if len(value) != field.dimension:
        raise ValueError(f"vector {field.name!r} requires dimension {field.dimension}, got {len(value)}")
    normalized = [float(component) for component in value]
    if not all(math.isfinite(component) for component in normalized):
        raise ValueError(f"vector {field.name!r} contains non-finite values")
    return normalized


def reject_unknown_keys(kind: str, values: Mapping[str, Any], allowed: frozenset[str]) -> None:
    """Reject source map keys outside the release profile.

    Args:
        kind: Payload map kind used in diagnostics.
        values: Incoming map values.
        allowed: Release-owned allowed names.

    Raises:
        ValueError: If unknown keys exist.
    """
    unknown = sorted(set(values) - allowed)
    if unknown:
        raise ValueError(f"{kind} contains fields outside profile: {unknown}")


def normalize_profile_payload(
    profile: TargetProfile,
    vectors: Mapping[str, Sequence[Any]] | None,
    texts: Mapping[str, str | None] | None,
    metadata: Mapping[str, str | None] | None,
    scalars: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Flatten and fully materialize a source payload under a code-owned profile.

    Args:
        profile: Release-owned target policy.
        vectors: Source vector map.
        texts: Source text map.
        metadata: Source metadata map.
        scalars: Allowed top-level scalar fields.

    Returns:
        Complete post-image with every allowed field present.

    Raises:
        ValueError: If unknown keys, missing required vectors, or invalid vectors are present.
    """
    vector_values = dict(vectors or {})
    text_values = dict(texts or {})
    metadata_values = dict(metadata or {})
    scalar_values = dict(scalars or {})
    vector_fields = {field.name: field for field in profile.vector_fields}
    reject_unknown_keys("vectors", vector_values, frozenset(vector_fields))
    reject_unknown_keys("texts", text_values, frozenset(profile.text_fields))
    reject_unknown_keys("metadata", metadata_values, frozenset(profile.metadata_fields))
    reject_unknown_keys("scalars", scalar_values, frozenset(profile.scalar_fields))
    missing_vectors = sorted(set(vector_fields) - set(vector_values))
    if missing_vectors:
        raise ValueError(f"upsert is missing required vectors: {missing_vectors}")
    result: dict[str, Any] = {}
    for name, field in vector_fields.items():
        result[name] = normalize_vector(vector_values[name], field)
    for name in profile.text_fields:
        result[name] = text_values.get(name)
    for name in profile.metadata_fields:
        result[name] = metadata_values.get(name)
    for name in profile.scalar_fields:
        result[name] = scalar_values.get(name)
    return result


def tombstone_payload(profile: TargetProfile) -> dict[str, None]:
    """Build a complete null post-image for a profile-owned tombstone.

    Args:
        profile: Release-owned target policy.

    Returns:
        Every payload field mapped to null.
    """
    return {name: None for name in profile.payload_fields}

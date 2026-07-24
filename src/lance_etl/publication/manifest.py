"""Small exact-version publication helpers shared by the local reconciler."""

from __future__ import annotations

import hashlib
import re
import uuid

import lance
import pyarrow as pa

PIN_PATTERN: re.Pattern[str] = re.compile(r"^candidate-[0-9a-f]{32}$")
"""Allowlist for immutable work-derived Lance publication pins."""


def candidate_pin_name(work_id: uuid.UUID) -> str:
    """Derive the immutable Lance pin name for one durable work generation.

    Args:
        work_id: Durable work identifier.

    Returns:
        Allowlisted tag name that is never shared by another work row.
    """
    name: str = f"candidate-{work_id.hex}"
    if PIN_PATTERN.fullmatch(name) is None:
        raise ValueError("invalid candidate publication pin")
    return name


def tag_version(dataset: lance.LanceDataset, name: str) -> int | None:
    """Resolve a Lance v8 tag while normalizing its missing-tag exception.

    Args:
        dataset: Open dataset whose tag namespace is queried.
        name: Exact tag name.

    Returns:
        Exact tagged version or ``None`` when the tag is absent.
    """
    try:
        value: int | None = dataset.tags.get_version(name)
    except ValueError as error:
        if "does not exist" not in str(error):
            raise
        return None
    return int(value) if value is not None else None


def schema_fingerprint(schema: pa.Schema) -> str:
    """Hash the exact Arrow schema including field metadata.

    Args:
        schema: Candidate dataset schema.

    Returns:
        Lowercase SHA-256 hex digest.
    """
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()

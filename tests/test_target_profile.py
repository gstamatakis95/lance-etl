"""Tests for release-owned schema and vector profile enforcement."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lance_etl.etl.profile import PRODUCTION_PROFILE, normalize_profile_payload, profile_by_id, tombstone_payload


def valid_vector() -> list[float]:
    """Return a valid production-profile vector."""
    return [float(index) for index in range(128)]


def test_profile_is_code_owned_and_versioned() -> None:
    """The production profile resolves only through the bundled registry."""
    assert profile_by_id("production-v1") is PRODUCTION_PROFILE
    with pytest.raises(ValueError, match="unknown release"):
        profile_by_id("airflow-user-value")


def test_payload_is_complete_and_nulls_omitted_optional_fields() -> None:
    """An UPSERT produces a complete post-image under the release schema."""
    payload = normalize_profile_payload(
        PRODUCTION_PROFILE,
        vectors={"vector": valid_vector()},
        texts={"text": "hello"},
        metadata={},
        scalars={"event_timestamp": datetime(2026, 7, 14, tzinfo=UTC)},
    )
    assert tuple(payload) == PRODUCTION_PROFILE.payload_fields
    assert payload["text"] == "hello"
    assert payload["cluster"] is None
    assert payload["category"] is None
    assert payload["ttl"] is None


@pytest.mark.parametrize("kind", ["vectors", "texts", "metadata", "scalars"])
def test_unknown_payload_fields_block(kind: str) -> None:
    """Arbitrary source map keys cannot grow the target schema.

    Args:
        kind: Payload category receiving the unknown field.
    """
    values: dict[str, object] = {
        "vectors": {"vector": valid_vector()},
        "texts": {},
        "metadata": {},
        "scalars": {},
    }
    values[kind] = {**values[kind], "surprise": "value"}
    with pytest.raises(ValueError, match="outside profile"):
        normalize_profile_payload(
            PRODUCTION_PROFILE,
            vectors=values["vectors"],
            texts=values["texts"],
            metadata=values["metadata"],
            scalars=values["scalars"],
        )


def test_invalid_vector_blocks_instead_of_becoming_null() -> None:
    """Dimension mismatch and non-finite values fail before a Lance write."""
    with pytest.raises(ValueError, match="dimension"):
        normalize_profile_payload(PRODUCTION_PROFILE, {"vector": [1.0]}, {}, {}, {})
    invalid = valid_vector()
    invalid[0] = float("inf")
    with pytest.raises(ValueError, match="non-finite"):
        normalize_profile_payload(PRODUCTION_PROFILE, {"vector": invalid}, {}, {}, {})


def test_required_vector_cannot_be_omitted() -> None:
    """Production upserts require every profile-owned vector."""
    with pytest.raises(ValueError, match="missing required"):
        normalize_profile_payload(PRODUCTION_PROFILE, {}, {}, {}, {})


def test_tombstone_clears_every_profile_field() -> None:
    """Tombstones retain no user payload values."""
    payload = tombstone_payload(PRODUCTION_PROFILE)
    assert tuple(payload) == PRODUCTION_PROFILE.payload_fields
    assert all(value is None for value in payload.values())

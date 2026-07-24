"""Canonical binary encodings for replay-safe source and event identities."""

from __future__ import annotations

import hashlib
import math
import struct
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

EVENT_DIGEST_HEADER: bytes = b"lance-etl-event-digest-v1\0"
SOURCE_DIGEST_HEADER: bytes = b"lance-etl-source-digest-v1\0"


def encode_length(size: int) -> bytes:
    """Encode a non-negative collection or byte length as unsigned big-endian.

    Args:
        size: Length to encode.

    Returns:
        Eight-byte unsigned big-endian representation.

    Raises:
        ValueError: If the size is negative or exceeds unsigned 64-bit range.
    """
    if size < 0 or size >= 1 << 64:
        raise ValueError(f"length is outside unsigned 64-bit range: {size}")
    return struct.pack(">Q", size)


def encode_bytes(value: bytes) -> bytes:
    """Encode a byte string with an unambiguous length prefix.

    Args:
        value: Bytes to encode.

    Returns:
        Length-prefixed byte string.
    """
    return encode_length(len(value)) + value


def encode_text(value: str) -> bytes:
    """Encode UTF-8 text with an unambiguous length prefix.

    Args:
        value: Text to encode.

    Returns:
        Length-prefixed UTF-8 bytes.
    """
    return encode_bytes(value.encode("utf-8"))


def encode_timestamp(value: datetime) -> bytes:
    """Encode an aware timestamp as signed UTC epoch microseconds.

    Args:
        value: Timezone-aware timestamp.

    Returns:
        Eight-byte signed epoch-microsecond representation.

    Raises:
        ValueError: If the timestamp is naive or outside signed 64-bit range.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = value.astimezone(UTC) - epoch
    micros = delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
    if micros < -(1 << 63) or micros >= 1 << 63:
        raise ValueError("timestamp is outside signed 64-bit microsecond range")
    return struct.pack(">q", micros)


def encode_mapping(value: Mapping[str, Any]) -> bytes:
    """Encode a string-keyed mapping in unsigned UTF-8 key order.

    Args:
        value: Mapping to encode.

    Returns:
        Canonical mapping bytes.

    Raises:
        TypeError: If a mapping key is not a string.
    """
    entries: list[tuple[bytes, Any]] = []
    for key, item in value.items():
        if not isinstance(key, str):
            raise TypeError(f"payload mapping keys must be strings, got {type(key).__name__}")
        entries.append((key.encode("utf-8"), item))
    entries.sort(key=lambda pair: pair[0])
    encoded = bytearray(b"m" + encode_length(len(entries)))
    for key_bytes, item in entries:
        encoded.extend(encode_bytes(key_bytes))
        encoded.extend(encode_value(item))
    return bytes(encoded)


def encode_sequence(value: Sequence[Any]) -> bytes:
    """Encode an ordered sequence without changing element order.

    Args:
        value: Sequence to encode.

    Returns:
        Canonical sequence bytes.
    """
    encoded = bytearray(b"l" + encode_length(len(value)))
    for item in value:
        encoded.extend(encode_value(item))
    return bytes(encoded)


def encode_value(value: Any) -> bytes:
    """Encode a supported scalar or nested payload value canonically.

    Args:
        value: Value to encode.

    Returns:
        Tagged canonical bytes.

    Raises:
        OverflowError: If an integer exceeds signed 64-bit range.
        TypeError: If the value type is unsupported.
        ValueError: If a float is non-finite or a timestamp is naive.
    """
    if value is None:
        return b"n"
    if isinstance(value, bool):
        return b"b\x01" if value else b"b\x00"
    if isinstance(value, int):
        if value < -(1 << 63) or value >= 1 << 63:
            raise OverflowError(f"integer is outside signed 64-bit range: {value}")
        return b"i" + struct.pack(">q", value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite floating-point values are not allowed")
        return b"f" + struct.pack(">d", value)
    if isinstance(value, str):
        return b"s" + encode_text(value)
    if isinstance(value, bytes):
        return b"y" + encode_bytes(value)
    if isinstance(value, datetime):
        return b"t" + encode_timestamp(value)
    if isinstance(value, Mapping):
        return encode_mapping(value)
    if isinstance(value, Sequence):
        return encode_sequence(value)
    raise TypeError(f"unsupported canonical payload type: {type(value).__name__}")


def canonical_event_digest(
    routing: tuple[str, str, str],
    record_id: str,
    operation: str,
    ts: datetime,
    payload: Mapping[str, Any],
) -> bytes:
    """Compute the frozen digest for one immutable business mutation.

    Delivery metadata such as Iceberg sequence, snapshot, file, and work identity is intentionally
    absent so an exact redelivery has the same digest.

    Args:
        routing: Tenant, namespace, and organization identity.
        record_id: Logical record identity.
        operation: Normalized mutation operation.
        ts: Query and retention timestamp.
        payload: Complete normalized mutation payload.

    Returns:
        Raw 32-byte SHA-256 digest.
    """
    encoded = bytearray(EVENT_DIGEST_HEADER)
    for component in routing:
        encoded.extend(encode_text(component))
    encoded.extend(encode_text(record_id))
    encoded.extend(encode_text(operation))
    encoded.extend(encode_timestamp(ts))
    encoded.extend(encode_mapping(payload))
    return hashlib.sha256(encoded).digest()


def canonical_source_digest(rows: Iterable[tuple[str, int, bytes]]) -> bytes:
    """Compute a partition-independent digest over terminal target mutations.

    Args:
        rows: Record ID, Iceberg source sequence, and 32-byte event digest tuples.

    Returns:
        Raw 32-byte SHA-256 digest.

    Raises:
        ValueError: If a record id is duplicated or a source sequence or digest is invalid.
    """
    ordered: list[tuple[bytes, int, bytes]] = []
    seen_record_ids: set[str] = set()
    for record_id, source_sequence, event_digest in rows:
        if record_id in seen_record_ids:
            raise ValueError(f"source digest contains duplicate record id {record_id!r}")
        seen_record_ids.add(record_id)
        if source_sequence < 0 or source_sequence >= 1 << 63:
            raise ValueError(f"source sequence is outside non-negative signed 64-bit range: {source_sequence}")
        if len(event_digest) != 32:
            raise ValueError(f"event digest must contain 32 bytes, got {len(event_digest)}")
        ordered.append((record_id.encode("utf-8"), source_sequence, event_digest))
    ordered.sort(key=lambda item: item[0])
    encoded = bytearray(SOURCE_DIGEST_HEADER)
    for vector_bytes, source_sequence, event_digest in ordered:
        encoded.extend(encode_bytes(vector_bytes))
        encoded.extend(struct.pack(">q", source_sequence))
        encoded.extend(event_digest)
    return hashlib.sha256(encoded).digest()

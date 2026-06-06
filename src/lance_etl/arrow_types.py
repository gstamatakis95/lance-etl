"""Resolve Arrow type specifications from strings for the column cast map.

Supports the common scalar types plus ``timestamp``, ``list<...>``, and ``fixed_size_list<..., size>`` so cast targets
can be supplied on the command line. Richer or deeply nested targets should be built as ``pyarrow`` types and passed to
the job programmatically.
"""

from __future__ import annotations

import pyarrow as pa

SCALAR_TYPES: dict[str, object] = {
    "bool": pa.bool_,
    "boolean": pa.bool_,
    "int8": pa.int8,
    "int16": pa.int16,
    "int32": pa.int32,
    "int64": pa.int64,
    "uint8": pa.uint8,
    "uint16": pa.uint16,
    "uint32": pa.uint32,
    "uint64": pa.uint64,
    "float16": pa.float16,
    "halffloat": pa.float16,
    "float": pa.float32,
    "float32": pa.float32,
    "double": pa.float64,
    "float64": pa.float64,
    "string": pa.string,
    "utf8": pa.string,
    "large_string": pa.large_string,
    "binary": pa.binary,
    "large_binary": pa.large_binary,
    "date32": pa.date32,
    "date64": pa.date64,
}


def resolve_arrow_type(spec: str) -> pa.DataType:
    """Resolve a single Arrow type from its string specification.

    Args:
        spec: A type spec such as ``"float16"``, ``"timestamp[us, UTC]"``, ``"list<float16>"``, or
            ``"fixed_size_list<float16, 768>"``.

    Returns:
        The corresponding Arrow data type.

    Raises:
        ValueError: If the specification is not recognised.
    """
    text: str = spec.strip()
    lowered: str = text.lower()

    if lowered in SCALAR_TYPES:
        return SCALAR_TYPES[lowered]()

    if lowered.startswith("timestamp"):
        inside: str = lowered[len("timestamp") :].strip().strip("[]")
        parts: list[str] = [p.strip() for p in inside.split(",")] if inside else ["us"]
        unit: str = parts[0] or "us"
        tz: object = parts[1] if len(parts) > 1 and parts[1] else None
        return pa.timestamp(unit, tz=tz)

    if lowered.startswith("list<") and text.endswith(">"):
        inner: str = text[text.index("<") + 1 : -1]
        return pa.list_(resolve_arrow_type(inner))

    if lowered.startswith("fixed_size_list<") and text.endswith(">"):
        body: str = text[text.index("<") + 1 : -1]
        inner_spec, size = body.rsplit(",", 1)
        return pa.list_(resolve_arrow_type(inner_spec.strip()), int(size.strip()))

    raise ValueError(f"unsupported type spec: {spec!r}")


def resolve_type_map(specs: dict[str, str]) -> dict[str, pa.DataType]:
    """Resolve a map of column name to type specification.

    Args:
        specs: Mapping of column name to Arrow type specification string.

    Returns:
        Mapping of column name to resolved Arrow data type.
    """
    return {name: resolve_arrow_type(spec) for name, spec in specs.items()}

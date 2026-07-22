"""Lightweight validation for dataset routing path segments."""

from __future__ import annotations

import re

ROUTING_SEGMENT_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
"""Bounded routing-segment contract shared by ETL, the control plane, and search."""


def validate_routing_segment(value: str, field_name: str) -> str:
    """Validate one shared serving-route segment.

    Args:
        value: Candidate segment.
        field_name: Field name used in the error.

    Returns:
        Validated value unchanged.

    Raises:
        ValueError: If the value is not a bounded allowlisted segment.
    """
    if not isinstance(value, str) or ROUTING_SEGMENT_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must match [A-Za-z0-9_-]{{1,128}}")
    return value

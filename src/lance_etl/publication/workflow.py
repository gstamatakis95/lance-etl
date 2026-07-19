"""Exact-version serving-replica prewarm evidence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class PrewarmResult:
    """Exact resolution reported by one required serving replica."""

    replica: str
    lance_uri: str
    lance_version: int


def validate_prewarm(results: tuple[PrewarmResult, ...], candidate_lance_uri: str, indexed_lance_version: int) -> None:
    """Require every configured replica to confirm the same exact candidate.

    Args:
        results: Required replica resolutions.
        candidate_lance_uri: Expected URI.
        indexed_lance_version: Expected exact version.

    Raises:
        ValueError: If no replicas responded, names repeat, or any resolution differs.
    """
    if not results:
        raise ValueError("publication requires at least one serving replica prewarm")
    replicas: Any = [result.replica for result in results]
    if len(replicas) != len(set(replicas)):
        raise ValueError("prewarm results contain duplicate replicas")
    mismatched: Any = [
        result.replica
        for result in results
        if result.lance_uri != candidate_lance_uri or result.lance_version != indexed_lance_version
    ]
    if mismatched:
        raise ValueError(f"serving replicas resolved a different publication candidate: {sorted(mismatched)}")

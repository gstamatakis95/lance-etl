"""Current Lance dataset creation and open-error contracts."""

from __future__ import annotations

DATA_STORAGE_VERSION: str = "2.1"
"""Required Lance file format for newly materialized datasets."""

DATASET_NOT_FOUND_MARKER: str = "was not found"
"""Stable pylance marker for a genuinely absent dataset."""


def dataset_absent(error: BaseException) -> bool:
    """Return whether an open error proves that a dataset does not exist.

    Args:
        error: Exception raised while opening a Lance dataset.

    Returns:
        Whether the exception is an unambiguous missing-dataset error.
    """
    if isinstance(error, FileNotFoundError):
        return True
    return isinstance(error, ValueError) and DATASET_NOT_FOUND_MARKER in str(error)

"""Parsers for the TexMex ``.fvecs`` / ``.ivecs`` vector file formats.

Each record is ``[int32 dimension (little-endian)][dimension x float32 or int32]``, so a well-formed file's size is an
exact multiple of ``(dimension + 1) * 4`` bytes. The readers validate that invariant, check every record header against
the leading dimension, and support reading arbitrary row slices so Spark executors can each load their own range.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np

HEADER_BYTES: int = 4
WORD_BYTES: int = 4


def vecs_dimension(path: Path | str) -> int:
    """Read the vector dimension from a vecs file's first record header.

    Args:
        path: The fvecs or ivecs file.

    Returns:
        The per-vector dimension.

    Raises:
        ValueError: If the file is too short to carry a header or the dimension is not positive.
    """
    with open(path, "rb") as handle:
        header: bytes = handle.read(HEADER_BYTES)
    if len(header) < HEADER_BYTES:
        raise ValueError(f"{path}: file too short to contain a vecs header")
    dimension: int = int(np.frombuffer(header, dtype="<i4")[0])
    if dimension <= 0:
        raise ValueError(f"{path}: invalid vecs dimension {dimension}")
    return dimension


def vecs_count(path: Path | str) -> int:
    """Return the number of vectors in a vecs file, validating its layout.

    Args:
        path: The fvecs or ivecs file.

    Returns:
        The vector count.

    Raises:
        ValueError: If the file size is not a multiple of the record stride.
    """
    dimension: int = vecs_dimension(path)
    stride_bytes: int = (dimension + 1) * WORD_BYTES
    size: int = os.path.getsize(path)
    if size % stride_bytes != 0:
        raise ValueError(f"{path}: size {size} is not a multiple of record stride {stride_bytes} (dim {dimension})")
    return size // stride_bytes


def read_vecs_rows(path: Path | str, start: int, count: int, dtype: str) -> np.ndarray:
    """Read a contiguous row slice from a vecs file.

    Args:
        path: The fvecs or ivecs file.
        start: First row to read.
        count: Number of rows to read.
        dtype: Element view, ``"<f4"`` for fvecs or ``"<i4"`` for ivecs.

    Returns:
        A ``(count, dimension)`` array of the requested element type.

    Raises:
        ValueError: If the slice extends past the file or any record header disagrees with the leading dimension.
    """
    dimension: int = vecs_dimension(path)
    total: int = vecs_count(path)
    if start < 0 or count < 0 or start + count > total:
        raise ValueError(f"{path}: slice [{start}, {start + count}) out of range for {total} rows")
    stride: int = dimension + 1
    raw: np.ndarray = np.fromfile(path, dtype="<i4", count=count * stride, offset=start * stride * WORD_BYTES)
    if raw.size != count * stride:
        raise ValueError(f"{path}: short read for slice [{start}, {start + count})")
    records: np.ndarray = raw.reshape(count, stride)
    if count and not np.all(records[:, 0] == dimension):
        raise ValueError(f"{path}: inconsistent record headers; expected dimension {dimension}")
    body: np.ndarray = np.ascontiguousarray(records[:, 1:])
    return body.view(dtype)


def read_fvecs(path: Path | str, limit: int | None = None) -> np.ndarray:
    """Read float vectors from an fvecs file.

    Args:
        path: The fvecs file.
        limit: Optional cap on rows read from the start of the file.

    Returns:
        A float32 ``(rows, dimension)`` array.
    """
    total: int = vecs_count(path)
    rows: int = total if limit is None else min(limit, total)
    return read_vecs_rows(path, 0, rows, "<f4")


def read_ivecs(path: Path | str, limit: int | None = None) -> np.ndarray:
    """Read integer vectors from an ivecs file.

    Args:
        path: The ivecs file.
        limit: Optional cap on rows read from the start of the file.

    Returns:
        An int32 ``(rows, dimension)`` array.
    """
    total: int = vecs_count(path)
    rows: int = total if limit is None else min(limit, total)
    return read_vecs_rows(path, 0, rows, "<i4")

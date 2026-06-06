"""Unit tests for the bench fvecs/ivecs parsers against tiny in-test fixtures."""

from __future__ import annotations

import struct
from pathlib import Path

import numpy as np
import pytest

from bench.fvecs import read_fvecs, read_ivecs, read_vecs_rows, vecs_count, vecs_dimension


def write_fvecs(path: Path, vectors: list[list[float]]) -> None:
    """Write vectors in fvecs layout: per record an int32 dim then dim float32 values.

    Args:
        path: Destination file.
        vectors: The float vectors.
    """
    with open(path, "wb") as handle:
        for vector in vectors:
            handle.write(struct.pack("<i", len(vector)))
            handle.write(struct.pack(f"<{len(vector)}f", *vector))


def write_ivecs(path: Path, vectors: list[list[int]]) -> None:
    """Write vectors in ivecs layout: per record an int32 dim then dim int32 values.

    Args:
        path: Destination file.
        vectors: The integer vectors.
    """
    with open(path, "wb") as handle:
        for vector in vectors:
            handle.write(struct.pack("<i", len(vector)))
            handle.write(struct.pack(f"<{len(vector)}i", *vector))


class TestFvecsParsing:
    """read_fvecs parses the layout exactly."""

    def test_round_trip(self, tmp_path: Path) -> None:
        """Vectors written in fvecs layout read back bit-exact as float32."""
        vectors: list[list[float]] = [[1.5, -2.0, 0.25, 8.0], [0.0, 3.5, -1.0, 2.0], [9.0, 9.5, -9.0, 0.5]]
        path: Path = tmp_path / "tiny.fvecs"
        write_fvecs(path, vectors)
        parsed: np.ndarray = read_fvecs(path)
        assert parsed.dtype == np.float32
        assert parsed.shape == (3, 4)
        np.testing.assert_array_equal(parsed, np.asarray(vectors, dtype=np.float32))

    def test_limit(self, tmp_path: Path) -> None:
        """The limit caps rows read from the start of the file."""
        path: Path = tmp_path / "tiny.fvecs"
        write_fvecs(path, [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
        parsed: np.ndarray = read_fvecs(path, limit=2)
        np.testing.assert_array_equal(parsed, np.asarray([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32))

    def test_dimension_and_count(self, tmp_path: Path) -> None:
        """Header inspection reports the dimension and validated record count."""
        path: Path = tmp_path / "tiny.fvecs"
        write_fvecs(path, [[1.0] * 8] * 5)
        assert vecs_dimension(path) == 8
        assert vecs_count(path) == 5

    def test_row_slice(self, tmp_path: Path) -> None:
        """Arbitrary row slices read the correct records."""
        path: Path = tmp_path / "tiny.fvecs"
        write_fvecs(path, [[float(i), float(i + 1)] for i in range(6)])
        parsed: np.ndarray = read_vecs_rows(path, 2, 3, "<f4")
        np.testing.assert_array_equal(parsed[:, 0], np.asarray([2.0, 3.0, 4.0], dtype=np.float32))

    def test_truncated_file_rejected(self, tmp_path: Path) -> None:
        """A file whose size is not a multiple of the record stride raises."""
        path: Path = tmp_path / "broken.fvecs"
        write_fvecs(path, [[1.0, 2.0, 3.0]])
        with open(path, "ab") as handle:
            handle.write(b"\x00")
        with pytest.raises(ValueError, match="not a multiple"):
            vecs_count(path)

    def test_inconsistent_record_header_rejected(self, tmp_path: Path) -> None:
        """A record header that disagrees with the leading dimension raises."""
        path: Path = tmp_path / "mixed.fvecs"
        with open(path, "wb") as handle:
            handle.write(struct.pack("<i", 2) + struct.pack("<2f", 1.0, 2.0))
            handle.write(struct.pack("<i", 7) + struct.pack("<2f", 3.0, 4.0))
        with pytest.raises(ValueError, match="inconsistent record headers"):
            read_fvecs(path)

    def test_out_of_range_slice_rejected(self, tmp_path: Path) -> None:
        """A slice extending past the file raises."""
        path: Path = tmp_path / "tiny.fvecs"
        write_fvecs(path, [[1.0, 2.0]])
        with pytest.raises(ValueError, match="out of range"):
            read_vecs_rows(path, 0, 2, "<f4")


class TestIvecsParsing:
    """read_ivecs parses the integer layout exactly."""

    def test_round_trip(self, tmp_path: Path) -> None:
        """Vectors written in ivecs layout read back as int32."""
        vectors: list[list[int]] = [[7, 1, 4], [0, 2, 9]]
        path: Path = tmp_path / "tiny.ivecs"
        write_ivecs(path, vectors)
        parsed: np.ndarray = read_ivecs(path)
        assert parsed.dtype == np.int32
        np.testing.assert_array_equal(parsed, np.asarray(vectors, dtype=np.int32))

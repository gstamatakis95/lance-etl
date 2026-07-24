"""Tests for current executor-side Arrow normalization."""

from __future__ import annotations

import pyarrow as pa
import pytest

from lance_etl.etl.arrow import apply_fsl_cast


@pytest.mark.parametrize("dimension", [None, 0])
def test_vector_cast_rejects_zero_dimension(dimension: int | None) -> None:
    """Empty vectors cannot establish an unusable fixed-size-list schema.

    Args:
        dimension: Inferred or explicit invalid dimension.
    """
    table: pa.Table = pa.table({"vector": pa.array([[]], type=pa.list_(pa.float32()))})
    with pytest.raises(ValueError, match="dimension must be positive"):
        apply_fsl_cast(table, "vector", {}, dimension)


def test_vector_cast_nulls_wrong_dimensions_and_counts_them() -> None:
    """Wrong-width vectors become null while valid vectors use the fixed-size-list type."""
    table: pa.Table = pa.table({"vector": pa.array([[1.0, 2.0], [3.0], None], type=pa.list_(pa.float32()))})
    counts: dict[str, int] = {}

    result: pa.Table = apply_fsl_cast(table, "vector", counts, 2)

    assert result.schema.field("vector").type == pa.list_(pa.float32(), 2)
    assert result["vector"].to_pylist() == [[1.0, 2.0], None, None]
    assert counts == {"vector": 1}


def test_vector_cast_handles_chunked_mixed_width_input_without_variable_to_fixed_cast() -> None:
    """Chunked mixed-width vectors normalize through bounded slicing without unsafe Arrow take kernels."""
    vectors: pa.ChunkedArray = pa.chunked_array(
        [
            pa.array([[1.0, 2.0], [], None], type=pa.list_(pa.float64())),
            pa.array([[3.0], [4.0, 5.0], [6.0, 7.0, 8.0]], type=pa.list_(pa.float64())),
        ]
    )
    table: pa.Table = pa.table({"vector": vectors})
    counts: dict[str, int] = {}

    result: pa.Table = apply_fsl_cast(table, "vector", counts, 2)

    assert result.schema.field("vector").type == pa.list_(pa.float32(), 2)
    assert result["vector"].to_pylist() == [[1.0, 2.0], None, None, None, [4.0, 5.0], None]
    assert counts == {"vector": 3}

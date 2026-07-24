"""PyArrow normalization helpers used by reconciler executor closures."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc


def apply_fsl_cast(
    table: pa.Table,
    column_name: str,
    invalid_counts: dict[str, int],
    dimension: int | None = None,
) -> pa.Table:
    """Cast a vector column to ``fixed_size_list<float32, dimension>``.

    Args:
        table: Table containing the vector column.
        column_name: Vector column name.
        invalid_counts: Mutable wrong-dimension counter by column.
        dimension: Required dimension, or ``None`` to infer the first non-null value.

    Returns:
        The table with the normalized vector column. A fully null inferred column is unchanged.

    Raises:
        ValueError: If an explicit or inferred vector dimension is not positive.
    """
    if dimension is not None and dimension < 1:
        raise ValueError(f"vector dimension must be positive, got {dimension}")
    column: pa.ChunkedArray = table.column(column_name)
    lengths: pa.ChunkedArray = pc.list_value_length(column)
    if dimension is None:
        observed: pa.ChunkedArray = lengths.drop_null()
        if len(observed) == 0:
            return table
        dimension = int(observed[0].as_py())
        if dimension < 1:
            raise ValueError(f"vector dimension must be positive, got {dimension}")
    matches: pa.ChunkedArray = pc.equal(lengths, dimension)
    mismatches: int = int(pc.sum(pc.invert(matches)).as_py() or 0)
    if mismatches:
        invalid_counts[column_name] = invalid_counts.get(column_name, 0) + mismatches
    target_type: pa.FixedSizeListType = pa.list_(pa.float32(), dimension)
    sliced: pa.ChunkedArray = pc.list_slice(column, 0, dimension, return_fixed_size_list=True)
    normalized: pa.ChunkedArray = pc.cast(sliced, target_type)
    valid_matches: pa.ChunkedArray = pc.fill_null(matches, False)
    normalized = pc.if_else(valid_matches, normalized, pa.scalar(None, target_type))
    index: int = table.schema.get_field_index(column_name)
    return table.set_column(index, column_name, normalized)

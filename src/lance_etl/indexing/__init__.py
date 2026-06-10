"""Indexing job package: distributed vector, scalar, and full-text index builds for Lance datasets.

Re-exports the public API so callers can write
``from lance_etl.indexing import LanceIndexer, IndexJobConfig``.
"""

from __future__ import annotations

from lance_etl.indexing.config import (
    FTS_OPTIONAL_PARAMS,
    METRIC_TO_DISTANCE,
    IndexJobConfig,
    bitmap_index_name,
    config_reusable,
    degrade_num_partitions,
    derive_num_partitions,
    fts_index_name,
    memory_bounded_num_partitions,
    scalar_index_name,
    vector_config_key,
    vector_index_name,
)
from lance_etl.indexing.handlers import (
    BitmapIndexHandler,
    BTreeIndexHandler,
    FtsIndexHandler,
    IndexHandler,
    VectorIndexHandler,
)
from lance_etl.indexing.optimize import (
    drop_existing_index,
    index_delta_count,
    load_vector_config,
    maintain_index_locally,
    merge_index_deltas,
    optimize_existing_index,
    write_vector_config,
)
from lance_etl.indexing.runner import (
    LanceIndexer,
    index_dataset_locally,
    index_skip_reason,
)
from lance_etl.indexing.segments import (
    STALE_FRAGMENT_MARKERS,
    TRAIN_SEMAPHORE,
    all_fragment_ids,
    build_and_commit_segments,
    build_scalar_segment,
    build_vector_segment,
    centroids_from_ipc,
    centroids_to_ipc,
    commit_index_with_retries,
    commit_segments,
    deserialize_segment,
    is_stale_fragment_error,
    lance_field_id,
    live_fragment_ids,
    serialize_segment,
    split_evenly,
)

__all__ = [
    "FTS_OPTIONAL_PARAMS",
    "METRIC_TO_DISTANCE",
    "STALE_FRAGMENT_MARKERS",
    "TRAIN_SEMAPHORE",
    "IndexJobConfig",
    "IndexHandler",
    "VectorIndexHandler",
    "BTreeIndexHandler",
    "BitmapIndexHandler",
    "FtsIndexHandler",
    "LanceIndexer",
    "bitmap_index_name",
    "config_reusable",
    "degrade_num_partitions",
    "derive_num_partitions",
    "fts_index_name",
    "memory_bounded_num_partitions",
    "scalar_index_name",
    "vector_config_key",
    "vector_index_name",
    "drop_existing_index",
    "index_delta_count",
    "load_vector_config",
    "maintain_index_locally",
    "merge_index_deltas",
    "optimize_existing_index",
    "write_vector_config",
    "index_dataset_locally",
    "index_skip_reason",
    "all_fragment_ids",
    "build_and_commit_segments",
    "build_scalar_segment",
    "build_vector_segment",
    "centroids_from_ipc",
    "centroids_to_ipc",
    "commit_index_with_retries",
    "commit_segments",
    "deserialize_segment",
    "is_stale_fragment_error",
    "lance_field_id",
    "live_fragment_ids",
    "serialize_segment",
    "split_evenly",
]

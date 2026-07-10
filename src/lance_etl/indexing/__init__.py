"""Indexing job package: distributed vector, scalar, and full-text index builds for Lance datasets.

Re-exports the consumer API so callers can write
``from lance_etl.indexing import LanceIndexer, IndexJobConfig``.
"""

from __future__ import annotations

from lance_etl.indexing.config import (
    IndexJobConfig,
    bitmap_index_name,
    degrade_num_partitions,
    derive_num_partitions,
    fts_index_name,
    scalar_index_name,
    vector_config_key,
    vector_index_name,
    zonemap_index_name,
)
from lance_etl.indexing.handlers import (
    BitmapIndexHandler,
    BTreeIndexHandler,
    FtsIndexHandler,
    IndexHandler,
    VectorIndexHandler,
    ZonemapIndexHandler,
    publish_fts_index,
)
from lance_etl.indexing.optimize import (
    centroid_sidecar_uri,
    index_delta_count,
    load_centroids,
    load_vector_config,
    merge_index_deltas,
    optimize_existing_index,
    write_vector_config,
)
from lance_etl.indexing.runner import (
    LanceIndexer,
    bootstrap_vector_index,
    build_one_shard,
    commit_one_index,
    merge_deltas_if_needed,
    plan_dataset_indexes,
    resolve_index_targets,
    shard_count,
)
from lance_etl.indexing.segments import (
    commit_segments,
    is_stale_fragment_error,
    lance_field_id,
    serialize_segment,
    split_evenly,
)

__all__ = [
    "IndexJobConfig",
    "IndexHandler",
    "VectorIndexHandler",
    "BTreeIndexHandler",
    "BitmapIndexHandler",
    "ZonemapIndexHandler",
    "FtsIndexHandler",
    "LanceIndexer",
    "bitmap_index_name",
    "degrade_num_partitions",
    "derive_num_partitions",
    "fts_index_name",
    "scalar_index_name",
    "vector_config_key",
    "vector_index_name",
    "zonemap_index_name",
    "centroid_sidecar_uri",
    "index_delta_count",
    "load_centroids",
    "load_vector_config",
    "merge_index_deltas",
    "optimize_existing_index",
    "write_vector_config",
    "bootstrap_vector_index",
    "build_one_shard",
    "commit_one_index",
    "merge_deltas_if_needed",
    "plan_dataset_indexes",
    "resolve_index_targets",
    "publish_fts_index",
    "shard_count",
    "commit_segments",
    "is_stale_fragment_error",
    "lance_field_id",
    "serialize_segment",
    "split_evenly",
]

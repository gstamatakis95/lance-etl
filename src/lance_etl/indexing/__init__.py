"""Indexing job package: distributed vector, scalar, and full-text index builds for Lance datasets.

Re-exports the consumer API so callers can write
``from lance_etl.indexing import LanceIndexer, IndexJobConfig``.
"""

from __future__ import annotations

from lance_etl.indexing.config import (
    IndexJobConfig as IndexJobConfig,
)
from lance_etl.indexing.config import (
    bitmap_index_name as bitmap_index_name,
)
from lance_etl.indexing.config import (
    degrade_num_partitions as degrade_num_partitions,
)
from lance_etl.indexing.config import (
    derive_num_partitions as derive_num_partitions,
)
from lance_etl.indexing.config import (
    fts_index_name as fts_index_name,
)
from lance_etl.indexing.config import (
    scalar_index_name as scalar_index_name,
)
from lance_etl.indexing.config import (
    vector_config_key as vector_config_key,
)
from lance_etl.indexing.config import (
    vector_index_name as vector_index_name,
)
from lance_etl.indexing.config import (
    zonemap_index_name as zonemap_index_name,
)
from lance_etl.indexing.handlers import (
    BitmapIndexHandler as BitmapIndexHandler,
)
from lance_etl.indexing.handlers import (
    BTreeIndexHandler as BTreeIndexHandler,
)
from lance_etl.indexing.handlers import (
    FtsIndexHandler as FtsIndexHandler,
)
from lance_etl.indexing.handlers import (
    IndexHandler as IndexHandler,
)
from lance_etl.indexing.handlers import (
    VectorIndexHandler as VectorIndexHandler,
)
from lance_etl.indexing.handlers import (
    ZonemapIndexHandler as ZonemapIndexHandler,
)
from lance_etl.indexing.handlers import (
    publish_fts_index as publish_fts_index,
)
from lance_etl.indexing.optimize import (
    centroid_sidecar_uri as centroid_sidecar_uri,
)
from lance_etl.indexing.optimize import (
    index_delta_count as index_delta_count,
)
from lance_etl.indexing.optimize import (
    load_centroids as load_centroids,
)
from lance_etl.indexing.optimize import (
    load_vector_config as load_vector_config,
)
from lance_etl.indexing.optimize import (
    merge_index_deltas as merge_index_deltas,
)
from lance_etl.indexing.optimize import (
    optimize_existing_index as optimize_existing_index,
)
from lance_etl.indexing.optimize import (
    write_vector_config as write_vector_config,
)
from lance_etl.indexing.runner import (
    LanceIndexer as LanceIndexer,
)
from lance_etl.indexing.runner import (
    bootstrap_vector_index as bootstrap_vector_index,
)
from lance_etl.indexing.runner import (
    build_one_shard as build_one_shard,
)
from lance_etl.indexing.runner import (
    commit_one_index as commit_one_index,
)
from lance_etl.indexing.runner import (
    plan_dataset_indexes as plan_dataset_indexes,
)
from lance_etl.indexing.runner import (
    resolve_index_targets as resolve_index_targets,
)
from lance_etl.indexing.runner import (
    shard_count as shard_count,
)
from lance_etl.indexing.segments import (
    commit_segments as commit_segments,
)
from lance_etl.indexing.segments import (
    is_stale_fragment_error as is_stale_fragment_error,
)
from lance_etl.indexing.segments import (
    lance_field_id as lance_field_id,
)
from lance_etl.indexing.segments import (
    serialize_segment as serialize_segment,
)
from lance_etl.indexing.segments import (
    split_evenly as split_evenly,
)

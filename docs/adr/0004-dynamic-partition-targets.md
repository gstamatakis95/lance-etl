# 0004. Dynamic write-partition targets and duplicate semantics

Status: Accepted

## Context

Routing was hard-coded to `{base}/{org}/{tenant}/{namespace}.lance`. Callers need deeper or different layouts,
for example a date partition `{org}/{tenant}/{namespace}/{event_date}.lance` derived from a `processing_timestamp`
column, passed as configuration rather than baked in.

## Decision

A single `partition_cols` list (default `["org_id", "tenant_id", "namespace"]`) defines routing end to end:
`routing_cols`, the collapse window, the repartition, the Arrow group-by, and the dataset URI all derive from it.
`partition_derivations` adds computed columns via `--partition-derive NAME=SOURCE:FORMAT` (a strftime pattern
translated to Spark `date_format`), applied before collapse so derived columns are usable as partition columns.
The legacy per-column rename flags were removed (breaking change, no compat shim).

## Consequences

`dataset_uri` builds arbitrary-depth paths with per-component validation against the path allowlist. Dataset
discovery for index and compact is depth-agnostic (recursive `*.lance` glob, excluding sidecars). Airflow
exposes the `partition_cols` knob as `lance_etl_partition_by`, defaulting to the legacy trio.

**Amendment ([0014](0014-drop-by-date-partitioning.md)):** The by-date partition target and the associated
`partition_derivations` / `--partition-derive` / strftime-to-Spark translation were removed. Each key now lives
in exactly one dataset, so the per-dataset `merge_insert` keyed on `key_col` is the sole dedup mechanism. The
`lance_etl_partition_derive` Airflow Variable and the `--partition-derive` CLI flag no longer exist. Generic
`partition_cols` path routing (for example the default `org_id/tenant_id/namespace` trio or any other stable
identity columns) is retained.

**Amendment (single-org DQ guard):** Because each dataset must contain rows for only its own routing key, the
`MaintenanceJob` now runs a cheap data-quality guard at the start of every maintenance pass. The guard calls
`dataset.count_rows(filter=predicate)` where the predicate is an OR over `"{col} != '{expected}'"` for each
routing column present as a stored column in the schema. The expected values are derived from the dataset URI
relative to `MaintenanceConfig.base_uri` using the same `partition_cols` ordering that built the path. A clean
dataset returns 0 and the guard costs at most a handful of metadata page reads. Lance prunes via zone-map and
page statistics on the named columns so the scan never touches the vector payload.

Pylance does not expose raw zone-map min/max from Python (`LanceFragment.metadata.to_json()` omits per-column
statistics), so a "read zone-map bounds directly" approach is not available. The pushdown count is the next
cheapest option: it is zone-map-accelerated and reads only the routing columns, not the vectors.

The guard emits a `dataset.org_contamination` metric (a distribution carrying the contaminating row count) when
contamination is found. The metric carries no org or tenant in its tags to keep cardinality low. When
`MaintenanceConfig.raise_on_contamination` is `True`, a `ContaminationError` is raised after logging. The guard
defaults on (`verify_single_org=True`) but requires `base_uri` to be set. When `base_uri` is `None` the guard is
silently skipped. When none of the routing columns exist as stored columns in the schema the guard is also skipped
with a warning.

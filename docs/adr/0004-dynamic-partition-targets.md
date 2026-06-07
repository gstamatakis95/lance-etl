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

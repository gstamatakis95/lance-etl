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
discovery for index and compact is depth-agnostic (recursive `*.lance` glob, excluding sidecars). Duplicate
semantics are explicit and allowed: a key whose partition value changes between runs leaves a stale copy in the
previously-routed dataset, and a delete only reaches the currently-routed dataset, so readers and the serving
layer dedup. The serving layer addresses this via [0006](0006-date-range-fanout-dedup.md) (dedup-keep-best
across date partitions). Airflow exposes the knobs as Variables defaulting to the legacy trio.

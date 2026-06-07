# 0020. Pivot named vectors and texts out of maps into concrete indexable columns

Status: Accepted

## Context

The intake `Record` contract carries a `vectors` map of named float arrays and a `texts` map of named text fields,
and the Iceberg source table mirrors that shape with `vectors MAP<STRING, ARRAY<FLOAT>>` and `texts MAP<STRING,
STRING>`. The ETL used to flatten every map column the same way, into positional `{col}_keys` / `{col}_values`
parallel list columns. That is fine for stored-only payload but it is the wrong shape for indexing. The IVF_RQ
vector index requires a `fixed_size_list<float32,dim>` column and the INVERTED full-text index requires a string
column. A flattened `vectors_values` list-of-lists is not a per-field indexable column, so a named vector or a named
text field could never be reached by an index. The dataset schema and the Record contract had drifted apart.

## Decision

`ETLConfig` gains `texts_col` (default `texts`), `vector_fields`, and `text_fields`. The ETL pivots the declared
keys out of their maps into concrete columns before write. Each name in `vector_fields` becomes a column holding
`vectors_col[name]`, cast to its `fixed_size_list` target by the existing `column_types` mechanism so the IVF_RQ
index can target it. Each name in `text_fields` becomes a string column holding `texts_col[name]` that the INVERTED
index can target. The pivot runs before collapse, the cast runs at merge time, so the order is pivot, collapse, cast,
merge. The `metadata` map keeps the old behavior: it stays stored-only payload, flattened to `metadata_keys` /
`metadata_values`. A declared key absent from a row yields NULL for that column. Undeclared keys are dropped with the
map. Every pivoted field name and map column name is validated against the existing `PATH_COMPONENT_PATTERN`
allowlist in `validate_schema`, and a pivoted name that collides with a routing, key, op, timestamp, window, or
metadata key/value column is rejected.

## Consequences

The written Lance dataset now aligns with the Record contract: a named vector is a concrete fixed-size-list column
and a named text field is a concrete string column, both index-eligible by name with no separate projection step.
Callers declare which keys to pivot through `--vector-field` / `--text-field` on the `etl` CLI or the `vector_fields`
/ `text_fields` config fields, and pair each vector field with a `--column-type` cast. The map columns themselves are
never written, consistent with Lance having no map type. This is a breaking change to the ETL input contract with no
compatibility shim. The bench source table emits the embedding under `vectors["vector"]` and the synthetic document
under `texts["text"]`, exercising the pivot end to end.

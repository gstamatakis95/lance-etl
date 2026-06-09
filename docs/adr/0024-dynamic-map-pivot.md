# 0024. Dynamic per-dataset map pivot: every key becomes a column

Status: Accepted — supersedes [0020](0020-map-pivot-to-concrete-columns.md) (static declared-key pivot)

## Context

ADR 0020 introduced the pivot of the `vectors` and `texts` map columns into concrete columns,
gated by the operator-declared `vector_fields` and `text_fields` lists on `ETLConfig`. Undeclared
keys were dropped silently and the `metadata` map was kept as positional `metadata_keys` /
`metadata_values` parallel arrays. That design required operators to enumerate every searchable
field at deploy time and meant metadata keys were never filter-eligible on the written datasets.

Two issues emerged in production. First, the static declaration was a deployment friction point:
adding a new embedding or text variant required a config change and a re-deploy before any data
for the new key would land in a concrete column. Second, the flattened `metadata_keys` /
`metadata_values` representation made per-key filtering impossible at the Lance layer, since a
predicate on `metadata["region"]` had no concrete column to reference.

The structural constraint is that a fleet of 30k+ orgs is power-law distributed: most orgs are
tiny and use a small subset of possible map keys, a few large orgs use many keys. A global
Spark-side pivot would produce a union of every key seen across all orgs and then broadcast that
wide schema to every dataset, polluting tiny-org datasets with columns they will never populate.

## Decision

Remove `vector_fields` and `text_fields` from `ETLConfig`. Every distinct key present in a
routing key group's data becomes a concrete column in that group's dataset, with the key as the
column name and the value as the column value. The pivot (`pivot_map_columns` in `etl.py`) runs
per-dataset on the executor, after the Spark shuffle collocates rows to their dataset, so the
schema of each Lance dataset contains only the keys that org actually uses.

Keys are used as column names as-is. Keys that would collide with an already-present column or a
reserved name (routing columns, key, op, timestamp, window) are silently skipped and counted.
The count is emitted as the `dataset.invalid_map_keys` metric with a WARNING log line naming the
dataset URI.

Vector columns extracted from the `vectors` map arrive as `list<float32>` (contract:
`ARRAY<FLOAT>`). The pivot infers the fixed-size-list dimension from the first non-null entry in
the column. If the inner type is float64 (Spark may widen `ARRAY<FLOAT>` in some environments),
it is normalized to float32 before the FSL cast. A vectors column whose every entry is null is
kept as a nullable `list<float32>` column with no FSL cast, since no dimension can be inferred
from an all-null column.

Text and metadata columns become `string` columns. Metadata keys are now first-class columns in
the dataset rather than positional array payloads, so a BTREE or BITMAP scalar index can cover any
metadata key that is present and has sufficient cardinality.

New keys appearing in a later ETL window are absorbed by the existing `add_columns` schema
evolution in `apply_merge`: the first batch containing a new key adds a nullable column to the
dataset and subsequent writes fill it. No operator intervention is required.

The `ts_col` default changes from `"timestamp"` to `"event_timestamp"` and the `window_column`
default changes from `"updated_at"` to `"processing_timestamp"`, aligning with the SQL contract
column names in `docs/iceberg-source-table.sql`.

The TTL column (`ttl`, `BIGINT` seconds per the SQL contract) is cast automatically to
`pa.duration("s")` by the ETL when present, via `ETLConfig.ttl_col` (default `"ttl"`). No
caller-supplied `column_types` entry is needed. The maintenance delete predicate
`event_timestamp + ttl < now` then evaluates natively.

## Simplification (post-acceptance amendment)

A subsequent simplification pass removed all string-spec type parsing from the ETL:

The `arrow_types.py` module (`resolve_arrow_type`, `resolve_type_map`) is deleted. The input
schema carries no type uncertainty: `docs/iceberg-source-table.sql` is the single typed contract.
Vectors are `MAP<STRING, ARRAY<FLOAT>>`, texts and metadata are `MAP<STRING, STRING>`, and ttl is
`BIGINT`. All casts are contract-driven and automatic. No caller-supplied type maps are accepted.

`ETLConfig.column_types` is removed. The FSL dimension is always inferred from data. The TTL cast
to `pa.duration("s")` is always applied when the column is present with an integer type.

`validate_schema` is strengthened to verify the full Iceberg/Spark schema against the contract:
required columns exist and carry their contracted Spark types (routing and key/op columns are
`StringType`, timestamp columns are `TimestampType` or `TimestampNTZType`), and optional columns
when present carry their contracted types (vectors map value is `ArrayType(FloatType|DoubleType)`,
texts/metadata maps are `MapType(StringType, StringType)`, ttl is `LongType` or `IntegerType`).
All violations are collected and reported together in one message referencing the SQL contract.

The per-dataset Lance schema is explicitly grow-only: no code path ever removes a dataset column.
Keys that stop appearing in the source leave their column in place with NULL values for new rows,
so existing readers and indexes are never broken.

## Consequences

Per-org schemas are minimal and heterogeneous: each org's dataset contains only the keys that org
uses, with no cross-org schema pollution. Operators no longer need to enumerate searchable fields
at deploy time: any key that arrives in the source data is automatically materialized as a column
in the relevant org's dataset.

Metadata keys are now real, filter-eligible, scalar-index-ready columns. A BTREE index on a
metadata key is possible where cardinality justifies it.

Keys that collide with an existing or reserved column are silently metered and skipped rather than
failing the job. This is the correct posture for a streaming pipeline: one bad key in one batch
should not stop all writes for that org.

The static `vector_fields` and `text_fields` fields are removed from `ETLConfig`. This is a
breaking change with no compatibility shim. Existing callers that set those fields must be updated.

`ETLConfig.column_types` is removed. This is a breaking change with no compatibility shim.
Existing callers that set it must be updated. The bench ingest config no longer passes a
`column_types` override: the FSL dimension is inferred automatically.

The `arrow_types.py` module is deleted. Any caller importing `resolve_arrow_type` or
`resolve_type_map` must be updated. The CLI wiring that previously used `resolve_type_map` to
parse `--column-type` argv is handled by a separate wiring pass.

The `validate_schema` method is strengthened from a minimal MapType presence check to a full
contract verification. Schema mismatches that previously went undetected at read time now raise
immediately with a clear message referencing the SQL contract.

The `materialize_maps` Spark-side method is removed. Map columns now ride through the Spark
shuffle intact and are not materialised until the executor processes each routing key's group.
This eliminates the Spark-level schema explosion that would have occurred had the pivot been done
globally before the shuffle.

# 0014. Drop by-date partitioning and cross-date fan-out

Status: Accepted

## Context

[0004](0004-dynamic-partition-targets.md) introduced `partition_cols` as a general routing mechanism and
`partition_derivations` as a way to derive extra partition columns from source columns via a Python strftime
pattern translated to Spark `date_format`. The canonical use was deriving an `event_date` column from a
`processing_timestamp` source so that each day's data landed in a separate dataset named
`{base}/{org}/{tenant}/{namespace}/{date}.lance`.

[0006](0006-date-range-fanout-dedup.md) added the serving-side counterpart: the `DatasetTarget` proto message
carried an optional `DateRange`, and a request spanning N days fanned out across N dataset URIs, ran the query
against each, and merged results with `domain/merge.rs` dedup-by-id keeping the best score (minimum distance for
vector, maximum relevance for FTS and hybrid), before truncating to k.

This design created sustained coupling complexity:

- The `PartitionDerivation` type, `strftime_to_spark_format` translator, and `--partition-derive` CLI flag had to
  stay in sync with Spark's `date_format` dialect. Any unsupported strftime directive silently produced wrong
  partition values unless caught by the translation function.
- `domain/merge.rs` (191 lines of dedup logic with `ScoreOrder` variants and a keyed HashMap pass) was the
  largest pure domain module. Its correctness depended on whether the caller wired `id_column` consistently.
- Each fan-out leg opened a separate dataset handle and performed a full query. N days meant N dataset opens,
  N index cache entries, N query executions, and an O(N * k) merge pass before truncation to k. The overhead
  scaled linearly with range width regardless of result density.
- The `SEARCH_API_FANOUT_CONCURRENCY` and `SEARCH_API_ID_COLUMN` env knobs controlled fan-out width and the
  dedup column name. Both were implicit contracts between the writer (ETL) and server config: a mismatch produced
  silently wrong deduplication.
- The gRPC proto carried a `DateRange` message with `start_date` and `end_date` string fields. Callers had to
  parse and validate YYYY-MM-DD strings. The server validated, built a day sequence, resolved each to a URI, and
  skipped missing days, meaning the 404 semantics were per-day, not per-request.
- The `Prewarm` and `Clusters` RPCs only accepted a single-day range, creating an asymmetry: multi-day was valid
  for search but not for maintenance operations.

By contrast, every deployment in practice uses the default `(org_id, tenant_id, namespace)` stable-identity
routing. No production pipeline routes on `event_date`. Time-bounded queries are handled by the client adding a
scalar filter on a timestamp column (for example `updated_at >= 2026-01-01`), which the typed `Filter` AST in
[0005](0005-rust-grpc-layering-typed-filter.md) handles correctly and efficiently through the Lance scanner
pushdown.

## Decision

Remove by-date partitioning entirely. Each search target resolves to exactly one dataset.

On the write side: delete `PartitionDerivation`, `strftime_to_spark_format`, `STRFTIME_TO_SPARK`, the
`partition_derivations` field from `ETLConfig`, and the `--partition-derive` CLI flag. Remove the
`lance_etl_partition_derive` Airflow Variable and the `lance_etl_window_column` Variable. Remove the
`parse_partition_derivations` helper from `cli.py`. The `validate_partition_spec` function drops derivation
validation. The `derive_partition_columns` ETL step is removed from the run pipeline.

On the serving side: delete `DateRange` from the proto and from `domain/target.rs`. Remove
`DatasetTarget.date_range`, `DatasetTarget.single_date()`, `parse_date`, `MAX_DATE_RANGE_DAYS`, and the
`DateRange` struct. Delete `domain/merge.rs` and all call sites in `lance/backend.rs`. The `LanceSearchBackend`
no longer performs any fan-out or cross-dataset dedup. Remove the `SEARCH_API_FANOUT_CONCURRENCY` and
`SEARCH_API_ID_COLUMN` env knobs.

Within-dataset RRF and Weighted hybrid fusion (in `domain/fusion.rs`) are kept unchanged. A hybrid search over
one dataset still runs vector and FTS legs and fuses them before returning top-k.

Date-range queries are now expressed by the calling layer as a scalar `Filter` on a timestamp column. That filter
is pushed down to the Lance scanner by `filter_to_expr` in [0005](0005-rust-grpc-layering-typed-filter.md).

## Consequences

`domain/merge.rs` is deleted. `domain/target.rs` drops `DateRange`, `parse_date`, `MAX_DATE_RANGE_DAYS`, and the
`single_date()` method. The proto field `DatasetTarget.date_range` is removed. All five Rust env knobs that
supported fan-out (`SEARCH_API_FANOUT_CONCURRENCY`, `SEARCH_API_ID_COLUMN`) are removed.

On the Python side, `PartitionDerivation`, `strftime_to_spark_format`, and `partition_derivations` are gone.
The CLI flags `--partition-derive`, `--window-column`, and `--ingested-at-col` are removed. The Airflow
Variables `lance_etl_partition_derive` and `lance_etl_window_column` are removed.

Existing callers that passed `date_range` in gRPC requests will receive a proto parsing error because the field
no longer exists. Migration path: remove `date_range` from the request and add an equivalent scalar filter on
the timestamp column. This is a breaking proto change and is intentional. The proto is pre-release and carries no
backward-compatibility guarantee.

[0006](0006-date-range-fanout-dedup.md) is superseded by this decision. [0004](0004-dynamic-partition-targets.md)
is amended: `partition_derivations` and by-date routing are gone, while generic `partition_cols` routing is
retained.

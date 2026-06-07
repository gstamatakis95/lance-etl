# 0021. gRPC event-time range on the vector, text, and hybrid search RPCs

Status: Accepted

## Context

Serving needs to answer "search only over events in this time window". [ADR 0016](0016-event-time-canonical-clock.md)
established the source event timestamp as the single canonical clock and noted that an event-time window is
naturally a scalar range filter on the event-timestamp column, pruned by a BTREE or zone-map on that column.
The search service had no way to express such a window. A caller could hand-build a `Between` predicate through
the typed filter AST from [ADR 0005](0005-rust-grpc-layering-typed-filter.md), but that pushes the choice of
column, the epoch-to-timestamp conversion, and the half-open-interval convention onto every client, and it does
not name the event-time intent on the wire.

## Decision

Add an optional `TimeRange { optional int64 start_ms; optional int64 end_ms; }` message (epoch milliseconds,
start inclusive, end exclusive, either bound optional) and an optional `time_range` field on
`VectorSearchRequest`, `TextSearchRequest`, and `HybridSearchRequest`. The window always applies to the
event-timestamp column. The column name is configurable through `DEFAULT_EVENT_TIMESTAMP_COLUMN`
(default `event_timestamp`) and the `SEARCH_API_EVENT_TIMESTAMP_COLUMN` env override, threaded to the backend.

The range is translated into a typed range predicate through the existing typed-filter path, never raw SQL. The
domain carries a transport- and engine-free `TimeRange`. The lance layer builds `column >= start` and/or
`column < end` as DataFusion expressions, where each epoch-millisecond bound becomes a literal of the column's
own Arrow type: a `ScalarValue` timestamp scaled to the column `TimeUnit` and carrying the column timezone, or a
plain integer literal when the column stores epoch integers. Matching the literal type to the column avoids any
cross-type coercion. The range predicate is ANDed with any caller-provided `Filter`, so the two compose. For a
hybrid request the single request-level window is applied to both the vector leg and the text leg. An absent
`time_range` leaves every search path behaving exactly as before. A window naming a column missing from the
dataset schema, or a column whose type is neither timestamp nor integer, is rejected as an invalid argument.

## Consequences

Event-time windowing is a first-class, low-friction wire field instead of a hand-built predicate, and it stays
within the no-raw-SQL guarantee: the bound is a typed DataFusion literal, the column name is the validated,
operator-configured event-timestamp column. Because the predicate is an ordinary scalar range on the
event-timestamp column, a BTREE or zone-map on that column prunes the scan, which is the efficient path called
out in ADR 0016. The change is additive and backward compatible: existing clients that never set `time_range`
are unaffected. The half-open convention (start inclusive, end exclusive) makes adjacent windows tile without
overlap or gaps. The conversion lives in one place, so a deployment whose event-timestamp column uses a
different unit or timezone is handled by reading the column type rather than by client-side guesswork.

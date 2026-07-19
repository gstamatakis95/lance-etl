# Date partitioning in the local reconciler

## Source partition contract

The registered Iceberg source uses route partition fields for tenant, namespace, and organization.
It may also carry an hour transform for physical pruning. The exact configured route-column names
come from `iceberg_sources`, not from a command-line flag.

The source adapter validates the active Iceberg partition specification before planning work. A
partition-spec change is blocked because manifest routing and exact-snapshot qualification depend on
that contract.

## Event time is not source progress

The dataset specification contains exactly one `EVENT_TIME` field. Event time drives search ranges
and optional TTL expiration. It does not define which source data has been processed.

Source progress uses:

- exact Iceberg snapshot ID
- direct parent snapshot ID
- Iceberg sequence number
- partition specification ID
- operation and commit evidence

Snapshot IDs are opaque identities. The reconciler never builds a source interval by sorting IDs or
by comparing wall-clock times.

## Planning touched datasets

For each qualified snapshot transition, the local reconciler reads manifest partition values to
discover affected `(tenant_id, namespace, org_id)` routes. It creates deterministic ingest work only
for those datasets. An hour partition may improve pruning but does not become a dataset identity or
a durable cursor.

## Reading one transition

Spark uses exact snapshot ID bounds. Timestamp options are first resolved through Iceberg metadata
when a library operation needs them. The production reconciliation path already has exact IDs from
`source_snapshots` and does not translate a scheduled time window.

## Search date ranges

The gRPC search request may carry a `TimeRange` with optional start and end milliseconds. The Rust
service converts it to a typed predicate on the specification's fixed event-time target column. The
start bound is inclusive and the end bound is exclusive.

Time filtering stays within the one resolved dataset and exact publication version. There is no
date-based dataset fan-out.

## TTL interaction

When the active spec contains a `TTL` field, maintenance computes expiry from event time plus the TTL
duration. Deletion materialization then follows the revision's `materialize_deletions` and
`materialize_deletions_threshold` settings. Source snapshot retention remains independent of TTL.

## Validation checklist

- Confirm the registered route-column mapping matches the Iceberg table.
- Confirm the active partition spec is the one stored in qualified source evidence.
- Confirm the dataset spec event-time field maps to the intended source column.
- Confirm snapshot lineage is direct and complete from the canonical baseline.
- Confirm range queries use typed bounds and the event-time scalar index.
- Never use an hourly schedule label as a replay or publication identity.
